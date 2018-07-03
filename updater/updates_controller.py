#!/usr/bin/env python3
"""
  updates_controller.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
import datetime
import logging
import time
from typing import Optional

from fields import SourcesCollection, ProcessingDefinition, LoadingDefinition
from .scheduler import Scheduler, UpdateRequest
from .db_controller import DbController
from .updater import Updater
from .db_controllers_collection import DbControllersCollection

logger = logging.getLogger(__name__)


class UpdatesController(object):
    def __init__(self, scheduler: Scheduler, updater: Updater,
                 sources_collection: SourcesCollection,
                 db_controllers_collection: DbControllersCollection):
        self._scheduler = scheduler
        self._updater = updater
        self._sources_collection = sources_collection
        self._db_controllers_collection = db_controllers_collection

    def _load_into_table(self, app_id: str, date: Optional[datetime.date],
                         table_suffix: str,
                         processing_definition: ProcessingDefinition,
                         loading_definition: LoadingDefinition,
                         db_controller: DbController):
        logger.info('Loading "{date}" into "{suffix}" of "{source}" '
                    'for "{app_id}"'.format(
            date=date or 'latest',
            source=loading_definition.source_name,
            app_id=app_id,
            suffix=table_suffix
        ))
        self._updater.update(app_id, date, table_suffix, db_controller,
                             processing_definition, loading_definition)

    def _load_interval(self, app_id: str, datetime_from: datetime, datetime_to: datetime,
                       table_suffix: str,
                       processing_definition: ProcessingDefinition,
                       loading_definition: LoadingDefinition,
                       db_controller: DbController):
        logger.info('Loading interval from "{datetime_from}" to "{datetime_to}" into "{suffix}" of "{source}" '
                    'for "{app_id}"'.format(
            datetime_from=datetime_from,
            datetime_to=datetime_to,
            source=loading_definition.source_name,
            app_id=app_id,
            suffix=table_suffix
        ))
        self._updater.update_interval(app_id, db_controller,
                                      loading_definition, processing_definition, datetime_from, table_suffix,
                                      datetime_to)

    def _archive(self, source: str, app_id: str, date: datetime.date,
                 table_suffix: str, db_controller: DbController):
        logger.info('Archiving "{date}" of "{source}" for "{app_id}"'.format(
            date=date,
            source=source,
            app_id=app_id
        ))
        db_controller.archive_table(table_suffix)

    def _update(self, update_request: UpdateRequest):
        source = update_request.source
        app_id = update_request.app_id
        date = update_request.date
        update_type = update_request.update_type
        if date is not None:
            table_suffix = '{}_{}'.format(app_id, date.strftime('%Y%m%d'))
        else:
            table_suffix = '{}_{}'.format(app_id, DbController.LATEST_SUFFIX)

        loading_definition = \
            self._sources_collection.loading_definition(source)
        processing_definition = \
            self._sources_collection.processing_definition(source)
        db_controller = \
            self._db_controllers_collection.db_controller(source)

        if update_type == UpdateRequest.LOAD_ONE_DATE:
            self._load_into_table(app_id, date, table_suffix,
                                  processing_definition, loading_definition,
                                  db_controller)
        elif update_type == UpdateRequest.ARCHIVE:
            self._archive(source, app_id, date, table_suffix, db_controller)
        elif update_type == UpdateRequest.LOAD_DATE_IGNORED:
            self._load_into_table(app_id, None, table_suffix,
                                  processing_definition, loading_definition,
                                  db_controller)
        elif update_type == UpdateRequest.LOAD_INTERVAL:
            table_suffix = '{}_{}'.format(app_id, update_request.interval_from.date().strftime('%Y%m%d'))
            if update_request.interval_to.minute - update_request.interval_from.minute == 59:
                table_suffix = '{}_{}'.format(table_suffix, update_request.interval_from.hour)
            else:
                table_suffix = '{}_{}_{}_{}'.format(table_suffix, update_request.interval_from.hour,
                                                    update_request.interval_from.minute,
                                                    (update_request.interval_to + datetime.timedelta(seconds=1)).minute)
            self._load_interval(app_id, update_request.interval_from, update_request.interval_to, table_suffix,
                                processing_definition, loading_definition, db_controller)

    def _step(self):
        update_requests = self._scheduler.update_requests()
        for update_request in update_requests:
            self._update(update_request)

    def run(self):
        logger.info("Starting updating loop")
        while True:
            try:
                self._step()
            except Exception as e:
                logger.warning(e, exc_info=True)
                time.sleep(10)

