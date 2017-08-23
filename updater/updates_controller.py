#!/usr/bin/env python
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
from typing import List

from fields import SourcesCollection
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

    @staticmethod
    def _table_suffix(app_id: str, date: datetime.date):
        return '{}_{}'.format(app_id, date.strftime('%Y%m%d'))

    def _load_into_table(self, source: str, update_request: UpdateRequest,
                         table_suffix: str, db_controller: DbController):
        logger.info('Loading "{date}" into "{suffix}" of "{source}" '
                    'for "{app_id}"'.format(
            date=update_request.date,
            source=source,
            app_id=update_request.app_id,
            suffix=table_suffix
        ))

        self._updater.update(
            update_request.app_id,
            update_request.date,
            table_suffix,
            db_controller,
            self._sources_collection.processing_definition(source),
            self._sources_collection.loading_definition(source)
        )

    def _archive(self, source: str, update_request: UpdateRequest,
                 table_suffix: str):
        logger.info('Archiving "{date}" of "{source}" for "{app_id}"'.format(
            date=update_request.date,
            source=source,
            app_id=update_request.app_id
        ))
        db_controller = self._db_controllers_collection.db_controller(source)
        db_controller.archive_table(table_suffix)

    def _update(self, update_request: UpdateRequest):
        update_type = update_request.update_type
        table_suffix = self._table_suffix(
            update_request.app_id,
            update_request.date
        )
        for source in self._sources_collection.source_names():
            db_controller = \
                self._db_controllers_collection.db_controller(source)
            if update_type == UpdateRequest.LOAD:
                db_controller.recreate_table(table_suffix)
                self._load_into_table(source, update_request, table_suffix,
                                      db_controller)
            elif update_type == UpdateRequest.ARCHIVE:
                self._archive(source, update_request, table_suffix)
            elif update_type == UpdateRequest.LOAD_INTO_ARCHIVE:
                self._load_into_table(source, update_request,
                                      DbController.ARCHIVE_SUFFIX,
                                      db_controller)

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
                logger.warning(e)
                time.sleep(10)
