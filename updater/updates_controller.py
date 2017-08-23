#!/usr/bin/env python
"""
  scheduler.py

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

from .scheduler import Scheduler, UpdateRequest
from .db_controller import DbController
from .updater import Updater

logger = logging.getLogger(__name__)


class UpdatesController(object):
    def __init__(self, scheduler: Scheduler, updater: Updater,
                 source_names: List[str]):
        self._scheduler = scheduler
        self._updater = updater
        self._source_names = source_names

    @staticmethod
    def _table_suffix(app_id: str, date: datetime.date):
        return '{}_{}'.format(app_id, date.strftime('%Y%m%d'))

    def _load_into_table(self, source: str, update_request: UpdateRequest,
                         table_suffix: str):
        logger.info('Loading "{date}" into "{suffix}" of "{source}" '
                    'for "{app_id}"'.format(
            date=update_request.date,
            source=source,
            app_id=update_request.app_id,
            suffix=table_suffix
        ))
        self._updater.update(
            source,
            update_request.app_id,
            update_request.date,
            table_suffix
        )

    def _archive(self, source: str, update_request: UpdateRequest,
                 table_suffix: str):
        logger.info('Archiving "{date}" of "{source}" for "{app_id}"'.format(
            date=update_request.date,
            source=source,
            app_id=update_request.app_id
        ))
        self._updater.archive(
            source,
            table_suffix
        )

    def _update(self, update_request: UpdateRequest):
        update_type = update_request.update_type
        table_suffix = self._table_suffix(
            update_request.app_id,
            update_request.date
        )
        for source in self._source_names:
            if update_type == UpdateRequest.LOAD:
                self._load_into_table(source, update_request, table_suffix)
            elif update_type == UpdateRequest.ARCHIVE:
                self._archive(source, update_request, table_suffix)
            elif update_type == UpdateRequest.LOAD_INTO_ARCHIVE:
                self._load_into_table(source, update_request,
                                      DbController.ARCHIVE_SUFFIX)

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
