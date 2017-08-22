#!/usr/bin/env python
"""
  scheduler.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
import logging
import time
from typing import List

from .scheduler import Scheduler, UpdateRequest
from .updater import Updater

logger = logging.getLogger(__name__)


class UpdatesController(object):
    def __init__(self, scheduler: Scheduler, updater: Updater,
                 source_names: List[str]):
        self._scheduler = scheduler
        self._updater = updater
        self._source_names = source_names

    def _update(self, update_request: UpdateRequest):
        for source in self._source_names:
            logger.info('Loading "{date_since}"-"{date_until}" '
                        'of "{source}" by "{dimension}" for "{app_id}"'.format(
                date_since=update_request.date_since,
                date_until=update_request.date_until,
                dimension=update_request.date_dimension,
                source=source,
                app_id=update_request.app_id
            ))
            self._updater.update(
                source,
                update_request.app_id,
                update_request.date_since,
                update_request.date_until,
                update_request.date_dimension
            )

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
