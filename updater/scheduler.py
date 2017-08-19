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
from typing import List, Tuple

from state import StateController
from .updater import Updater

logger = logging.getLogger(__name__)


class Scheduler(object):
    def __init__(self, state_controller: StateController, updater: Updater,
                 app_ids: List[str], source_names: List[str],
                 update_interval: datetime.timedelta,
                 update_limit: datetime.timedelta,
                 fresh_limit: datetime.timedelta):
        self._state_controller = state_controller
        self._updater = updater
        self._app_ids = app_ids
        self._source_names = source_names
        self._update_interval = update_interval
        self._update_limit = update_limit
        self._fresh_limit = fresh_limit

    def _wait_if_needed(self):
        wait_time = self._state_controller.wait_time(self._update_interval)
        if wait_time:
            logger.info('Sleep for {}'.format(wait_time))
            time.sleep(wait_time.total_seconds())

    def _update_dates(self,
                      dates_to_update: List[Tuple[str, datetime.date]]):
        for (app_id, date) in dates_to_update:
            for source in self._source_names:
                logger.info('Loading "{date}" of "{source}" for "{app_id}"'.format(
                    date=date,
                    source=source,
                    app_id=app_id
                ))
                self._updater.update(source, app_id, date)
            self._state_controller.mark_updated(app_id, date)

    def _step(self):
        self._wait_if_needed()
        dates_to_update = self._state_controller.dates_to_update(
            app_ids=self._app_ids,
            source_names=self._source_names,
            update_interval=self._update_interval,
            update_limit=self._update_limit,
            fresh_limit=self._fresh_limit
        )
        if len(dates_to_update) > 0:
            self._update_dates(dates_to_update)
        else:
            logger.info('Everything is up-to-date')
        self._state_controller.finish_updates()

    def run(self):
        logger.info("Starting updating loop")
        while True:
            try:
                self._step()
            except KeyboardInterrupt:
                logger.info('Interrupted. Saving state...')
                self._state_controller.save()
                return
            # except Exception as e:
            #     logger.warning(e)
            #     time.sleep(10)
