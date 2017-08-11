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
                 api_keys: List[str],
                 update_interval: datetime.timedelta,
                 update_limit: datetime.timedelta,
                 fresh_limit: datetime.timedelta):
        self._state_controller = state_controller
        self._updater = updater
        self._api_keys = api_keys
        self._update_interval = update_interval
        self._update_limit = update_limit
        self._fresh_limit = fresh_limit

    def _wait_if_needed(self):
        wait_time = self._state_controller.wait_time(self._update_interval)
        if wait_time:
            logger.info('Sleep for {}'.format(wait_time))
            time.sleep(wait_time.total_seconds())

    def _update_dates(self, dates_to_update: List[Tuple[str, datetime.date]]):
        for (api_key, date) in dates_to_update:
            logger.info('Loading "{date}" for "{api_key}"'.format(
                date=date,
                api_key=api_key
            ))
            self._updater.update(api_key, date)
            self._state_controller.mark_updated(api_key, date)

    def _step(self):
        self._wait_if_needed()
        dates_to_update = self._state_controller.dates_to_update(
            api_keys=self._api_keys,
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
            except Exception as e:
                logger.warning(e)
                time.sleep(10)
