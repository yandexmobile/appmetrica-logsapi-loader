#!/usr/bin/env python
"""
  scheduler.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
from datetime import datetime, date, time, timedelta
import logging
from time import sleep
from typing import List, Optional, Generator

import pandas as pd

from state import StateStorage, AppIdState
from logs_api import LogsApiClient

logger = logging.getLogger(__name__)


class UpdateRequest(object):
    def __init__(self, app_id: str, date_since: datetime, date_until: datetime,
                 date_dimension: str):
        self.app_id = app_id
        self.date_since = date_since
        self.date_until = date_until
        self.date_dimension = date_dimension


class Scheduler(object):
    def __init__(self, state_storage: StateStorage, app_ids: List[str],
                 update_limit: timedelta, update_interval: timedelta,
                 fresh_limit: timedelta):
        self._state_storage = state_storage
        self._app_ids = app_ids
        self._update_limit = update_limit
        self._update_interval = update_interval
        self._fresh_limit = fresh_limit
        self._state = None

    def _load_state(self):
        self._state = self._state_storage.load()

    def _save_state(self):
        self._state_storage.save(self._state)

    def _get_or_create_app_id_state(self, app_id: str) -> AppIdState:
        app_id_states = [s for s in self._state.app_id_states
                         if s.app_id == app_id]
        if len(app_id_states) == 0:
            app_id_state = AppIdState(app_id)
            self._state.app_id_states.append(app_id_state)
        else:
            app_id_state = app_id_states[0]
        return app_id_state

    def _mark_date_updated(self, app_id_state: AppIdState, p_date: date,
                           now: Optional[datetime] = None):
        logger.debug('Data for {} of {} is updated'.format(
            p_date, app_id_state.app_id
        ))
        app_id_state.date_updates[p_date] = now or datetime.now()
        self._save_state()

    def _finish_updates(self, now: datetime = None):
        logger.debug('Updates are finished')
        self._state.last_update_time = now or datetime.now()
        self._save_state()

    def _wait_time(self, update_interval: timedelta,
                   now: datetime = None) \
            -> Optional[timedelta]:
        if not self._state.last_update_time:
            return None
        now = now or datetime.utcnow()
        delta = self._state.last_update_time - now + update_interval
        if delta.total_seconds() < 0:
            return None
        return delta

    def _wait_if_needed(self):
        wait_time = self._wait_time(self._update_interval)
        if wait_time:
            logger.info('Sleep for {}'.format(wait_time))
            sleep(wait_time.total_seconds())

    def _step(self, app_id_state: AppIdState):
        started_at = datetime.now()
        date_to = started_at.date()
        date_from = date_to - self._update_limit
        for pd_date in pd.date_range(date_from, date_to):
            p_date = pd_date.to_pydatetime().date()  # type: date
            updated_at = app_id_state.date_updates.get(p_date)
            if updated_at:
                last_event_date = datetime.combine(p_date, time.max)
                updated = started_at - updated_at < self._update_interval
                fresh = updated_at - last_event_date < self._fresh_limit
                if updated or not fresh:
                    continue
            datetime_since = datetime.combine(p_date, time.min)
            datetime_until = datetime.combine(p_date, time.max)
            yield UpdateRequest(
                app_id_state.app_id,
                datetime_since,
                datetime_until,
                LogsApiClient.DATE_DIMENSION_CREATE
            )
            self._mark_date_updated(app_id_state, p_date)

    def update_requests(self) \
            -> Generator[UpdateRequest, None, None]:
        self._load_state()
        self._wait_if_needed()
        for app_id in self._app_ids:
            app_id_state = self._get_or_create_app_id_state(app_id)
            for update_request in self._step(app_id_state):
                yield update_request
        self._finish_updates()
