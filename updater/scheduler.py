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

from state import StateStorage, AppIdState, InitializationSate
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
                 update_limit: timedelta, update_interval: timedelta):
        self._state_storage = state_storage
        self._app_ids = app_ids
        self._update_limit = update_limit
        self._update_interval = update_interval
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

    def _start_initialization(self, app_id_state: AppIdState,
                              date_start: date, date_until: date,
                              now: datetime = None):
        logger.debug('Initialization for {} is started'.format(
            app_id_state.app_id
        ))
        app_id_state.inited = False
        app_id_state.initialization_state = InitializationSate(
            now or datetime.now(),
            date_start,
            date_until
        )
        self._save_state()

    def _mark_date_inited(self, app_id_state: AppIdState, date: date):
        logger.debug('Updated date for {}: {}'.format(
            app_id_state.app_id, date
        ))
        i_state = app_id_state.initialization_state
        if i_state is not None and i_state.date_start <= date:
            i_state.date_start = date + timedelta(days=1)
        self._save_state()

    def _finish_initialization(self, app_id_state: AppIdState):
        logger.debug('Initialization for {} is finished'.format(
            app_id_state.app_id
        ))
        app_id_state.inited = True
        app_id_state.initialization_state = None
        self._save_state()

    def _mark_updated_until(self, app_id_state: AppIdState,
                            date_until: datetime):
        logger.debug('Data for {} is updated until: {}'.format(
            app_id_state.app_id, date_until
        ))
        app_id_state.updated_until = date_until
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

    def _initialize(self, app_id_state: AppIdState) \
            -> Generator[UpdateRequest, None, None]:
        i_state = app_id_state.initialization_state
        if i_state is None:
            date_to = datetime.now().date()
            date_from = date_to - self._update_limit
            self._start_initialization(app_id_state, date_from, date_to)
            i_state = app_id_state.initialization_state
        dates = pd.date_range(i_state.date_start,
                              i_state.date_until)
        for pd_date in dates:
            p_date = pd_date.to_pydatetime().date()  # type: date
            datetime_since = datetime.combine(p_date, time.min)
            datetime_till = datetime.combine(p_date, time.max)
            yield UpdateRequest(app_id_state.app_id,
                                datetime_since, datetime_till,
                                LogsApiClient.DATE_DIMENSION_CREATE)
            self._mark_date_inited(app_id_state, p_date)
        received_since = i_state.started_at
        received_until = datetime.now() - timedelta(hours=1)
        yield UpdateRequest(app_id_state.app_id,
                            received_since, received_until,
                            LogsApiClient.DATE_DIMENSION_RECEIVE)
        self._mark_updated_until(app_id_state, received_until)
        self._finish_initialization(app_id_state)

    def _update_step(self, app_id_state: AppIdState) \
            -> Generator[UpdateRequest, None, None]:
        received_since = app_id_state.updated_until + timedelta(seconds=1)
        received_until = datetime.now() - timedelta(hours=1)
        yield UpdateRequest(app_id_state.app_id,
                            received_since, received_until,
                            LogsApiClient.DATE_DIMENSION_RECEIVE)
        self._mark_updated_until(app_id_state, received_until)

    def _step(self, app_id_state: AppIdState):
        if app_id_state.inited:
            return self._update_step(app_id_state)
        else:
            return self._initialize(app_id_state)

    def update_requests(self) \
            -> Generator[UpdateRequest, None, None]:
        self._load_state()
        self._wait_if_needed()
        for app_id in self._app_ids:
            app_id_state = self._get_or_create_app_id_state(app_id)
            for update_request in self._step(app_id_state):
                yield update_request
        self._finish_updates()
