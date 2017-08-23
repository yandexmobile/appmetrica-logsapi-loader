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

logger = logging.getLogger(__name__)


class UpdateRequest(object):
    ARCHIVE = 'archive'
    LOAD = 'load'
    LOAD_INTO_ARCHIVE = 'load_into_archive'

    def __init__(self, app_id: str, p_date: date, update_type: str):
        self.app_id = app_id
        self.date = p_date
        self.update_type = update_type


class Scheduler(object):
    ARCHIVED_DATE = datetime(3000, 1, 1, tzinfo=datetime.now().tzinfo)

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

    def _mark_date_archived(self, app_id_state: AppIdState, p_date: date):
        logger.debug('Data for {} of {} is archived'.format(
            p_date, app_id_state.app_id
        ))
        app_id_state.date_updates[p_date] = self.ARCHIVED_DATE
        self._save_state()

    def _is_date_archived(self, app_id_state: AppIdState, p_date: date):
        updated_at = app_id_state.date_updates.get(p_date)
        return updated_at is not None and updated_at == self.ARCHIVED_DATE

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

    def _archive_old_dates(self, app_id_state: AppIdState):
        for p_date, updated_at in app_id_state.date_updates.items():
            if self._is_date_archived(app_id_state, p_date):
                continue
            last_event_date = datetime.combine(p_date, time.max)
            fresh = updated_at - last_event_date < self._fresh_limit
            if not fresh:
                yield UpdateRequest(app_id_state.app_id, p_date,
                                    UpdateRequest.ARCHIVE)
                self._mark_date_archived(app_id_state, p_date)

    def _update_date(self, app_id_state: AppIdState, p_date: date,
                     started_at: datetime) \
            -> Generator[UpdateRequest, None, None]:
        updated_at = app_id_state.date_updates.get(p_date)
        last_event_date = datetime.combine(p_date, time.max)
        if updated_at:
            updated = started_at - updated_at < self._update_interval
            if updated:
                return
        last_event_delta = (updated_at or started_at) - last_event_date
        fresh = last_event_delta < self._fresh_limit
        if fresh:
            yield UpdateRequest(app_id_state.app_id, p_date,
                                UpdateRequest.LOAD)
            self._mark_date_updated(app_id_state, p_date)
        else:
            yield UpdateRequest(app_id_state.app_id, p_date,
                                UpdateRequest.LOAD_INTO_ARCHIVE)
            self._mark_date_archived(app_id_state, p_date)

    def update_requests(self) \
            -> Generator[UpdateRequest, None, None]:
        self._load_state()
        self._wait_if_needed()
        started_at = datetime.now()
        for app_id in self._app_ids:
            app_id_state = self._get_or_create_app_id_state(app_id)
            date_to = started_at.date()
            date_from = date_to - self._update_limit

            updates = self._archive_old_dates(app_id_state)
            for update_request in updates:
                yield update_request

            for pd_date in pd.date_range(date_from, date_to):
                p_date = pd_date.to_pydatetime().date()  # type: date
                updates = self._update_date(app_id_state, p_date, started_at)
                for update_request in updates:
                    yield update_request
        self._finish_updates()
