#!/usr/bin/env python
"""
  controller.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
from datetime import datetime, date, time, timedelta
from typing import Optional, List, Tuple

import pandas as pd

from .state import State
from .storage import StateStorage


class StateController(object):
    def __init__(self, state_storage: StateStorage):
        self._state_storage = state_storage
        self._state = self._state_storage.load()

    def save(self):
        self._state_storage.save(self._state)

    def is_valid_scheme(self, db_scheme: str):
        return self._state.db_scheme == db_scheme

    def update_db_scheme(self, db_scheme: str):
        if self.is_valid_scheme(db_scheme):
            return
        self._state = State()
        self._state.db_scheme = db_scheme
        self.save()

    def mark_updated(self, app_id: str, date: date,
                     now: datetime = None):
        date_str = date.strftime('%Y-%m-%d')
        ts = (now or datetime.utcnow()).timestamp()
        app_id_states = self._state.date_update_time.get(app_id, dict())
        app_id_states[date_str] = ts
        self._state.date_update_time[app_id] = app_id_states
        self.save()

    def finish_updates(self, now: datetime = None):
        ts = (now or datetime.utcnow()).timestamp()
        self._state.last_update_time = ts
        self.save()

    def wait_time(self, update_interval: timedelta,
                  now: datetime = None) \
            -> Optional[timedelta]:
        if not self._state.last_update_time:
            return None
        time = datetime.fromtimestamp(self._state.last_update_time)
        now = now or datetime.utcnow()
        delta = time + update_interval - now
        if delta.total_seconds() < 0:
            return None
        return delta

    def is_first_update(self):
        return self._state.last_update_time is None

    def dates_to_update(self, app_ids: List[str],
                        update_interval: timedelta,
                        update_limit: timedelta,
                        fresh_limit: timedelta) \
            -> List[Tuple[str, date]]:
        now = datetime.today()
        result = []
        for app_id in app_ids:
            app_id_states = self._state.date_update_time.get(app_id, dict())
            date_to = now.date()
            date_from = date_to - update_limit
            for pd_date in pd.date_range(date_from, date_to):
                date = pd_date.to_pydatetime().date()  # type: date
                date_str = date.strftime('%Y-%m-%d')
                updated_at_ts = app_id_states.get(date_str)
                if updated_at_ts:
                    updated_at = datetime.fromtimestamp(updated_at_ts)
                    last_event_date = datetime.combine(date, time.max)
                    updated = now - updated_at < update_interval
                    fresh = updated_at - last_event_date < fresh_limit
                    if updated or not fresh:
                        continue
                result.append((app_id, date))
        return result
