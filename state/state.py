#!/usr/bin/env python
"""
  state.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
from datetime import datetime, date
from typing import Optional, List, Dict


class AppIdState(object):
    __slots__ = [
        "app_id",
        "date_updates",
    ]

    def __init__(self, app_id: str,
                 date_updates: Optional[Dict[date, datetime]] = None):
        self.app_id = app_id
        self.date_updates = date_updates or dict()


class State(object):
    __slots__ = [
        "last_update_time",
        "app_id_states",
    ]

    def __init__(self, last_update_time: Optional[datetime] = None,
                 app_id_states: Optional[List[AppIdState]] = None):
        self.last_update_time = last_update_time
        self.app_id_states = app_id_states or []  # type: List[AppIdState]
