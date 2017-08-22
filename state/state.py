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
from typing import Optional, List


class InitializationSate(object):
    __slots__ = [
        "started_at",
        "date_start",
        "date_until",
    ]

    def __init__(self, started_at: datetime,
                 date_start: date, date_until: date):
        self.started_at = started_at
        self.date_start = date_start
        self.date_until = date_until


class AppIdState(object):
    __slots__ = [
        "app_id",
        "inited",
        "initialization_state",
        "updated_until",
    ]

    def __init__(self, app_id: str,
                 inited: bool = False,
                 initialization_state: Optional[InitializationSate] = None,
                 updated_until: Optional[datetime] = None):
        self.app_id = app_id
        self.inited = inited
        self.initialization_state = initialization_state
        self.updated_until = updated_until


class State(object):
    __slots__ = [
        "last_update_time",
        "app_id_states",
    ]

    def __init__(self, last_update_time: Optional[datetime] = None,
                 app_id_states: Optional[List[AppIdState]] = None):
        self.last_update_time = last_update_time
        self.app_id_states = app_id_states or []  # type: List[AppIdState]
