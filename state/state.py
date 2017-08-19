#!/usr/bin/env python
"""
  state.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
from typing import Optional, Dict


class State(object):
    def __init__(self):
        self.last_update_time = None  # type: Optional[float]
        self.date_update_time = dict()  # type: Dict[str, Dict[str, float]]
