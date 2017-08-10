#!/usr/bin/env python
"""
  storage.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
from abc import abstractmethod

from .state import State


class StateStorage(object):
    @abstractmethod
    def load(self) -> State:
        pass

    @abstractmethod
    def save(self, state: State):
        pass
