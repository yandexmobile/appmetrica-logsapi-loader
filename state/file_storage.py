#!/usr/bin/env python3
"""
  file_storage.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
import json

from .json_serialization import StateJSONEncoder, StateJSONDecoder
from .state import State
from .storage import StateStorage


class FileStateStorage(StateStorage):
    def __init__(self, file_name):
        self.file_name = file_name

    def load(self) -> State:
        try:
            state = json.load(open(self.file_name, 'r'), cls=StateJSONDecoder)
            return state
        except FileNotFoundError:
            return State()
        except json.JSONDecodeError:
            return State()

    def save(self, state: State):
        json.dump(state, open(self.file_name, 'w'),
                  indent=4, sort_keys=True, cls=StateJSONEncoder)
