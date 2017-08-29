#!/usr/bin/env python3
"""
  field.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
from typing import Optional, Callable

from pandas import DataFrame, Series

Converter = Optional[Callable[[DataFrame], Series]]


class Field(object):
    def __init__(self, load_name: str, db_name: str, db_type: str,
                 required: bool, generated: bool, converter: Converter):
        self.load_name = load_name
        self.db_name = db_name
        self.db_type = db_type
        self.required = required
        self.generated = generated
        self.converter = converter
