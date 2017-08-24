#!/usr/bin/env python
"""
  source.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
from typing import List, Optional

from .field import Field


class Source(object):
    def __init__(self, load_name: str, db_name: str, date_field_name:str,
                 sampling_field_name: Optional[str],
                 key_field_names: List[str],
                 date_ignored: bool,
                 fields: List[Field]):
        self.load_name = load_name
        self.db_name = db_name
        self.date_field_name = date_field_name
        self.sampling_field_name = sampling_field_name
        self.key_field_names = key_field_names
        self.date_ignored = date_ignored
        self.fields = sorted(fields, key=lambda f: f.load_name)
