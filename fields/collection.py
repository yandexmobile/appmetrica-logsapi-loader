#!/usr/bin/env python
"""
  collection.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
from typing import Callable, List, Tuple

from pandas import DataFrame, Series

from .field import Converter
from .declaration import fields


class FieldsCollection(object):
    def __init__(self, source, requested_fields, key_fields):
        self._fields = [f for f in fields[source]
                        if f.required or f.load_name in requested_fields]
        key_fields = key_fields or []
        key_fields = [f for f in self._fields if f.load_name in key_fields]
        if len(key_fields) == 0:
            key_fields = self._fields
        self._key_fields = key_fields

    def get_load_fields(self) -> List[str]:
        return [f.load_name for f in self._fields if not f.generated]

    def get_db_fields(self) -> List[Tuple[str, str]]:
        return [(f.db_name, f.db_type) for f in self._fields]

    def get_db_keys(self) -> List[str]:
        return [f.db_name for f in self._key_fields]

    def get_export_keys_list(self) -> List[str]:
        return [f.load_name for f in self._fields]

    def get_converters(self) -> List[Tuple[str, Converter]]:
        return [(f.load_name, f.converter) for f in self._fields
                if f.converter]
