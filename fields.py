#!/usr/bin/env python
"""
  fields.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
from typing import Callable, List, Tuple

from pandas import DataFrame, Series

from fields_declaration import fields


class Field(object):
    def __init__(self, load_name: str, db_name: str, db_type: str,
                 required: bool, generated: bool,
                 converter: Callable[[DataFrame], Series]):
        self.load_name = load_name
        self.db_name = db_name
        self.db_type = db_type
        self.required = required
        self.generated = generated
        self.converter = converter


class FieldsCollection(object):
    def __init__(self, source, requested_fields, key_fields):
        self._fields = [f for f in self._to_fields(fields[source])
                        if f.required or f.load_name in requested_fields]
        key_fields = key_fields or []
        key_fields = [f for f in self._fields if f.load_name in key_fields]
        if len(key_fields) == 0:
            key_fields = self._fields
        self._key_fields = key_fields

    @staticmethod
    def _to_fields(field_args):
        args = sorted(field_args, key=lambda t: t[0])
        for (load_name, db_name, db_type, converter, req, gen) in args:
            yield Field(
                load_name=load_name,
                db_name=db_name,
                db_type=db_type,
                required=req,
                generated=gen,
                converter=converter
            )

    def get_load_fields(self) -> List[str]:
        return [f.load_name for f in self._fields if not f.generated]

    def get_db_fields(self) -> List[Tuple[str, str]]:
        return [(f.db_name, f.db_type) for f in self._fields]

    def get_db_keys(self) -> List[str]:
        return [f.db_name for f in self._key_fields]

    def get_export_keys_list(self) -> List[str]:
        return [f.load_name for f in self._fields]

    def get_converters(self) -> List[Tuple[str, Callable[[DataFrame], Series]]]:
        return [(f.load_name, f.converter) for f in self._fields
                if f.converter]
