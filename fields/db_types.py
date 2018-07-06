#!/usr/bin/env python3
"""
  db_types.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
from typing import Tuple


def db_string(db_name: str) -> Tuple[str, str]:
    return db_name, 'String'


def db_int16(db_name: str) -> Tuple[str, str]:
    return db_name, 'Int16'


def db_int64(db_name: str) -> Tuple[str, str]:
    return db_name, 'Int64'


def db_uint64(db_name: str) -> Tuple[str, str]:
    return db_name, 'UInt64'


def db_date(db_name: str) -> Tuple[str, str]:
    return db_name, 'Date'


def db_datetime(db_name: str) -> Tuple[str, str]:
    return db_name, 'DateTime'


def db_bool(db_name: str) -> Tuple[str, str]:
    return db_name, 'UInt8'
