"""
  __init__.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
from .db import Database
from .clickhouse import ClickhouseDatabase

__all__ = (
    "Database",
    "ClickhouseDatabase",
)
