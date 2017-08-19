#!/usr/bin/env python
"""
  converters.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
import datetime

import math
from pandas import DataFrame, Series

from .field import Converter


def timestamp_to_date(field_name: str):
    def converter(df: DataFrame) -> Series:
        def to_date(ts):
            if math.isnan(ts):
                return '1970-01-01'
            return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d')

        col = df[field_name]  # type: Series
        return col.apply(to_date)

    return converter  # type: Converter


def timestamp_to_datetime(field_name: str):
    def converter(df: DataFrame) -> Series:
        def to_datetime(ts):
            if math.isnan(ts):
                return '1970-01-01 00:00:00'
            return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        col = df[field_name]  # type: Series
        return col.apply(to_datetime)

    return converter  # type: Converter


def str_to_hash(field_name: str):
    def converter(df: DataFrame) -> Series:
        def to_hash(s):
            return abs(hash(s))

        col = df[field_name]  # type: Series
        return col.apply(to_hash)

    return converter  # type: Converter


def str_to_bool(field_name: str):
    def converter(df: DataFrame) -> Series:
        col = df[field_name]  # type: Series
        return col.apply(lambda x: int(x))

    return converter  # type: Converter
