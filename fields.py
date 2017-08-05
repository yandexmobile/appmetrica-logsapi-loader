#!/usr/bin/env python
"""
  fields.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
import datetime
from typing import Callable, List, Tuple

from pandas import DataFrame, Series


def _timestamp_to_date(field_name: str):
    def converter(df: DataFrame) -> Series:
        def to_date(ts):
            return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d')

        col = df[field_name]  # type: Series
        return col.apply(to_date)

    return converter


def _timestamp_to_datetime(field_name: str):
    def converter(df: DataFrame) -> Series:
        return df[field_name]

    return converter


def _str_to_hash(field_name: str):
    def converter(df: DataFrame) -> Series:
        col = df[field_name]  # type: Series
        return col.apply(hash)

    return converter


# Load_Name DB_Name, DB_Type Converter Required Generated
event_fields = [
    ("appmetrica_device_id", "DeviceID", "String", None, True, False),
    ("device_id_hash", "DeviceIDHash", "UInt64", _str_to_hash('appmetrica_device_id'), True, True),
    ("event_timestamp", "EventTimestamp", "UInt64", None, True, False),
    ("event_date", "EventDate", "Date", _timestamp_to_date("event_timestamp"), True, True),
    ("api_key", "APIKey", "UInt64", None, True, True),
    ("ios_ifa", "IFA", "String", None, False, False),
    ("ios_ifv", "IFV", "String", None, False, False),
    ("google_aid", "GoogleAID", "String", None, False, False),
    ("windows_aid", "WindowsAID", "String", None, False, False),
    ("os_name", "OSName", "String", None, False, False),
    ("os_version", "OSVersion", "String", None, False, False),
    ("device_manufacturer", "Manufacturer", "String", None, False, False),
    ("device_model", "Model", "String", None, False, False),
    ("device_type", "DeviceType", "String", None, False, False),
    ("device_locale", "Locale", "String", None, False, False),
    ("country_iso_code", "Country", "String", None, False, False),
    ("city", "City", "String", None, False, False),

    ("app_version_name", "AppVersionName", "String", None, False, False),
    ("app_package_name", "AppPackageName", "String", None, False, False),

    ("event_name", "EventName", "String", None, False, False),
    ("event_datetime", "EventDateTime", "DateTime", _timestamp_to_datetime("event_timestamp"), False, True),
    ("event_json", "EventParameters", "String", None, False, False),
    ("event_receive_timestamp", "ReceiveTimestamp", "UInt64", None, False, False),
    ("event_receive_date", "ReceiveDate", "Date", _timestamp_to_date("event_receive_timestamp"), False, True),
]


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
    def __init__(self, field_args, requested_fields, key_fields):
        self._fields = [f for f in self._to_fields(field_args)
                        if f.required or f.load_name in requested_fields]
        if not key_fields or len(key_fields) == 0:
            self._key_fields = [f.db_name for f in self._fields]
        else:
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
        return [f.db_name for f in self._fields
                if f.db_name in self._key_fields]

    def get_export_keys_list(self) -> List[str]:
        return [f.load_name for f in self._fields]

    def get_converters(self) -> List[Tuple[str, Callable[[DataFrame], Series]]]:
        return [(f.load_name, f.converter) for f in self._fields
                if f.converter]
