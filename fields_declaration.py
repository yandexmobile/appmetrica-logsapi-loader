#!/usr/bin/env python
"""
  fields_declaration.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
import datetime

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
_common_fileds = [
    ("appmetrica_device_id", "DeviceID", "String", None, True, False),
    ("device_id_hash", "DeviceIDHash", "UInt64", _str_to_hash('appmetrica_device_id'), True, True),
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
]
_event_fields = _common_fileds + [
    ("event_name", "EventName", "String", None, False, False),
    ("event_datetime", "EventDateTime", "DateTime", _timestamp_to_datetime("event_timestamp"), False, True),
    ("event_timestamp", "EventTimestamp", "UInt64", None, True, False),
    ("event_date", "EventDate", "Date", _timestamp_to_date("event_timestamp"), True, True),
    ("event_json", "EventParameters", "String", None, False, False),
    ("event_receive_timestamp", "ReceiveTimestamp", "UInt64", None, False, False),
    ("event_receive_date", "ReceiveDate", "Date", _timestamp_to_date("event_receive_timestamp"), False, True),
]
_crash_fields = _common_fileds + [
    ("crash", "Crash", "String", None, False, False),
    ("crash_id", "CrashID", "String", None, False, False),
    ("crash_group_id", "CrashGroupID", "String", None, False, False),
    ("crash_timestamp", "EventTimestamp", "UInt64", None, True, False),
    ("crash_date", "EventDate", "Date", _timestamp_to_date("crash_timestamp"), True, True),
    ("crash_datetime", "EventDateTime", "DateTime", _timestamp_to_datetime("crash_timestamp"), False, True),
    ("crash_receive_timestamp", "ReceiveTimestamp", "UInt64", None, False, False),
    ("crash_receive_date", "ReceiveDate", "Date", _timestamp_to_date("crash_receive_timestamp"), False, True),
    ("crash_receive_datetime", "ReceiveDateTime", "DateTime", _timestamp_to_datetime("crash_receive_date"), False, True),
]

fields = {
    'events': _event_fields,
    'crashes': _crash_fields,
}
