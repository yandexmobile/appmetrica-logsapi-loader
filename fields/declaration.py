#!/usr/bin/env python
"""
  declaration.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
from typing import List

from .helpers import *
from .db_types import *
from .converters import *

_common_fields = [
    system_defined("app_id", db_uint64("AppID")),

    required("appmetrica_device_id", db_string("DeviceID")),
    required("device_id_hash", db_uint64("DeviceIDHash"), str_to_hash('appmetrica_device_id')),

    optional("ios_ifa", db_string("IFA")),
    optional("ios_ifv", db_string("IFV")),
    optional("google_aid", db_string("GoogleAID")),
    optional("windows_aid", db_string("WindowsAID")),
    optional("os_name", db_string("OSName")),
    optional("os_version", db_string("OSVersion")),
    optional("device_manufacturer", db_string("Manufacturer")),
    optional("device_model", db_string("Model")),
    optional("device_type", db_string("DeviceType")),
    optional("device_locale", db_string("Locale")),
    optional("country_iso_code", db_string("Country")),
    optional("city", db_string("City")),

    optional("app_version_name", db_string("AppVersionName")),
    optional("app_package_name", db_string("AppPackageName")),
]  # type: List[Field]

_event_fields = _common_fields + [
    required("event_timestamp", db_uint64("EventTimestamp")),

    optional("event_name", db_string("EventName")),
    optional("event_json", db_string("EventParameters")),
    optional("event_receive_timestamp", db_uint64("ReceiveTimestamp")),

    required("event_date", db_date("EventDate"), timestamp_to_date("event_timestamp")),
    optional("event_datetime", db_datetime("EventDateTime"), timestamp_to_datetime("event_timestamp")),
    optional("event_receive_date", db_date("ReceiveDate"), timestamp_to_date("event_receive_timestamp")),
    optional("event_receive_datetime", db_date("ReceiveDateTime"), timestamp_to_datetime("event_receive_timestamp")),
]  # type: List[Field]

_crash_fields = _common_fields + [
    required("crash_timestamp", db_uint64("EventTimestamp")),
    required("crash_receive_timestamp", db_uint64("ReceiveTimestamp")),

    optional("crash", db_string("Crash")),
    optional("crash_id", db_string("CrashID")),
    optional("crash_group_id", db_string("CrashGroupID")),

    required("crash_date", db_date("EventDate"), timestamp_to_date("crash_timestamp")),
    optional("crash_datetime", db_datetime("EventDateTime"), timestamp_to_datetime("crash_timestamp")),
    optional("crash_receive_date", db_date("ReceiveDate"), timestamp_to_date("crash_receive_timestamp")),
    optional("crash_receive_datetime", db_date("ReceiveDateTime"), timestamp_to_datetime("crash_receive_timestamp")),
]  # type: List[Field]

fields = {
    'events': _event_fields,
    'crashes': _crash_fields,
}
