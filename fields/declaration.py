#!/usr/bin/env python3
"""
  declaration.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
from typing import List

from settings import EVENTS_JSON_MAPPING
from .helpers import *
from .db_types import *
from .converters import *
from .source import Source
import json

_system_defined_fields = [
    system_defined("app_id", db_uint64("AppID")),
    system_defined("load_datetime", db_datetime("LoadDateTime")),
]  # type: List[Field]

_device_fields = [
    optional("ios_ifa", db_string("IFA")),
    optional("ios_ifv", db_string("IFV")),
    optional("google_aid", db_string("GoogleAID")),
    optional("windows_aid", db_string("WindowsAID")),
    optional("os_name", db_string("OSName")),
    optional("os_version", db_string("OSVersion")),
    optional("device_manufacturer", db_string("Manufacturer")),
    optional("device_model", db_string("Model")),
    optional("device_type", db_string("DeviceType")),
]  # type: List[Field]

_located_device_fields = _device_fields + [
    optional("country_iso_code", db_string("Country")),
    optional("city", db_string("City")),
]  # type: List[Field]

_sdk_device_fields = _located_device_fields + [
    required("appmetrica_device_id", db_string("DeviceID")),

    optional("device_locale", db_string("Locale")),
    optional("connection_type", db_string("ConnectionType")),
    optional("operator_name", db_string("OperatorName")),
    optional("mcc", db_string("MCC")),
    optional("mnc", db_string("MNC")),
]  # type: List[Field]

_app_fields = [
    optional("app_version_name", db_string("AppVersionName")),
    optional("app_package_name", db_string("AppPackageName")),
]  # type: List[Field]

_core_click_fields = [
    optional("publisher_id", db_string("PublisherID")),
    optional("tracking_id", db_string("TrackingID")),
    optional("publisher_name", db_string("PublisherName")),
    optional("tracker_name", db_string("TrackerName")),

    required("click_timestamp", db_uint64("ClickTimestamp")),

    required("click_date", db_date("ClickDate"), timestamp_to_date("click_timestamp")),
    optional("click_datetime", db_datetime("ClickDateTime"), timestamp_to_datetime("click_timestamp")),
    optional("click_ipv6", db_string("ClickIPV6")),
    optional("click_url_parameters", db_string("ClickURLParameters")),
    optional("click_id", db_string("ClickID")),
    optional("click_user_agent", db_string("ClickUserAgent")),
]  # type: List[Field]
_click_fields = _system_defined_fields + _device_fields + _core_click_fields
_click_keys = [
    "publisher_id",
    "tracking_id",
]
_clicks_source = Source("clicks", "clicks", "click_date", "click_ipv6",
                        _click_keys, False, _click_fields)

_core_installation_fields = [
    optional("match_type", db_string("MatchType")),
    required("install_timestamp", db_uint64("InstallTimestamp")),
    optional("install_datetime", db_datetime("InstallDateTime"), timestamp_to_datetime("install_timestamp")),
    optional("install_ipv6", db_string("InstallIPV6")),
]
_installation_fields = _core_click_fields + _located_device_fields + _app_fields + _core_installation_fields + [
    required("appmetrica_device_id", db_string("DeviceID")),
    required("install_date", db_date("InstallDate"), timestamp_to_date("install_timestamp")),
    optional("install_receive_timestamp", db_uint64("ReceiveTimestamp")),
    optional("is_reinstallation", db_bool("IsReinstallation"), str_to_bool('is_reinstallation'), False),
]  # type: List[Field]
_installation_keys = _click_keys + [
    "match_type",
]
_installations_source = Source("installations", "installations_all", "install_date", "appmetrica_device_id",
                               _installation_keys, False, _installation_fields)

_postback_fields = _core_click_fields + _core_installation_fields + _device_fields + _app_fields + [
    optional("event_name", db_string("EventName")),
    optional("conversion_timestamp", db_uint64("ConversionTimestamp")),
    optional("conversion_datetime", db_datetime("ConversionDateTime"), timestamp_to_datetime("conversion_timestamp")),
    optional("cost_model", db_string("CostModel")),
    optional("postback_url", db_string("PostbackUrl")),
    optional("postback_url_parameters", db_string("PostbackUrlParameters")),
    optional("notifying_status", db_string("NotifyingStatus")),
    optional("response_code", db_int16("ResponseCode")),
    optional("response_body", db_string("ReponseBody")),

    required("attempt_timestamp", db_uint64("AttemptTimestamp")),
    required("attempt_date", db_date("AttemptDate"), timestamp_to_date("attempt_timestamp")),
    optional("attempt_datetime", db_datetime("AttemptDateTime"), timestamp_to_datetime("attempt_timestamp")),
]  # type: List[Field]
_postback_key = [
    "postback_type",
    "response_code",
]
_postbacks_source = Source("postbacks", "postbacks", "attempt_date", None,
                           _postback_key, False, _postback_fields)


def json_unescaper(df: DataFrame) -> Series:
    result = list()
    for f in df['event_json']:
        unquoted = f.replace('"{', '{').replace('}"', '}').replace('"', "'")
        result.append(unquoted)
    return Series(result)


_STRING = 'String'
_INT16 = 'Int16'
_UINT64 = 'UInt64'
_INT64 = 'Int64'
_DATE = 'Date'
_DATETIME = 'DateTime'
_BOOL = 'UInt8'

_event_json_mapping = EVENTS_JSON_MAPPING
# _event_json_mapping = {
#     'gadget_slot': _STRING,
#     'level': _INT16,
# }

_event_json_names = _event_json_mapping.keys()


def _db_field_name(name: str) -> str:
    return f"_{name}"


def _mapping_to_db_field(name, type) -> Field:
    db_name = _db_field_name(name)
    type_lower = type.lower()
    if type_lower == _STRING.lower():
        return optional(db_name, db_string(db_name), generated=True)
    elif type_lower == _INT16.lower():
        return optional(db_name, db_int16(db_name), generated=True)
    elif type_lower == _UINT64.lower():
        return optional(db_name, db_uint64(db_name), generated=True)
    elif type_lower == _INT64.lower():
        return optional(db_name, db_int64(db_name), generated=True)
    elif type_lower == _DATE.lower():
        return optional(db_name, db_date(db_name), generated=True)
    elif type_lower == _DATETIME.lower():
        return optional(db_name, db_datetime(db_name), generated=True)
    elif type_lower == _BOOL.lower():
        return optional(db_name, db_bool(db_name), generated=True)


def _json_extractor(df: DataFrame) -> DataFrame:
    result = dict()
    for name in _event_json_names:
        result[_db_field_name(name)] = list()
    for f in df['event_json']:
        parsed = json.loads(f)
        parsed = {k.lower(): v for k, v in parsed.items()}
        for name in _event_json_names:
            result[_db_field_name(name)].append(parsed.get(name.lower()))
    return DataFrame.from_dict(result)


_event_json_fields = [
                         optional("event_json", db_string("EventParameters"), extractor=_json_extractor,
                                  generated=False),
                         # optional("_gadget_slot", db_string("_gadget_slot"), generated=True),
                         # optional("_level", db_int16("_level"), generated=True)
                     ] + [_mapping_to_db_field(k, _event_json_mapping[k]) for k in _event_json_mapping.keys()]

_event_fields = _system_defined_fields + _sdk_device_fields + _app_fields + _event_json_fields + [
    required("event_timestamp", db_uint64("EventTimestamp")),

    optional("event_name", db_string("EventName")),
    optional("event_receive_timestamp", db_uint64("ReceiveTimestamp")),

    required("event_date", db_date("EventDate"), timestamp_to_date("event_timestamp")),
    optional("event_datetime", db_datetime("EventDateTime"), timestamp_to_datetime("event_timestamp")),
    optional("event_receive_date", db_date("ReceiveDate"), timestamp_to_date("event_receive_timestamp")),
    optional("event_receive_datetime", db_datetime("ReceiveDateTime"),
             timestamp_to_datetime("event_receive_timestamp")),
]  # type: List[Field]
_event_key = [
    "event_name",
    "device_id_hash",
]
_events_source = Source("events", "events", "event_date", "appmetrica_device_id",
                        _event_key, False, _event_fields)

_push_token_fields = _sdk_device_fields + _app_fields + [
    optional("token", db_string("Token")),
    required("token_timestamp", db_uint64("TokenTimestamp")),
    required("token_date", db_date("TokenDate"), timestamp_to_date("token_timestamp")),
    optional("token_datetime", db_datetime("TokenDateTime"), timestamp_to_datetime("token_timestamp")),
    optional("token_receive_timestamp", db_uint64("ReceiveTimestamp")),
    optional("token_receive_date", db_date("ReceiveDate"), timestamp_to_date("token_receive_timestamp")),
    optional("token_receive_datetime", db_datetime("ReceiveDateTime"),
             timestamp_to_datetime("token_receive_timestamp")),
]  # type: List[Field]
_push_token_key = [
    "device_id_hash",
]
_push_tokens_source = Source("push_tokens", "push_tokens", "token_date", "appmetrica_device_id",
                             _push_token_key, True, _push_token_fields)

_crash_fields = _system_defined_fields + _sdk_device_fields + _app_fields + [
    required("crash_timestamp", db_uint64("CrashTimestamp")),
    required("crash_receive_timestamp", db_uint64("ReceiveTimestamp")),

    optional("crash", db_string("Crash")),
    optional("crash_id", db_string("CrashID")),
    optional("crash_group_id", db_string("CrashGroupID")),

    required("crash_date", db_date("CrashDate"), timestamp_to_date("crash_timestamp")),
    optional("crash_datetime", db_datetime("CrashDateTime"), timestamp_to_datetime("crash_timestamp")),
    optional("crash_receive_date", db_date("ReceiveDate"), timestamp_to_date("crash_receive_timestamp")),
    optional("crash_receive_datetime", db_datetime("ReceiveDateTime"),
             timestamp_to_datetime("crash_receive_timestamp")),
]  # type: List[Field]
_crash_key = [
    "crash_id",
    "crash_group_id",
    "device_id_hash",
]
_crashes_source = Source("crashes", "crashes", "crash_date", "appmetrica_device_id",
                         _crash_key, False, _crash_fields)

_error_fields = _system_defined_fields + _sdk_device_fields + _app_fields + [
    required("error_timestamp", db_uint64("ErrorTimestamp")),

    optional("error", db_string("Error")),
    optional("error_id", db_string("ErrorID")),
    optional("error_receive_timestamp", db_uint64("ReceiveTimestamp")),

    required("error_date", db_date("ErrorDate"), timestamp_to_date("error_timestamp")),
    optional("error_datetime", db_datetime("ErrorDateTime"), timestamp_to_datetime("error_timestamp")),
    optional("error_receive_date", db_date("ReceiveDate"), timestamp_to_date("error_receive_timestamp")),
    optional("error_receive_datetime", db_datetime("ReceiveDateTime"),
             timestamp_to_datetime("error_receive_timestamp")),
]  # type: List[Field]
_error_key = [
    "event_name",
    "device_id_hash",
]
_errors_source = Source("errors", "errors", "error_date", "appmetrica_device_id",
                        _error_key, False, _error_fields)

_sessions_start_fields = _system_defined_fields + _sdk_device_fields + _app_fields + [
    required("session_start_timestamp", db_uint64("SessionStartTimestamp")),
    optional("session_start_receive_timestamp", db_uint64("ReceiveTimestamp")),

    required("session_start_date", db_date("SessionStartDate"), timestamp_to_date("session_start_timestamp")),
    optional("session_start_datetime", db_datetime("SessionStartDateTime"),
             timestamp_to_datetime("session_start_timestamp")),
    optional("session_start_receive_date", db_date("ReceiveDate"),
             timestamp_to_date("session_start_receive_timestamp")),
    optional("session_start_receive_datetime", db_datetime("ReceiveDateTime"),
             timestamp_to_datetime("session_start_receive_timestamp")),
]  # type: List[Field]
_sessions_start_key = [
    "device_id_hash",
]
_sessions_starts_source = Source("sessions_starts", "sessions_starts", "session_start_date", "appmetrica_device_id",
                                 _sessions_start_key, False, _sessions_start_fields)

sources = [
    _clicks_source,
    _installations_source,
    _postbacks_source,
    _events_source,
    _push_tokens_source,
    _crashes_source,
    _errors_source,
    _sessions_starts_source
]
