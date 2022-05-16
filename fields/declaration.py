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

from .helpers import *
from .db_types import *
from .converters import *
from .source import Source


_system_defined_fields = [
    system_defined("app_id", db_uint64("app_id")),
    system_defined("load_datetime", db_datetime("load_datetime")),
]  # type: List[Field]

_device_fields = [
    optional("ios_ifa", db_string("ios_ifa")),
    optional("ios_ifv", db_string("ios_ifv")),
    optional("google_aid", db_string("google_aid")),
    optional("windows_aid", db_string("windows_aid")),
    optional("os_name", db_string("os_name")),
    optional("os_version", db_string("os_version")),
    optional("device_manufacturer", db_string("device_manufacturer")),
    optional("device_model", db_string("device_model")),
    optional("device_type", db_string("device_type")),
]  # type: List[Field]

_located_device_fields = _device_fields + [
    optional("country_iso_code", db_string("country_iso_code")),
    optional("city", db_string("city")),
]  # type: List[Field]

_sdk_device_fields = _located_device_fields + [
    required("appmetrica_device_id", db_string("appmetrica_device_id")),

    optional("device_locale", db_string("device_locale")),
    optional("connection_type", db_string("connection_type")),
    optional("operator_name", db_string("operator_name")),
    optional("mcc", db_string("mcc")),
    optional("mnc", db_string("mnc")),
]  # type: List[Field]

_app_fields = [
    optional("app_version_name", db_string("app_version_name")),
    optional("app_package_name", db_string("app_package_name")),
]  # type: List[Field]


_core_click_fields = [
    optional("publisher_id", db_string("publisher_id")),
    optional("tracking_id", db_string("tracking_id")),
    optional("publisher_name", db_string("publisher_name")),
    optional("tracker_name", db_string("tracker_name")),

    required("click_timestamp", db_uint64("click_timestamp")),

    required("click_date", db_date("click_date"), timestamp_to_date("click_timestamp")),
    optional("click_datetime", db_datetime("click_datetime"), timestamp_to_datetime("click_timestamp")),
    optional("click_ipv6", db_string("click_ipv6")),
    optional("click_url_parameters", db_string("click_url_parameters")),
    optional("click_id", db_string("click_id")),
    optional("click_user_agent", db_string("click_user_agent")),
]  # type: List[Field]
_click_fields = _system_defined_fields + _device_fields + _core_click_fields
_click_keys = [
    "publisher_id",
    "tracking_id",
]
_clicks_source = Source("clicks", "clicks", "click_date", "click_ipv6",
                        _click_keys, False, _click_fields)


_core_installation_fields = [
    optional("match_type", db_string("match_type")),
    required("install_timestamp", db_uint64("install_timestamp")),
    optional("install_datetime", db_datetime("install_datetime"), timestamp_to_datetime("install_timestamp")),
    optional("install_ipv6", db_string("install_ipv6")),
]
_installation_fields = _core_click_fields + _located_device_fields + _app_fields + _core_installation_fields + [
    required("install_date", db_date("install_date"), timestamp_to_date("install_timestamp")),
    optional("install_receive_timestamp", db_uint64("install_receive_timestamp")),
    optional("is_reinstallation", db_bool("is_reinstallation"), str_to_bool('is_reinstallation'), False),
]  # type: List[Field]
_installation_keys = _click_keys + [
    "match_type",
]
_installations_source = Source("installations", "installations_all", "install_date", "install_ipv6",
                               _installation_keys, False, _installation_fields)


_postback_fields = _core_click_fields + _core_installation_fields + _device_fields + _app_fields + [
    optional("event_name", db_string("event_name")),
    optional("conversion_timestamp", db_uint64("conversion_timestamp")),
    optional("conversion_datetime", db_datetime("conversion_datetime"), timestamp_to_datetime("conversion_timestamp")),
    optional("cost_model", db_string("cost_model")),
    optional("postback_url", db_string("postback_url")),
    optional("postback_url_parameters", db_string("postback_url_parameters")),
    optional("notifying_status", db_string("notifying_status")),
    optional("response_code", db_int16("response_code")),
    optional("response_body", db_string("response_body")),

    required("attempt_timestamp", db_uint64("attempt_timestamp")),
    required("attempt_date", db_date("attempt_date"), timestamp_to_date("attempt_timestamp")),
    optional("attempt_datetime", db_datetime("attempt_datetime"), timestamp_to_datetime("attempt_timestamp")),
]  # type: List[Field]
_postback_key = [
    "postback_type",
    "response_code",
]
_postbacks_source = Source("postbacks", "postbacks", "attempt_date", None,
                           _postback_key, False, _postback_fields)


_event_fields = _system_defined_fields + _sdk_device_fields + _app_fields + [
    required("event_timestamp", db_uint64("event_timestamp")),

    optional("event_name", db_string("event_name")),
    optional("event_json", db_string("event_json")),
    optional("event_receive_timestamp", db_uint64("event_receive_timestamp")),

    required("event_date", db_date("event_date"), timestamp_to_date("event_timestamp")),
    optional("event_datetime", db_datetime("event_datetime"), timestamp_to_datetime("event_timestamp")),
    optional("event_receive_date", db_date("event_receive_date"), timestamp_to_date("event_receive_timestamp")),
    optional("event_receive_datetime", db_datetime("event_receive_datetime"), timestamp_to_datetime("event_receive_timestamp")),
]  # type: List[Field]
_event_key = [
    "event_name",
    "device_id_hash",
]
_events_source = Source("events", "events", "event_date", "appmetrica_device_id",
                        _event_key, False, _event_fields)


_push_token_fields = _sdk_device_fields + _app_fields + [
    optional("token", db_string("token")),
    required("token_timestamp", db_uint64("token_timestamp")),
    required("token_date", db_date("token_date"), timestamp_to_date("token_timestamp")),
    optional("token_datetime", db_datetime("token_datetime"), timestamp_to_datetime("token_timestamp")),
    optional("token_receive_timestamp", db_uint64("token_receive_timestamp")),
    optional("token_receive_date", db_date("token_receive_date"), timestamp_to_date("token_receive_timestamp")),
    optional("token_receive_datetime", db_datetime("token_receive_datetime"), timestamp_to_datetime("token_receive_timestamp")),
]  # type: List[Field]
_push_token_key = [
    "device_id_hash",
]
_push_tokens_source = Source("push_tokens", "push_tokens", "token_date", "appmetrica_device_id",
                             _push_token_key, True, _push_token_fields)


_crash_fields = _system_defined_fields + _sdk_device_fields + _app_fields + [
    required("crash_timestamp", db_uint64("crash_timestamp")),
    required("crash_receive_timestamp", db_uint64("crash_receive_timestamp")),

    optional("crash", db_string("crash")),
    optional("crash_id", db_string("crash_id")),
    optional("crash_group_id", db_string("crash_group_id")),

    required("crash_date", db_date("crash_date"), timestamp_to_date("crash_timestamp")),
    optional("crash_datetime", db_datetime("crash_datetime"), timestamp_to_datetime("crash_timestamp")),
    optional("crash_receive_date", db_date("crash_receive_date"), timestamp_to_date("crash_receive_timestamp")),
    optional("crash_receive_datetime", db_datetime("crash_receive_datetime"), timestamp_to_datetime("crash_receive_timestamp")),
]  # type: List[Field]
_crash_key = [
    "crash_id",
    "crash_group_id",
    "device_id_hash",
]
_crashes_source = Source("crashes", "crashes", "crash_date", "appmetrica_device_id",
                         _crash_key, False, _crash_fields)


_error_fields = _system_defined_fields + _sdk_device_fields + _app_fields + [
    required("error_timestamp", db_uint64("error_timestamp")),

    optional("error", db_string("error")),
    optional("error_id", db_string("error_id")),
    optional("error_receive_timestamp", db_uint64("error_receive_timestamp")),

    required("error_date", db_date("error_date"), timestamp_to_date("error_timestamp")),
    optional("error_datetime", db_datetime("error_datetime"), timestamp_to_datetime("error_timestamp")),
    optional("error_receive_date", db_date("error_receive_date"), timestamp_to_date("error_receive_timestamp")),
    optional("error_receive_datetime", db_datetime("error_receive_datetime"), timestamp_to_datetime("error_receive_timestamp")),
]  # type: List[Field]
_error_key = [
    "event_name",
    "device_id_hash",
]
_errors_source = Source("errors", "errors", "error_date", "appmetrica_device_id",
                        _error_key, False, _error_fields)


_sessions_start_fields = _system_defined_fields + _sdk_device_fields + _app_fields + [
    required("session_start_timestamp", db_uint64("session_start_timestamp")),
    optional("session_start_receive_timestamp", db_uint64("session_start_receive_timestamp")),

    required("session_start_date", db_date("session_start_date"), timestamp_to_date("session_start_timestamp")),
    optional("session_start_datetime", db_datetime("session_start_datetime"), timestamp_to_datetime("session_start_timestamp")),
    optional("session_start_receive_date", db_date("session_start_receive_date"), timestamp_to_date("session_start_receive_timestamp")),
    optional("session_start_receive_datetime", db_datetime("session_start_receive_datetime"), timestamp_to_datetime("session_start_receive_timestamp")),
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
