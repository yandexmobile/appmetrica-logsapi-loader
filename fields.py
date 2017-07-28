#!/usr/bin/env python
import datetime


def timestamp_to_date(field_name):
    def converter(df):
        def to_date(ts):
            return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d')

        return list(map(to_date, df[field_name]))

    return converter


def timestamp_to_datetime(field_name):
    def converter(df):
        return df[field_name]

    return converter


def str_to_hash(field_name):
    def converter(df):
        return list(map(hash, df[field_name]))

    return converter


generated_params = [
    "device_id_hash",
    "event_date",
    "api_key",
    "event_datetime",
    "event_receive_date",
]

required = [
    ("appmetrica_device_id", "DeviceID", "String", None),
    ("device_id_hash", "DeviceIDHash", "UInt64", str_to_hash('appmetrica_device_id')),
    ("event_timestamp", "EventTimestamp", "UInt64", None),
    ("event_date", "EventDate", "Date", timestamp_to_date("event_timestamp")),
    ("api_key", "APIKey", "UInt64", None),
]

optional = [
    ("ios_ifa", "IFA", "String", None),
    ("ios_ifv", "IFV", "String", None),
    ("google_aid", "GoogleAID", "String", None),
    ("windows_aid", "WindowsAID", "String", None),
    ("os_name", "OSName", "String", None),
    ("os_version", "OSVersion", "String", None),
    ("device_manufacturer", "Manufacturer", "String", None),
    ("device_model", "Model", "String", None),
    ("device_type", "DeviceType", "String", None),
    ("device_locale", "Locale", "String", None),
    ("country_iso_code", "Country", "String", None),
    ("city", "City", "String", None),

    ("app_version_name", "AppVersionName", "String", None),
    ("app_package_name", "AppPackageName", "String", None),

    ("event_name", "EventName", "String", None),
    ("event_datetime", "EventDateTime", "DateTime", timestamp_to_datetime("event_timestamp")),
    ("event_json", "EventParameters", "String", None),
    ("event_receive_timestamp", "ReceiveTimestamp", "UInt64", None),
    ("event_receive_date", "ReceiveDate", "Date", timestamp_to_date("event_receive_timestamp")),
]
