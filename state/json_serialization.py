#!/usr/bin/env python
"""
  state.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
from calendar import timegm
from datetime import datetime, date
from json import JSONEncoder, JSONDecoder
from typing import Dict, Any

from .state import State, AppIdState


DATE_FORMAT = '%Y-%m-%d'


def _from_unix_time(u: int) -> datetime:
    return datetime.utcfromtimestamp(u)


def _to_unix_time(d: datetime) -> int:
    return timegm(d.timetuple())


class StateJSONEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return _to_unix_time(o)
        elif isinstance(o, date):
            return o.strftime(DATE_FORMAT)
        elif isinstance(o, AppIdState):
            date_updates = dict()
            for d, dt in o.date_updates.items():
                date_updates[d.strftime(DATE_FORMAT)] = dt
            return {
                "app_id": o.app_id,
                "date_updates": date_updates,
            }
        elif isinstance(o, State):
            return {
                "last_update_time": o.last_update_time,
                "app_id_states": o.app_id_states
            }


def _parse_date_updates(json_object: Dict[str, Any]):
    date_updates = dict()
    for target_date_str, update_ts in json_object.items():
        target_dt = datetime.strptime(target_date_str, DATE_FORMAT)
        target_date = target_dt.date()
        update_dt = _from_unix_time(update_ts)
        date_updates[target_date] = update_dt
    return date_updates


def _parse_app_id_state(json_object: Dict[str, Any]):
    date_updates = _parse_date_updates(json_object["date_updates"])
    return AppIdState(json_object["app_id"], date_updates)


def _parse_state(json_object: Dict[str, Any]):
    last_update_time = None
    if json_object["last_update_time"] is not None:
        last_update_time = _from_unix_time(json_object["last_update_time"])
    app_id_states = []
    if "app_id_states" in json_object.keys():
        app_id_states = map(_parse_app_id_state, json_object["app_id_states"])
    return State(last_update_time, list(app_id_states))


def _hook(json_object):
    if "app_id_states" in json_object:
        return _parse_state(json_object)
    else:
        return json_object


class StateJSONDecoder(JSONDecoder):
    def __init__(self):
        super().__init__(object_hook=_hook)
