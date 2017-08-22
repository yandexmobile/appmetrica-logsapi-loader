#!/usr/bin/env python
"""
  file_storage.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
from datetime import datetime, date
import json

from .state import State, AppIdState, InitializationSate
from .storage import StateStorage


class FileStateStorage(StateStorage):
    def __init__(self, file_name):
        self.file_name = file_name

    def load(self) -> State:
        try:
            state_dict = json.load(open(self.file_name, 'r'))
            app_id_states = []
            for app_id_dict in state_dict['app_id_states']:
                i_state_dict = app_id_dict.get('initialization_state')
                if i_state_dict is not None:
                    i_state = InitializationSate(
                        datetime.utcfromtimestamp(i_state_dict['started_at']),
                        date.fromordinal(i_state_dict['date_start']),
                        date.fromordinal(i_state_dict['date_until'])
                    )
                else:
                    i_state = None
                updated_until = None
                if app_id_dict['updated_until'] is not None:
                    updated_until = \
                        datetime.utcfromtimestamp(app_id_dict['updated_until'])
                app_id_state = AppIdState(
                    app_id_dict['app_id'],
                    app_id_dict['inited'],
                    i_state,
                    updated_until
                )
                app_id_states.append(app_id_state)
            last_update_time = None
            if state_dict['last_update_time'] is not None:
                last_update_time = \
                    datetime.utcfromtimestamp(state_dict['last_update_time'])
            state = State(last_update_time, app_id_states)
            return state
        except FileNotFoundError:
            return State()
        except json.JSONDecodeError:
            return State()

    def save(self, state: State):
        app_id_dicts = []
        for app_state in state.app_id_states:
            app_id_dict = {
                'app_id': app_state.app_id,
                'inited': app_state.inited,
            }
            if app_state.updated_until:
                app_id_dict['updated_until'] = \
                    app_state.updated_until.timestamp()
            else:
                app_id_dict['updated_until'] = None
            i_state = app_state.initialization_state
            if i_state is not None:
                i_state_dict = {
                    'started_at': i_state.started_at.timestamp(),
                    'date_start': i_state.date_start.toordinal(),
                    'date_until': i_state.date_until.toordinal(),
                }
                app_id_dict['initialization_state'] = i_state_dict
            app_id_dicts.append(app_id_dict)
        state_dict = {
            'app_id_states': app_id_dicts,
        }
        if state.last_update_time:
            state_dict['last_update_time'] = state.last_update_time.timestamp()
        else:
            state_dict['last_update_time'] = None
        json.dump(state_dict, open(self.file_name, 'w'),
                  indent=4, sort_keys=True)