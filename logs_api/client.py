#!/usr/bin/env python
"""
  client.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
import datetime
import json
import logging
from typing import List, Dict, Any, Optional

import requests

import version

logger = logging.getLogger(__name__)


class LogsApiError(Exception):
    def __init__(self, status_code: int, text: str):
        self.status_code = status_code
        self.text = text


class LogsApiClient(object):
    DATE_DIMENSION_CREATE = 'default'
    DATE_DIMENSION_RECEIVE = 'receive'

    def __init__(self, token: str, host: str):
        self.token = token
        self.host = host
        self._user_agent = '{app}/{version}'.format(
            app = version.__app__,
            version = version.__version__,
        )

    def app_creation_date(self, app_id: str) -> str:
        url = '{host}/management/v1/application/{app_id}'.format(
            host=self.host,
            app_id=app_id
        )
        params = {
            'oauth_token': self.token
        }
        headers = {
            'User-Agent': self._user_agent
        }

        r = requests.get(url, params=params, headers=headers)
        create_date = None
        try:
            if r.status_code == 200:
                app_details = json.load(r.text)
                if ('application' in app_details) \
                        and ('create_date' in app_details['application']):
                    create_date = app_details['application']['create_date']
        except json.JSONDecodeError:
            pass
        return create_date

    def logs_api_export(self, app_id: str, table: str, fields: List[str],
                        date_since: Optional[datetime.datetime],
                        date_until: Optional[datetime.datetime],
                        date_dimension: Optional[str],
                        parts_count: int,
                        part_number: int,
                        force_recreate: bool):
        url = '{host}/logs/v1/export/{table}.csv'.format(
            host=self.host,
            table=table
        )

        date_format = '%Y-%m-%d %H:%M:%S'
        params = {
            'application_id': app_id,
            'fields': ','.join(fields),
            'oauth_token': self.token
        }  # type:Dict[str, Any]
        if date_since and date_until:
            date_dimension = \
                date_dimension or LogsApiClient.DATE_DIMENSION_CREATE
            params.update({
                'date_since': date_since.strftime(date_format),
                'date_until': date_until.strftime(date_format),
                'date_dimension': date_dimension,
            })
        if parts_count > 1:
            params.update({
                'parts_count': parts_count,
                'part_number': part_number,
            })

        headers = {
            'User-Agent': self._user_agent,
            'Accept-Encoding': 'gzip',
        }
        if force_recreate:
            headers['Cache-Control'] = 'no-cache'

        response = requests.get(url, params=params, headers=headers,
                                stream=True)
        if response.status_code != 200:
            raise LogsApiError(response.status_code, response.text)
        return response
