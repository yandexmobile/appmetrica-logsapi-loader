#!/usr/bin/env python
"""
  logs-api.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
import json

import logging

import io
from typing import List

import pandas as pd
import requests
import time
from pandas import DataFrame

logger = logging.getLogger(__name__)


class LogsApiClient(object):
    def __init__(self, token):
        self.token = token

    def app_creation_date(self, api_key: str) -> str:
        url = 'https://api.appmetrica.yandex.ru/management/v1/application' \
              '/{api_key}'.format(api_key=api_key)
        params = {
            'oauth_token': self.token
        }

        r = requests.get(url, params=params)
        create_date = None
        if r.status_code == 200:
            app_details = json.load(r.text)
            if ('application' in app_details) \
                    and ('create_date' in app_details['application']):
                create_date = app_details['application']['create_date']
        return create_date

    def load(self, api_key:str, table:str, fields:List[str],
             date_from:str, date_to:str) -> DataFrame:
        url = 'https://api.appmetrica.yandex.ru/logs/v1/export/{table}.csv'\
            .format(table=table)
        params = {
            'application_id': api_key,
            'date_since': '{} 00:00:00'.format(date_from),
            'date_until': '{} 23:59:59'.format(date_to),
            'date_dimension': 'default',
            'fields': ','.join(fields),
            'oauth_token': self.token
        }

        r = requests.get(url, params=params)
        while r.status_code != 200:
            logger.debug('Logs API response code: {}'.format(r.status_code))
            if r.status_code != 202:
                raise ValueError(r.text)

            time.sleep(10)
            r = requests.get(url, params=params)

        df = pd.read_csv(io.StringIO(r.text))
        return df
