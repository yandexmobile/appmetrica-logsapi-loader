#!/usr/bin/env python
"""
  run.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""

import logging
import time

import settings
from db import ClickhouseDatabase
from fields import FieldsCollection, event_fields
from logs_api import LogsApiClient
from updater import Updater

logger = logging.getLogger(__name__)


def setup_logging(debug: bool = False) -> None:
    logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    level = logging.INFO
    if debug:
        level = logging.DEBUG
    logging.basicConfig(format=logging_format, level=level)


def main():
    setup_logging(settings.DEBUG)

    api_keys = settings.API_KEYS
    fields_collection = FieldsCollection(event_fields,
                                         settings.FIELDS, settings.KEY_FIELDS)
    logs_api_client = LogsApiClient(settings.TOKEN)
    database = ClickhouseDatabase(settings.CH_HOST,
                                  settings.CH_USER, settings.CH_PASSWORD,
                                  settings.CH_DATABASE)
    updater = Updater(logs_api_client, database, fields_collection,
                      'events', settings.CH_TABLE)

    logger.info("Starting updater loop "
                "(interval = {} seconds)".format(settings.FETCH_INTERVAL))
    updater.prepare()

    is_first = True
    while True:
        try:
            if is_first:
                logger.info('Loading historical data')
                is_first = False
                days_delta = settings.INITIAL_PERIOD
            else:
                logger.info("Run Logs API fetch")
                days_delta = settings.CHECK_PERIOD
            updater.update(api_keys, days_delta)
            time.sleep(settings.FETCH_INTERVAL)
        except KeyboardInterrupt:
            return


if __name__ == '__main__':
    main()
