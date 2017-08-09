#!/usr/bin/env python
"""
  run.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
import csv
import datetime
import logging
import time

import settings
from db import ClickhouseDatabase
from fields import FieldsCollection
from logs_api import LogsApiClient
from updater import Updater
from state_storage import FileStateStorage, StateController

logger = logging.getLogger(__name__)


def setup_logging(debug: bool = False) -> None:
    logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    level = logging.INFO
    if debug:
        level = logging.DEBUG
    logging.basicConfig(format=logging_format, level=level)


def __main():
    setup_logging(settings.DEBUG)
    logs_api_client = LogsApiClient(settings.TOKEN)
    fields = ["event_name","event_timestamp","appmetrica_device_id","os_name","country_iso_code","city","event_json","device_model"]
    gen = logs_api_client.load('185600', 'events', fields,
                              datetime.date(2017, 7, 30),
                              datetime.date(2017, 8, 7))
    reader = csv.DictReader(gen)  # type: csv.DictReader
    load_keys = reader.fieldnames
    for row in reader:
        print(row)


def main():
    setup_logging(settings.DEBUG)

    api_keys = settings.API_KEYS
    source_name = settings.SOURCE_NAME
    fields_collection = FieldsCollection(source_name,
                                         settings.FIELDS, settings.KEY_FIELDS)
    logs_api_client = LogsApiClient(settings.TOKEN,
                                    settings.REQUEST_CHUNK_ROWS)
    database = ClickhouseDatabase(settings.CH_HOST,
                                  settings.CH_USER, settings.CH_PASSWORD,
                                  settings.CH_DATABASE)
    state_storage = FileStateStorage('state.json')
    state_controller = StateController(state_storage)
    updater = Updater(logs_api_client, database, fields_collection,
                      state_controller, source_name, settings.CH_TABLE)

    logger.info("Starting updater loop")
    updater.prepare()

    while True:
        try:
            wait_time = state_controller.wait_time(settings.UPDATE_INTERVAL)
            if wait_time:
                logger.info('Sleep for {}'.format(wait_time))
                time.sleep(wait_time.total_seconds())
            dates_to_update = state_controller.dates_to_update(
                api_keys=api_keys,
                update_interval=settings.UPDATE_INTERVAL,
                update_limit=settings.UPDATE_LIMIT,
                fresh_limit=settings.FRESH_LIMIT
            )
            if len(dates_to_update) == 0:
                logger.info('Everything is up-to-date')
            else:
                for (api_key, date) in dates_to_update:
                    logger.info('Loading "{date}" for "{api_key}"'.format(
                        date=date,
                        api_key=api_key
                    ))
                    updater.update(api_key, date)
            state_controller.finish_updates()
        except KeyboardInterrupt:
            logger.info('Interrupted. Saving state...')
            state_controller.save()
            return
        except Exception as e:
            logger.warning(e)
            time.sleep(10)


if __name__ == '__main__':
    main()
