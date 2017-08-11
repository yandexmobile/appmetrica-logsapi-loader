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

import settings
from db import ClickhouseDatabase
from fields import FieldsCollection
from logs_api import LogsApiClient, Loader
from state import FileStateStorage, StateController
from updater import Updater, DbController, Scheduler

logger = logging.getLogger(__name__)


def setup_logging(debug: bool = False) -> None:
    logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    level = logging.INFO
    if debug:
        level = logging.DEBUG
    logging.basicConfig(format=logging_format, level=level)


def main():
    setup_logging(settings.DEBUG)

    fields_collection = FieldsCollection(settings.SOURCE_NAME,
                                         settings.FIELDS, settings.KEY_FIELDS)

    logs_api_client = LogsApiClient(settings.TOKEN, settings.LOGS_API_HOST)
    logs_api_loader = Loader(logs_api_client, settings.REQUEST_CHUNK_ROWS)

    database = ClickhouseDatabase(settings.CH_HOST,
                                  settings.CH_USER, settings.CH_PASSWORD,
                                  settings.CH_DATABASE)

    state_storage = FileStateStorage(settings.STATE_FILE_PATH)
    state_controller = StateController(state_storage)

    db_controller = DbController(database, settings.CH_TABLE,
                                 fields_collection)
    updater = Updater(logs_api_loader, db_controller, fields_collection)
    scheduler = Scheduler(state_controller, updater,
                          settings.APP_IDS,
                          settings.UPDATE_INTERVAL,
                          settings.UPDATE_LIMIT,
                          settings.FRESH_LIMIT)

    db_controller.prepare(state_controller)
    scheduler.run()


if __name__ == '__main__':
    main()
