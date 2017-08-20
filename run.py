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
from fields import SourcesCollection
from logs_api import LogsApiClient, Loader
from state import FileStateStorage, StateController
from updater import Updater, Scheduler

logger = logging.getLogger(__name__)


def setup_logging(debug: bool = False) -> None:
    logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    level = logging.INFO
    if debug:
        level = logging.DEBUG
    logging.basicConfig(format=logging_format, level=level)


def main():
    setup_logging(debug=settings.DEBUG)

    sources_collection = SourcesCollection(
        requested_sources=settings.SOURCES
    )
    logs_api_client = LogsApiClient(
        token=settings.TOKEN,
        host=settings.LOGS_API_HOST
    )
    logs_api_loader = Loader(
        client=logs_api_client,
        chunk_size=settings.REQUEST_CHUNK_ROWS
    )
    database = ClickhouseDatabase(
        url=settings.CH_HOST,
        login=settings.CH_USER,
        password=settings.CH_PASSWORD,
        db_name=settings.CH_DATABASE
    )
    state_storage = FileStateStorage(
        file_name=settings.STATE_FILE_PATH
    )
    state_controller = StateController(
        state_storage=state_storage
    )
    updater = Updater(
        loader=logs_api_loader,
        sources_collection=sources_collection,
        db=database
    )
    scheduler = Scheduler(
        state_controller=state_controller,
        updater=updater,
        app_ids=settings.APP_IDS,
        source_names=sources_collection.source_names(),
        update_interval=settings.UPDATE_INTERVAL,
        update_limit=settings.UPDATE_LIMIT,
        fresh_limit=settings.FRESH_LIMIT
    )
    scheduler.run()


if __name__ == '__main__':
    main()
