#!/usr/bin/env python3
"""
  db_controllers_collection.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
import logging
from typing import Dict

from db import Database
from fields import SourcesCollection
from .db_controller import DbController

logger = logging.getLogger(__name__)


class DbControllersCollection(object):
    def __init__(self, db: Database, sources_collection: SourcesCollection):
        self._db = db
        self._sources_collection = sources_collection
        self._db_controllers = dict()  # type: Dict[str, DbController]

    def db_controller(self, source: str) -> DbController:
        if source in self._db_controllers.keys():
            db_controller = self._db_controllers[source]
        else:
            db_table_definition = \
                self._sources_collection.db_table_definition(source)
            db_controller = DbController(self._db, db_table_definition)
            db_controller.prepare()
            self._db_controllers[source] = db_controller
        return db_controller
