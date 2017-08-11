#!/usr/bin/env python
"""
  db.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
import logging
from abc import abstractmethod
from typing import Tuple, List

logger = logging.getLogger(__name__)


class Database(object):
    def __init__(self, db_name):
        self._db_name = db_name

    @property
    def db_name(self):
        return self._db_name

    @abstractmethod
    def database_exists(self):
        pass

    @abstractmethod
    def drop_database(self):
        pass

    @abstractmethod
    def create_database(self):
        pass

    @abstractmethod
    def table_exists(self, table_name: str):
        pass

    @abstractmethod
    def drop_table(self, table_name: str):
        pass

    @abstractmethod
    def create_table(self, table_name: str, engine: str,
                     fields: List[Tuple[str, str]]):
        pass

    @abstractmethod
    def query(self, query_text: str):
        pass

    @abstractmethod
    def insert(self, table_name: str, tsv_content: str):
        pass
