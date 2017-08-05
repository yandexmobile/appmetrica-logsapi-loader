#!/usr/bin/env python
"""
  db.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
from abc import abstractmethod

import logging
from typing import Tuple, List

import requests

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


class ClickhouseDatabase(Database):
    QUERY_LOG_LIMIT = 200

    def __init__(self, url: str, login: str, password: str, db_name: str):
        super().__init__(db_name)
        self.url = url
        self.login = login
        self.password = password

    def _get_clickhouse_auth(self) -> Tuple[str, str]:
        auth = None
        if self.login:
            auth = (self.login, self.password)
        return auth

    def _query_clickhouse(self, query_text: str, **params):
        log_data = query_text.replace('\n', ' ')
        if len(log_data) > self.QUERY_LOG_LIMIT:
            log_data = log_data[:self.QUERY_LOG_LIMIT] + '[...]'
        logger.debug('Query ClickHouse: {} >>> {}'.format(params, log_data))
        auth = self._get_clickhouse_auth()
        r = requests.post(self.url, data=query_text, params=params, auth=auth)
        if r.status_code == 200:
            return r.text
        else:
            raise ValueError(r.text)

    def _upload_clickhouse_data(self, table_name: str, content: str) -> str:
        query = 'INSERT INTO {db}.{table} FORMAT TabSeparatedWithNames' \
            .format(db=self.db_name, table=table_name)
        return self._query_clickhouse(content, query=query)

    def database_exists(self):
        query = 'SHOW DATABASES'
        dbs = self._query_clickhouse(query).strip().split('\n')
        return self.db_name in dbs

    def drop_database(self):
        query = 'DROP DATABASE IF EXISTS {db}'.format(
            db=self.db_name
        )
        self._query_clickhouse(query)

    def create_database(self):
        query = 'CREATE DATABASE {db}'.format(db=self.db_name)
        self._query_clickhouse(query)

    def table_exists(self, table_name: str):
        query = 'SHOW TABLES FROM {db}'.format(db=self.db_name)
        tables = self._query_clickhouse(query).strip().split('\n')
        return table_name in tables

    def drop_table(self, table_name: str):
        query = 'DROP TABLE IF EXISTS {db}.{table}'.format(
            db=self.db_name,
            table=table_name
        )
        self._query_clickhouse(query)

    def create_table(self, table_name: str, engine: str,
                     fields: List[Tuple[str, str]]):
        fields_string = ','.join(('{} {}'.format(f, f_type)
                                  for (f, f_type) in fields))
        q = 'CREATE TABLE {db}.{table} ({fields}) ENGINE = {engine}'.format(
            db=self.db_name,
            table=table_name,
            fields=fields_string,
            engine=engine
        )
        self._query_clickhouse(q)

    def query(self, query_text: str):
        self._query_clickhouse(query_text)

    def insert(self, table_name: str, tsv_content: str):
        query = 'INSERT INTO {db}.{table} FORMAT TabSeparatedWithNames' \
            .format(db=self.db_name, table=table_name)
        return self._query_clickhouse(tsv_content, query=query)
