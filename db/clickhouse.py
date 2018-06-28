#!/usr/bin/env python3
"""
  clickhouse.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
import logging
import re
from typing import Tuple, List

import requests

from .db import Database

logger = logging.getLogger(__name__)


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
        log_data = query_text
        if len(log_data) > self.QUERY_LOG_LIMIT:
            log_data = log_data[:self.QUERY_LOG_LIMIT] + '[...]'
        log_data = log_data.replace('\n', ' ')
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

    def _table_engine(self, date_field: str, sampling_field: str,
                      primary_key_fields: List[str]):
        primary_keys = [date_field] + primary_key_fields
        merge_tree_args = [date_field]
        if sampling_field:
            sampling_expression = 'cityHash64({})'.format(sampling_field)
            primary_keys.append(sampling_expression)
            merge_tree_args.append(sampling_expression)
        primary_keys_expression = '({})'.format(', '.join(primary_keys))
        merge_tree_args += [primary_keys_expression, '8192']
        engine = 'MergeTree({merge_tree_args})'.format(
            merge_tree_args=', '.join(merge_tree_args)
        )
        return engine

    def create_table(self, table_name: str, fields: List[Tuple[str, str]],
                     date_field: str, sampling_field: str,
                     primary_key_fields: List[str]):
        fields_string = ','.join(('{} {}'.format(f, f_type)
                                  for (f, f_type) in fields))
        engine = self._table_engine(date_field, sampling_field,
                                    primary_key_fields)
        q = '''
            CREATE TABLE {db}.{table} ({fields})
            ENGINE = {engine}
        '''.format(
            db=self.db_name,
            table=table_name,
            fields=fields_string,
            engine=engine
        )
        self._query_clickhouse(q)

    def rename_table(self, from_table_name: str, to_table_name: str):
        q = '''
            RENAME TABLE {db}.{from_table} TO {db}.{to_table}
        '''.format(db=self.db_name,
                   from_table=from_table_name,
                   to_table=to_table_name)
        self._query_clickhouse(q)

    def create_merge_table(self, table_name: str,
                           fields: List[Tuple[str, str]],
                           merge_re: str):
        fields_string = ','.join(('{} {}'.format(f, f_type)
                                  for (f, f_type) in fields))
        q = '''
            CREATE TABLE {db}.{table} ({fields})
            ENGINE = Merge({db}, '{merge_re}')
        '''.format(
            db=self.db_name,
            table=table_name,
            fields=fields_string,
            merge_re=merge_re
        )
        self._query_clickhouse(q)

    def is_valid_scheme(self, table_name: str, fields: List[Tuple[str, str]],
                        date_field: str, sampling_field: str,
                        primary_key_fields: List[str]) -> bool:
        scheme_query = 'SHOW CREATE TABLE {db}.{table}'.format(
            db=self.db_name,
            table=table_name
        )
        curr_scheme = self._query_clickhouse(scheme_query)  # type:str
        engine = self._table_engine(date_field, sampling_field,
                                    primary_key_fields)
        fields_re = re.compile('\s*,\s*'.join(('{}\s*{}'.format(f, f_type)
                                               for (f, f_type) in fields)))
        # TODO: check engine?
        match = fields_re.search(curr_scheme)
        return match is not None

    def query(self, query_text: str):
        self._query_clickhouse(query_text)

    def _create_table_like(self, source_table: str, new_table: str):
        query = self._query_clickhouse('SHOW CREATE TABLE {db}.{table}'.format(
            db=self.db_name,
            table=source_table
        ))  # type:str
        if query:
            new_query = query.replace(
                'CREATE TABLE {}.{}'.format(self.db_name, source_table),
                'CREATE TABLE {}.{}'.format(self.db_name, new_table)
            )
            self._query_clickhouse(new_query)

    def insert(self, table_name: str, tsv_content: str):
        query = 'INSERT INTO {db}.{table} FORMAT TabSeparatedWithNames' \
            .format(db=self.db_name, table=table_name)
        return self._query_clickhouse(tsv_content, query=query)

    def copy_data(self, source_table: str, target_table: str):
        query = '''
            INSERT INTO {db}.{to_table} 
                SELECT *
                FROM {db}.{from_table}
        '''.format(
            db=self.db_name,
            from_table=source_table,
            to_table=target_table,
        )
        self._query_clickhouse(query)

    def _copy_data_distinct(self, source_table: str, target_table: str,
                            unique_fields: List[str]):
        query = '''
            INSERT INTO {db}.{to_table} 
                SELECT *
                FROM {db}.{from_table} AS ins
                WHERE NOT (
                    ({unique_fields}) GLOBAL IN (
                        SELECT {unique_fields} 
                        FROM {db}.{to_table}
                    )
                )
        '''.format(
            db=self.db_name,
            from_table=source_table,
            to_table=target_table,
            unique_fields=', '.join(unique_fields)
        )
        self._query_clickhouse(query)

    def insert_distinct(self, table_name: str, tsv_content: str,
                        unique_fields: List[str], temp_table_name: str):
        self.drop_table(temp_table_name)
        self._create_table_like(table_name, temp_table_name)
        self.insert(temp_table_name, tsv_content)
        self._copy_data_distinct(temp_table_name, table_name, unique_fields)
        pass
