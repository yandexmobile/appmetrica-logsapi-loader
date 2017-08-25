#!/usr/bin/env python
"""
  db_controller.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
import logging

from pandas import DataFrame

from db import Database
from fields import DbTableDefinition

logger = logging.getLogger(__name__)

# TODO: Allow customizing
_escape_characters = {
    '\b': '\\\\b',
    '\r': '\\\\r',
    '\f': '\\\\f',
    '\n': '\\\\n',
    '\t': '\\\\t',
    '\0': '\\\\0',
    '\'': '\\\\\'',
    '\\\\': '\\\\\\\\',
}


class DbController(object):
    ARCHIVE_SUFFIX = 'old'
    ALL_SUFFIX = 'all'
    LATEST_SUFFIX = 'all'

    def __init__(self, db: Database, definition: DbTableDefinition):
        self._db = db
        self._definition = definition

    def table_name(self, suffix: str):
        return '{}_{}'.format(self._definition.table_name, suffix)

    @property
    def merge_re(self):
        return '^{}.*'.format(self._definition.table_name)

    @property
    def date_field(self):
        return self._definition.date_field

    @property
    def sampling_field(self):
        return self._definition.sampling_field

    @property
    def primary_keys(self):
        return self._definition.primary_keys

    def _prepare_db(self):
        if not self._db.database_exists():
            self._db.create_database()
            logger.info('Database "{}" created'.format(self._db.db_name))

    def _prepare_table(self):
        table_name = self.table_name(self.ALL_SUFFIX)
        table_exists = self._db.table_exists(table_name)
        if not table_exists:
            self._db.create_merge_table(table_name,
                                        self._definition.field_types.items(),
                                        self.merge_re)

    def prepare(self):
        self._prepare_db()
        self._prepare_table()

    def _fetch_export_fields(self, df: DataFrame) -> DataFrame:
        logger.debug("Fetching exporting fields")
        return df[self._definition.export_fields]

    def _escape_data(self, df: DataFrame) -> DataFrame:
        logger.debug("Escaping symbols")
        escape_chars = dict()
        string_cols = list(df.select_dtypes(include=['object']).columns)
        for col, type in self._definition.column_types.items():
            if type == 'String' and col in string_cols:
                escape_chars[col] = _escape_characters
        df.replace(escape_chars, regex=True, inplace=True)
        return df

    @staticmethod
    def _export_data_to_tsv(df: DataFrame) -> str:
        logger.debug("Exporting data to csv")
        return df.to_csv(index=False, sep='\t')

    def _create_table(self, table_name):
        self._db.create_table(
            table_name,
            self._definition.field_types.items(),
            self.date_field,
            self.sampling_field,
            self.primary_keys
        )

    def _ensure_table_created(self, table_name):
        if not self._db.table_exists(table_name):
            self._create_table(table_name)

    def archive_table(self, table_suffix: str):
        source_table_name = self.table_name(table_suffix)
        if not self._db.table_exists(source_table_name):
            logger.warning('Table to archive is not exist: {}'.format(
                source_table_name
            ))
            return
        archive_table_name = self.table_name(self.ARCHIVE_SUFFIX)
        self._ensure_table_created(archive_table_name)
        self._db.copy_data(source_table_name, archive_table_name)
        self._db.drop_table(source_table_name)

    def recreate_table(self, table_suffix: str):
        table_name = self.table_name(table_suffix)
        self._db.drop_table(table_name)
        self._create_table(table_name)

    def ensure_table_created(self, table_suffix: str):
        table_name = self.table_name(table_suffix)
        self._ensure_table_created(table_name)

    def insert_data(self, df: DataFrame, table_suffix: str):
        df = self._fetch_export_fields(df)
        df = self._escape_data(df)  # TODO: Works too slow
        logger.debug("Inserting {} rows".format(len(df)))
        tsv = self._export_data_to_tsv(df)
        table_name = self.table_name(table_suffix)
        self._db.insert(table_name, tsv)
