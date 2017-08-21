#!/usr/bin/env python
"""
  db_controller.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
import datetime
import logging

from pandas import DataFrame

from db import Database
from fields import DbTableDefinition
from state import StateController

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
    def __init__(self, db: Database, definition: DbTableDefinition):
        self._db = db
        self._definition = definition

    @property
    def table_name(self):
        return self._definition.table_name

    @property
    def date_field(self):
        return self._definition.date_field

    @property
    def sampling_field(self):
        return self._definition.sampling_field

    @property
    def primary_keys(self):
        return self._definition.primary_keys

    @property
    def temp_table_name(self):
        return '{}_tmp'.format(self.table_name)

    @property
    def scheme(self):
        return str((
            self.date_field,
            self.sampling_field,
            tuple(self._definition.field_types.items())
        ))

    def _prepare_db(self):
        if not self._db.database_exists():
            self._db.create_database()
            logger.info('Database "{}" created'.format(self._db.db_name))

    def _prepare_table(self):
        table_exists = self._db.table_exists(self.table_name)
        field_types = self._definition.field_types.items()
        if not table_exists:
            should_create = True
        else:
            scheme_valid = self._db.is_valid_scheme(self.table_name,
                                                    field_types,
                                                    self.date_field,
                                                    self.sampling_field,
                                                    self.primary_keys)
            should_create = not scheme_valid
        if should_create:
            self._db.drop_table(self.table_name)
            self._db.create_table(self.table_name, field_types,
                                  self.date_field, self.sampling_field,
                                  self.primary_keys)
            logger.info('Table "{}" created'.format(self.table_name))

    def prepare(self):
        self._prepare_db()
        self._prepare_table()

    def _fetch_export_fields(self, df: DataFrame) -> DataFrame:
        logger.debug("Fetching exporting fields")
        return df[self._definition.export_fields]

    def _escape_data(self, df: DataFrame) -> DataFrame:
        logger.debug("Escaping bad symbols")
        escape_chars = dict()
        string_cols = list(df.select_dtypes(include=['object']).columns)
        for col, type in self._definition.column_types.items():
            if type == 'String' and col in string_cols:
                escape_chars[col] = _escape_characters
        df.replace(escape_chars, regex=True, inplace=True)
        return df

    def _export_data_to_tsv(self, df: DataFrame) -> str:
        logger.debug("Exporting data to csv")
        return df.to_csv(index=False, sep='\t')

    def insert_data(self, df: DataFrame):
        df = self._fetch_export_fields(df)
        df = self._escape_data(df)  # TODO: Works too slow
        tsv = self._export_data_to_tsv(df)
        self._db.insert_distinct(
            table_name=self.table_name,
            tsv_content=tsv,
            unique_fields=self._definition.unique_keys,
            temp_table_name=self.temp_table_name
        )

    def cleanup(self):
        self._db.drop_table(self.temp_table_name)
