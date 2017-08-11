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
from fields import FieldsCollection
from state import StateController

logger = logging.getLogger(__name__)

# TODO: Allow customizing
_escape_characters = {
    # '\b': '\\\\b',
    # '\r': '\\\\r',
    # '\f': '\\\\f',
    '\n': '\\\\n',
    '\t': '\\\\t',
    # '\0': '\\\\0',
    # '\'': '\\\\\'',
    # '\\\\': '\\\\\\\\',
}


class DbController(object):
    def __init__(self, db: Database, table_name: str,
                 fields: FieldsCollection):
        self._db = db
        self._table_name = table_name
        self._fields = fields
        if (self.date_field, 'Date') not in self._fields.get_db_fields():
            raise ValueError('EventDate with type Date is required')
        if (self.sampling_field, 'String') not in self._fields.get_db_fields():
            raise ValueError('DeviceID with type String is required')

    @property
    def date_field(self):
        return "EventDate"

    @property
    def sampling_field(self):
        return "DeviceID"

    @property
    def temp_table_name(self):
        return '{}_tmp'.format(self._table_name)

    @property
    def scheme(self):
        return str((
            self._fields.source,
            self.date_field,
            self.sampling_field,
            tuple(self._fields.get_db_fields())
        ))

    def _prepare_db(self):
        if not self._db.database_exists():
            self._db.create_database()
            logger.info('Database "{}" created'.format(self._db.db_name))

    def _prepare_table(self, state_controller: StateController):
        table_exists = self._db.table_exists(self._table_name)
        scheme_valid = state_controller.is_valid_scheme(self.scheme)
        if not table_exists or not scheme_valid:
            self._db.drop_table(self._table_name)
            self._db.create_table(self._table_name,
                                  self._fields.get_db_fields(),
                                  self.date_field, self.sampling_field)
            state_controller.update_db_scheme(self.scheme)
            logger.info('Table "{}" created'.format(self._table_name))

    def prepare(self, state_controller: StateController):
        self._prepare_db()
        self._prepare_table(state_controller)

    def _fetch_export_fields(self, df: DataFrame) -> DataFrame:
        logger.debug("Fetching exporting fields")
        return df[self._fields.get_export_keys_list()]

    def _escape_data(self, df: DataFrame) -> DataFrame:
        logger.debug("Escaping bad symbols")
        escape_chars = dict()
        string_cols = list(df.select_dtypes(include=['object']).columns)
        for col, type in self._fields.get_field_types():
            if type == 'String' and col in string_cols:
                escape_chars[col] = _escape_characters
        df.replace(escape_chars, regex=True, inplace=True)
        return df

    def _export_data_to_tsv(self, df: DataFrame) -> str:
        logger.debug("Exporting data to csv")
        return df.to_csv(index=False, sep='\t')

    def insert_data(self, df: DataFrame, date: datetime.date):
        df = self._fetch_export_fields(df)
        df = self._escape_data(df)  # TODO: Works too slow
        tsv = self._export_data_to_tsv(df)
        self._db.insert_distinct(
            table_name=self._table_name,
            tsv_content=tsv,
            key_fields_list=self._fields.get_db_keys(),
            date_field=self.date_field,
            start_date=date,
            end_date=date,
            temp_table_name=self.temp_table_name
        )

    def cleanup(self):
        self._db.drop_table(self.temp_table_name)
