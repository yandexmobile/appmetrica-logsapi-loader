#!/usr/bin/env python
"""
  updater.py

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
from logs_api import Loader
from state import StateController

logger = logging.getLogger(__name__)


class Updater(object):
    def __init__(self,
                 loader: Loader,
                 db: Database,
                 fields_collection: FieldsCollection,
                 state_controller: StateController,
                 source_name: str,
                 table_name: str):
        self._loader = loader
        self._db = db
        self._table_name = table_name
        self._state_controller = state_controller
        self._source_name = source_name
        self._temp_table_name = '{}_tmp'.format(table_name)
        self._date_field = 'EventDate'
        self._sampling_field = 'DeviceID'
        self._load_fields = fields_collection.get_load_fields()
        self._db_fields = fields_collection.get_db_fields()
        self._db_fields_str = ', '.join((f_name for (f_name, f_type)
                                         in self._db_fields))
        self._key_fields_str = ', '.join(fields_collection.get_db_keys())
        self._export_fields = fields_collection.get_export_keys_list()
        self._filed_converters = fields_collection.get_converters()

    def _cleanup(self):
        self._db.drop_table(self._temp_table_name)

    def _process_data(self, api_key, df):
        df = df.drop_duplicates().copy()
        df['api_key'] = api_key
        for name, converter in self._filed_converters:
            df[name] = converter(df)
        return df

    def _get_update_data(self, df):
        escape_characters = {
            '\b': '\\\\b',
            '\r': '\\\\r',
            '\f': '\\\\f',
            '\n': '\\\\n',
            '\t': '\\\\t',
            '\0': '\\\\0',
            '\'': '\\\\\'',
            '\\\\': '\\\\\\\\',
        }
        sub_df = df[self._export_fields]
        escaped = sub_df.replace(escape_characters, regex=True)
        tsv = escaped.to_csv(index=False, sep='\t')
        return tsv

    def _upload_data_frame(self, df: DataFrame,
                           api_key: str, date: datetime.date):
        self._cleanup()

        updated_df = self._process_data(api_key, df)
        insert_tsv = self._get_update_data(updated_df)
        self._db.insert_distinct(
            table_name=self._table_name,
            tsv_content=insert_tsv,
            key_fields_list=self._key_fields_str,
            date_field=self._date_field,
            start_date=date,
            end_date=date,
            temp_table_name=self._temp_table_name)

    def prepare(self):
        if not self._db.database_exists():
            self._db.create_database()
            logger.info('Database "{}" created'.format(self._db.db_name))
        scheme = str((
                     self._source_name, self._date_field, self._sampling_field,
                     tuple(self._db_fields)))
        table_exists = self._db.table_exists(self._table_name)
        scheme_valid = self._state_controller.is_valid_scheme(scheme)
        if not table_exists or not scheme_valid:
            self._db.drop_table(self._table_name)
            self._db.create_table(self._table_name, self._db_fields,
                                  self._date_field, self._sampling_field)
            self._state_controller.update_db_scheme(scheme)
            logger.info('Table "{}" created'.format(self._table_name))

    def update(self, api_key: str, date: datetime.date):
        df_it = self._loader.load(api_key, self._source_name,
                                  self._load_fields, date)
        for df in df_it:
            self._upload_data_frame(df, api_key, date)
        self._cleanup()
        self._state_controller.mark_updated(api_key, date)
