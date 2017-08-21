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
from typing import List, Tuple, Dict

from pandas import DataFrame, Series

from db import Database
from fields import SourcesCollection, Converter, ProcessingDefinition
from logs_api import Loader
from state import StateController
from .db_controller import DbController

logger = logging.getLogger(__name__)


class Updater(object):
    def __init__(self, loader: Loader, sources_collection: SourcesCollection,
                 db: Database):
        self._loader = loader
        self._sources_collection = sources_collection
        self._db_controllers = dict()
        self._db = db

    @staticmethod
    def _preprocess(df: DataFrame) -> DataFrame:
        df.drop_duplicates(inplace=True)
        return df

    @staticmethod
    def _ensure_types(df: DataFrame, types: Dict[str, str]) -> DataFrame:
        for col, db_type in types.items():
            if not col in df.columns:
                continue
            series = df[col]  # type: Series
            if 'Int' in db_type:  # and series.hasnans():
                df[col] = series.fillna(0).astype(db_type.lower())
        return df

    @staticmethod
    def _append_system_fields(df: DataFrame, app_id: str) -> DataFrame:
        df['app_id'] = app_id
        df['load_datetime'] = int(datetime.datetime.now().timestamp())
        return df

    @staticmethod
    def _apply_converters(df: DataFrame,
                          converters: Dict[str, Converter])\
            -> DataFrame:
        for name, converter in converters.items():
            df[name] = converter(df)
        return df

    def _process_data(self, app_id: str, df: DataFrame,
                      processing_definition: ProcessingDefinition):
        df = df.copy()  # type: DataFrame
        df = self._preprocess(df)
        df = self._ensure_types(df, processing_definition.field_types)
        df = self._append_system_fields(df, app_id)
        df = self._apply_converters(df, processing_definition.field_converters)
        return df

    def _cached_db_controller(self, source: str):
        if source in self._db_controllers.keys():
            db_controller = self._db_controllers[source]
        else:
            db_table_definition = \
                self._sources_collection.db_table_definition(source)
            db_controller = DbController(self._db, db_table_definition)
            db_controller.prepare()
            self._db_controllers[source] = db_controller
        return db_controller

    def _load(self, source: str, app_id: str, date: datetime.date):
        loading_definition = \
            self._sources_collection.loading_definition(source)
        df_it = self._loader.load(app_id, loading_definition.source_name,
                                  loading_definition.fields, date)
        return df_it

    def update(self, source: str, app_id: str, date: datetime.date):
        db_controller = self._cached_db_controller(source)
        processing_definition = \
            self._sources_collection.processing_definition(source)
        df_it = self._load(source, app_id, date)
        for df in df_it:
            logger.debug("Start processing data chunk")
            upload_df = self._process_data(app_id, df, processing_definition)
            db_controller.insert_data(upload_df, date)
        db_controller.cleanup()
