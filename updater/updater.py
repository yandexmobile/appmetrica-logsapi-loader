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
from typing import Dict, Optional

import pandas as pd
from pandas import DataFrame, Series

from db import Database
from fields import SourcesCollection, Converter, ProcessingDefinition, \
    LoadingDefinition
from logs_api import Loader, LogsApiClient
from .db_controller import DbController

logger = logging.getLogger(__name__)


class Updater(object):
    def __init__(self, loader: Loader):
        self._loader = loader

    @staticmethod
    def _ensure_types(df: DataFrame, types: Dict[str, str]) -> DataFrame:
        logger.debug('Ensuring types')
        for col, db_type in types.items():
            if col not in df.columns:
                continue
            series = df[col]  # type: Series
            if 'Int' in db_type:
                series.fillna(0, inplace=True)
                df[col] = series.astype(db_type.lower())
        return df

    @staticmethod
    def _append_system_fields(df: DataFrame, app_id: str) -> DataFrame:
        logger.debug('Appending system fields')
        df['app_id'] = app_id
        df['load_datetime'] = int(datetime.datetime.now().timestamp())
        return df

    @staticmethod
    def _apply_converters(df: DataFrame,
                          converters: Dict[str, Converter]) \
            -> DataFrame:
        logger.debug('Applying converters')
        for name, converter in converters.items():
            df[name] = converter(df)
        return df

    def _process_data(self, app_id: str, df: DataFrame,
                      processing_definition: ProcessingDefinition):
        df = df.copy()  # type: DataFrame
        df = self._ensure_types(df, processing_definition.field_types)
        df = self._append_system_fields(df, app_id)
        df = self._apply_converters(df, processing_definition.field_converters)
        return df

    def _load(self, app_id: str, loading_definition: LoadingDefinition,
              date_from: Optional[datetime.datetime],
              date_to: Optional[datetime.datetime],
              date_dimension: Optional[str]):
        df_it = self._loader.load(app_id, loading_definition.source_name,
                                  loading_definition.fields,
                                  date_from, date_to, date_dimension)
        return df_it

    def update(self, app_id: str, date: Optional[datetime.date],
               table_suffix: str, db_controller: DbController,
               processing_definition: ProcessingDefinition,
               loading_definition: LoadingDefinition):
        since, until = None, None
        if date:
            since = datetime.datetime.combine(date, datetime.time.min)
            until = datetime.datetime.combine(date, datetime.time.max)
        db_controller.recreate_table(table_suffix)

        df_it = self._load(app_id, loading_definition, since, until,
                           LogsApiClient.DATE_DIMENSION_CREATE)
        for df in df_it:
            logger.debug("Start processing data chunk")
            upload_df = self._process_data(app_id, df,
                                           processing_definition)
            db_controller.insert_data(upload_df, table_suffix)
