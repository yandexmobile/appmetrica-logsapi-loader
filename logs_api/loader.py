#!/usr/bin/env python
"""
  loader.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
import datetime
import logging
import re
import time
from typing import List, Generator, Tuple

import pandas as pd
import requests
from pandas import DataFrame
from urllib3.exceptions import ProtocolError

from .client import LogsApiClient, LogsApiError

logger = logging.getLogger(__name__)


class Loader(object):
    def __init__(self, client: LogsApiClient, chunk_size: int):
        self.client = client
        self._chunk_size = chunk_size
        self._progress_re = re.compile(r'.*Progress is (?P<progress>\d+)%.*')

    def _split_response(self, response: requests.Response):
        compression = response.headers.get('Content-Encoding')
        return pd.read_csv(response.raw,
                           compression=compression,
                           encoding=response.encoding,
                           chunksize=self._chunk_size,
                           iterator=True)

    def _process_error(self, status_code: int, text: str, parts_count: int,
                       part_number: int, progress: int) \
            -> Tuple[int, int, int]:
        logger.debug(text)
        if status_code == 202:
            progress_match = self._progress_re.match(text)
            if progress_match:
                new_progress = int(progress_match.group('progress'))
                if new_progress != progress:
                    progress = new_progress
                    logger.info('Preparation progress: {}%'.format(
                        progress
                    ))
            time.sleep(10)
        elif status_code == 429:
            logger.info('Too many requests. Waiting...')
            time.sleep(60)
        elif status_code == 400 \
                and 'Try to use more parts.' in text:
            parts_count *= 2
            part_number = 0
            logger.info('Request is too big. Parts count: {}'.format(
                parts_count
            ))
        else:
            raise ValueError('[{}] {}'.format(status_code, text))
        return parts_count, part_number, progress

    def load(self, app_id: str, table: str, fields: List[str],
             date: datetime.date) \
            -> Generator[DataFrame, None, None]:
        parts_count = 1
        part_number = 0
        progress = None
        while part_number < parts_count:
            try:
                r = self.client.logs_api_export(app_id=app_id, table=table,
                                                fields=fields,
                                                date_from=date,
                                                date_to=date,
                                                parts_count=parts_count,
                                                part_number=part_number)
                if parts_count > 1:
                    logger.info('Processing part {} from {}'.format(
                        part_number, parts_count
                    ))
                lines_count = 0
                for df in self._split_response(r):
                    yield df
                    lines_count += len(df)
                    logger.info('Lines loaded: {}'.format(lines_count))
                part_number += 1
            except LogsApiError as e:
                parts_count, part_number, progress = \
                    self._process_error(e.status_code, e.text, parts_count,
                                        part_number, progress)
            except ProtocolError as e:
                logger.warning(e)
                continue
