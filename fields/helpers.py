#!/usr/bin/env python3
"""
  helpers.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
from typing import Callable, Optional, Tuple, Dict

from pandas import DataFrame, Series

from .field import Field


def field(load_name: str, db: Tuple[str, str], required: bool, generated: bool,
          converter: Optional[Callable[[DataFrame], Series]],
          extractor: Optional[Callable[[DataFrame], DataFrame]]) -> Field:
    return Field(load_name=load_name,
                 db_name=db[0],
                 db_type=db[1],
                 required=required,
                 generated=generated,
                 converter=converter,
                 extractor=extractor)


def system_defined(load_name: str, db: Tuple[str, str],
                   extractor: Callable[[DataFrame], DataFrame] = None) -> Field:
    return field(load_name=load_name,
                 db=db,
                 required=True,
                 generated=True,
                 converter=None,
                 extractor=extractor)


def required(load_name: str, db: Tuple[str, str],
             converter: Callable[[DataFrame], Series] = None,
             generated: Optional[bool] = None,
             extractor: Callable[[DataFrame], DataFrame] = None) -> Field:
    if generated is None:
        generated = converter is not None
    return field(load_name=load_name,
                 db=db,
                 required=True,
                 generated=generated,
                 converter=converter,
                 extractor=extractor)


def optional(load_name: str, db: Tuple[str, str],
             converter: Callable[[DataFrame], Series] = None,
             generated: Optional[bool] = None,
             extractor: Callable[[DataFrame], Dict[str, list]] = None) -> Field:
    if generated is None:
        generated = converter is not None
    return field(load_name=load_name,
                 db=db,
                 required=False,
                 generated=generated,
                 converter=converter,
                 extractor=extractor)
