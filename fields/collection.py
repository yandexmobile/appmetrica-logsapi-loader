#!/usr/bin/env python3
"""
  collection.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
from typing import List, Iterable

from .declaration import sources
from .source import Source


class SchedulingDefinition(object):
    def __init__(self, sources: Iterable[Source]):
        self.date_required_sources = []
        self.date_ignored_sources = []
        for source in sources:
            source_name = source.load_name
            if source.date_ignored:
                self.date_ignored_sources.append(source_name)
            else:
                self.date_required_sources.append(source_name)


class LoadingDefinition(object):
    def __init__(self, source: Source):
        self.source_name = source.load_name
        self.fields = []
        for field in source.fields:
            field_name = field.load_name
            if not field.generated:
                self.fields.append(field_name)


class DbTableDefinition(object):
    def __init__(self, source: Source, name=None):
        self.table_name = source.db_name if name is None else name
        self.primary_keys = []
        self.column_types = dict()
        self.field_types = dict()
        self.export_fields = []
        self.sampling_field = None
        for field in source.fields:
            field_name = field.load_name
            if field_name == source.date_field_name:
                self.date_field = field.db_name
            if field_name == source.sampling_field_name:
                self.sampling_field = field.db_name
            if field_name in source.key_field_names:
                self.primary_keys.append(field.db_name)
            self.field_types[field.db_name] = field.db_type
            self.column_types[field_name] = field.db_type
            self.export_fields.append(field_name)


class ProcessingDefinition(object):
    def __init__(self, source: Source):
        self.field_converters = dict()
        self.field_types = dict()
        for field in source.fields:
            field_name = field.load_name
            if field.converter:
                self.field_converters[field_name] = field.converter
            self.field_types[field_name] = field.db_type


class SourcesCollection(object):
    def __init__(self, requested_sources: List[str]):
        self._source_names = []
        self._sources = dict()
        for source in sources:
            source_name = source.load_name
            if len(requested_sources) == 0 or source_name in requested_sources:
                self._source_names.append(source_name)
                self._sources[source_name] = source

    def source_names(self):
        return self._source_names

    def scheduling_definition(self) -> SchedulingDefinition:
        return SchedulingDefinition(self._sources.values())

    def loading_definition(self, source_name) -> LoadingDefinition:
        return LoadingDefinition(self._sources[source_name])

    def processing_definition(self, source_name) -> ProcessingDefinition:
        return ProcessingDefinition(self._sources[source_name])

    def db_table_definition(self, source_name) -> DbTableDefinition:
        return DbTableDefinition(self._sources[source_name])
