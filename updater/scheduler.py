#!/usr/bin/env python3
"""
  scheduler.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""
from datetime import datetime, date, time, timedelta
import logging
from time import sleep
from typing import List, Optional, Generator, Dict

import pandas as pd

from state import StateStorage, AppIdState
from fields import SchedulingDefinition

logger = logging.getLogger(__name__)


class UpdateRequest(object):
    ARCHIVE = 'archive'
    LOAD_ONE_DATE = 'load_one_date'
    LOAD_DATE_IGNORED = 'load_date_ignored'
    LOAD_INTERVAL = 'load_interval'

    def __init__(self, source: str, app_id: str, p_date: Optional[date],
                 update_type: str, interval_from: Optional[datetime] = None, interval_to: Optional[datetime] = None):
        self.source = source
        self.app_id = app_id
        self.date = p_date
        self.update_type = update_type
        self.interval_from = interval_from
        self.interval_to = interval_to


class Scheduler(object):
    ARCHIVED_DATE = datetime(3000, 1, 1)
    _SCHEDULE_INTERVAL_MINUTES = 'interval_minutes'
    _SCHEDULE_HOURLY_AT = 'hourly_at'
    _SCHEDULE_EVERY_10TH = 'every_10th'
    _SCHEDULE_DAILY_AT_HOUR = 'daily_at_hour'

    def __init__(self, state_storage: StateStorage,
                 scheduling_definition: SchedulingDefinition,
                 app_ids: List[str], update_limit: timedelta,
                 update_schedule: Dict[str, int], load_interval: timedelta,
                 fresh_limit: timedelta):
        self._state_storage = state_storage
        self._definition = scheduling_definition
        self._app_ids = app_ids
        self._update_limit = update_limit
        self._update_schedule = update_schedule
        # self._update_interval = update_interval
        self._load_interval = load_interval
        self._fresh_limit = fresh_limit
        self._state = None

    def _update_interval(self) -> int:
        return self._update_schedule.get(self._SCHEDULE_INTERVAL_MINUTES, 0)

    def _load_state(self):
        self._state = self._state_storage.load()

    def _save_state(self):
        self._state_storage.save(self._state)

    def _get_or_create_app_id_state(self, app_id: str) -> AppIdState:
        app_id_states = [s for s in self._state.app_id_states
                         if s.app_id == app_id]
        if len(app_id_states) == 0:
            app_id_state = AppIdState(app_id)
            self._state.app_id_states.append(app_id_state)
        else:
            app_id_state = app_id_states[0]
        return app_id_state

    def _mark_date_updated(self, app_id_state: AppIdState, p_date: date,
                           now: Optional[datetime] = None):
        logger.debug('Data for {} of {} is updated'.format(
            p_date, app_id_state.app_id
        ))
        app_id_state.date_updates[p_date] = now or datetime.now()
        self._save_state()

    def _mark_date_archived(self, app_id_state: AppIdState, p_date: date):
        logger.debug('Data for {} of {} is archived'.format(
            p_date, app_id_state.app_id
        ))
        app_id_state.date_updates[p_date] = self.ARCHIVED_DATE
        self._save_state()

    def _is_date_archived(self, app_id_state: AppIdState, p_date: date):
        updated_at = app_id_state.date_updates.get(p_date)
        return updated_at is not None and updated_at == self.ARCHIVED_DATE

    def _finish_updates(self, now: datetime = None):
        logger.debug('Updates are finished')
        self._state.last_update_time = now or datetime.now()
        self._save_state()

    def _wait_time(self,
                   schedule: Dict[str, int],
                   now: datetime = None) \
            -> Optional[timedelta]:
        if not self._state.last_update_time:
            return None

        now = now or datetime.now()
        delta = timedelta(seconds=0)
        if schedule.get(self._SCHEDULE_INTERVAL_MINUTES):
            delta = self._state.last_update_time - now + timedelta(minutes=schedule[self._SCHEDULE_INTERVAL_MINUTES])
        elif schedule.get(self._SCHEDULE_HOURLY_AT):
            next_update = self._state.last_update_time.replace(minute=0, second=0, microsecond=0) \
                          + timedelta(hours=1, minutes=schedule[self._SCHEDULE_HOURLY_AT])
            delta = next_update - now
        elif schedule.get(self._SCHEDULE_EVERY_10TH):
            hour = self._state.last_update_time.replace(minute=0, second=0, microsecond=0)
            tens_since_hour = (self._state.last_update_time.minute // 10) * 10
            next_update = hour + timedelta(minutes=tens_since_hour) + timedelta(
                minutes=schedule[self._SCHEDULE_EVERY_10TH])
            if self._state.last_update_time > next_update:
                next_update = next_update + timedelta(minutes=10)
            delta = next_update - now
        elif schedule.get(self._SCHEDULE_DAILY_AT_HOUR):
            since_last = now - self._state.last_update_time
            if since_last > timedelta(hours=24):
                return None
            update_hour = schedule.get(self._SCHEDULE_DAILY_AT_HOUR)
            next_update = now.replace(hour=update_hour)
            if next_update < now:
                next_update = next_update + timedelta(days=1)

            delta = next_update - now
        else:
            raise Exception("No schedule")

        if delta.total_seconds() < 0:
            return None
        return delta

    @staticmethod
    def round_based(x, base=5):
        return int(base * round(float(x) / base))

    def _wait_if_needed(self):
        wait_time = self._wait_time(self._update_schedule)
        if wait_time:
            logger.info('Sleep for {}'.format(wait_time))
            sleep(wait_time.total_seconds())

    def _archive_old_dates(self, app_id_state: AppIdState):
        logger.info('Archiving old tables')
        for p_date, updated_at in app_id_state.date_updates.items():
            logger.info('checking date {}'.format(p_date))
            if self._is_date_archived(app_id_state, p_date):
                logger.info('date is archived')
                continue

            last_event_date = datetime.combine(p_date, time.max)
            fresh = datetime.now() - last_event_date < self._fresh_limit
            logger.debug('updated_at:{} last_event_date:{} fresh limit:{}'.format(updated_at, last_event_date,
                                                                                  self._fresh_limit))
            if not fresh:
                logger.info('archiving date')
                for source in self._definition.date_required_sources:
                    yield UpdateRequest(source, app_id_state.app_id, p_date,
                                        UpdateRequest.ARCHIVE)
                self._mark_date_archived(app_id_state, p_date)

    def _update_date(self, app_id_state: AppIdState, p_date: date,
                     started_at: datetime) \
            -> Generator[UpdateRequest, None, None]:
        sources = self._definition.date_required_sources
        updated_at = app_id_state.date_updates.get(p_date)
        last_event_date = datetime.combine(p_date, time.max)
        if updated_at:
            minutes = (started_at - updated_at).total_seconds() // 60
            updated = minutes < self._update_interval()
            if updated:
                return
        last_event_delta = (updated_at or started_at) - last_event_date
        for source in sources:
            yield UpdateRequest(source, app_id_state.app_id, p_date,
                                UpdateRequest.LOAD_ONE_DATE)
        self._mark_date_updated(app_id_state, p_date)

        fresh = last_event_delta < self._fresh_limit
        if not fresh:
            for source in sources:
                yield UpdateRequest(source, app_id_state.app_id, p_date,
                                    UpdateRequest.ARCHIVE)
            self._mark_date_archived(app_id_state, p_date)

    def _update_date_ignored_fields(self, app_id: str):
        for source in self._definition.date_ignored_sources:
            yield UpdateRequest(source, app_id, None,
                                UpdateRequest.LOAD_DATE_IGNORED)

    @staticmethod
    def _filter_without_state(dates, state: AppIdState):
        return [d for d in dates if d.date() not in state.date_updates]

    def update_requests(self) \
            -> Generator[UpdateRequest, None, None]:
        if self._load_interval.total_seconds() > 0:
            return self.mainstream_update()
        else:
            return self.date_update_requests()

    def date_update_requests(self) \
            -> Generator[UpdateRequest, None, None]:
        self._load_state()
        self._wait_if_needed()
        started_at = datetime.now()
        for app_id in self._app_ids:
            app_id_state = self._get_or_create_app_id_state(app_id)
            date_to = started_at.date()
            date_from = date_to - self._update_limit

            updates = self._archive_old_dates(app_id_state)
            for update_request in updates:
                yield update_request

            date_range = pd.date_range(date_from, date_to).tolist()

            new = self._filter_without_state(date_range, app_id_state)
            result_set = set(new)
            result_set.remove(pd.Timestamp(year=date_to.year, month=date_to.month, day=date_to.day))
            if len(date_range) > 2:
                result_set.add(date_range[0])  # oldest date(may be archived
                result_set.add(date_range[-2])  # yesterday

            logger.debug("dates to update {}".format(result_set))
            for pd_date in sorted(result_set):
                p_date = pd_date.to_pydatetime().date()  # type: date
                updates = self._update_date(app_id_state, p_date, started_at)
                for update_request in updates:
                    yield update_request

            updates = self._update_date_ignored_fields(app_id_state.app_id)
            for update_request in updates:
                yield update_request
        self._finish_updates()

    def _update_between(self, app_id_state: AppIdState, dt_from: datetime, dt_to: datetime) \
            -> Generator[UpdateRequest, None, None]:
        sources = self._definition.date_required_sources
        for source in sources:
            yield UpdateRequest(source, app_id_state.app_id, None,
                                UpdateRequest.LOAD_INTERVAL, dt_from, dt_to)

    def mainstream_update(self) \
            -> Generator[UpdateRequest, None, None]:
        self._load_state()
        self._wait_if_needed()
        started_at = datetime.utcnow()
        datetime_to = started_at - timedelta(minutes=started_at.minute % (self._load_interval.seconds // 60),
                                             seconds=started_at.second,
                                             microseconds=started_at.microsecond)
        datetime_from = datetime_to - self._load_interval
        datetime_to = datetime_to - timedelta(seconds=1)
        for app_id in self._app_ids:
            app_id_state = self._get_or_create_app_id_state(app_id)
            updates = self._update_between(app_id_state, datetime_from, datetime_to)
            for update_request in updates:
                yield update_request

        self._finish_updates()
