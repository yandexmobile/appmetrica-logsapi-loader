#!/usr/bin/env python
"""
  logs_api_int_script.py

  This file is a part of the AppMetrica.

  Copyright 2017 YANDEX

  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at:
        https://yandex.com/legal/metrica_termsofuse/
"""

import datetime
import json
import time
import requests
import logging
import pandas as pd
import io

import settings

logger = logging.getLogger(__name__)


def setup_logging(debug: bool = False) -> None:
    logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    level = logging.INFO
    if debug:
        level = logging.DEBUG
    logging.basicConfig(format=logging_format, level=level)


def get_create_date(api_key, token):
    app_details_url = 'https://api.appmetrica.yandex.ru/management/v1/application/{id}?oauth_token={token}'.format(
        id=api_key,
        token=token
    )
    r = requests.get(app_details_url)
    create_date = None
    if r.status_code == 200:
        app_details = json.load(r.text)
        if ('application' in app_details) \
                and ('create_date' in app_details['application']):
            create_date = app_details['application']['create_date']
    return create_date


def load_logs_api_data(api_key, date1, date2, token):
    url_tmpl = 'https://api.appmetrica.yandex.ru/logs/v1/export/events.csv?application_id={api_key}&date_since={date1}%2000%3A00%3A00&date_until={date2}%2023%3A59%3A59&date_dimension=default&fields=event_name%2Cevent_timestamp%2Cappmetrica_device_id%2Cos_name%2Ccountry_iso_code%2Ccity&oauth_token={token}'
    url = url_tmpl.format(api_key=api_key,
                          date1=date1,
                          date2=date2,
                          token=token)

    r = requests.get(url)

    while r.status_code != 200:
        logger.debug('Logs API response code: {}'.format(r.status_code))
        if r.status_code != 202:
            raise ValueError(r.text)

        time.sleep(10)
        r = requests.get(url)

    df = pd.read_csv(io.StringIO(r.text))
    df['event_date'] = list(map(lambda x: datetime.datetime
                                .fromtimestamp(x)
                                .strftime('%Y-%m-%d'), df.event_timestamp))
    return df


def get_clickhouse_auth():
    auth = None
    if settings.CH_USER:
        auth = (settings.CH_USER, settings.CH_PASSWORD)
    return auth


def query_clickhouse(data, params=None):
    """Returns ClickHouse response"""
    log_data = data
    if len(log_data) > 200:
        log_data = log_data[:200] + '[...]'
    logger.debug('Query ClickHouse:\n{}\n\tHTTP params: {}'
                 .format(log_data, params))
    host = settings.CH_HOST
    auth = get_clickhouse_auth()
    r = requests.post(host, data=data, params=params, auth=auth)
    if r.status_code == 200:
        return r.text
    else:
        raise ValueError(r.text)


def get_clickhouse_data(query):
    """Returns ClickHouse response"""
    return query_clickhouse(query)


def upload_clickhouse_data(table, content):
    """Uploads data to table in ClickHouse"""
    query_dict = {
        'query': 'INSERT INTO ' + table + ' FORMAT TabSeparatedWithNames '
    }
    return query_clickhouse(content, params=query_dict)


def drop_table(db, table):
    q = 'DROP TABLE IF EXISTS {db}.{table}'.format(
        db=db,
        table=table
    )

    get_clickhouse_data(q)


def database_exists(db):
    q = 'SHOW DATABASES'
    dbs = get_clickhouse_data(q).strip().split('\n')
    return db in dbs


def database_create(db):
    q = 'CREATE DATABASE {db}'.format(db=db)
    get_clickhouse_data(q)


def table_exists(db, table):
    q = 'SHOW TABLES FROM {db}'.format(db=db)
    tables = get_clickhouse_data(q).strip().split('\n')
    return table in tables


def table_create(db, table):
    q = '''
    CREATE TABLE {db}.{table} (
        EventDate Date,
        DeviceID String,
        EventName String,
        EventTimestamp UInt64,
        AppPlatform String,
        Country String,
        APIKey UInt64
    )
    ENGINE = MergeTree(EventDate, 
                        cityHash64(DeviceID), 
                        (EventDate, cityHash64(DeviceID)), 
                        8192)
    '''.format(
        db=db,
        table=table
    )
    get_clickhouse_data(q)


def create_tmp_table_for_insert(db, table, date1, date2):
    q = '''
        CREATE TABLE tmp_data_ins ENGINE = MergeTree(EventDate, 
                                            cityHash64(DeviceID),
                                            (EventDate, cityHash64(DeviceID)), 
                                            8192)
        AS
        SELECT
            EventDate,
            DeviceID,
            EventName,
            EventTimestamp,
            AppPlatform,
            Country,
            APIKey
        FROM tmp_data
        WHERE NOT ((EventDate, 
                    DeviceID,
                    EventName,
                    EventTimestamp,
                    AppPlatform,
                    Country,
                    APIKey) 
            GLOBAL IN (SELECT
                EventDate,
                DeviceID,
                EventName,
                EventTimestamp,
                AppPlatform,
                Country,
                APIKey
            FROM {db}.{table}
            WHERE EventDate >= '{date1}' AND EventDate <= '{date2}'))
    '''.format(
        db=db,
        table=table,
        date1=date1,
        date2=date2
    )

    get_clickhouse_data(q)


def insert_data_to_prod(db, table):
    q = '''
        INSERT INTO {db}.{table}
            SELECT
                EventDate,
                DeviceID,
                EventName,
                EventTimestamp,
                AppPlatform,
                Country,
                APIKey
            FROM tmp_data_ins
    '''.format(
        db=db,
        table=table
    )

    get_clickhouse_data(q)


def process_date(date, token, api_key, db, table):
    df = load_logs_api_data(api_key, date, date, token)
    df = df.drop_duplicates()
    df['api_key'] = api_key

    drop_table('default', 'tmp_data')
    drop_table('default', 'tmp_data_ins')

    table_create('default', 'tmp_data')

    upload_clickhouse_data(
        'tmp_data',
        df[['event_date',
            'appmetrica_device_id',
            'event_name',
            'event_timestamp',
            'os_name',
            'country_iso_code',
            'api_key']].to_csv(index=False, sep='\t')
    )
    create_tmp_table_for_insert(db, table, date, date)
    insert_data_to_prod(db, table)
    drop_table('default', 'tmp_data')
    drop_table('default', 'tmp_data_ins')


def main(first_flag=False):
    setup_logging(settings.DEBUG)

    days_delta = 7
    if first_flag:
        days_delta = settings.HISTORY_PERIOD

    time_delta = datetime.timedelta(days_delta)
    today = datetime.datetime.today()
    date1 = (today - time_delta).strftime('%Y-%m-%d')
    date2 = today.strftime('%Y-%m-%d')

    database = settings.CH_DATABASE
    if not database_exists(database):
        database_create(database)
        logger.info('Database "{}" created'.format(database))

    table = settings.CH_TABLE
    if not table_exists(database, table):
        table_create(database, table)
        logger.info('Table "{}" created'.format(table))

    logger.info('Loading period {} - {}'.format(date1, date2))
    token = settings.TOKEN
    api_keys = settings.API_KEYS
    for api_key in api_keys:
        logger.info('Processing API key: {}'.format(api_key))
        for date in pd.date_range(date1, date2):
            date_str = date.strftime('%Y-%m-%d')
            logger.info('Loading data for {}'.format(date_str))
            process_date(date_str, token, api_key, database, table)
    logger.info('Finished loading data')


if __name__ == '__main__':
    main()
