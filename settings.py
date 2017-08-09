#!/usr/bin/env python
import datetime
import json
from os import environ
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

DEBUG = environ.get('DEBUG', '0') == '1'

TOKEN = environ['TOKEN']
API_KEYS = json.loads(environ['API_KEYS'])
SOURCE_NAME = environ['SOURCE_NAME']
FIELDS = json.loads(environ['FIELDS'])
KEY_FIELDS = set(json.loads(environ.get('KEY_FIELDS', '[]')))  # empty = all fields

UPDATE_LIMIT = datetime.timedelta(days=int(environ.get('UPDATE_LIMIT', '30')))
FRESH_LIMIT = datetime.timedelta(days=int(environ.get('FRESH_LIMIT', '7')))
UPDATE_INTERVAL = datetime.timedelta(hours=int(environ.get('FETCH_INTERVAL', '12')))
REQUEST_CHUNK_SIZE = int(environ.get('REQUEST_CHUNK_SIZE', '134217728'))  # 128 Mb

CH_HOST = environ.get('CH_HOST', 'http://localhost:8123')
CH_USER = environ.get('CH_USER')
CH_PASSWORD = environ.get('CH_PASSWORD')
CH_DATABASE = environ.get('CH_DATABASE', 'mobile')
CH_TABLE = environ.get('CH_TABLE', 'events_all')
