#!/usr/bin/env python
import json
from os import environ
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

DEBUG = environ.get('DEBUG', '0') == '1'

TOKEN = environ['TOKEN']
API_KEYS = json.loads(environ['API_KEYS'])
FIELDS = json.loads(environ['FIELDS'])
KEY_FIELDS = set(json.loads(environ.get('KEY_FIELDS', '[]')))  # empty = all fields
INITIAL_PERIOD = int(environ.get('INITIAL_PERIOD', '30'))  # 30 days
CHECK_PERIOD = int(environ.get('CHECK_PERIOD', '7'))  # 7 days
FETCH_INTERVAL = int(environ.get('FETCH_INTERVAL', '43200'))  # 12 hours

CH_HOST = environ.get('CH_HOST', 'http://localhost:8123')
CH_USER = environ.get('CH_USER')
CH_PASSWORD = environ.get('CH_PASSWORD')
CH_DATABASE = environ.get('CH_DATABASE', 'mobile')
CH_TABLE = environ.get('CH_TABLE', 'events_all')
