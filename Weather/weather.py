import sys
import requests
import json
import logging
from logging.config import dictConfig
import time
import datetime


logging_config = dict(
    version=1,
    formatters={
        'f': {'format': '[%(levelname)s] %(asctime)s %(name)s [%(lineno)d] %(message)s'}
    },
    handlers={
        'h': {'class': 'logging.StreamHandler', 'formatter': 'f', 'level': logging.DEBUG},
        'file': {
            'class': 'logging.handlers.RotatingFileHandler', 'level': logging.DEBUG,
            'formatter': 'f',
            'filename': sys.argv[0].split('.')[0] + '.log',
            'maxBytes': 10485760,
            'backupCount': 10,
        },
    },
    root={'handlers': ['h', 'file'], 'level': logging.DEBUG}
)
dictConfig(logging_config)
logger = logging.getLogger('Weather')


DATA_URL = "http://127.0.0.1:3000/api/Weather"

API = "https://api.darksky.net/forecast/{key}/{latitude},{longitude},{timestamp}"
KEY = "c7e615903a16478b8c70bd2eedbdfe95"
latitude = "40.13501"
longitude = "116.477023"


def get_weather_data(ts=None):
    _now = datetime.datetime.now()
    _date = datetime.datetime(_now.year, _now.month, _now.day)
    _ts = int(time.mktime(_date.timetuple()))
    ret = requests.get(API.format(key=KEY, latitude=latitude, longitude=longitude, timestamp=ts or _ts)).content
    try:
        return json.loads(ret)
    except ValueError as _:
        logger.error(_)
        return None


def save_weather_data(d):
    try:
        r = requests.post(DATA_URL, data=d)
        logger.info(r)
    except Exception as e:
        logger.error(e)


if __name__ == '__main__':
    now = datetime.datetime.now()
    timestamp = int(time.mktime(datetime.datetime(now.year, now.month, now.day).timetuple()))

    if sys.argv[1] == 'history':
        for _ in range(90):
            new_timestamp = timestamp-_*86400
            _data = get_weather_data(ts=new_timestamp)
            _data['date'] = str(datetime.datetime.fromtimestamp(new_timestamp))
            logger.info(datetime.datetime.fromtimestamp(new_timestamp))
            logger.info(_data)
            save_weather_data(json.dumps(_data))
    else:
        _data = get_weather_data(ts=timestamp)
        _data['date'] = str(datetime.datetime.fromtimestamp(timestamp))
        logger.info(datetime.datetime.fromtimestamp(timestamp))
        save_weather_data(json.dumps(_data))


