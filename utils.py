from __future__ import (absolute_import, division,
                        print_function, unicode_literals)

import json
import argparse
import requests
import platform

DATE_FORMAT = '%Y-%m-%d'

class Structure:
    def __init__(self, **kwds):
        self.__dict__.update(kwds)

    def __str__(self):
        return json.dumps(self.__dict__, sort_keys=True, indent=2)

    def __repr__(self):
        return json.dumps(self.__dict__, sort_keys=True, indent=2)


def validate_user_request(user_request):
    '''Validates initial user request'''
    assert user_request.source in ['hits', 'visits'], 'Invalid source'


def validate_cli_options(options):
    '''Validates command line options'''
    assert options.source is not None, \
        'Source must be specified in CLI options'
    if options.mode is None:
        assert (options.start_date is not None) \
            and (options.end_date is not None), \
            'Dates or mode must be specified'
    else:
        assert options.mode in ['history', 'regular', 'regular_early'], \
            'Wrong mode in CLI options'


def get_cli_options():
    '''Returns command line options'''
    parser = argparse.ArgumentParser()
    parser.add_argument('-start_date', help = 'Start of period')
    parser.add_argument('-end_date', help = 'End of period')
    parser.add_argument('-mode', help = 'Mode (one of [history, reqular, regular_early])')
    parser.add_argument('-source', help = 'Source (hits or visits)')
    options = parser.parse_args()
    validate_cli_options(options)
    return options


def get_counter_creation_date(counter_id, token):
    '''Returns create date for counter'''
    host = 'https://api-metrika.yandex.ru'
    url = '{host}/management/v1/counter/{counter_id}' \
        .format(counter_id=counter_id, host=host)

    headers = {'Authorization': 'OAuth ' + token}

    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        date = json.loads(r.text)['counter']['create_time'].split('T')[0]
        return date


def get_config():
    '''Returns user config'''
    with open('./configs/config.json') as input_file:
        config = json.loads(input_file.read())

    assert 'counter_id' in config, 'CounterID must be specified in config'
    assert 'token' in config, 'Token must be specified in config'
    assert 'retries' in config, 'Number of retries should be specified in config'
    assert 'retries_delay' in config, 'Delay between retries should be specified in config'
    return config


def get_ch_fields_config():
    '''Returns config for ClickHouse columns\'s datatypes'''
    with open('./configs/ch_types.json') as input_file:
        ch_field_types = json.loads(input_file.read())
    return ch_field_types

def get_python_version():
    return platform.python_version()
