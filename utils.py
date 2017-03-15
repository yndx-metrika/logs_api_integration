import argparse
import re
import json
import requests

DATE_FORMAT = '%Y-%m-%d'


class Structure:
    def __init__(self, **kwds):
        self.__dict__.update(kwds)

    def __str__(self):
        return json.dumps(self.__dict__, sort_keys=True, indent=2)

    def __repr__(self):
        return json.dumps(self.__dict__, sort_keys=True, indent=2)


def validate_user_request(user_request):
    """Validates initial user request"""
    assert user_request.source in ['hits', 'visits'], 'Invalid source'


def validate_cli_options(options):
    """Validates command line options"""
    assert options.source is not None, 'Source must be specified in CLI options'
    if options.mode is None:
        assert (options.start_date is not None) \
               and (options.end_date is not None), 'Dates or mode must be specified'
    else:
        assert options.mode in ['history', 'regular', 'regular_early'], \
            'Wrong mode in CLI options'


def get_cli_options():
    """Returns command line options"""
    parser = argparse.ArgumentParser()
    parser.add_argument('-start_date', help='Start of period')
    parser.add_argument('-end_date', help='End of period')
    parser.add_argument('-mode', help='Mode (one of [history, reqular, regular_early])')
    parser.add_argument('-source', help='Source (hits or visits)')
    parser.add_argument('-dest', help='Destination (clickhouse or vertica)')
    options = parser.parse_args()
    validate_cli_options(options)
    return options


def get_counter_creation_date(counter_id, token):
    """Returns create date for counter"""
    host = 'https://api-metrika.yandex.ru'
    url = '{host}/management/v1/counter/{counter_id}?oauth_token={token}' \
        .format(counter_id=counter_id, token=token, host=host)

    r = requests.get(url)
    if r.status_code == 200:
        date = json.loads(r.text)['counter']['create_time'].split('T')[0]
        return date


def get_config():
    """Returns user config"""
    with open('./configs/config.json') as input_file:
        config = json.loads(input_file.read())

    assert 'counter_id' in config, 'CounterID must be specified in config'
    assert 'token' in config, 'Token must be specified in config'
    assert 'retries' in config, 'Number of retries should be specified in config'
    assert 'retries_delay' in config, 'Delay between retries should be specified in config'
    assert ('clickhouse' in config) or ('vertica' in config), 'Destination should be specified in config'
    return config


def camel_to_snake(name):
    """Converts camal-case string to snake-case (underscore-separated)"""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def get_fields_config(dbtype='clickhouse'):
    """Returns config for ClickHouse columns\'s datatypes"""
    if (dbtype is None) or (dbtype == 'clickhouse'):
        prefix = 'ch'
    elif dbtype == 'vertica':
        prefix = 'vt'
    else:
        raise ValueError('Wrong argument: ' + str(dbtype))
    with open('./configs/{prefix}_types.json'.format(prefix=prefix)) as input_file:
        ch_field_types = json.loads(input_file.read())
    return ch_field_types
