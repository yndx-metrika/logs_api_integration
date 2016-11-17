import requests
import urllib
import json
import utils
import pandas as pd
import StringIO
import clickhouse
import datetime
import logging


logger = logging.getLogger('logs_api')

HOST = 'http://faced-vesel4ak-u-0.mtrs.yandex.ru:8082'


def get_estimation(user_request):
    '''Returns estimation of Logs API (whether it's possible to load data and max period in days)'''
    url_params = urllib.urlencode(
        [
            ('date1', user_request.start_date_str),
            ('date2', user_request.end_date_str),
            ('source', user_request.source),
            ('fields', ','.join(user_request.fields)),
            ('oauth_token', user_request.token)
        ]
    )

    url = '{host}/management/v1/counter/{counter_id}/logrequests/evaluate?'\
        .format(host=HOST, counter_id=user_request.counter_id) + url_params

    r = requests.get(url)
    logger.debug(r.text)
    if r.status_code == 200:
        return json.loads(r.text)['log_request_evaluation']
    else:
        raise ValueError(r.text)


def get_api_requests(user_request):
    '''Returns list of API requests for UserRequest'''
    api_requests = []
    estimation = get_estimation(user_request)
    if estimation['possible']:
        api_request = utils.Structure(
            user_request=user_request,
            date1_str=user_request.start_date_str,
            date2_str=user_request.end_date_str,
            status='new'
        )
        api_requests.append(api_request)
    else:
        start_date = datetime.datetime.strptime(
            user_request.start_date_str,
            utils.DATE_FORMAT
        )

        end_date = datetime.datetime.strptime(
            user_request.end_date_str,
            utils.DATE_FORMAT
        )

        days = (end_date - start_date).days
        num_requests = int(days/estimation['max_possible_day_quantity']) + 1
        days_in_period = int(days/num_requests) + 1
        for i in range(num_requests):
            date1 = start_date + datetime.timedelta(i*days_in_period)
            date2 = min(
                end_date,
                start_date + datetime.timedelta((i+1)*days_in_period - 1)
            )

            api_request = utils.Structure(
                user_request=user_request,
                date1_str=date1.strftime(utils.DATE_FORMAT),
                date2_str=date2.strftime(utils.DATE_FORMAT),
                status='new'
            )
            api_requests.append(api_request)

    return api_requests


def create_task(api_request):
    '''Creates a Logs API task to generate data'''
    url_params = urllib.urlencode(
        [
            ('date1', api_request.date1_str),
            ('date2', api_request.date2_str),
            ('source', api_request.user_request.source),
            ('fields', ','.join(api_request.user_request.fields)),
            ('oauth_token', api_request.user_request.token)
        ]
    )
    url = '{host}/management/v1/counter/{counter_id}/logrequests?'\
        .format(host=HOST,
                counter_id=api_request.user_request.counter_id) \
          + url_params

    r = requests.post(url)
    logger.debug(r.text)
    if r.status_code == 200:
        logger.debug(json.dumps(json.loads(r.text)['log_request'], indent=2))
        response = json.loads(r.text)['log_request']
        api_request.status = response['status']
        api_request.request_id = response['request_id']
        # api_request.size = response['size']
        return response
    else:
        raise ValueError(r.text)


def update_status(api_request):
    '''Returns current tasks\'s status'''
    url = '{host}/management/v1/counter/{counter_id}/logrequest/{request_id}?oauth_token={token}' \
        .format(request_id=api_request.request_id,
                counter_id=api_request.user_request.counter_id,
                token=api_request.user_request.token,
                host=HOST)

    r = requests.get(url)
    logger.debug(r.text)
    if r.status_code == 200:
        status = json.loads(r.text)['log_request']['status']
        api_request.status = status
        if status == 'processed':
            size = len(json.loads(r.text)['log_request']['parts'])
            api_request.size = size
        return api_request
    else:
        raise ValueError(r.text)


def save_data(api_request, part):
    '''Loads data chunk from Logs API and saves to ClickHouse'''
    url = '{host}/management/v1/counter/{counter_id}/logrequest/{request_id}/part/{part}/download?oauth_token={token}' \
        .format(
            host=HOST,
            counter_id=api_request.user_request.counter_id,
            request_id=api_request.request_id,
            part=part,
            token=api_request.user_request.token
        )

    r = requests.get(url)
    if r.status_code != 200:
        logger.debug(r.text)
        raise ValueError(r.text)

    logger.info('\n'.join(r.text.split('\n')[:5]))

    splitted_text = r.text.split('\n')
    headers_num = len(splitted_text[0].split('\t'))
    splitted_text_filtered = filter(lambda x: len(x.split('\t')) == headers_num, r.text.split('\n'))
    num_filtered = len(splitted_text) - len(splitted_text_filtered)
    if num_filtered != 0:
        logger.warning('%d rows were filtered out')

    df = pd.read_csv(StringIO.StringIO(r.text), sep='\t')

    clickhouse.save_data(api_request.user_request.source,
                         api_request.user_request.fields,
                         df)

    api_request.status = 'saved'


def clean_data(api_request):
    '''Cleans generated data on server'''
    url = '{host}/management/v1/counter/{counter_id}/logrequest/{request_id}/clean?oauth_token={token}' \
        .format(host=HOST,
                counter_id=api_request.user_request.counter_id,
                token=api_request.user_request.token,
                request_id=api_request.request_id)
    r = requests.post(url)
    logger.debug(r.text)
    if r.status_code != 200:
        raise ValueError(r.text)

    api_request.status = json.loads(r.text)['log_request']['status']
    return json.loads(r.text)['log_request']
