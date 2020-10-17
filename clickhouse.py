from __future__ import (absolute_import, division,
                        print_function, unicode_literals)

import requests
import urllib
import urllib3
import utils
import sys
import logging

config = utils.get_config()
CH_HOST = config['clickhouse']['host']
CH_USER = config['clickhouse']['user']
CH_PASSWORD = config['clickhouse']['password']
CH_VISITS_TABLE = config['clickhouse']['visits_table']
CH_HITS_TABLE = config['clickhouse']['hits_table']
CH_DATABASE = config['clickhouse']['database']
SSL_VERIFY = (config['disable_ssl_verification_for_clickhouse'] == 0)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger('logs_api')

def get_clickhouse_data(query, host=CH_HOST):
    '''Returns ClickHouse response'''
    logger.debug(query)
    if (CH_USER == '') and (CH_PASSWORD == ''):
        r = requests.post(host, data=query, verify=SSL_VERIFY)
    else:
        r = requests.post(host, data=query, auth=(CH_USER, CH_PASSWORD), verify=SSL_VERIFY)
    if r.status_code == 200:
        return r.text
    else:
        raise ValueError(r.text)


def upload(table, content, host=CH_HOST):
    '''Uploads data to table in ClickHous'''

    # with open('content.txt', 'w') as the_file:
    #     the_file.write(content.encode('utf8'))

    insert_query = get_insert_query(table, content)

    # with open('insert_query.txt', 'w') as the_file:
    #     the_file.write(insert_query)

    if (CH_USER == '') and (CH_PASSWORD == ''):
        r = requests.post(host, data=insert_query, verify=SSL_VERIFY)
    else:
        r = requests.post(host, data=insert_query,
                          auth=(CH_USER, CH_PASSWORD), verify=SSL_VERIFY)
    result = r.text
    if r.status_code == 200:
        return result
    else:
        raise ValueError(r.text)


def get_insert_query(table, content):
    '''Convert content to Insert Query'''
    insert_template_template = 'INSERT INTO {table} ({fields}) VALUES {data}'
    content_lines = content.split("\n")
    dimension_list = content_lines[0].split()
    field_type_list = get_field_type_list(dimension_list)

    column_list = list(map(get_ch_field_name, dimension_list))
    columns = ', '.join(column_list)
    body_lines = content_lines[1:]
    rows = []
    for body_line in body_lines:
        row_values = body_line.split("\t")
        row_values_with_quotes = prepare_row_for_insert(row_values, field_type_list)
        row = '(' + ', '.join(row_values_with_quotes) + ')'
        rows.append(row)
    data = ', '.join(rows)

    return insert_template_template.format(table=table, fields=columns, data=data).encode('utf-8')


def prepare_row_for_insert(row_values, field_type_list):
    row_values_quoted = []
    for row_position, row_value in enumerate(row_values):
        field_type = field_type_list[row_position]
        row_value_quoted = row_value
        if field_type.startswith('Array'):
            row_value_quoted = prepare_array_for_insert(row_value)
        else:
            row_value_quoted = "'" + row_value + "'"
        row_values_quoted.append(row_value_quoted)
    return row_values_quoted


def prepare_array_for_insert(string):
    string_without_brackets = string.strip("'[]")
    if string_without_brackets != '':
        string_list = string_without_brackets.split("','")
        return "[" + ",".join(list(map(prepare_array_element, string_list))) + "]"
    return string


def prepare_array_element(string):
    return "'" + addslashes(string) + "'"


def addslashes(string):
    replace = ["\\", '"', "'", "\0", ]
    for i in replace:
        if i in string:
            string = string.replace(i, '\\' + i)
    return string


def get_field_type_list(dimension_list):
    field_type_list = []
    dimension_to_field_type = utils.get_ch_fields_config()
    for dimension in dimension_list:
        field_type_list.append(dimension_to_field_type[dimension])
    return field_type_list


def get_source_table_name(source, with_db=True):
    '''Returns table name in database'''
    if source == 'hits':
        if with_db:
            return '{db}.{table}'.format(db=CH_DATABASE, table=CH_HITS_TABLE)
        else:
            return CH_HITS_TABLE
    if source == 'visits':
        if with_db:
            return '{db}.{table}'.format(db=CH_DATABASE, table=CH_VISITS_TABLE)
        else:
            return CH_VISITS_TABLE


def get_tables():
    '''Returns list of tables in database'''
    return get_clickhouse_data('SHOW TABLES FROM {db}'.format(db=CH_DATABASE))\
        .strip().split('\n')

def get_dbs():
    ''''Returns list of databases'''
    return get_clickhouse_data('SHOW DATABASES')\
        .strip().split('\n')


def is_table_present(source):
    '''Returns whether table for data is already present in database'''
    return get_source_table_name(source, with_db=False) in get_tables()

def is_db_present():
    '''Returns whether a database is already present in clickhouse'''
    return CH_DATABASE in get_dbs()

def create_db():
    '''Creates database in clickhouse'''
    return get_clickhouse_data('CREATE DATABASE {db}'.format(db=CH_DATABASE))


def get_ch_field_name(field_name):
    '''Converts Logs API parameter name to ClickHouse column name'''
    prefixes = ['ym:s:', 'ym:pv:']
    for prefix in prefixes:
        field_name = field_name.replace(prefix, '')
    return field_name[0].upper() + field_name[1:]


def drop_table(source):
    '''Drops table in ClickHouse'''
    query = 'DROP TABLE IF EXISTS {table}'.format(
        table=get_source_table_name(source))
    get_clickhouse_data(query)


def create_table(source, fields):
    '''Creates table in ClickHouse for hits/visits with particular fields'''
    tmpl = '''
        CREATE TABLE {table_name} (
            {fields}
        ) ENGINE = {engine}
    '''
    field_tmpl = '{name} {type}'
    field_statements = []

    table_name = get_source_table_name(source)
    if source == 'hits':
        if ('ym:pv:date' in fields) and ('ym:pv:clientID' in fields):
            engine = 'MergeTree(Date, intHash32(ClientID), (Date, intHash32(ClientID)), 8192)'
        else:
            engine = 'Log'

    if source == 'visits':
        if ('ym:s:date' in fields) and ('ym:s:clientID' in fields):
            engine = 'MergeTree(Date, intHash32(ClientID), (Date, intHash32(ClientID)), 8192)'
        else:
            engine = 'Log'

    ch_field_types = utils.get_ch_fields_config()
    ch_fields = map(get_ch_field_name, fields)
    
    for i in range(len(fields)):
        field_statements.append(field_tmpl.format(name= ch_fields[i],
            type=ch_field_types[fields[i]]))
    
    field_statements = sorted(field_statements)
    query = tmpl.format(table_name=table_name,
                        engine=engine,
                        fields=',\n'.join(sorted(field_statements)))

    get_clickhouse_data(query)


def save_data(source, fields, data):
    '''Inserts data into ClickHouse table'''

    if not is_db_present():
        logger.info('Database created')
        create_db()

    if not is_table_present(source):
        logger.info('Table created')
        create_table(source, fields)

    upload(get_source_table_name(source), data)


def is_data_present(start_date_str, end_date_str, source):
    '''Returns whether there is a records in database for particular date range and source'''
    if not is_db_present():
        return False

    if not is_table_present(source):
        return False

    table_name = get_source_table_name(source)
    query = '''
        SELECT count()
        FROM {table}
        WHERE Date >= '{start_date}' AND Date <= '{end_date}'
    '''.format(table=table_name,
               start_date=start_date_str,
               end_date=end_date_str)

    visits = get_clickhouse_data(query, CH_HOST)
    is_null = (visits == '') or (visits.strip() == '0')
    return not is_null

