import logging
import pyodbc
import gzip
import utils

config = utils.get_config()
VT_HOST = config['vertica']['host']
VT_USER = config['vertica']['user']
VT_PASSWORD = config['vertica']['password']
VT_VISITS_TABLE = config['vertica']['visits_table']
VT_HITS_TABLE = config['vertica']['hits_table']
VT_DATABASE = config['vertica']['database']

logger = logging.getLogger('logs_api')


def get_message(name):
    """Returns errors and warning string"""
    if name == 'connect_error':
        return 'Unable to connect to Vertica:\n\tHOST={server},\n\tDATABASE={db},\n\tUSER={user}' \
            .format(server=VT_HOST, db=VT_DATABASE, user=VT_USER)
    elif name == 'close_warning':
        return 'Unable to close the connection to Vertica:\n\tHOST={server},\n\tDATABASE={db},\n\tUSER={user}' \
            .format(server=VT_HOST, db=VT_DATABASE, user=VT_USER)
    else:
        raise ValueError('Wrong argument: ' + name)


def get_cursor():
    connection_string = 'Driver=Vertica;Servername={server};Port=5433;Database={db};UserName={user};Password={psw}' \
        .format(server=VT_HOST, db=VT_DATABASE, user=VT_USER, psw=VT_PASSWORD)
    try:
        con = pyodbc.connect(connection_string)
        cursor = con.cursor()
    except Exception as e:
        logger.critical(get_message('connect_error'))
        raise e
    return cursor, con


def get_data(cursor, query):
    """Returns Vertica response"""
    logger.debug(query)
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
    except Exception as e:
        logger.critical(get_message('connect_error'))
        raise e
    return rows


def upload(cursor, table, content):
    """Uploads data to table in Vertica"""
    filename = 'content.tsv.gz'
    with gzip.open('content.tsv.gz', 'w') as log:
        log.write(bytes(content.encode(encoding='utf8')))
    query = """COPY {table} FROM LOCAL '{file}' GZIP DELIMITER E'\t';""".format(table=table, file=filename)
    try:
        cursor.execute(query)
    except Exception as e:
        logger.critical("Unable to COPY FROM LOCAL FILE '{file}' TO TABLE {table}".format(file=filename, table=table))
        raise e


def get_source_table_name(source):
    """Returns table name in database"""
    if source == 'hits':
        return VT_HITS_TABLE
    if source == 'visits':
        return VT_VISITS_TABLE


def get_tables(cursor):
    """Returns list of tables in a database"""
    rows = get_data(cursor, """SELECT table_schema, table_name from TABLES""")
    result = []
    for r in rows:
        result.append('.'.join(r).lower())
    return result


def is_table_present(cursor, source):
    """Returns whether table for data is already present in database"""
    return get_source_table_name(source) in get_tables(cursor)


def get_vt_field_name(field_name):
    """Converts Logs API parameter name to Vertica column name"""
    prefixes = ['ym:s:', 'ym:pv:']
    for prefix in prefixes:
        field_name = field_name.replace(prefix, '')
    return utils.camel_to_snake(field_name)


def drop_table(cursor, source):
    """Drops table in Vertica"""
    table_name = get_source_table_name(source)
    query = 'DROP TABLE IF EXISTS {table};'.format(table=table_name)
    try:
        cursor.execute(query)
    except Exception as e:
        logger.critical('Unable to DROP table ' + table_name)
        raise e


def create_table(cursor, source, fields):
    """Creates table in Vertica for hits/visits with particular fields"""
    tmpl = '''
        CREATE TABLE {table_name} AS (
            {fields}
        ) ORDER BY {order_clause}
          SEGMENTED BY HASH({segmentation_clause}) ALL NODES;
    '''
    field_tmpl = '{name} {type}'
    field_statements = []

    table_name = get_source_table_name(source)

    vt_field_types = utils.get_fields_config('vertica')
    vt_fields = map(get_vt_field_name, fields)

    order_clause = ', '.join(vt_fields[:5])
    segmentation_clause = ', '.join(vt_fields[:3])

    for i in range(len(fields)):
        field_statements.append(field_tmpl.format(name=vt_fields[i],
                                                  type=vt_field_types[fields[i]]))

    query = tmpl.format(table_name=table_name,
                        order_clause=order_clause,
                        segmentation_clause=segmentation_clause,
                        fields=',\n'.join(field_statements))

    try:
        cursor.execute(query)
    except Exception as e:
        logger.critical('Unable to CREATE table ' + table_name)
        raise e


def save_data(source, fields, data):
    """Inserts data into Vertica table"""
    cursor, con = get_cursor()

    if not is_table_present(cursor, source):
        create_table(cursor, source, fields)

    upload(cursor, get_source_table_name(source), data)

    try:
        con.close()
    except Exception as e:
        logger.warning(get_message('close_warning'))


def is_data_present(start_date_str, end_date_str, source):
    """Returns whether there is a records in database for particular date range and source"""
    cursor, con = get_cursor()

    if not is_table_present(cursor, source):
        return False

    table_name = get_source_table_name(source)
    query = '''
        SELECT count(*) cnt
        FROM {table}
        WHERE date between '{start_date}' AND '{end_date}';
    '''.format(table=table_name, start_date=start_date_str, end_date=end_date_str)

    rows = get_data(cursor, query)

    try:
        con.close()
    except Exception as e:
        logger.warning(get_message('close_warning'))

    return rows[0][0] > 0

