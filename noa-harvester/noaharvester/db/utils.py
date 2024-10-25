from configparser import ConfigParser
import psycopg

def load_config(filename='database.ini', section='postgresql-sentinel-status'):
    # TODO: has default section? Does check or not?
    parser = ConfigParser()
    parser.read(filename)

    # get section, default to postgresql
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return config


def connect(config):
    """ Connect to the PostgreSQL database server """
    with psycopg.connect(config) as conn:
        print('Connected to the PostgreSQL server.')
        with conn.cursor() as curs:
            curs.execute("SQL1")
