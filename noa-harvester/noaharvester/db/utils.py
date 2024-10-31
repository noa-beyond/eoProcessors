"""DB utility functions for postgre interfacing"""

import os
from pathlib import Path
from configparser import ConfigParser
import psycopg
# pylint: disable=E1129 # False-positive for psycopg connection context manager
# TODO: after integration tests, remove helper functions and make
# table specific functions generic (by posting table name also)


def get_local_config(filename="database.ini", section="sentinel_status"):
    """
    Gets local config, found by default in database.ini
     of the same path as utils
    """
    # TODO: has default section? Does check or not?
    parser = ConfigParser()
    config_path = str(Path(Path(__file__).parent, filename).resolve())
    parser.read(config_path)

    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = str(param[1])
    else:
        raise RuntimeError(f"Section {section} not found in the {filename} file")

    return config


def get_env_config():
    """Get db configuration from env variables: proper way in Kubernetes ecosystem"""
    # TODO make checks
    # TODO check if other db access is needed: (like "section" in local config)
    config = {}
    config["user"] = os.getenv("DB_USER")
    config["password"] = os.getenv("DB_PASSWORD")
    config["host"] = os.getenv("DB_HOST")
    config["port"] = os.getenv("DB_PORT")
    config["dbname"] = os.getenv("DB_NAME")
    if not all(config.values()):
        return None
    return config


def describe_table(config, table):
    """Helper to get all columns of a table"""
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position;
        """

    with psycopg.connect(**config) as conn:
        with conn.cursor() as curs:
            curs.execute(sql, (table,))
            columns = [row[0] for row in curs.fetchall()]
            print("Column names:", columns)


def query_uuid(config, uuid):
    """Get specific uuid from products"""
    sql = """
        SELECT *
        FROM products
        where products.id=%s
        """

    with psycopg.connect(**config) as conn:
        with conn.cursor() as curs:
            curs.execute(sql, (uuid,))
            # It should be one...
            return curs.fetchone()


def update_uuid(config, table, uuid, column, value):
    """Update by uuid"""
    sql = f"""
        UPDATE {table}
        SET {column} = %s
        WHERE products.id = %s;
    """
    if column == "id":
        # Do not explain
        return False

    with psycopg.connect(**config) as conn:
        with conn.cursor() as curs:
            curs.execute(sql, (value, uuid))
            conn.commit()

            # If updated succesfully, return True
            if curs.rowcount > 0:
                return True
            return False


def query_all_items(config):
    """Helper to get all items from products table"""
    sql = """
        SELECT *
        FROM products
    """

    with psycopg.connect(**config) as conn:
        print("Connected to the PostgreSQL server.")
        with conn.cursor() as curs:
            print("Querying")
            curs.execute(sql)
            results = curs.fetchall()
            for row in results:
                print(row)
