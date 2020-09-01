import os
import psycopg2
from functools import lru_cache


class DbConnection:

    def __init__(self):
        pass

    @staticmethod
    @lru_cache(maxsize=1)
    def get_db_connection_instance():
        return psycopg2.connect(host=os.environ.get('DATABASE_HOST'), database=os.environ.get('DATABASE_NAME'),
                                user=os.environ.get('DATABASE_USER'), password=os.environ.get('DATABASE_PASSWORD'))

    @staticmethod
    @lru_cache(maxsize=1)
    def get_pincode_db_connection_instance():
        return psycopg2.connect(host=os.environ.get('DATABASE_HOST_PINCODE'), database=os.environ.get('DATABASE_NAME'),
                                user=os.environ.get('DATABASE_USER'), password=os.environ.get('DATABASE_PASSWORD'))