import os
import psycopg2
from functools import lru_cache
from hashids import Hashids
from datetime import datetime

hashids = Hashids(min_length=6, salt="thoda namak shamak daalte hai")


class DbConnection:
    def __init__(self):
        pass

    @staticmethod
    @lru_cache(maxsize=1)
    def get_db_connection_instance():
        return psycopg2.connect(
            host=os.environ.get("DATABASE_HOST"),
            database=os.environ.get("DATABASE_NAME"),
            user=os.environ.get("DATABASE_USER"),
            password=os.environ.get("DATABASE_PASSWORD"),
        )

    @staticmethod
    @lru_cache(maxsize=1)
    def get_pincode_db_connection_instance():
        return psycopg2.connect(
            host=os.environ.get("DATABASE_HOST_PINCODE"),
            database=os.environ.get("DATABASE_NAME"),
            user=os.environ.get("DATABASE_USER"),
            password=os.environ.get("DATABASE_PASSWORD"),
        )

    @staticmethod
    @lru_cache(maxsize=1)
    def get_users_db_connection_instance():
        return psycopg2.connect(
            host=os.environ.get("DATABASE_HOST"),
            database=os.environ.get("DATABASE_NAME_USER"),
            user=os.environ.get("DATABASE_USER"),
            password=os.environ.get("DATABASE_PASSWORD"),
        )


class UrlShortner:
    def __init__(self):
        pass

    @staticmethod
    def get_short_url(url, cur=None):
        try:
            conn = None
            if not cur:
                conn = DbConnection.get_db_connection_instance()
                cur = conn.cursor()
            cur.execute(
                """INSERT INTO url_shortner (url, clicks, date_created) VALUES (%s, %s, %s) 
                            ON CONFLICT (url) DO UPDATE SET url = excluded.url RETURNING id;""",
                (url, 0, datetime.utcnow()),
            )
            url_id = cur.fetchone()[0]
            hash_id = hashids.encode(url_id)
            if conn:
                conn.commit()
                conn.close()
            return "https://wiq.app/" + hash_id
        except Exception:
            return url
