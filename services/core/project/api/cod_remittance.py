import psycopg2, requests, os, json
import logging
from datetime import datetime, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)
"""
host = os.environ('DTATBASE_HOST')
database = os.environ('DTATBASE_NAME')
user = os.environ('DTATBASE_USER')
password = os.environ('DTATBASE_PASSWORD')
conn = psycopg2.connect(host=host, database=database, user=user, password=password)
"""
conn = psycopg2.connect(host="wareiq-core-prod2.cvqssxsqruyc.us-east-1.rds.amazonaws.com", database="core_prod", user="postgres", password="aSderRFgd23")


def lambda_handler():
    cur = conn.cursor()

    cur.execute("select distinct(client_prefix) FROM orders aa order by client_prefix")
    all_clients = cur.fetchall()
    insert_tuple = list()
    insert_value_str = ""
    remittance_date = datetime.utcnow() + timedelta(hours=5.5) + timedelta(days=8)
    for client in all_clients:
        remittance_id = client[0] + "_" + str(remittance_date.date())
        last_remittance_id = client[0] + "_" + str((remittance_date-timedelta(days=7)).date())
        cur.execute("SELECT * from cod_remittance WHERE remittance_id=%s", (last_remittance_id,))
        try:
            cur.fetchone()[0]
        except Exception as e:
            insert_tuple.append(
                (client[0], remittance_id, remittance_date-timedelta(days=7), 'processing', datetime.utcnow() + timedelta(hours=5.5)))
            insert_value_str += "%s,"
        insert_tuple.append((client[0], remittance_id, remittance_date, 'processing', datetime.utcnow()+timedelta(hours=5.5)))
        insert_value_str += "%s,"

    insert_value_str = insert_value_str.rstrip(",")

    cur.execute("INSERT INTO cod_remittance (client_prefix, remittance_id, remittance_date, status, date_created) VALUES __IVS__;".replace('__IVS__', insert_value_str), tuple(insert_tuple))

    conn.commit()