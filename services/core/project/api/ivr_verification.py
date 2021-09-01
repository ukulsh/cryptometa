import psycopg2, requests, os, json
import logging
from datetime import datetime, timedelta
from .queries import *

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
    current_time = datetime.now() - timedelta(days=1)
    current_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
    cur.execute(get_details_cod_verify_ivr.replace('__ORDER_TIME__', current_time))
    all_orders = cur.fetchall()

    for order in all_orders:
        try:
            customer_phone = str(order[1]).replace(" ", "")
            customer_phone = "0" + customer_phone[-10:]
            data = {
                'From': customer_phone,
                'CallerId': '01141182252',
                'Url': 'http://my.exotel.com/wareiq1/exoml/start_voice/262896',
                'CustomField': str(order[0])
            }
            req = requests.post(
                'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Calls/connect',
                data=data)
        except Exception as e:
            logger.error("Call unsuccessful for "+str(order[0])+"\nError: "+str(e.args[0]))

    cur.execute(get_details_ndr_verify_ivr.replace('__ORDER_TIME__', current_time))
    all_orders = cur.fetchall()

    for order in all_orders:
        try:
            customer_phone = str(order[1]).replace(" ", "")
            customer_phone = "0" + customer_phone[-10:]
            data = {
                'From': customer_phone,
                'CallerId': '01141182252',
                'Url': 'http://my.exotel.com/wareiq1/exoml/start_voice/276525',
                'CustomField': str(order[0])
            }
            req = requests.post(
                'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Calls/connect',
                data=data)
        except Exception as e:
            logger.error("Call unsuccessful for " + str(order[0]) + "\nError: " + str(e.args[0]))
