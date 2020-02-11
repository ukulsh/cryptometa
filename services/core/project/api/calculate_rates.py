import psycopg2, requests, os, json
import logging
from math import ceil
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
conn_2 = psycopg2.connect(host="wareiq-core-prod.cvqssxsqruyc.us-east-1.rds.amazonaws.com", database="core_prod", user="postgres", password="aSderRFgd23")

def lambda_handler():
    cur = conn.cursor()
    cur_2 =conn_2.cursor()

    current_time = datetime.now() - timedelta(days=1)
    current_time = current_time.strftime('%Y-%m-%d')
    cur.execute(select_orders_to_calculate_query.replace('__STATUS_TIME__', current_time))
    all_orders=cur.fetchall()

    for order in all_orders:
        try:
            courier_id = order[2]
            if courier_id==8:
                courier_id=1

            cur_2.execute("SELECT city from city_pin_mapping where pincode='%s';"%order[7])
            pickup_city = cur_2.fetchone()
            if not pickup_city:
                logger.info("pickup city not found: " + str(order[0]))
                continue
            pickup_city = pickup_city[0]
            cur_2.execute("SELECT city from city_pin_mapping where pincode='%s';" % order[8])
            deliver_city = cur_2.fetchone()
            if not deliver_city:
                logger.info("deliver city not found: " + str(order[0]))
                continue
            deliver_city = deliver_city[0]
            zone_select_tuple = (pickup_city, deliver_city, courier_id)
            cur_2.execute("SELECT zone_value from city_zone_mapping where zone=%s and city=%s and courier_id=%s;", zone_select_tuple)
            delivery_zone = cur_2.fetchone()
            if not delivery_zone:
                logger.info("deliver zone not found: " + str(order[0]))
                continue
            delivery_zone = delivery_zone[0]

            if delivery_zone in ('D1', 'D2'):
                delivery_zone='D'
            if delivery_zone in ('C1', 'C2'):
                delivery_zone='C'

            charged_weight=order[4] if order[4] else 0
            if order[3] and order[3]>charged_weight:
                charged_weight=order[3]

            if not charged_weight:
                logger.info("charged weight not found: " + str(order[0]))
                continue

            cost_select_tuple = (order[6], order[2])
            cur.execute("SELECT __ZONE__, cod_min, cod_ratio, rto_ratio from cost_to_clients WHERE client_prefix=%s and courier_id=%s;".replace(
                '__ZONE__', zone_column_mapping[delivery_zone]), cost_select_tuple)
            charge_rate_values = cur.fetchone()
            if not charge_rate_values:
                logger.info("charge_rate_values not found: " + str(order[0]))
                continue

            charge_rate = charge_rate_values[0]

            multiple = ceil(charged_weight/0.5)

            forward_charge = charge_rate*multiple
            forward_charge_gst = forward_charge*1.18

            rto_charge = 0
            rto_charge_gst = 0
            cod_charge = 0
            cod_charged_gst = 0
            if order[13] == 'RTO':
                rto_charge = forward_charge*charge_rate_values[3]
                rto_charge_gst = forward_charge_gst*charge_rate_values[3]
            else:
                if order[11] and order[11].lower() == 'cod':
                    if order[12]:
                        cod_charge = order[12]*(charge_rate_values[2]/100)
                        if charge_rate_values[1]>cod_charge:
                            cod_charge = charge_rate_values[1]
                    else:
                        cod_charge = charge_rate_values[1]

                    cod_charged_gst = cod_charge*1.18

            if order[9]:
                deduction_time=order[9]
            elif order[10]:
                deduction_time=order[10]
            else:
                deduction_time=datetime.now()

            total_charge = forward_charge+cod_charge+rto_charge
            total_charge_gst = forward_charge_gst+rto_charge_gst+cod_charged_gst
            insert_rates_tuple = (charged_weight, delivery_zone, deduction_time, cod_charge, cod_charged_gst,
                                  forward_charge, forward_charge_gst,rto_charge,rto_charge_gst,order[0],
                                  total_charge,total_charge_gst,datetime.now(),datetime.now())

            cur.execute(insert_into_deduction_query, insert_rates_tuple)

            conn.commit()
        except Exception as e:
            logger.error("couldn't calculate order: " + str(order[0]) + "\nError: " + str(e))

    cur.close()
    cur_2.close()


zone_column_mapping = {
                       'A': 'zone_a',
                       'B': 'zone_b',
                       'C': 'zone_c',
                       'D': 'zone_d',
                       'E': 'zone_e',
                       }