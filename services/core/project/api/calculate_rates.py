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
            if courier_id in (4,8,11,12,13):
                courier_id=1

            if courier_id in (5,):
                courier_id=2

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
            if not delivery_zone:
                logger.info("deliver zone not found: " + str(order[0]))
                continue

            calculate_courier_cost(cur, delivery_zone, order)

            if delivery_zone in ('D1', 'D2'):
                delivery_zone='D'
            if delivery_zone in ('C1', 'C2'):
                delivery_zone='C'

            charged_weight=order[4] if order[4] else 0
            if order[6] != 'NASHER':
                if order[3] and order[3]>charged_weight:
                    charged_weight=order[3]
            else:
                if courier_id==1:
                    volumetric_weight = (order[14]['length']*order[14]['breadth']*order[14]['height'])/4500
                else:
                    volumetric_weight = (order[14]['length']*order[14]['breadth']*order[14]['height'])/5000
                if volumetric_weight>charged_weight:
                    charged_weight = volumetric_weight

            if not charged_weight:
                logger.info("charged weight not found: " + str(order[0]))
                continue

            try:
                if order[6] != 'NASHER' or (order[6] == 'NASHER' and charged_weight<10.0):
                    cost_select_tuple = (order[6], order[2])
                    cur.execute("SELECT __ZONE__, cod_min, cod_ratio, rto_ratio from cost_to_clients WHERE client_prefix=%s and courier_id=%s;".replace(
                        '__ZONE__', zone_column_mapping[delivery_zone]), cost_select_tuple)
                    charge_rate_values = cur.fetchone()
                    if not charge_rate_values:
                        cur.execute(
                            """INSERT INTO client_deductions (weight_charged, zone, shipment_id) VALUES (%s,%s,%s) RETURNING id;""",
                            (charged_weight, delivery_zone, order[0]))

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
                else:
                    charge_rate_values = (None, 32, 1.5, 1)
                    intial_charge = nasher_zonal_mapping[delivery_zone][0]
                    next_weight = charged_weight-10.0
                    charge_rate = nasher_zonal_mapping[delivery_zone][1]
                    multiple = ceil(next_weight / 1.0)

                    forward_charge = charge_rate * multiple + intial_charge
                    forward_charge_gst = forward_charge * 1.18

                    rto_charge = 0
                    rto_charge_gst = 0
                    cod_charge = 0
                    cod_charged_gst = 0
                    if order[13] == 'RTO':
                        rto_charge = forward_charge * charge_rate_values[3]
                        rto_charge_gst = forward_charge_gst * charge_rate_values[3]
                    else:
                        if order[11] and order[11].lower() == 'cod':
                            if order[12]:
                                cod_charge = order[12] * (charge_rate_values[2] / 100)
                                if charge_rate_values[1] > cod_charge:
                                    cod_charge = charge_rate_values[1]
                            else:
                                cod_charge = charge_rate_values[1]

                            cod_charged_gst = cod_charge * 1.18

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

            except Exception as e:
                logger.error("couldn't calculate order: " + str(order[0]) + "\nError: " + str(e))
                cur.execute(
                    """INSERT INTO client_deductions (weight_charged, zone, shipment_id) VALUES (%s,%s,%s) RETURNING id;""",
                    (charged_weight, delivery_zone, order[0]))
                continue
            conn.commit()
        except Exception as e:
            logger.error("couldn't calculate order: " + str(order[0]) + "\nError: " + str(e))

    ndr_push_reattempts(cur)
    conn.commit()
    cur.close()
    cur_2.close()


def calculate_courier_cost(cur, delivery_zone, order):
    try:
        charged_weight = order[4] if order[4] else 0
        volumetric_weight = (order[14]['length'] * order[14]['breadth'] * order[14]['height']) / 5000
        if volumetric_weight > charged_weight:
            charged_weight = volumetric_weight

        cost_select_tuple = (order[2], )
        cur.execute(
            "SELECT __ZONE__, __ZONE___add, cod_min, cod_ratio, rto_ratio, first_step, next_step from courier_costs WHERE courier_id=%s;".replace(
                '__ZONE__', zone_column_mapping_courier[delivery_zone]), cost_select_tuple)
        charge_rate_values = cur.fetchone()
        if not charge_rate_values:
            logger.info("courier cost not found: " + str(order[0]))
            return None

        if order[2] != 8:
            first_step_cost = charge_rate_values[0]
            next_step_cost = 0
            if (charged_weight - charge_rate_values[5])>0:
                next_step_cost = ceil((charged_weight - charge_rate_values[5])/charge_rate_values[6])*charge_rate_values[1]

            forward_charge = first_step_cost + next_step_cost

            rto_charge = 0
            cod_charge = 0
            if order[13] == 'RTO':
                if order[2] not in (11,12):
                    rto_charge = forward_charge * charge_rate_values[4]
                else:
                    rto_multiple = ceil(charged_weight)
                    if order[2] == 11:
                        rto_charge = rto_multiple*rto_heavy_2[delivery_zone]
                    else:
                        rto_charge = rto_multiple*rto_heavy_1[delivery_zone]

            else:
                if order[11] and order[11].lower() == 'cod':
                    if order[12]:
                        cod_charge = order[12] * (charge_rate_values[3] / 100)
                        if charge_rate_values[2] > cod_charge:
                            cod_charge = charge_rate_values[2]
                    else:
                        cod_charge = charge_rate_values[2]
        else:
            first_step_cost = charge_rate_values[0]
            next_step_cost = 0
            if (charged_weight - charge_rate_values[5]) > 3:
                second_step_cost = 3*bulk_second_step[delivery_zone]
                next_step_cost = ceil((charged_weight - 5) / charge_rate_values[6]) * \
                                 charge_rate_values[1]
            else:
                second_step_cost = ceil((charged_weight - charge_rate_values[5]))*bulk_second_step[delivery_zone]

            forward_charge = first_step_cost + second_step_cost+ next_step_cost

            rto_charge = 0
            cod_charge = 0
            if order[13] == 'RTO':
                rto_multiple = ceil(charged_weight)
                rto_charge = rto_multiple * rto_bulk[delivery_zone]
            else:
                if order[11] and order[11].lower() == 'cod':
                    if order[12]:
                        cod_charge = order[12] * (charge_rate_values[3] / 100)
                        if charge_rate_values[2] > cod_charge:
                            cod_charge = charge_rate_values[2]
                    else:
                        cod_charge = charge_rate_values[2]

        total_charge = forward_charge+rto_charge+cod_charge

        if order[9]:
            deduction_time=order[9]
        elif order[10]:
            deduction_time=order[10]
        else:
            deduction_time=datetime.now()

        insert_rates_tuple = (charged_weight, delivery_zone, deduction_time, cod_charge, forward_charge, rto_charge, order[0],
                              total_charge, datetime.now(), datetime.now())

        cur.execute(insert_into_courier_cost_query, insert_rates_tuple)
        conn.commit()

    except Exception as e:
        logger.error("couldn't calculate courier cost order: " + str(order[0]) + "\nError: " + str(e))


zone_column_mapping = {
                       'A': 'zone_a',
                       'B': 'zone_b',
                       'C': 'zone_c',
                       'D': 'zone_d',
                       'E': 'zone_e',
                       }

zone_column_mapping_courier = {
                       'A': 'zone_a',
                       'B': 'zone_b',
                       'C1': 'zone_c1',
                       'C': 'zone_c1',
                       'C2': 'zone_c2',
                       'D1': 'zone_d1',
                       'D': 'zone_d1',
                       'D2': 'zone_d2',
                       'E': 'zone_e',
                       }

nasher_zonal_mapping = {
                       'A': [74, 9],
                       'B': [107, 13],
                       'C': [126, 15],
                       'D': [144, 17],
                       'E': [149, 17],
                       }

rto_heavy_1 = {
               'A': 11,
               'B': 12,
               'C1': 15,
               'C': 15,
               'C2': 15,
               'D1': 18,
               'D': 18,
               'D2': 18,
               'E': 21,
               }

rto_heavy_2 = {
               'A': 10,
               'B': 12,
               'C1': 17,
               'C': 17,
               'C2': 17,
               'D1': 18,
               'D': 18,
               'D2': 18,
               'E': 22,
               }

rto_bulk = {
               'A': 20,
               'B': 23,
               'C1': 24,
               'C': 24,
               'C2': 26,
               'D1': 28,
               'D': 28,
               'D2': 30,
               'E': 34,
               }

bulk_second_step = {
               'A': 26,
               'B': 28,
               'C1': 30,
               'C': 30,
               'C2': 32,
               'D1': 36,
               'D': 36,
               'D2': 38,
               'E': 52,
               }


def ndr_push_reattempts(cur):
    time_after = datetime.utcnow() - timedelta(days=2, hours=5.5)
    cur.execute("""select cc.awb, dd.id, dd.api_key from ndr_verification aa
                    left join orders bb on aa.order_id=bb.id
                    left join shipments cc on cc.order_id=bb.id
                    left join master_couriers dd on cc.courier_id=dd.id
                    where aa.ndr_verified=false
                    and aa.verification_time>%s""", (time_after,))

    all_orders = cur.fetchall()
    for order in all_orders:
        try:
            if order[1] in (1, 2, 8, 11, 12):  # Delhivery
                headers = {"Authorization": "Token " + order[2],
                           "Content-Type": "application/json"}
                delhivery_url = "https://track.delhivery.com/api/p/update"
                delivery_shipments_body = json.dumps({"data": [{"waybill": order[0],
                                                                "act": "RE-ATTEMPT"}]})

                req = requests.post(delhivery_url, headers=headers, data=delivery_shipments_body)
            elif order[1] in (5, 13):  # Xpressbees
                headers = {"Content-Type": "application/json",
                           "XBKey": order[2]}
                body = {"ShippingID": order[0]}
                xpress_url = "http://xbclientapi.xbees.in/POSTShipmentService.svc/UpdateNDRDeferredDeliveryDate"
                req = requests.post(xpress_url, headers=headers, data=json.dumps(body))
        except Exception as e:
            logger.error("NDR push failed for: " + order[0])