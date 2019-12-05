import psycopg2, requests, os, json, pytz
from datetime import datetime

from .queries import *
"""
host = os.environ('DTATBASE_HOST')
database = os.environ('DTATBASE_NAME')
user = os.environ('DTATBASE_USER')
password = os.environ('DTATBASE_PASSWORD')
conn = psycopg2.connect(host=host, database=database, user=user, password=password)
"""
conn = psycopg2.connect(host="wareiq-core-prod.cvqssxsqruyc.us-east-1.rds.amazonaws.com", database="core_prod", user="postgres", password="postgres")

def lambda_handler():
    cur = conn.cursor()
    cur.execute(fetch_client_couriers_query)
    for courier in cur.fetchall():
        get_orders_data_tuple = (courier[4], courier[1], courier[1], courier[4])
        cur.execute(get_orders_to_ship_query, get_orders_data_tuple)
        shipments = list()
        all_orders = cur.fetchall()
        last_shipped_order_id = 0
        headers = {"Authorization": "Token " + courier[14],
                   "Content-Type": "application/json"}
        for order in all_orders:
            if order[0]>last_shipped_order_id:
                last_shipped_order_id = order[0]
            try:
                #check pincode serviceability
                check_url="https://track.delhivery.com/c/api/pin-codes/json/?filter_codes=%s"%str(order[18])
                req = requests.get(check_url, headers=headers)
                if not req.json()['delivery_codes']:
                    insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                            dimensions, volumetric_weight, weight, remark, return_point_id, routing_code)
                                                            VALUES  %s"""
                    insert_shipments_data_tuple = list()
                    insert_shipments_data_tuple.append(("", "Fail", order[0], None,
                          None, None, None, None, "Pincode not serviceable", None, None),)
                    cur.execute(insert_shipments_data_query, tuple(insert_shipments_data_tuple))
                    continue

                shipment_data = dict()
                shipment_data['city'] = order[17]
                shipment_data['weight'] = sum(order[-2])*1000
                shipment_data['add'] = order[15]
                if order[16]:
                    shipment_data['add'] += '\n' + order[16]
                shipment_data['phone'] = order[21]
                shipment_data['payment_mode'] = order[26]
                shipment_data['name'] = order[13]
                if order[14]:
                    shipment_data['name'] += " " + order[14]
                shipment_data['product_quantity'] = sum(order[-1])
                shipment_data['pin'] = order[18]
                shipment_data['state'] = order[19]
                shipment_data['order_date'] = str(order[2])
                shipment_data['total_amount'] = order[27]
                shipment_data['country'] = order[20]
                shipment_data['client'] = courier[15]
                shipment_data['order'] = order[1]
                if order[26].lower() == "cod":
                    shipment_data['cod_amount'] = order[27]

                shipments.append(shipment_data)
            except Exception as e:
                print("couldn't assign order: "+str(order[1])+"\nError: "+str(e))

        pickup_points_tuple = (courier[1],)
        cur.execute(get_pickup_points_query, pickup_points_tuple)

        pickup_point = cur.fetchone()  #change this as we get to dynamic pickups

        pick_add = pickup_point[4]
        if pickup_point[5]:
            pick_add += "\n"+pickup_point[5]
        pickup_location = {"city": pickup_point[6],
                           "name": pickup_point[9],
                           "pin": pickup_point[8],
                           "country": pickup_point[7],
                           "phone": pickup_point[3],
                           "add": pick_add,
        }

        shipments_divided = [shipments[i * 15:(i + 1) * 15] for i in range((len(shipments) + 15 - 1) // 15)]
        return_data = list()

        for new_shipments in shipments_divided:

            delivery_shipments_body = {"data":json.dumps({"shipments":new_shipments, "pickup_location": pickup_location}), "format":"json"}
            delhivery_url = courier[16] + "api/cmu/create.json"

            req = requests.post(delhivery_url, headers=headers, data=delivery_shipments_body)

            return_data += req.json()['packages']

        insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                        dimensions, volumetric_weight, weight, remark, return_point_id, routing_code)
                                        VALUES  %s"""

        for i in range(len(return_data)-1):
            insert_shipments_data_query += ",%s"

        insert_shipments_data_query += ";"

        orders_dict = dict()
        for prev_order in all_orders:
            orders_dict[prev_order[1]] = (prev_order[0], prev_order[-3], prev_order[-2], prev_order[-1])

        order_status_change_ids = list()
        insert_shipments_data_tuple = list()
        for package in return_data:
            if package['waybill']:
                order_status_change_ids.append(orders_dict[package['refnum']][0])
            dimensions = orders_dict[package['refnum']][1][0]
            dimensions['length'] = dimensions['length']*orders_dict[package['refnum']][3][0]
            weight = orders_dict[package['refnum']][2][0]*orders_dict[package['refnum']][3][0]
            for idx, dim in enumerate(orders_dict[package['refnum']][1]):
                if idx==0:
                    continue
                dimensions['length'] += dim['length']*(orders_dict[package['refnum']][3][idx])
                weight += orders_dict[package['refnum']][2][idx]*(orders_dict[package['refnum']][3][idx])

            volumetric_weight = (dimensions['length']*dimensions['breadth']*dimensions['height'])/5000

            remark = ''
            if package['remarks']:
                remark = package['remarks'][0]

            data_tuple = (package['waybill'], package['status'], orders_dict[package['refnum']][0], pickup_point[1],
                          courier[9], json.dumps(dimensions), volumetric_weight, weight, remark, pickup_point[2], package['sort_code'])
            insert_shipments_data_tuple.append(data_tuple)

        if insert_shipments_data_tuple:
            insert_shipments_data_tuple = tuple(insert_shipments_data_tuple)
            cur.execute(insert_shipments_data_query, insert_shipments_data_tuple)

        if last_shipped_order_id:
            last_shipped_data_tuple = (last_shipped_order_id, datetime.now(tz=pytz.timezone('Asia/Calcutta')), courier[1])
            cur.execute(update_last_shipped_order_query, last_shipped_data_tuple)

        if order_status_change_ids:
            if len(order_status_change_ids) == 1:
                cur.execute(update_orders_status_query % (("(%s)")%str(order_status_change_ids[0])))
            else:
                cur.execute(update_orders_status_query, (tuple(order_status_change_ids),))

        conn.commit()
    cur.close()