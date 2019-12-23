import psycopg2, requests, os, json, pytz
import logging
from datetime import datetime

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
conn = psycopg2.connect(host="wareiq-core-prod.cvqssxsqruyc.us-east-1.rds.amazonaws.com", database="core_prod", user="postgres", password="postgres")
conn_2 = psycopg2.connect(host="wareiq-core-prod.cvqssxsqruyc.us-east-1.rds.amazonaws.com", database="users_prod", user="postgres", password="postgres")

def lambda_handler():
    cur = conn.cursor()
    cur_2 = conn_2.cursor()
    cur.execute(fetch_client_couriers_query)
    exotel_idx = 0
    exotel_sms_data = {
        'From': 'LM-WAREIQ'
    }
    for courier in cur.fetchall():
        if courier[10] in ("Delhivery", "Delhivery Surface Standard"):
            get_orders_data_tuple = (courier[4], courier[1], courier[1], courier[4])
            cur.execute(get_orders_to_ship_query, get_orders_data_tuple)
            shipments = list()
            all_orders = cur.fetchall()
            last_shipped_order_id = 0
            pickup_points_tuple = (courier[1],)
            cur.execute(get_pickup_points_query, pickup_points_tuple)

            pickup_point = cur.fetchone()  # change this as we get to dynamic pickups

            headers = {"Authorization": "Token " + courier[14],
                       "Content-Type": "application/json"}
            for order in all_orders:
                if order[17].lower() in ("bengaluru", "bangalore", "banglore") and courier[1] == "MIRAKKI":
                    continue
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

                    package_string = ""
                    for idx, prod in enumerate(order[40]):
                        package_string += prod + " (" + str(order[35][idx]) + ") + "
                    package_string += "Shipping"
                    shipment_data = dict()
                    shipment_data['city'] = order[17]
                    shipment_data['weight'] = sum(order[34])*1000
                    shipment_data['add'] = order[15]
                    if order[16]:
                        shipment_data['add'] += '\n' + order[16]
                    shipment_data['phone'] = order[21]
                    shipment_data['payment_mode'] = order[26]
                    shipment_data['name'] = order[13]
                    if order[14]:
                        shipment_data['name'] += " " + order[14]
                    shipment_data['product_quantity'] = sum(order[35])
                    shipment_data['pin'] = order[18]
                    shipment_data['state'] = order[19]
                    shipment_data['order_date'] = str(order[2])
                    shipment_data['total_amount'] = order[27]
                    shipment_data['country'] = order[20]
                    shipment_data['client'] = courier[15]
                    shipment_data['order'] = order[1]
                    shipment_data['products_desc'] = package_string
                    shipment_data['return_add'] = pickup_point[13]
                    if pickup_point[14]:
                        shipment_data['return_add'] += '\n' + pickup_point[14]
                    shipment_data['return_city'] = pickup_point[15]
                    shipment_data['return_state'] = pickup_point[19]
                    shipment_data['return_country'] = pickup_point[16]
                    shipment_data['return_pin'] = pickup_point[17]
                    shipment_data['return_name'] = pickup_point[20]
                    shipment_data['return_phone'] = pickup_point[12]
                    if order[26].lower() == "cod":
                        shipment_data['cod_amount'] = order[27]

                    shipments.append(shipment_data)
                except Exception as e:
                    print("couldn't assign order: "+str(order[1])+"\nError: "+str(e))

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
                                            dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, 
                                            channel_fulfillment_id, tracking_link)
                                            VALUES  %s"""

            for i in range(len(return_data)-1):
                insert_shipments_data_query += ",%s"

            insert_shipments_data_query += ";"

            orders_dict = dict()
            for prev_order in all_orders:
                orders_dict[prev_order[1]] = (prev_order[0], prev_order[33], prev_order[34], prev_order[35],
                                              prev_order[36], prev_order[37], prev_order[38], prev_order[39],
                                              prev_order[5], prev_order[9])

            order_status_change_ids = list()
            insert_shipments_data_tuple = list()
            for package in return_data:
                fulfillment_id = None
                tracking_link = None
                if package['waybill']:
                    order_status_change_ids.append(orders_dict[package['refnum']][0])
                    cur_2.execute(
                        "select client_name from clients where client_prefix='%s'" % orders_dict[package['refnum']][9])
                    client_name = cur_2.fetchone()
                    customer_phone = orders_dict[package['refnum']][8].replace(" ", "")
                    customer_phone = "0" + customer_phone[-10:]

                    sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                    sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                    exotel_sms_data[sms_to_key] = customer_phone
                    exotel_sms_data[
                        sms_body_key] = "Dear Customer, thank you for ordering from %s. Your order will be shipped by Delhivery with AWB number %s. " \
                                        "You can track your order using this AWB number." % (client_name[0], str(package['waybill']))
                    exotel_idx += 1

                    try:
                        create_fulfillment_url = "https://%s:%s@%s/admin/api/2019-10/orders/%s/fulfillments.json" % (
                            orders_dict[package['refnum']][4], orders_dict[package['refnum']][5],
                            orders_dict[package['refnum']][6], orders_dict[package['refnum']][7])
                        tracking_link = "https://www.delhivery.com/track/package/%s" % str(package['waybill'])
                        ful_header = {'Content-Type': 'application/json'}
                        fulfil_data = {
                            "fulfillment": {
                                "tracking_number": str(package['waybill']),
                                "tracking_urls": [
                                    tracking_link
                                ],
                                "tracking_company": "Delhivery",
                                "location_id": 16721477681,
                                "notify_customer": False
                            }
                        }
                        try:
                            req_ful = requests.post(create_fulfillment_url, data=json.dumps(fulfil_data),
                                                    headers=ful_header)
                            fulfillment_id = str(req_ful.json()['fulfillment']['id'])
                        except Exception as e:
                            logger.error("Couldn't update shopify for: " + str(package['refnum'])
                                         + "\nError: " + str(e.args))
                    except Exception as e:
                        logger.error("Couldn't update shopify for: " + str(package['refnum'])
                                     + "\nError: " + str(e.args))

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
                              courier[9], json.dumps(dimensions), volumetric_weight, weight, remark, pickup_point[2],
                              package['sort_code'], fulfillment_id, tracking_link)
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
        elif courier[10] == "Shadowfax":
            get_orders_data_tuple = (courier[4], courier[1], courier[1], courier[4])
            cur.execute(get_orders_to_ship_query, get_orders_data_tuple)
            all_orders = cur.fetchall()
            last_shipped_order_id = 0
            headers = {"Authorization": "Token " + courier[14],
                       "Content-Type": "application/json"}
            pickup_points_tuple = (courier[1],)
            cur.execute(get_pickup_points_query, pickup_points_tuple)
            order_status_change_ids = list()

            pickup_point = cur.fetchone()  # change this as we get to dynamic pickups

            for order in all_orders:
                if order[17].lower() not in ("bengaluru", "bangalore", "banglore") or courier[1] != "MIRAKKI":
                    continue
                if order[0] > last_shipped_order_id:
                    last_shipped_order_id = order[0]

                fulfillment_id = None
                tracking_link = None
                try:
                    # check pincode serviceability
                    check_url = courier[16]+"/v1/serviceability/?pickup_pincode=%s&delivery_pincode=%s&format=json" % (str(pickup_point[8]),str(order[18]))
                    req = requests.get(check_url, headers=headers)
                    if not req.json()['Serviceability']:
                        insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                                            dimensions, volumetric_weight, weight, remark, return_point_id, routing_code)
                                                                            VALUES  %s"""
                        insert_shipments_data_tuple = list()
                        insert_shipments_data_tuple.append(("", "Fail", order[0], None,
                                                            None, None, None, None, "Pincode not serviceable", None,
                                                            None), )
                        cur.execute(insert_shipments_data_query, tuple(insert_shipments_data_tuple))
                        continue

                    package_string = ""
                    for idx, prod in enumerate(order[40]):
                        package_string += prod + " (" + str(order[35][idx]) + ") + "
                    package_string += "Shipping"

                    dimensions = order[33][0]
                    dimensions['length'] = dimensions['length'] * order[35][0]
                    weight = order[34][0] * order[35][0]
                    for idx, dim in enumerate(order[33]):
                        if idx == 0:
                            continue
                        dimensions['length'] += dim['length'] * (order[35][idx])
                        weight += order[34][idx] * (order[35][idx])

                    volumetric_weight = (dimensions['length'] * dimensions['breadth'] * dimensions['height']) / 5000

                    customer_phone = order[21].replace(" ", "")
                    customer_phone = "0" + customer_phone[-10:]

                    customer_name = order[13]
                    if order[14]:
                        customer_name += " "+ order[14]

                    shadowfax_shipment_body = {
                                               "order_details": {
                                                    "client_order_id":  order[1],
                                                    "actual_weight": sum(order[34]) * 1000,
                                                    "volumetric_weight": volumetric_weight,
                                                    "product_value": order[27],
                                                    "payment_mode":  order[26],
                                                    "total_amount":order[27]
                                                },
                                                "customer_details": {
                                                    "name": customer_name,
                                                    "contact": customer_phone,
                                                    "address_line_1": order[15],
                                                    "address_line_2": order[16],
                                                    "city": order[17],
                                                    "state": order[19],
                                                    "pincode": int(order[18])
                                                },
                                                "pickup_details": {
                                                    "name": pickup_point[11],
                                                    "contact": pickup_point[3],
                                                    "address_line_1": pickup_point[4],
                                                    "address_line_2": pickup_point[5],
                                                    "city": pickup_point[6],
                                                    "state": pickup_point[10],
                                                    "pincode": int(pickup_point[8])
                                                },
                                                "rts_details": {
                                                    "name": pickup_point[20],
                                                    "contact": pickup_point[12],
                                                    "address_line_1": pickup_point[13],
                                                    "address_line_2": pickup_point[14],
                                                    "city": pickup_point[15],
                                                    "state": pickup_point[19],
                                                    "pincode": int(pickup_point[17])
                                                },
                                                "product_details": [{
                                                    "sku_name": package_string,
                                                    "price":order[27]
                                                }]
                                            }
                    if order[26].lower() == "cod":
                        shadowfax_shipment_body["order_details"]["cod_amount"]= order[27]
                    shadowfax_url = courier[16] + "/v1/clients/orders/?format=json"
                    req = requests.post(shadowfax_url, headers=headers, data=json.dumps(shadowfax_shipment_body))
                    return_data_raw = req.json()
                    insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                                                                    dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, 
                                                                                                    channel_fulfillment_id, tracking_link)
                                                                                                    VALUES  %s"""
                    if not return_data_raw['errors']:
                        order_status_change_ids.append(order[0])
                        return_data = return_data_raw['data']
                        data_tuple = tuple([(
                        return_data['awb_number'], return_data_raw['message'], order[0], pickup_point[1],
                        courier[9], json.dumps(dimensions), volumetric_weight, weight, "", pickup_point[2],
                        "", None, None)])
                        cur_2.execute("select client_name from clients where client_prefix='%s'" % order[9])
                        client_name = cur_2.fetchone()
                        customer_phone = order[5].replace(" ", "")
                        customer_phone = "0" + customer_phone[-10:]

                        sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                        sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                        exotel_sms_data[sms_to_key] = customer_phone
                        exotel_sms_data[
                            sms_body_key] = "Dear Customer, thank you for ordering from %s. Your order will be shipped by Shadowfax with AWB number %s. " \
                                            "You can track your order using this AWB number." % (
                                            client_name[0], str(return_data_raw['data']['awb_number']))
                        exotel_idx += 1

                        try:
                            create_fulfillment_url = "https://%s:%s@%s/admin/api/2019-10/orders/%s/fulfillments.json" % (
                                order[36], order[37],
                                order[38], order[39])
                            tracking_link = "https://www.delhivery.com/track/package/%s" % str(return_data_raw['data']['awb_number'])
                            ful_header = {'Content-Type': 'application/json'}
                            fulfil_data = {
                                "fulfillment": {
                                    "tracking_number": str(return_data_raw['data']['awb_number']),
                                    "tracking_urls": [
                                        tracking_link
                                    ],
                                    "tracking_company": "Delhivery",
                                    "location_id": 16721477681,
                                    "notify_customer": False
                                }
                            }
                            try:
                                req_ful = requests.post(create_fulfillment_url, data=json.dumps(fulfil_data),
                                                        headers=ful_header)
                                fulfillment_id = str(req_ful.json()['fulfillment']['id'])
                            except Exception as e:
                                logger.error("Couldn't update shopify for: " + str(order[1])
                                             + "\nError: " + str(e.args))
                        except Exception as e:
                            logger.error("Couldn't update shopify for: " + str(order[1])
                                         + "\nError: " + str(e.args))

                    else:
                        data_tuple = tuple([(
                            None, return_data_raw['message'], order[0], pickup_point[1],
                            courier[9], json.dumps(dimensions), volumetric_weight, weight, return_data_raw['errors'], pickup_point[2],
                            "", fulfillment_id, tracking_link)])

                    cur.execute(insert_shipments_data_query, data_tuple)

                except Exception as e:
                    print("couldn't assign order: " + str(order[1]) + "\nError: " + str(e))

            if last_shipped_order_id:
                last_shipped_data_tuple = (
                last_shipped_order_id, datetime.now(tz=pytz.timezone('Asia/Calcutta')), courier[1])
                cur.execute(update_last_shipped_order_query, last_shipped_data_tuple)

            if order_status_change_ids:
                if len(order_status_change_ids) == 1:
                    cur.execute(update_orders_status_query % (("(%s)") % str(order_status_change_ids[0])))
                else:
                    cur.execute(update_orders_status_query, (tuple(order_status_change_ids),))

            conn.commit()

    if exotel_idx:
        logger.info("Sending messages...count:" + str(exotel_idx))
        try:
            lad = requests.post(
                'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend',
                data=exotel_sms_data)
        except Exception as e:
            logger.error("messages not sent." + "   Error: " + str(e.args[0]))

    cur.close()