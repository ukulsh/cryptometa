import psycopg2, requests, os, json, pytz
import logging
from datetime import datetime, timedelta
from requests_oauthlib.oauth1_session import OAuth1Session
from zeep import Client

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
cur_2 = conn_2.cursor()


def lambda_handler(courier_name=None, order_ids=None):
    cur = conn.cursor()
    order_id_tuple = "()"
    if courier_name and order_ids: #creating courier details list for manual shipping
        if len(order_ids)==1:
            order_id_tuple = "('"+str(order_ids[0])+"')"
        else:
            order_id_tuple = str(tuple(order_ids))
        cur.execute("""DELETE FROM 	order_status where order_id in %s;
                       DELETE FROM shipments where order_id in %s;"""%(order_id_tuple, order_id_tuple))
        conn.commit()
        cur.execute("SELECT DISTINCT(client_prefix) from orders where id in %s"%order_id_tuple)
        client_list  = cur.fetchall()
        cur.execute("""SELECT bb.id,bb.courier_name,bb.logo_url,bb.date_created,bb.date_updated,bb.api_key,bb.api_password,
                    bb.api_url FROM master_couriers bb WHERE courier_name='%s'"""%courier_name)
        courier_details = cur.fetchone()
        all_couriers = list()
        for client in client_list:
            all_couriers.append((None, client[0], None, 1, None, None, None, None, "")+courier_details)

    else:
        cur.execute(delete_failed_shipments_query)
        time_now = datetime.utcnow()
        if time_now.hour == 22 and 0<time_now.minute<30:
            time_now = time_now-timedelta(days=30)
            cur.execute("""delete from shipments where order_id in 
                            (select id from orders where order_date>%s and status='NEW')
                            and remark = 'Pincode not serviceable'""", (time_now, ))
        conn.commit()
        cur.execute(fetch_client_couriers_query)
        all_couriers=cur.fetchall()

    for courier in all_couriers:
        if courier[10].startswith('Delhivery'):
            ship_delhivery_orders(cur, courier, courier_name, order_ids, order_id_tuple)

        elif courier[10] == "Delhivery" and courier[1] in ('BEYONDUW'):
            ship_vinculum_orders(cur, courier, courier_name, order_ids, order_id_tuple)

        elif courier[10] == "Shadowfax":
            ship_shadowfax_orders(cur, courier, courier_name, order_ids, order_id_tuple)

        elif courier[10].startswith('Xpressbees'):
            ship_xpressbees_orders(cur, courier, courier_name, order_ids, order_id_tuple)

        elif courier[10].startswith('Bluedart'):
            ship_bluedart_orders(cur, courier, courier_name, order_ids, order_id_tuple)

        elif courier[10].startswith('Ecom'):
            ship_ecom_orders(cur, courier, courier_name, order_ids, order_id_tuple)

    cur.close()


def cod_verification_text(order, exotel_idx, cur):
    cod_confirmation_link = "https://track.wareiq.com/core/v1/passthru/cod?CustomField=%s" % str(order[0])
    """
    short_url = requests.get(
        "https://cutt.ly/api/api.php?key=f445d0bb52699d2f870e1832a1f77ef3f9078&short=%s" % cod_confirmation_link)
    short_url_track = short_url.json()['url']['shortLink']
    """
    insert_cod_ver_tuple = (order[0], cod_confirmation_link, datetime.now())
    cur.execute("INSERT INTO cod_verification (order_id, verification_link, date_created) VALUES (%s,%s,%s);",
                insert_cod_ver_tuple)
    client_name = order[51]
    customer_phone = order[5].replace(" ", "")
    customer_phone = "0" + customer_phone[-10:]

    sms_to_key = "Messages[%s][To]" % str(exotel_idx)
    sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

    sms_body_key_data = "Dear Customer, You recently placed an order from %s worth INR %s. " \
                        "Please click on the link (%s) to verify. " \
                        "Your order will be shipped soon after confirmation." % (
                            client_name, str(order[27]), cod_confirmation_link)

    return sms_to_key, sms_body_key, customer_phone, sms_body_key_data


def get_delivery_zone(pick_pincode, del_pincode):
    cur_2.execute("SELECT city from city_pin_mapping where pincode='%s';" % str(pick_pincode).rstrip())
    pickup_city = cur_2.fetchone()
    if not pickup_city:
        return None
    pickup_city = pickup_city[0]
    cur_2.execute("SELECT city from city_pin_mapping where pincode='%s';" % str(del_pincode).rstrip())
    deliver_city = cur_2.fetchone()
    if not deliver_city:
        return None
    deliver_city = deliver_city[0]
    zone_select_tuple = (pickup_city, deliver_city)
    cur_2.execute("SELECT zone_value from city_zone_mapping where zone=%s and city=%s;",
                  zone_select_tuple)
    delivery_zone = cur_2.fetchone()
    if not delivery_zone:
        return None
    delivery_zone = delivery_zone[0]
    if not delivery_zone:
        return None

    if delivery_zone in ('D1', 'D2'):
        delivery_zone = 'D'
    if delivery_zone in ('C1', 'C2'):
        delivery_zone = 'C'

    return delivery_zone


def ship_delhivery_orders(cur, courier, courier_name, order_ids, order_id_tuple, backup_param=True):
    exotel_idx = 0
    exotel_sms_data = {
        'From': 'LM-WAREIQ'
    }
    if courier_name and order_ids:
        orders_to_ship_query = get_orders_to_ship_query.replace("__ORDER_SELECT_FILTERS__",
                                                                """and aa.id in %s""" % order_id_tuple)
    else:
        orders_to_ship_query = get_orders_to_ship_query.replace("__ORDER_SELECT_FILTERS__", """and aa.status='NEW'
                                                                                            and ll.id is null""")
    get_orders_data_tuple = (courier[1], courier[1])
    if courier[3] == 2:
        orders_to_ship_query = orders_to_ship_query.replace('__PRODUCT_FILTER__',
                                                            "and ship_courier[1]='%s'" % courier[10])
    else:
        orders_to_ship_query = orders_to_ship_query.replace('__PRODUCT_FILTER__', '')

    cur.execute(orders_to_ship_query, get_orders_data_tuple)
    all_orders = cur.fetchall()
    pickup_point_order_dict = dict()
    orders_dict = dict()
    for order in all_orders:
        if order[41]:
            if order[41] not in pickup_point_order_dict:
                pickup_point_order_dict[order[41]] = [order]
            else:
                pickup_point_order_dict[order[41]].append(order)

    for pickup_id, all_new_orders in pickup_point_order_dict.items():

        shipments = list()
        last_shipped_order_id = 0
        pickup_points_tuple = (pickup_id,)
        cur.execute(get_pickup_points_query, pickup_points_tuple)

        pickup_point = cur.fetchone()  # change this as we get to dynamic pickups

        headers = {"Authorization": "Token " + courier[14],
                   "Content-Type": "application/json"}
        for order in all_new_orders:
            try:
                zone = None
                try:
                    zone = get_delivery_zone(pickup_point[8], order[18])
                except Exception as e:
                    logger.error("couldn't find zone: " + str(order[0]) + "\nError: " + str(e))

                orders_dict[str(order[0])] = (order[0], order[33], order[34], order[35],
                                              order[36], order[37], order[38], order[39],
                                              order[5], order[9], order[45], order[46],
                                              order[51], order[52], zone)

                if order[17].lower() in ("bengaluru", "bangalore", "banglore") and courier[1] in ("SOHOMATTRESS", ) and order[26].lower() != 'pickup':
                    continue

                if courier[1] == "ZLADE" and courier[10]=="Delhivery Surface Standard" and zone and zone not in ('A','B') and order[26].lower() != 'pickup':
                    continue

                if not order[52]:
                    weight = order[34][0] * order[35][0]
                    volumetric_weight = (order[33][0]['length'] * order[33][0]['breadth'] * order[33][0]['height'])*order[35][0] / 5000
                    for idx, dim in enumerate(order[33]):
                        if idx == 0:
                            continue
                        volumetric_weight += (dim['length'] * dim['breadth'] * dim['height'])*order[35][idx] / 5000
                        weight += order[34][idx] * (order[35][idx])
                else:
                    weight = float(order[52])
                    volumetric_weight = float(order[52])

                if courier[10] == "Delhivery Surface Standard":
                    weight_counted = weight if weight > volumetric_weight else volumetric_weight
                    new_courier_name = None
                    if weight_counted > 14:
                        new_courier_name = "Delhivery 20 KG"
                    elif weight_counted > 6:
                        new_courier_name = "Delhivery 10 KG"
                    elif weight_counted > 1.5:
                        new_courier_name = "Delhivery 2 KG"
                    if new_courier_name:
                        try:
                            cur.execute("""SELECT id, courier_name, logo_url, date_created, date_updated, api_key, 
                                                                            api_password, api_url FROM master_couriers
                                                                            WHERE courier_name='%s'""" % new_courier_name)
                            courier_data = cur.fetchone()
                            courier_new = list(courier)
                            courier_new[2] = courier_data[0]
                            courier_new[3] = 1
                            courier_new[9] = courier_data[0]
                            courier_new[10] = courier_data[1]
                            courier_new[11] = courier_data[2]
                            courier_new[12] = courier_data[3]
                            courier_new[13] = courier_data[4]
                            courier_new[14] = courier_data[5]
                            courier_new[15] = courier_data[6]
                            courier_new[16] = courier_data[7]
                            ship_delhivery_orders(cur, tuple(courier_new), new_courier_name, [order[0]], "(" + str(order[0]) + ")", backup_param=False)
                        except Exception as e:
                            logger.error("Couldn't assign backup courier for: " + str(order[0]) + "\nError: " + str(e.args))
                            pass

                        continue

                time_2_days = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=1)
                if order[47] and not (order[50] and order[2] < time_2_days):
                    if order[26].lower() == 'cod' and not order[42] and order[43]:
                        continue  # change this to continue later
                    if order[26].lower() == 'cod' and not order[43]:
                        try:  ## Cod confirmation  text
                            sms_to_key, sms_body_key, customer_phone, sms_body_key_data = cod_verification_text(
                                order, exotel_idx, cur)
                            if not order[53]:
                                exotel_sms_data[sms_to_key] = customer_phone
                                exotel_sms_data[sms_body_key] = sms_body_key_data
                                exotel_idx += 1
                        except Exception as e:
                            logger.error(
                                "Cod confirmation not sent. Order id: " + str(order[0]))
                        continue
                if order[0] > last_shipped_order_id:
                    last_shipped_order_id = order[0]

                # check delhivery pincode serviceability
                check_url = "https://track.delhivery.com/c/api/pin-codes/json/?filter_codes=%s" % str(order[18])
                req = requests.get(check_url, headers=headers)
                if not req.json()['delivery_codes']:
                    cur.execute("select * from client_couriers where client_prefix=%s and priority=%s;",(courier[1], courier[3]+1))
                    qs = cur.fetchone()
                    if not (qs and backup_param):
                        insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                                dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, zone)
                                                                VALUES  %s"""
                        insert_shipments_data_tuple = list()
                        insert_shipments_data_tuple.append(("", "Fail", order[0], None,
                                                            None, None, None, None, "Pincode not serviceable", None,
                                                            None, zone), )
                        cur.execute(insert_shipments_data_query, tuple(insert_shipments_data_tuple))
                    continue

                package_string = ""
                if order[40]:
                    for idx, prod in enumerate(order[40]):
                        package_string += prod + " (" + str(order[35][idx]) + ") + "
                    package_string += "Shipping"
                else:
                    package_string += "WareIQ package"

                shipment_data = dict()
                shipment_data['city'] = order[17]
                shipment_data['weight'] = weight
                shipment_data['add'] = order[15]
                if order[16]:
                    shipment_data['add'] += '\n' + order[16]
                shipment_data['phone'] = order[21]
                shipment_data['payment_mode'] = order[26]
                shipment_data['name'] = order[13]
                if order[14]:
                    shipment_data['name'] += " " + order[14]
                shipment_data['product_quantity'] = sum(order[35]) if order[35] else 1
                shipment_data['pin'] = order[18]
                shipment_data['state'] = order[19]
                shipment_data['order_date'] = str(order[2])
                shipment_data['total_amount'] = order[27]
                shipment_data['country'] = order[20]
                shipment_data['client'] = courier[15]
                shipment_data['order'] = str(order[0])
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
                if order[49] and order[49][0]:
                    shipment_data['category_of_goods'] = order[49][0]
                if order[26].lower() == "cod":
                    shipment_data['cod_amount'] = order[27]

                shipments.append(shipment_data)
            except Exception as e:
                logger.error("couldn't assign order: " + str(order[0]) + "\nError: " + str(e))

        pick_add = pickup_point[4]
        if pickup_point[5]:
            pick_add += "\n" + pickup_point[5]
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
            delivery_shipments_body = {
                "data": json.dumps({"shipments": new_shipments, "pickup_location": pickup_location}), "format": "json"}
            delhivery_url = courier[16] + "api/cmu/create.json"

            req = requests.post(delhivery_url, headers=headers, data=delivery_shipments_body)
            if req.json().get('rmk')=='ClientWarehouse matching query does not exist.':
                pickup_phone = pickup_point[3].replace(" ", "")
                pickup_phone = pickup_phone[-10:]
                warehouse_creation = {  "phone": pickup_phone,
                                        "city": pickup_point[6],
                                        "name": pickup_point[9],
                                        "pin": str(pickup_point[8]),
                                        "address": pick_add,
                                        "country": pickup_point[7],
                                        "registered_name": pickup_point[11],
                                        "return_address": str(pickup_point[13])+str(pickup_point[14]),
                                        "return_pin": str(pickup_point[17]),
                                        "return_city": pickup_point[15],
                                        "return_state": pickup_point[19],
                                        "return_country": pickup_point[16]}
                create_warehouse_url = courier[16] + "api/backend/clientwarehouse/create/"
                requests.post(create_warehouse_url, headers=headers, data=json.dumps(warehouse_creation))
                req = requests.post(delhivery_url, headers=headers, data=delivery_shipments_body)

            return_data += req.json()['packages']

        insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                        dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, 
                                        channel_fulfillment_id, tracking_link, zone)
                                        VALUES  %s"""

        for i in range(len(return_data) - 1):
            insert_shipments_data_query += ",%s"

        insert_shipments_data_query += " RETURNING id,awb;"

        order_status_change_ids = list()
        insert_shipments_data_tuple = list()
        insert_order_status_dict = dict()
        for package in return_data:
            fulfillment_id = None
            tracking_link = None
            if package['waybill']:
                order_status_change_ids.append(orders_dict[package['refnum']][0])
                client_name = str(orders_dict[package['refnum']][12])
                customer_phone = orders_dict[package['refnum']][8].replace(" ", "")
                customer_phone = "0" + customer_phone[-10:]

                sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                exotel_sms_data[sms_to_key] = customer_phone
                try:
                    tracking_link_wareiq = "http://webapp.wareiq.com/tracking/" + str(package['waybill'])
                    """
                    short_url = requests.get("https://cutt.ly/api/api.php?key=f445d0bb52699d2f870e1832a1f77ef3f9078&short=%s"%tracking_link_wareiq)
                    short_url_track = short_url.json()['url']['shortLink']
                    """
                    exotel_sms_data[
                        sms_body_key] = "Received: Your order from %s. Track here: %s . Thanks!" % (
                                            client_name, tracking_link_wareiq)
                except Exception:
                    pass

                exotel_idx += 1

                if orders_dict[package['refnum']][9] == "NASHER":
                    try:
                        nasher_url = "https://www.nashermiles.com/alexandria/api/v1/shipment/create"
                        nasher_headers = {"Content-Type": "application/x-www-form-urlencoded",
                                          "Authorization": "Basic c2VydmljZS5hcGl1c2VyOllQSGpBQXlXY3RWYzV5MWg="}
                        nasher_body = {
                            "order_id": package['refnum'],
                            "awb_number": str(package['waybill']),
                            "tracking_link": "http://webapp.wareiq.com/tracking/" + str(package['waybill'])}
                        req = requests.post(nasher_url, headers=nasher_headers, data=json.dumps(nasher_body))
                    except Exception as e:
                        logger.error("Couldn't update shopify for: " + str(package['refnum'])
                                     + "\nError: " + str(e.args))

            remark = ''
            if package['remarks']:
                remark = package['remarks'][0]

            if not orders_dict[package['refnum']][13]:
                dimensions = orders_dict[package['refnum']][1][0]
                weight = orders_dict[package['refnum']][2][0] * orders_dict[package['refnum']][3][0]
                volumetric_weight = (dimensions['length'] * dimensions['breadth'] * dimensions['height'])*orders_dict[package['refnum']][3][0]/5000
                for idx, dim in enumerate(orders_dict[package['refnum']][1]):
                    if idx == 0:
                        continue
                    volumetric_weight += (dim['length'] * dim['breadth'] * dim['height'])*orders_dict[package['refnum']][3][idx] / 5000
                    weight += orders_dict[package['refnum']][2][idx] * (orders_dict[package['refnum']][3][idx])

                if dimensions['length'] and dimensions['breadth']:
                    dimensions['height'] = round((volumetric_weight * 5000) / (dimensions['length'] * dimensions['breadth']))
            else:
                dimensions = {"length": 1, "breadth": 1, "height": 1}
                weight = float(orders_dict[package['refnum']][13])
                volumetric_weight = float(orders_dict[package['refnum']][13])

            data_tuple = (package['waybill'], package['status'], orders_dict[package['refnum']][0], pickup_point[1],
                          courier[9], json.dumps(dimensions), volumetric_weight, weight, remark, pickup_point[2],
                          package['sort_code'], fulfillment_id, tracking_link, orders_dict[package['refnum']][14])
            insert_shipments_data_tuple.append(data_tuple)
            insert_order_status_dict[package['waybill']] = [orders_dict[package['refnum']][0], courier[9],
                                                            None, "UD", "Received", "Consignment Manifested",
                                                            pickup_point[6], pickup_point[6],
                                                            datetime.utcnow() + timedelta(hours=5.5)]

        if insert_shipments_data_tuple:
            insert_shipments_data_tuple = tuple(insert_shipments_data_tuple)
            cur.execute(insert_shipments_data_query, insert_shipments_data_tuple)
            shipment_ret = cur.fetchall()
            order_status_add_query = """INSERT INTO order_status (order_id, courier_id, shipment_id, 
                                                            status_code, status, status_text, location, location_city, 
                                                            status_time) VALUES """
            order_status_tuple_list = list()
            for ship_temp in shipment_ret:
                insert_order_status_dict[ship_temp[1]][2] = ship_temp[0]
                order_status_add_query += "%s,"
                order_status_tuple_list.append(tuple(insert_order_status_dict[ship_temp[1]]))

            order_status_add_query = order_status_add_query.rstrip(',')
            order_status_add_query += ";"

            cur.execute(order_status_add_query, tuple(order_status_tuple_list))

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


def ship_shadowfax_orders(cur, courier, courier_name, order_ids, order_id_tuple, backup_param=True):
    exotel_idx = 0
    exotel_sms_data = {
        'From': 'LM-WAREIQ'
    }
    if courier_name and order_ids:
        orders_to_ship_query = get_orders_to_ship_query.replace("__ORDER_SELECT_FILTERS__",
                                                                """and aa.id in %s""" % order_id_tuple)
    else:
        orders_to_ship_query = get_orders_to_ship_query.replace("__ORDER_SELECT_FILTERS__", """and aa.status='NEW'
                                                                                            and ll.id is null""")
    get_orders_data_tuple = (courier[1], courier[1])

    cur.execute(orders_to_ship_query, get_orders_data_tuple)
    all_orders = cur.fetchall()
    pickup_point_order_dict = dict()
    for order in all_orders:
        if order[41]:
            if order[41] not in pickup_point_order_dict:
                pickup_point_order_dict[order[41]] = [order]
            else:
                pickup_point_order_dict[order[41]].append(order)

    for pickup_id, all_new_orders in pickup_point_order_dict.items():
        last_shipped_order_id = 0
        headers = {"Authorization": "Token " + courier[14],
                   "Content-Type": "application/json"}
        pickup_points_tuple = (pickup_id,)
        cur.execute(get_pickup_points_query, pickup_points_tuple)
        order_status_change_ids = list()

        pickup_point = cur.fetchone()  # change this as we get to dynamic pickups

        for order in all_new_orders:
            if order[17].lower() not in ("bengaluru", "bangalore", "banglore") and courier[1] == "MIRAKKI":
                continue
            zone = None
            try:
                zone = get_delivery_zone(pickup_point[8], order[18])
            except Exception as e:
                logger.error("couldn't find zone: " + str(order[0]) + "\nError: " + str(e))
            time_2_days = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=1)
            if order[47] and not (order[50] and order[2] < time_2_days):
                if order[26].lower() == 'cod' and not order[42] and order[43]:
                    continue
                if order[26].lower() == 'cod' and not order[43]:
                    try:  ## Cod confirmation  text
                        sms_to_key, sms_body_key, customer_phone, sms_body_key_data = cod_verification_text(
                            order, exotel_idx, cur)
                        if not order[53]:
                            exotel_sms_data[sms_to_key] = customer_phone
                            exotel_sms_data[sms_body_key] = sms_body_key_data
                            exotel_idx += 1
                    except Exception as e:
                        logger.error(
                            "Cod confirmation not sent. Order id: " + str(order[0]))
                    continue

            if order[0] > last_shipped_order_id:
                last_shipped_order_id = order[0]

            fulfillment_id = None
            tracking_link = None
            try:
                # check pincode serviceability
                check_url = courier[16] + "/v1/serviceability/?pickup_pincode=%s&delivery_pincode=%s&format=json" % (
                str(pickup_point[8]), str(order[18]))
                req = requests.get(check_url, headers=headers)
                if not req.json()['Serviceability']:
                    cur.execute("select * from client_couriers where client_prefix=%s and priority=%s;",
                                (courier[1], courier[3] + 1))
                    qs = cur.fetchone()
                    if not (qs and backup_param):
                        insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                                            dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, zone)
                                                                            VALUES  %s"""
                        insert_shipments_data_tuple = list()
                        insert_shipments_data_tuple.append(("", "Fail", order[0], None,
                                                            None, None, None, None, "Pincode not serviceable", None,
                                                            None, zone), )
                        cur.execute(insert_shipments_data_query, tuple(insert_shipments_data_tuple))
                    continue

                package_string = ""
                for idx, prod in enumerate(order[40]):
                    package_string += prod + " (" + str(order[35][idx]) + ") + "
                package_string += "Shipping"

                dimensions = order[33][0]
                dimensions['length'] = dimensions['length'] * order[35][0]
                weight = order[34][0] * order[35][0]
                volumetric_weight = (dimensions['length'] * dimensions['breadth'] * dimensions['height']) / 5000
                for idx, dim in enumerate(order[33]):
                    if idx == 0:
                        continue
                    dim['length'] += dim['length'] * (order[35][idx])
                    volumetric_weight += (dim['length'] * dim['breadth'] * dim['height']) / 5000
                    weight += order[34][idx] * (order[35][idx])

                if dimensions['length'] and dimensions['breadth']:
                    dimensions['height'] = round(
                        (volumetric_weight * 5000) / (dimensions['length'] * dimensions['breadth']))

                customer_phone = order[21].replace(" ", "")
                customer_phone = "0" + customer_phone[-10:]

                customer_name = order[13]
                if order[14]:
                    customer_name += " " + order[14]

                shadowfax_shipment_body = {
                    "order_details": {
                        "client_order_id": order[1],
                        "actual_weight": sum(order[34]) * 1000,
                        "volumetric_weight": volumetric_weight,
                        "product_value": order[27],
                        "payment_mode": order[26],
                        "total_amount": order[27]
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
                        "price": order[27]
                    }]
                }
                if order[26].lower() == "cod":
                    shadowfax_shipment_body["order_details"]["cod_amount"] = order[27]
                shadowfax_url = courier[16] + "/v1/clients/orders/?format=json"
                req = requests.post(shadowfax_url, headers=headers, data=json.dumps(shadowfax_shipment_body))
                return_data_raw = req.json()
                insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                                                                dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, 
                                                                                                channel_fulfillment_id, tracking_link, zone)
                                                                                                VALUES  %s RETURNING id;"""
                if not return_data_raw['errors']:
                    order_status_change_ids.append(order[0])
                    return_data = return_data_raw['data']
                    client_name = str(order[51])
                    customer_phone = order[5].replace(" ", "")
                    customer_phone = "0" + customer_phone[-10:]

                    sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                    sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                    exotel_sms_data[sms_to_key] = customer_phone
                    try:
                        tracking_link_wareiq = "http://webapp.wareiq.com/tracking/" + str(
                            return_data_raw['data']['awb_number'])
                        """
                        short_url = requests.get(
                            "https://cutt.ly/api/api.php?key=f445d0bb52699d2f870e1832a1f77ef3f9078&short=%s" % tracking_link_wareiq)
                        short_url_track = short_url.json()['url']['shortLink']
                        """
                        exotel_sms_data[
                            sms_body_key] = "Received: Your order from %s. Track here: %s . Thanks!" % (client_name, tracking_link_wareiq)
                    except Exception:
                        pass

                    exotel_idx += 1

                    data_tuple = tuple([(
                        return_data['awb_number'], return_data_raw['message'], order[0], pickup_point[1],
                        courier[9], json.dumps(dimensions), volumetric_weight, weight, "", pickup_point[2],
                        "", fulfillment_id, tracking_link, zone)])

                else:
                    data_tuple = tuple([(
                        "", return_data_raw['message'], order[0], pickup_point[1],
                        courier[9], json.dumps(dimensions), volumetric_weight, weight, return_data_raw['errors'],
                        pickup_point[2],
                        "", fulfillment_id, tracking_link, zone)])

                cur.execute(insert_shipments_data_query, data_tuple)
                ship_temp = cur.fetchone()
                order_status_add_query = """INSERT INTO order_status (order_id, courier_id, shipment_id, 
                                                                        status_code, status, status_text, location, location_city, 
                                                                        status_time) VALUES %s"""

                order_status_add_tuple = [(order[0], courier[9],
                                           ship_temp[0], "UD", "Received", "Consignment Manifested",
                                           pickup_point[6], pickup_point[6], datetime.utcnow() + timedelta(hours=5.5))]

                cur.execute(order_status_add_query, tuple(order_status_add_tuple))

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


def ship_xpressbees_orders(cur, courier, courier_name, order_ids, order_id_tuple, backup_param=True):
    exotel_idx = 0
    exotel_sms_data = {
        'From': 'LM-WAREIQ'
    }
    if courier_name and order_ids:
        orders_to_ship_query = get_orders_to_ship_query.replace("__ORDER_SELECT_FILTERS__",
                                                                """and aa.id in %s""" % order_id_tuple)
    else:
        orders_to_ship_query = get_orders_to_ship_query.replace("__ORDER_SELECT_FILTERS__", """and aa.status='NEW'
                                                                                            and ll.id is null""")
    get_orders_data_tuple = (courier[1], courier[1])

    cur.execute(orders_to_ship_query, get_orders_data_tuple)
    all_orders = cur.fetchall()
    pickup_point_order_dict = dict()
    for order in all_orders:
        if order[41]:
            if order[41] not in pickup_point_order_dict:
                pickup_point_order_dict[order[41]] = [order]
            else:
                pickup_point_order_dict[order[41]].append(order)

    cur.execute("select max(awb) from shipments where courier_id=%s;" % str(courier[9]))
    last_assigned_awb = cur.fetchone()[0]
    last_assigned_awb = int(last_assigned_awb)

    for pickup_id, all_new_orders in pickup_point_order_dict.items():
        last_shipped_order_id = 0
        headers = {"Content-Type": "application/json"}
        pickup_points_tuple = (pickup_id,)
        cur.execute(get_pickup_points_query, pickup_points_tuple)
        order_status_change_ids = list()

        pickup_point = cur.fetchone()  # change this as we get to dynamic pickups

        for order in all_new_orders:

            if order[26].lower()=='pickup':
                try:
                    cur.execute("""SELECT id, courier_name, logo_url, date_created, date_updated, api_key, 
                                                                    api_password, api_url FROM master_couriers
                                                                    WHERE courier_name='%s'""" %"Delhivery Surface Standard")
                    courier_data = cur.fetchone()
                    courier_new = list(courier)
                    courier_new[2] = courier_data[0]
                    courier_new[3] = 1
                    courier_new[9] = courier_data[0]
                    courier_new[10] = courier_data[1]
                    courier_new[11] = courier_data[2]
                    courier_new[12] = courier_data[3]
                    courier_new[13] = courier_data[4]
                    courier_new[14] = courier_data[5]
                    courier_new[15] = courier_data[6]
                    courier_new[16] = courier_data[7]
                    ship_delhivery_orders(cur, tuple(courier_new), "Delhivery Surface Standard", [order[0]], "(" + str(order[0]) + ")", backup_param=False)
                except Exception as e:
                    logger.error("Couldn't assign backup courier for: " + str(order[0]) + "\nError: " + str(e.args))
                    pass

                continue

            weight = float(order[52]) if order[52] else 0
            volumetric_weight = float(order[52]) if order[52] else 0

            if not order[52]:
                try:
                    weight = order[34][0] * order[35][0]
                    volumetric_weight = (order[33][0]['length'] * order[33][0]['breadth'] * order[33][0]['height']) * \
                                        order[35][0] / 5000
                    for idx, dim in enumerate(order[33]):
                        if idx == 0:
                            continue
                        volumetric_weight += (dim['length'] * dim['breadth'] * dim['height']) * order[35][idx] / 5000
                        weight += order[34][idx] * (order[35][idx])
                except Exception:
                    pass

            if courier[10] == "Xpressbees Surface" and weight and volumetric_weight:
                weight_counted = weight if weight > volumetric_weight else volumetric_weight
                new_courier_name = None
                if weight_counted > 3:
                    new_courier_name = "Xpressbees 5 KG"
                if new_courier_name:
                    try:
                        cur.execute("""SELECT id, courier_name, logo_url, date_created, date_updated, api_key, 
                                                                        api_password, api_url FROM master_couriers
                                                                        WHERE courier_name='%s'""" % new_courier_name)
                        courier_data = cur.fetchone()
                        courier_new = list(courier)
                        courier_new[2] = courier_data[0]
                        courier_new[3] = 1
                        courier_new[9] = courier_data[0]
                        courier_new[10] = courier_data[1]
                        courier_new[11] = courier_data[2]
                        courier_new[12] = courier_data[3]
                        courier_new[13] = courier_data[4]
                        courier_new[14] = courier_data[5]
                        courier_new[15] = courier_data[6]
                        courier_new[16] = courier_data[7]
                        ship_xpressbees_orders(cur, tuple(courier_new), new_courier_name, [order[0]],
                                              "(" + str(order[0]) + ")", backup_param=False)
                    except Exception as e:
                        logger.error("Couldn't assign backup courier for: " + str(order[0]) + "\nError: " + str(e.args))
                        pass

                    continue

            zone = None
            try:
                zone = get_delivery_zone(pickup_point[8], order[18])
            except Exception as e:
                logger.error("couldn't find zone: " + str(order[0]) + "\nError: " + str(e))

            time_2_days = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=1)
            if order[47] and not (order[50] and order[2] < time_2_days):
                if order[26].lower() == 'cod' and not order[42] and order[43]:
                    continue
                if order[26].lower() == 'cod' and not order[43]:
                    if order[26].lower() == 'cod' and not order[43]:
                        try:  ## Cod confirmation  text
                            sms_to_key, sms_body_key, customer_phone, sms_body_key_data = cod_verification_text(
                                order, exotel_idx, cur)
                            if not order[53]:
                                exotel_sms_data[sms_to_key] = customer_phone
                                exotel_sms_data[sms_body_key] = sms_body_key_data
                                exotel_idx += 1
                        except Exception as e:
                            logger.error(
                                "Cod confirmation not sent. Order id: " + str(order[0]))
                        continue

            if order[0] > last_shipped_order_id:
                last_shipped_order_id = order[0]

            fulfillment_id = None
            tracking_link = None
            try:
                package_string = ""
                package_quantity = 0
                for idx, prod in enumerate(order[40]):
                    package_string += prod + " (" + str(order[35][idx]) + ") + "
                    package_quantity += order[35][idx]
                package_string += "Shipping"

                dimensions = order[33][0]
                dimensions['length'] = dimensions['length'] * order[35][0]
                weight = order[34][0] * order[35][0]
                volumetric_weight = (dimensions['length'] * dimensions['breadth'] * dimensions['height']) / 5000
                for idx, dim in enumerate(order[33]):
                    if idx == 0:
                        continue
                    dim['length'] += dim['length'] * (order[35][idx])
                    volumetric_weight += (dim['length'] * dim['breadth'] * dim['height']) / 5000
                    weight += order[34][idx] * (order[35][idx])
                if dimensions['length'] and dimensions['breadth']:
                    dimensions['height'] = round(
                        (volumetric_weight * 5000) / (dimensions['length'] * dimensions['breadth']))

                customer_phone = order[21].replace(" ", "")
                customer_phone = "0" + customer_phone[-10:]

                customer_name = order[13]
                if order[14]:
                    customer_name += " " + order[14]

                pickup_address = pickup_point[4]
                if pickup_point[5]:
                    pickup_address += pickup_point[5]

                customer_address = order[15]
                if order[16]:
                    customer_address += order[16]

                rto_address = pickup_point[13]
                if pickup_point[14]:
                    rto_address += pickup_point[14]
                last_assigned_awb += 1
                xpressbees_shipment_body = {
                    "XBkey": courier[14],
                    "VersionNumber": "V6",
                    "ManifestDetails": {
                        "OrderType": order[26],
                        "OrderNo": order[1],
                        "PaymentStatus": order[26],
                        "PickupVendor": pickup_point[11],
                        "PickVendorPhoneNo": pickup_point[3],
                        "PickVendorAddress": pickup_address,
                        "PickVendorCity": pickup_point[6],
                        "PickVendorState": pickup_point[10],
                        "PickVendorPinCode": pickup_point[8],
                        "CustomerName": customer_name,
                        "CustomerCity": order[17],
                        "CustomerState": order[19],
                        "ZipCode": order[18],
                        "CustomerAddressDetails": [{
                            "Type": "Primary",
                            "Address": customer_address
                        }],
                        "CustomerMobileNumberDetails": [{
                            "Type": "Primary",
                            "MobileNo": customer_phone
                        }],
                        "RTOName": pickup_point[20],
                        "RTOMobileNo": pickup_point[12],
                        "RTOAddress": rto_address,
                        "RTOToCity": pickup_point[15],
                        "RTOToState": pickup_point[19],
                        "RTOPinCode": pickup_point[17],
                        "PhyWeight": sum(order[34]),
                        "VolWeight": volumetric_weight,
                        "AirWayBillNO": str(last_assigned_awb),
                        "Quantity": package_quantity,
                        "PickupVendorCode": pickup_point[9],
                        "IsOpenDelivery": "0",
                        "DeclaredValue": order[27],
                        "GSTMultiSellerInfo": [{
                            "ProductDesc": package_string,
                            "SellerName": pickup_point[11],
                            "SellerAddress": pickup_address,
                            "SupplySellerStatePlace": pickup_point[10],
                            "SellerPincode": int(pickup_point[8]),
                            "HSNCode": "3304"
                        }]}}

                if order[26].lower() == "cod":
                    xpressbees_shipment_body["ManifestDetails"]["CollectibleAmount"] = order[27]
                xpressbees_url = courier[16] + "POSTShipmentService.svc/AddManifestDetails"
                req = requests.post(xpressbees_url, headers=headers, data=json.dumps(xpressbees_shipment_body))
                while req.json()['AddManifestDetails'][0]['ReturnMessage'] == 'AWB Already Exists':
                    last_assigned_awb += 1
                    xpressbees_shipment_body['ManifestDetails']['AirWayBillNO'] = str(last_assigned_awb)
                    req = requests.post(xpressbees_url, headers=headers,
                                        data=json.dumps(xpressbees_shipment_body))
                return_data_raw = req.json()
                insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                                                                dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, 
                                                                                                channel_fulfillment_id, tracking_link, zone)
                                                                                                VALUES  %s RETURNING id;"""
                if return_data_raw['AddManifestDetails'][0]['ReturnMessage'] == 'successful':
                    order_status_change_ids.append(order[0])
                    data_tuple = tuple([(
                        return_data_raw['AddManifestDetails'][0]['AWBNo'],
                        return_data_raw['AddManifestDetails'][0]['ReturnMessage'],
                        order[0], pickup_point[1], courier[9], json.dumps(dimensions), volumetric_weight, weight,
                        "", pickup_point[2], "", fulfillment_id, tracking_link, zone)])
                    client_name = str(order[51])
                    customer_phone = order[5].replace(" ", "")
                    customer_phone = "0" + customer_phone[-10:]

                    sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                    sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                    exotel_sms_data[sms_to_key] = customer_phone
                    try:
                        tracking_link_wareiq = "http://webapp.wareiq.com/tracking/" + str(
                            return_data_raw['AddManifestDetails'][0]['AWBNo'])
                        """
                        short_url = requests.get(
                            "https://cutt.ly/api/api.php?key=f445d0bb52699d2f870e1832a1f77ef3f9078&short=%s" % tracking_link_wareiq)
                        short_url_track = short_url.json()['url']['shortLink']
                        """
                        exotel_sms_data[
                            sms_body_key] = "Received: Your order from %s. Track here: %s . Thanks!" % (client_name, tracking_link_wareiq)
                    except Exception:
                        pass

                    exotel_idx += 1


                else:
                    cur.execute("select * from client_couriers where client_prefix=%s and priority=%s;",
                                (courier[1], courier[3] + 1))
                    qs = cur.fetchone()
                    if not (qs and backup_param):
                        insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                                dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, zone)
                                                                VALUES  %s"""
                        insert_shipments_data_tuple = list()
                        insert_shipments_data_tuple.append(("", "Fail", order[0], None,
                                                            None, None, None, None, "Pincode not serviceable", None,
                                                            None, zone), )
                        cur.execute(insert_shipments_data_query, tuple(insert_shipments_data_tuple))
                    continue

                cur.execute(insert_shipments_data_query, data_tuple)
                ship_temp = cur.fetchone()
                order_status_add_query = """INSERT INTO order_status (order_id, courier_id, shipment_id, 
                                            status_code, status, status_text, location, location_city, 
                                            status_time) VALUES %s"""

                order_status_add_tuple = [(order[0], courier[9],
                                           ship_temp[0], "UD", "Received", "Consignment Manifested",
                                           pickup_point[6], pickup_point[6], datetime.utcnow() + timedelta(hours=5.5))]

                cur.execute(order_status_add_query, tuple(order_status_add_tuple))

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


def ship_ecom_orders(cur, courier, courier_name, order_ids, order_id_tuple, backup_param=True):
    exotel_idx = 0
    exotel_sms_data = {
        'From': 'LM-WAREIQ'
    }
    if courier_name and order_ids:
        orders_to_ship_query = get_orders_to_ship_query.replace("__ORDER_SELECT_FILTERS__",
                                                                """and aa.id in %s""" % order_id_tuple)
    else:
        orders_to_ship_query = get_orders_to_ship_query.replace("__ORDER_SELECT_FILTERS__", """and aa.status='NEW'
                                                                                            and ll.id is null""")
    get_orders_data_tuple = (courier[1], courier[1])

    cur.execute(orders_to_ship_query, get_orders_data_tuple)
    all_orders = cur.fetchall()
    pickup_point_order_dict = dict()
    for order in all_orders:
        if order[41]:
            if order[41] not in pickup_point_order_dict:
                pickup_point_order_dict[order[41]] = [order]
            else:
                pickup_point_order_dict[order[41]].append(order)

    cur.execute("""select max(awb) from shipments aa
                    left join orders bb on aa.order_id=bb.id
                    left join orders_payments cc on cc.order_id=bb.id
                    where courier_id=%s
                    and payment_mode ilike 'cod';""" % str(courier[9]))

    last_assigned_awb_cod = cur.fetchone()[0]
    last_assigned_awb_cod = int(last_assigned_awb_cod)

    cur.execute("""select max(awb) from shipments aa
                    left join orders bb on aa.order_id=bb.id
                    left join orders_payments cc on cc.order_id=bb.id
                    where courier_id=%s
                    and (payment_mode ilike 'prepaid' or payment_mode ilike 'paid');""" % str(courier[9]))

    last_assigned_awb_ppd = cur.fetchone()[0]
    last_assigned_awb_ppd = int(last_assigned_awb_ppd)

    for pickup_id, all_new_orders in pickup_point_order_dict.items():
        last_shipped_order_id = 0
        headers = {"Content-Type": "application/json"}
        pickup_points_tuple = (pickup_id,)
        cur.execute(get_pickup_points_query, pickup_points_tuple)
        order_status_change_ids = list()

        pickup_point = cur.fetchone()  # change this as we get to dynamic pickups

        for order in all_new_orders:
            if order[26].lower()=='pickup':
                try:
                    cur.execute("""SELECT id, courier_name, logo_url, date_created, date_updated, api_key, 
                                                                    api_password, api_url FROM master_couriers
                                                                    WHERE courier_name='%s'""" %"Delhivery Surface Standard")
                    courier_data = cur.fetchone()
                    courier_new = list(courier)
                    courier_new[2] = courier_data[0]
                    courier_new[3] = 1
                    courier_new[9] = courier_data[0]
                    courier_new[10] = courier_data[1]
                    courier_new[11] = courier_data[2]
                    courier_new[12] = courier_data[3]
                    courier_new[13] = courier_data[4]
                    courier_new[14] = courier_data[5]
                    courier_new[15] = courier_data[6]
                    courier_new[16] = courier_data[7]
                    ship_delhivery_orders(cur, tuple(courier_new), "Delhivery Surface Standard", [order[0]], "(" + str(order[0]) + ")", backup_param=False)
                except Exception as e:
                    logger.error("Couldn't assign backup courier for: " + str(order[0]) + "\nError: " + str(e.args))
                    pass

                continue

            zone = None
            try:
                zone = get_delivery_zone(pickup_point[8], order[18])
            except Exception as e:
                logger.error("couldn't find zone: " + str(order[0]) + "\nError: " + str(e))

            time_2_days = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=1)
            if order[47] and not (order[50] and order[2] < time_2_days):
                if order[26].lower() == 'cod' and not order[42] and order[43]:
                    continue
                if order[26].lower() == 'cod' and not order[43]:
                    if order[26].lower() == 'cod' and not order[43]:
                        try:  ## Cod confirmation  text
                            sms_to_key, sms_body_key, customer_phone, sms_body_key_data = cod_verification_text(
                                order, exotel_idx, cur)
                            if not order[53]:
                                exotel_sms_data[sms_to_key] = customer_phone
                                exotel_sms_data[sms_body_key] = sms_body_key_data
                                exotel_idx += 1
                        except Exception as e:
                            logger.error(
                                "Cod confirmation not sent. Order id: " + str(order[0]))
                        continue

            if order[0] > last_shipped_order_id:
                last_shipped_order_id = order[0]

            fulfillment_id = None
            tracking_link = None
            try:
                package_string = ""
                package_quantity = 0
                for idx, prod in enumerate(order[40]):
                    package_string += prod + " (" + str(order[35][idx]) + ") + "
                    package_quantity += order[35][idx]
                package_string += "Shipping"

                dimensions = order[33][0]
                dimensions['length'] = dimensions['length'] * order[35][0]
                weight = order[34][0] * order[35][0]
                volumetric_weight = (dimensions['length'] * dimensions['breadth'] * dimensions['height']) / 5000
                for idx, dim in enumerate(order[33]):
                    if idx == 0:
                        continue
                    dim['length'] += dim['length'] * (order[35][idx])
                    volumetric_weight += (dim['length'] * dim['breadth'] * dim['height']) / 5000
                    weight += order[34][idx] * (order[35][idx])
                if dimensions['length'] and dimensions['breadth']:
                    dimensions['height'] = round(
                        (volumetric_weight * 5000) / (dimensions['length'] * dimensions['breadth']))

                customer_phone = order[21].replace(" ", "")
                customer_phone = "0" + customer_phone[-10:]

                customer_name = order[13]
                if order[14]:
                    customer_name += " " + order[14]

                pickup_address = pickup_point[4]
                if pickup_point[5]:
                    pickup_address += pickup_point[5]

                customer_address = order[15]
                if order[16]:
                    customer_address += order[16]

                rto_address = pickup_point[13]
                if pickup_point[14]:
                    rto_address += pickup_point[14]
                if order[26].lower() == "cod":
                    last_assigned_awb_cod += 1
                    last_assigned_awb = last_assigned_awb_cod
                else:
                    last_assigned_awb_ppd += 1
                    last_assigned_awb = last_assigned_awb_ppd
                order_type = ""
                if order[26].lower() in ("cod", "cash on delivery"):
                    order_type = "COD"
                if order[26].lower() in ("prepaid", "paid"):
                    order_type = "PPD"
                json_input = {
                        "PRODUCT": order_type,
                        "ORDER_NUMBER": order[1],
                        "AWB_NUMBER": str(last_assigned_awb),
                        "PICKUP_NAME": pickup_point[9],
                        "PICKUP_MOBILE": pickup_point[3][-10:],
                        "PICKUP_PHONE": pickup_point[3][-10:],
                        "PICKUP_ADDRESS_LINE1": pickup_address,
                        "PICKUP_ADDRESS_LINE2": "",
                        "PICKUP_PINCODE": pickup_point[8],
                        "CONSIGNEE": customer_name,
                        "CONSIGNEE_ADDRESS1": customer_address,
                        "CONSIGNEE_ADDRESS2": "",
                        "CONSIGNEE_ADDRESS3": "",
                        "DESTINATION_CITY": order[17],
                        "STATE": order[19],
                        "MOBILE": customer_phone[-10:],
                        "TELEPHONE": customer_phone[-10:],
                        "PINCODE": order[18],
                        "ITEM_DESCRIPTION": package_string,
                        "PIECES": package_quantity,
                        "RETURN_NAME": pickup_point[18],
                        "RETURN_MOBILE": pickup_point[12][-10:],
                        "RETURN_PHONE": pickup_point[12][-10:],
                        "RETURN_ADDRESS_LINE1": rto_address,
                        "RETURN_ADDRESS_LINE2": "",
                        "RETURN_PINCODE": pickup_point[17],
                        "ACTUAL_WEIGHT": sum(order[34]),
                        "VOLUMETRIC_WEIGHT": volumetric_weight,
                        "LENGTH": dimensions['length'],
                        "BREADTH": dimensions['breadth'],
                        "HEIGHT": dimensions['height'],
                        "DG_SHIPMENT": "false",
                        "DECLARED_VALUE": order[27]}

                if order[26].lower() == "cod":
                    json_input["COLLECTABLE_VALUE"] = order[27]
                else:
                    json_input["COLLECTABLE_VALUE"] = 0

                ecom_url = courier[16] + "/apiv3/manifest_awb/"
                req = requests.post(ecom_url,  data={"username": courier[14] , "password": courier[15],
                                                    "json_input": json.dumps([json_input])})
                while req.json()['shipments'][0]['reason'] == 'INCORRECT_AWB_NUMBER':
                    last_assigned_awb += 1
                    json_input['AWB_NUMBER'] = str(last_assigned_awb)
                    req = requests.post(ecom_url, data={"username": courier[14], "password": courier[15],
                                                        "json_input": json.dumps(json_input)})
                return_data_raw = req.json()
                insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                                                                dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, 
                                                                                                channel_fulfillment_id, tracking_link, zone)
                                                                                                VALUES  %s RETURNING id;"""
                if return_data_raw['shipments'][0]['success']:
                    order_status_change_ids.append(order[0])

                    data_tuple = tuple([(
                        return_data_raw['shipments'][0]['awb'],
                        return_data_raw['shipments'][0]['reason'],
                        order[0], pickup_point[1], courier[9], json.dumps(dimensions), volumetric_weight, weight,
                        "", pickup_point[2], "", fulfillment_id, tracking_link, zone)])
                    client_name = str(order[51])
                    customer_phone = order[5].replace(" ", "")
                    customer_phone = "0" + customer_phone[-10:]

                    sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                    sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                    exotel_sms_data[sms_to_key] = customer_phone
                    try:
                        tracking_link_wareiq = "http://webapp.wareiq.com/tracking/" + str(
                            return_data_raw['shipments'][0]['awb'])
                        """
                        short_url = requests.get(
                            "https://cutt.ly/api/api.php?key=f445d0bb52699d2f870e1832a1f77ef3f9078&short=%s" % tracking_link_wareiq)
                        short_url_track = short_url.json()['url']['shortLink']
                        """
                        exotel_sms_data[
                            sms_body_key] = "Received: Your order from %s. Track here: %s . Thanks!" % (client_name, tracking_link_wareiq)
                    except Exception:
                        pass
                    exotel_idx += 1

                else:
                    cur.execute("select * from client_couriers where client_prefix=%s and priority=%s;",
                                (courier[1], courier[3] + 1))
                    qs = cur.fetchone()
                    if not (qs and backup_param):
                        insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                                dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, zone)
                                                                VALUES  %s"""
                        insert_shipments_data_tuple = list()
                        insert_shipments_data_tuple.append(("", "Fail", order[0], None,
                                                            None, None, None, None, "Pincode not serviceable", None,
                                                            None, zone), )
                        cur.execute(insert_shipments_data_query, tuple(insert_shipments_data_tuple))
                    continue

                cur.execute(insert_shipments_data_query, data_tuple)
                ship_temp = cur.fetchone()
                order_status_add_query = """INSERT INTO order_status (order_id, courier_id, shipment_id, 
                                            status_code, status, status_text, location, location_city, 
                                            status_time) VALUES %s"""

                order_status_add_tuple = [(order[0], courier[9],
                                           ship_temp[0], "UD", "Received", "Consignment Manifested",
                                           pickup_point[6], pickup_point[6], datetime.utcnow() + timedelta(hours=5.5))]

                cur.execute(order_status_add_query, tuple(order_status_add_tuple))

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


def ship_bluedart_orders(cur, courier, courier_name, order_ids, order_id_tuple, backup_param=True):
    exotel_idx = 0
    exotel_sms_data = {
        'From': 'LM-WAREIQ'
    }
    if courier_name and order_ids:
        orders_to_ship_query = get_orders_to_ship_query.replace("__ORDER_SELECT_FILTERS__",
                                                                """and aa.id in %s""" % order_id_tuple)
    else:
        orders_to_ship_query = get_orders_to_ship_query.replace("__ORDER_SELECT_FILTERS__", """and aa.status='NEW'
                                                                                            and ll.id is null""")
    get_orders_data_tuple = (courier[1], courier[1])
    if courier[3] == 2:
        orders_to_ship_query = orders_to_ship_query.replace('__PRODUCT_FILTER__',
                                                            "and ship_courier[1]='%s'" % courier[10])
    else:
        orders_to_ship_query = orders_to_ship_query.replace('__PRODUCT_FILTER__', '')

    cur.execute(orders_to_ship_query, get_orders_data_tuple)
    all_orders = cur.fetchall()
    pickup_point_order_dict = dict()
    for order in all_orders:
        if order[41]:
            if order[41] not in pickup_point_order_dict:
                pickup_point_order_dict[order[41]] = [order]
            else:
                pickup_point_order_dict[order[41]].append(order)

    bluedart_url = courier[16] + "/Ver1.9/ShippingAPI/WayBill/WayBillGeneration.svc?wsdl"
    waybill_client = Client(bluedart_url)
    check_url = "https://netconnect.bluedart.com/Ver1.9/ShippingAPI/Finder/ServiceFinderQuery.svc?wsdl"
    pincode_client = Client(check_url)

    for pickup_id, all_new_orders in pickup_point_order_dict.items():

        last_shipped_order_id = 0
        pickup_points_tuple = (pickup_id,)
        cur.execute(get_pickup_points_query, pickup_points_tuple)
        order_status_change_ids = list()

        pickup_point = cur.fetchone()  # change this as we get to dynamic pickups

        login_id = courier[15].split('|')[0]
        customer_code = courier[15].split('|')[1]
        area_code = courier[15].split('|')[2]
        client_profile = {
                        "LoginID": login_id,
                        "LicenceKey": courier[14],
                        "Api_type": "S",
                        "Version": "1.9"
                    }
        for order in all_new_orders:

            zone = None
            try:
                zone = get_delivery_zone(pickup_point[8], order[18])
            except Exception as e:
                logger.error("couldn't find zone: " + str(order[0]) + "\nError: " + str(e))

            time_2_days = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=1)
            if order[47] and not (order[50] and order[2] < time_2_days):
                if order[26].lower() == 'cod' and not order[42] and order[43]:
                    continue  # change this to continue later
                if order[26].lower() == 'cod' and not order[43]:
                    try:  ## Cod confirmation  text
                        sms_to_key, sms_body_key, customer_phone, sms_body_key_data = cod_verification_text(
                            order, exotel_idx, cur)
                        if not order[53]:
                            exotel_sms_data[sms_to_key] = customer_phone
                            exotel_sms_data[sms_body_key] = sms_body_key_data
                            exotel_idx += 1
                    except Exception as e:
                        logger.error(
                            "Cod confirmation not sent. Order id: " + str(order[0]))
                    continue
            if order[0] > last_shipped_order_id:
                last_shipped_order_id = order[0]
            try:
                # check delhivery pincode serviceability

                request_data = {
                    'pinCode': str(order[18]),
                    "profile": client_profile
                }
                req = pincode_client.service.GetServicesforPincode(**request_data)

                if not (req['ApexInbound'] == 'Yes' or req['eTailCODAirInbound'] == 'Yes' or req['eTailPrePaidAirInbound'] == 'Yes'):
                    cur.execute("select * from client_couriers where client_prefix=%s and priority=%s;",
                                (courier[1], courier[3] + 1))
                    qs = cur.fetchone()
                    if not (qs and backup_param):
                        insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                        dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, zone)
                                                        VALUES  %s"""
                        insert_shipments_data_tuple = list()
                        insert_shipments_data_tuple.append(("", "Fail", order[0], None,
                                                            None, None, None, None, "Pincode not serviceable", None,
                                                            None, zone), )
                        cur.execute(insert_shipments_data_query, tuple(insert_shipments_data_tuple))
                    continue

                fulfillment_id = None
                tracking_link = None

                shipper = dict()
                consignee = dict()
                services = dict()
                return_address = dict()

                customer_phone = order[21].replace(" ", "")
                customer_phone = "0" + customer_phone[-10:]

                customer_name = order[13]
                if order[14]:
                    customer_name += " " + order[14]

                customer_address = order[15]
                if order[16]:
                    customer_address += order[16]

                consignee['ConsigneeName'] = customer_name
                consignee['ConsigneeAddress1'] = customer_address
                consignee['ConsigneePincode'] = str(order[18])
                consignee['ConsigneeMobile'] = customer_phone

                shipper['CustomerCode'] = customer_code
                shipper['OriginArea'] = area_code
                shipper['CustomerName'] = courier[1]

                pickup_address = pickup_point[4]
                if pickup_point[5]:
                    pickup_address += pickup_point[5]

                rto_address = pickup_point[13]
                if pickup_point[14]:
                    rto_address += pickup_point[14]

                shipper['CustomerAddress1'] = pickup_address
                shipper['CustomerPincode'] = str(pickup_point[8])
                shipper['CustomerMobile'] = str(pickup_point[3])

                return_address['ReturnAddress1'] = rto_address
                return_address['ReturnPincode'] = str(pickup_point[17])
                return_address['ReturnMobile'] = str(pickup_point[12])

                package_string = ""
                package_quantity = 0
                for idx, prod in enumerate(order[40]):
                    package_string += prod + " (" + str(order[35][idx]) + ") + "
                    package_quantity += order[35][idx]
                package_string += "Shipping"

                services['ProductCode'] = 'A'
                services['ProductType'] = 'Dutiables'
                services['DeclaredValue'] = order[27]
                services['ItemCount'] = 1
                services['CreditReferenceNo'] = order[9] + str(order[10]) + str(order[1])

                if order[26].lower() == "cod":
                    services["SubProductCode"] = "C"
                    services["CollectableAmount"] = order[27]
                elif order[26].lower() in ("prepaid", "pre-paid"):
                    services["SubProductCode"] = "P"
                else:
                    pass

                time_now = datetime.utcnow() + timedelta(hours=5.5)
                if time_now.hour > 14:
                    pickup_time = time_now + timedelta(days=1)
                else:
                    pickup_time = time_now

                services['PickupDate'] = pickup_time.strftime('%Y-%m-%d')
                services['PickupTime'] = "1400"

                dimensions = order[33][0]
                dimensions['length'] = dimensions['length'] * order[35][0]
                weight = order[34][0] * order[35][0]
                volumetric_weight = (dimensions['length'] * dimensions['breadth'] * dimensions['height']) / 5000
                for idx, dim in enumerate(order[33]):
                    if idx == 0:
                        continue
                    dim['length'] += dim['length'] * (order[35][idx])
                    volumetric_weight += (dim['length'] * dim['breadth'] * dim['height']) / 5000
                    weight += order[34][idx] * (order[35][idx])
                if dimensions['length'] and dimensions['breadth']:
                    dimensions['height'] = round(
                        (volumetric_weight * 5000) / (dimensions['length'] * dimensions['breadth']))

                services['ActualWeight'] = weight
                services['PieceCount'] = 1
                services['Dimensions'] = {"Dimension": {"Length": dimensions['length'], "Breadth": dimensions['breadth'],
                                                        "Height": dimensions['height'], "Count": 1}}
                services['itemdtl'] = {"ItemDetails": {"ItemID": str(order[1]),"ItemName": package_string, "ItemValue": order[27]}}

                request_data = {
                    "Request": {'Shipper': shipper, 'Consignee': consignee, 'Services': services, 'Returnadds': return_address},
                    "Profile": client_profile
                }

                req = waybill_client.service.GenerateWayBill(**request_data)
                insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                                                                            dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, 
                                                                                                            channel_fulfillment_id, tracking_link, zone)
                                                                                                            VALUES  %s RETURNING id;"""
                if req['AWBNo']:
                    order_status_change_ids.append(order[0])

                    data_tuple = tuple([(
                        req['AWBNo'],"",order[0], pickup_point[1], courier[9], json.dumps(dimensions), volumetric_weight, weight,
                        "", pickup_point[2], "", fulfillment_id, tracking_link, zone)])
                    client_name = str(order[51])
                    customer_phone = order[5].replace(" ", "")
                    customer_phone = "0" + customer_phone[-10:]

                    sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                    sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                    exotel_sms_data[sms_to_key] = customer_phone
                    try:
                        tracking_link_wareiq = "http://webapp.wareiq.com/tracking/" + str(
                            req['AWBNo'])
                        """
                        short_url = requests.get(
                            "https://cutt.ly/api/api.php?key=f445d0bb52699d2f870e1832a1f77ef3f9078&short=%s" % tracking_link_wareiq)
                        short_url_track = short_url.json()['url']['shortLink']
                        """
                        exotel_sms_data[
                            sms_body_key] = "Received: Your order from %s. Track here: %s . Thanks!" % (client_name, tracking_link_wareiq)
                    except Exception:
                        pass

                    exotel_idx += 1

                else:
                    cur.execute("select * from client_couriers where client_prefix=%s and priority=%s;",
                                (courier[1], courier[3] + 1))
                    qs = cur.fetchone()
                    if not (qs and backup_param):
                        insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                                            dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, zone)
                                                                            VALUES  %s"""
                        insert_shipments_data_tuple = list()
                        insert_shipments_data_tuple.append(("", "Fail", order[0], None,
                                                            None, None, None, None, "Pincode not serviceable", None,
                                                            None, zone), )
                        cur.execute(insert_shipments_data_query, tuple(insert_shipments_data_tuple))
                    continue

                cur.execute(insert_shipments_data_query, data_tuple)
                ship_temp = cur.fetchone()
                order_status_add_query = """INSERT INTO order_status (order_id, courier_id, shipment_id, 
                                                        status_code, status, status_text, location, location_city, 
                                                        status_time) VALUES %s"""

                order_status_add_tuple = [(order[0], courier[9],
                                           ship_temp[0], "UD", "Received", "Consignment Manifested",
                                           pickup_point[6], pickup_point[6],
                                           datetime.utcnow() + timedelta(hours=5.5))]

                cur.execute(order_status_add_query, tuple(order_status_add_tuple))

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


def ship_vinculum_orders(cur, courier, courier_name, order_ids, order_id_tuple):
    exotel_idx = 0
    exotel_sms_data = {
        'From': 'LM-WAREIQ'
    }
    get_orders_data_tuple = (courier[1], courier[1])
    if courier[3] == 2:
        orders_to_ship_query = get_orders_to_ship_query.replace('__PRODUCT_FILTER__',
                                                                "and ship_courier[1]='%s'" % courier[10])
    else:
        orders_to_ship_query = get_orders_to_ship_query.replace('__PRODUCT_FILTER__', '')
    cur.execute(orders_to_ship_query, get_orders_data_tuple)
    all_orders = cur.fetchall()
    pickup_point_order_dict = dict()
    for order in all_orders:
        if order[41] not in pickup_point_order_dict:
            pickup_point_order_dict[order[41]] = [order]
        else:
            pickup_point_order_dict[order[41]].append(order)

    for pickup_id, all_new_orders in pickup_point_order_dict.items():
        last_shipped_order_id = 0

        order_status_change_ids = list()
        pickup_points_tuple = (pickup_id,)
        cur.execute(get_pickup_points_query, pickup_points_tuple)

        pickup_point = cur.fetchone()

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        for order in all_new_orders:
            try:
                if order[26].lower() == 'cod' and not order[43]:
                    cod_confirmation_link = "http://track.wareiq.com/core/v1/passthru/cod?CustomField=%s" % str(
                        order[0])
                    """
                    short_url = requests.get(
                        "https://cutt.ly/api/api.php?key=f445d0bb52699d2f870e1832a1f77ef3f9078&short=%s" % cod_confirmation_link)
                    short_url_track = short_url.json()['url']['shortLink']
                    """
                    insert_cod_ver_tuple = (order[0], cod_confirmation_link, datetime.now())
                    cur.execute(
                        "INSERT INTO cod_verification (order_id, verification_link, date_created) VALUES (%s,%s,%s);",
                        insert_cod_ver_tuple)
                    client_name = str(order[51])
                    customer_phone = order[5].replace(" ", "")
                    customer_phone = "0" + customer_phone[-10:]

                    sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                    sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                    exotel_sms_data[sms_to_key] = customer_phone

                    exotel_sms_data[
                        sms_body_key] = "Dear Customer, You recently placed an order from %s with order id %s. " \
                                        "Please click on the link (%s) to verify. " \
                                        "Your order will be shipped soon after confirmation." % (
                                            client_name, str(order[1]), cod_confirmation_link)

                    exotel_idx += 1

                form_data = {"RequestBody": json.dumps({
                    "order_no": order[1],
                    "statuses": [""],
                    "order_location": "DWH",
                    "date_from": "",
                    "date_to": "",
                    "pageNumber": ""
                }),
                    "ApiKey": courier[8].split('|')[0],
                    "ApiOwner": courier[8].split('|')[1],
                }

                req = requests.post("https://dtdc.vineretail.com/RestWS/api/eretail/v1/order/shipDetail",
                                    headers=headers, data=form_data)
                return_data_raw = req.json()['response']

                dimensions = order[33][0]
                dimensions['length'] = dimensions['length'] * order[35][0]
                weight = order[34][0] * order[35][0]
                for idx, dim in enumerate(order[33]):
                    if idx == 0:
                        continue
                    dimensions['length'] += dim['length'] * (order[35][idx])
                    weight += order[34][idx] * (order[35][idx])

                volumetric_weight = (dimensions['length'] * dimensions['breadth'] * dimensions['height']) / 5000

                customer_name = order[13]
                if order[14]:
                    customer_name += " " + order[14]

                insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                            dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, 
                                                            channel_fulfillment_id, tracking_link)
                                                            VALUES  %s RETURNING id;"""

                if return_data_raw.get("responselist"):
                    if order[0] > last_shipped_order_id:
                        last_shipped_order_id = order[0]
                    order_status_change_ids.append(order[0])
                    return_data = return_data_raw['responselist'][0]
                    data_tuple = tuple([(
                        return_data['awbno'], "Success", order[0], pickup_point[1],
                        courier[9], json.dumps(dimensions), volumetric_weight, weight, "", pickup_point[2],
                        "", None, None)])
                    client_name = str(order[51])
                    customer_phone = order[5].replace(" ", "")
                    customer_phone = "0" + customer_phone[-10:]

                    sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                    sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                    exotel_sms_data[sms_to_key] = customer_phone
                    try:
                        tracking_link_wareiq = "http://webapp.wareiq.com/tracking/" + str(return_data['awbno'])
                        """
                        short_url = requests.get(
                            "https://cutt.ly/api/api.php?key=f445d0bb52699d2f870e1832a1f77ef3f9078&short=%s" % tracking_link_wareiq)
                        short_url_track = short_url.json()['url']['shortLink']
                        """
                        exotel_sms_data[
                            sms_body_key] = "Received: Your order from %s. Track here: %s. Thanks!" % (client_name, tracking_link_wareiq)
                    except Exception:
                        pass

                    exotel_idx += 1

                    cur.execute(insert_shipments_data_query, data_tuple)
                    ship_temp = cur.fetchone()
                    order_status_add_query = """INSERT INTO order_status (order_id, courier_id, shipment_id, 
                                                            status_code, status, status_text, location, location_city, 
                                                            status_time) VALUES %s"""

                    order_status_add_tuple = [(order[0], courier[9],
                                               ship_temp[0], "UD", "Received", "Consignment Manifested",
                                               pickup_point[6], pickup_point[6],
                                               datetime.utcnow() + timedelta(hours=5.5))]

                    cur.execute(order_status_add_query, tuple(order_status_add_tuple))

            except Exception as e:
                logger.error("couldn't assign order: " + str(order[1]) + "\nError: " + str(e))

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