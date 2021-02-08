import psycopg2, requests, os, json, pytz
import logging
from datetime import datetime, timedelta
from requests_oauthlib.oauth1_session import OAuth1Session
from zeep import Client
from app.db_utils import DbConnection
#from fedex.config import FedexConfig

from .queries import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)

conn = DbConnection.get_db_connection_instance()
conn_2 = DbConnection.get_pincode_db_connection_instance()
cur_2 = conn_2.cursor()


def ship_orders(courier_name=None, order_ids=None, force_ship=None):
    cur = conn.cursor()
    order_id_tuple = "()"
    if courier_name and order_ids:  # creating courier details list for manual shipping
        if len(order_ids) == 1:
            order_id_tuple = "('" + str(order_ids[0]) + "')"
        else:
            order_id_tuple = str(tuple(order_ids))
        cur.execute("""DELETE FROM 	order_status where order_id in %s;
                           DELETE FROM shipments where order_id in %s;""" % (order_id_tuple, order_id_tuple))
        conn.commit()
        cur.execute("SELECT DISTINCT(client_prefix) from orders where id in %s" % order_id_tuple)
        client_list = cur.fetchall()
        cur.execute("""SELECT bb.id,bb.courier_name,bb.logo_url,bb.date_created,bb.date_updated,bb.api_key,bb.api_password,
                        bb.api_url FROM master_couriers bb WHERE courier_name='%s'""" % courier_name)
        courier_details = cur.fetchone()
        all_couriers = list()
        for client in client_list:
            all_couriers.append((None, client[0], None, 1, None, None, None, None, "") + courier_details)

    else:
        cur.execute(delete_failed_shipments_query)
        time_now = datetime.utcnow()
        if time_now.hour == 22 and 0 < time_now.minute < 30:
            time_now = time_now - timedelta(days=30)
            cur.execute("""delete from shipments where order_id in 
                                (select id from orders where order_date>%s and status='NEW')
                                and remark = 'Pincode not serviceable'""", (time_now,))
        conn.commit()
        cur.execute(fetch_client_couriers_query)
        all_couriers = cur.fetchall()

    for courier in all_couriers:
        if courier[10].startswith('Delhivery'):
            ship_delhivery_orders(cur, courier, courier_name, order_ids, order_id_tuple, force_ship=force_ship)

        elif courier[10] == "Shadowfax":
            ship_shadowfax_orders(cur, courier, courier_name, order_ids, order_id_tuple, force_ship=force_ship)

        elif courier[10].startswith('Xpressbees'):
            ship_xpressbees_orders(cur, courier, courier_name, order_ids, order_id_tuple, force_ship=force_ship)

        elif courier[10].startswith('Bluedart'):
            ship_bluedart_orders(cur, courier, courier_name, order_ids, order_id_tuple, force_ship=force_ship)

        elif courier[10].startswith('Ecom'):
            ship_ecom_orders(cur, courier, courier_name, order_ids, order_id_tuple, force_ship=force_ship)

        elif courier[10].startswith('Self Ship'):
            ship_selfshp_orders(cur, courier, courier_name, order_ids, order_id_tuple, force_ship=force_ship)

        # elif courier[10].startswith('SDD'):
        #     ship_sdd_orders(cur, courier, courier_name, order_ids, order_id_tuple, force_ship=force_ship)

        # elif courier[10].startswith('FedEx'):
        #     ship_fedex_orders(cur, courier, courier_name, order_ids, order_id_tuple, force_ship=force_ship)

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


def ship_delhivery_orders(cur, courier, courier_name, order_ids, order_id_tuple, backup_param=True, force_ship=None):
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

        last_invoice_no = pickup_point[22] if pickup_point[22] else 0

        headers = {"Authorization": "Token " + courier[14],
                   "Content-Type": "application/json"}
        for order in all_new_orders:
            try:
                if not order[54]:
                    last_invoice_no = invoice_order(cur, last_invoice_no, pickup_point[23], order[0], pickup_id)
                if order[26].lower() == 'cod' and not order[27] and not force_ship:
                    continue
                zone = None
                try:
                    zone = get_delivery_zone(pickup_point[8], order[18])
                except Exception as e:
                    logger.error("couldn't find zone: " + str(order[0]) + "\nError: " + str(e))

                orders_dict[str(order[0])] = (order[0], order[33], order[34], order[35],
                                              order[36], order[37], order[38], order[39],
                                              order[5], order[9], order[45], order[46],
                                              order[51], order[52], zone, order[54])

                if order[17].lower() in ("bengaluru", "bangalore", "banglore") and courier[1] in ("SOHOMATTRESS",) and \
                        order[26].lower() != 'pickup' and not force_ship:
                    continue

                if courier[1] == "ZLADE" and courier[10] == "Delhivery Surface Standard" and zone and zone not in (
                'A', 'B') and order[26].lower() != 'pickup' and not force_ship:
                    continue

                if not order[52]:
                    weight = order[34][0] * order[35][0]
                    volumetric_weight = (order[33][0]['length'] * order[33][0]['breadth'] * order[33][0]['height']) * \
                                        order[35][0] / 5000
                    for idx, dim in enumerate(order[33]):
                        if idx == 0:
                            continue
                        volumetric_weight += (dim['length'] * dim['breadth'] * dim['height']) * order[35][idx] / 5000
                        weight += order[34][idx] * (order[35][idx])
                else:
                    weight = float(order[52])
                    volumetric_weight = float(order[52])

                if courier[10] == "Delhivery Surface Standard" and not force_ship:
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
                            ship_delhivery_orders(cur, tuple(courier_new), new_courier_name, [order[0]],
                                                  "(" + str(order[0]) + ")", backup_param=False)
                        except Exception as e:
                            logger.error(
                                "Couldn't assign backup courier for: " + str(order[0]) + "\nError: " + str(e.args))
                            pass

                        continue

                time_2_days = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=1)
                if order[47] and not (order[50] and order[2] < time_2_days) and not force_ship:
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
                    cur.execute("select * from client_couriers where client_prefix=%s and priority=%s;",
                                (courier[1], courier[3] + 1))
                    qs = cur.fetchone()
                    if not (qs and backup_param) or force_ship:
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

                shipping_phone = order[21] if order[21] else order[5]
                shipping_phone = ''.join(e for e in str(shipping_phone) if e.isalnum())
                shipping_phone = "0" + shipping_phone[-10:]
                shipment_data = dict()
                shipment_data['city'] = order[17]
                shipment_data['weight'] = weight
                shipment_data['add'] = order[15]
                if order[16]:
                    shipment_data['add'] += '\n' + order[16]
                shipment_data['phone'] = shipping_phone
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
            if req.json().get('rmk') == 'ClientWarehouse matching query does not exist.':
                pickup_phone = pickup_point[3].replace(" ", "")
                pickup_phone = pickup_phone[-10:]
                warehouse_creation = {"phone": pickup_phone,
                                      "city": pickup_point[6],
                                      "name": pickup_point[9],
                                      "pin": str(pickup_point[8]),
                                      "address": pick_add,
                                      "country": pickup_point[7],
                                      "registered_name": pickup_point[11],
                                      "return_address": str(pickup_point[13]) + str(pickup_point[14]),
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
                                            VALUES  """

        order_status_change_ids = list()
        insert_shipments_data_tuple = list()
        insert_order_status_dict = dict()
        for package in return_data:
            try:
                fulfillment_id = None
                tracking_link = None
                if package['waybill']:

                    order_status_change_ids.append(orders_dict[package['refnum']][0])
                    client_name = str(orders_dict[package['refnum']][12])
                    customer_phone = orders_dict[package['refnum']][8].replace(" ", "")
                    customer_phone = "0" + customer_phone[-10:]

                    sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                    sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                    if orders_dict[package['refnum']][11]==7:
                        push_awb_easyecom(orders_dict[package['refnum']][7],
                                          orders_dict[package['refnum']][4],
                                          package['waybill'], courier, cur, orders_dict[package['refnum']][9])

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

                if 'COD' in remark or 'blocked' in remark:
                    continue

                if not orders_dict[package['refnum']][13]:
                    dimensions = orders_dict[package['refnum']][1][0]
                    weight = orders_dict[package['refnum']][2][0] * orders_dict[package['refnum']][3][0]
                    volumetric_weight = (dimensions['length'] * dimensions['breadth'] * dimensions['height']) * \
                                        orders_dict[package['refnum']][3][0] / 5000
                    for idx, dim in enumerate(orders_dict[package['refnum']][1]):
                        if idx == 0:
                            continue
                        volumetric_weight += (dim['length'] * dim['breadth'] * dim['height']) * \
                                             orders_dict[package['refnum']][3][idx] / 5000
                        weight += orders_dict[package['refnum']][2][idx] * (orders_dict[package['refnum']][3][idx])

                    if dimensions['length'] and dimensions['breadth']:
                        dimensions['height'] = round(
                            (volumetric_weight * 5000) / (dimensions['length'] * dimensions['breadth']))
                else:
                    dimensions = {"length": 1, "breadth": 1, "height": 1}
                    weight = float(orders_dict[package['refnum']][13])
                    volumetric_weight = float(orders_dict[package['refnum']][13])

                data_tuple = (package['waybill'], package['status'], orders_dict[package['refnum']][0], pickup_point[1],
                              courier[9], json.dumps(dimensions), volumetric_weight, weight, remark, pickup_point[2],
                              package['sort_code'], fulfillment_id, tracking_link, orders_dict[package['refnum']][14])
                insert_shipments_data_tuple.append(data_tuple)
                insert_shipments_data_query += "%s,"
                insert_order_status_dict[package['waybill']] = [orders_dict[package['refnum']][0], courier[9],
                                                                None, "UD", "Received", "Consignment Manifested",
                                                                pickup_point[6], pickup_point[6],
                                                                datetime.utcnow() + timedelta(hours=5.5)]

            except Exception as e:
                logger.error("Order not shipped. Remarks: " + str(package['remarks']) + "\nError: " + str(e.args[0]))

        if insert_shipments_data_tuple:
            insert_shipments_data_tuple = tuple(insert_shipments_data_tuple)
            insert_shipments_data_query = insert_shipments_data_query.strip(",")
            insert_shipments_data_query += " RETURNING id,awb;"
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

        cur.execute("UPDATE client_pickups SET invoice_last=%s WHERE id=%s;", (last_invoice_no, pickup_id))

        conn.commit()

    if exotel_idx:
        logger.info("Sending messages...count:" + str(exotel_idx))
        try:
            lad = requests.post(
                'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend',
                data=exotel_sms_data)
        except Exception as e:
            logger.error("messages not sent." + "   Error: " + str(e.args[0]))


def ship_shadowfax_orders(cur, courier, courier_name, order_ids, order_id_tuple, backup_param=True, force_ship=None):
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
        last_invoice_no = pickup_point[22] if pickup_point[22] else 0
        for order in all_new_orders:
            if not order[54]:
                last_invoice_no = invoice_order(cur, last_invoice_no, pickup_point[23], order[0], pickup_id)
            if order[26].lower() == 'cod' and not order[27] and not force_ship:
                continue
            if force_ship and order[26].lower() == 'pickup':
                continue
            zone = None
            try:
                zone = get_delivery_zone(pickup_point[8], order[18])
            except Exception as e:
                logger.error("couldn't find zone: " + str(order[0]) + "\nError: " + str(e))
            time_2_days = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=1)
            if order[47] and not (order[50] and order[2] < time_2_days) and not force_ship:
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
                    if not (qs and backup_param) or force_ship:
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

                shipping_phone = order[21] if order[21] else order[5]
                shipping_phone = ''.join(e for e in str(shipping_phone) if e.isalnum())
                shipping_phone = "0" + shipping_phone[-10:]

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
                        "contact": shipping_phone,
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
                            sms_body_key] = "Received: Your order from %s. Track here: %s . Thanks!" % (
                        client_name, tracking_link_wareiq)
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

        cur.execute("UPDATE client_pickups SET invoice_last=%s WHERE id=%s;", (last_invoice_no, pickup_id))

        conn.commit()

    if exotel_idx:
        logger.info("Sending messages...count:" + str(exotel_idx))
        try:
            lad = requests.post(
                'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend',
                data=exotel_sms_data)
        except Exception as e:
            logger.error("messages not sent." + "   Error: " + str(e.args[0]))


def ship_xpressbees_orders(cur, courier, courier_name, order_ids, order_id_tuple, backup_param=True, force_ship=None):
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

        last_invoice_no = pickup_point[22] if pickup_point[22] else 0
        for order in all_new_orders:
            if not order[54]:
                last_invoice_no = invoice_order(cur, last_invoice_no, pickup_point[23], order[0], pickup_id)
            if order[26].lower() == 'cod' and not order[27] and not force_ship:
                continue
            if force_ship and order[26].lower() == 'pickup':
                continue
            if order[26].lower() == 'pickup':
                try:
                    cur.execute("""SELECT id, courier_name, logo_url, date_created, date_updated, api_key, 
                                                                        api_password, api_url FROM master_couriers
                                                                        WHERE courier_name='%s'""" % "Delhivery Surface Standard")
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
                    ship_delhivery_orders(cur, tuple(courier_new), "Delhivery Surface Standard", [order[0]],
                                          "(" + str(order[0]) + ")", backup_param=False)
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

            if courier[10] == "Xpressbees Surface" and weight and volumetric_weight and not force_ship:
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
            if order[47] and not (order[50] and order[2] < time_2_days) and not force_ship:
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
                    dim['length'] = dim['length'] * (order[35][idx])
                    volumetric_weight += (dim['length'] * dim['breadth'] * dim['height']) / 5000
                    weight += order[34][idx] * (order[35][idx])
                if dimensions['length'] and dimensions['breadth']:
                    dimensions['height'] = round(
                        (volumetric_weight * 5000) / (dimensions['length'] * dimensions['breadth']))

                shipping_phone = order[21] if order[21] else order[5]
                shipping_phone = ''.join(e for e in str(shipping_phone) if e.isalnum())
                shipping_phone = "0" + shipping_phone[-10:]

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
                            "MobileNo": shipping_phone
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

                    if order[46] == 7:
                        push_awb_easyecom(order[39],order[36], return_data_raw['AddManifestDetails'][0]['AWBNo'], courier, cur, order[9])

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
                            sms_body_key] = "Received: Your order from %s. Track here: %s . Thanks!" % (
                        client_name, tracking_link_wareiq)
                    except Exception:
                        pass

                    exotel_idx += 1


                else:
                    cur.execute("select * from client_couriers where client_prefix=%s and priority=%s;",
                                (courier[1], courier[3] + 1))
                    qs = cur.fetchone()
                    if not (qs and backup_param) or force_ship:
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

        cur.execute("UPDATE client_pickups SET invoice_last=%s WHERE id=%s;", (last_invoice_no, pickup_id))

        conn.commit()

    if exotel_idx:
        logger.info("Sending messages...count:" + str(exotel_idx))
        try:
            lad = requests.post(
                'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend',
                data=exotel_sms_data)
        except Exception as e:
            logger.error("messages not sent." + "   Error: " + str(e.args[0]))


def ship_ecom_orders(cur, courier, courier_name, order_ids, order_id_tuple, backup_param=True, force_ship=None):
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

    cur.execute("""select awb from shipments aa
                        left join orders bb on aa.order_id=bb.id
                        left join orders_payments cc on cc.order_id=bb.id
                        where courier_id=%s
                        and payment_mode ilike 'cod'
                        order by aa.id DESC
                        LIMIT 1;""" % str(courier[9]))

    last_assigned_awb_cod = cur.fetchone()[0]
    last_assigned_awb_cod = int(last_assigned_awb_cod)

    cur.execute("""select awb from shipments aa
                        left join orders bb on aa.order_id=bb.id
                        left join orders_payments cc on cc.order_id=bb.id
                        where courier_id=%s
                        and (payment_mode ilike 'prepaid' or payment_mode ilike 'paid')
                        order by aa.id DESC
                        LIMIT 1;""" % str(courier[9]))

    last_assigned_awb_ppd = cur.fetchone()[0]
    last_assigned_awb_ppd = int(last_assigned_awb_ppd)

    for pickup_id, all_new_orders in pickup_point_order_dict.items():
        last_shipped_order_id = 0
        headers = {"Content-Type": "application/json"}
        pickup_points_tuple = (pickup_id,)
        cur.execute(get_pickup_points_query, pickup_points_tuple)
        order_status_change_ids = list()

        pickup_point = cur.fetchone()  # change this as we get to dynamic pickups

        last_invoice_no = pickup_point[22] if pickup_point[22] else 0

        for order in all_new_orders:
            if not order[54]:
                last_invoice_no = invoice_order(cur, last_invoice_no, pickup_point[23], order[0], pickup_id)
            if order[26].lower() == 'cod' and not order[27] and not force_ship:
                continue
            if force_ship and order[26].lower() == 'pickup':
                continue
            if order[26].lower() == 'pickup':
                try:
                    cur.execute("""SELECT id, courier_name, logo_url, date_created, date_updated, api_key, 
                                                                        api_password, api_url FROM master_couriers
                                                                        WHERE courier_name='%s'""" % "Delhivery Surface Standard")
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
                    ship_delhivery_orders(cur, tuple(courier_new), "Delhivery Surface Standard", [order[0]],
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
            if order[47] and not (order[50] and order[2] < time_2_days) and not force_ship:
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

                shipping_phone = order[21] if order[21] else order[5]
                shipping_phone = ''.join(e for e in str(shipping_phone) if e.isalnum())
                shipping_phone = shipping_phone[-10:]

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
                    "PICKUP_PINCODE": str(pickup_point[8]),
                    "CONSIGNEE": customer_name,
                    "CONSIGNEE_ADDRESS1": customer_address,
                    "CONSIGNEE_ADDRESS2": "",
                    "CONSIGNEE_ADDRESS3": "",
                    "DESTINATION_CITY": order[17],
                    "STATE": order[19],
                    "MOBILE": shipping_phone,
                    "TELEPHONE": shipping_phone,
                    "PINCODE": order[18],
                    "ITEM_DESCRIPTION": package_string,
                    "PIECES": package_quantity,
                    "RETURN_NAME": pickup_point[18],
                    "RETURN_MOBILE": pickup_point[12][-10:],
                    "RETURN_PHONE": pickup_point[12][-10:],
                    "RETURN_ADDRESS_LINE1": rto_address,
                    "RETURN_ADDRESS_LINE2": "",
                    "RETURN_PINCODE": str(pickup_point[17]),
                    "ACTUAL_WEIGHT": sum(order[34]),
                    "VOLUMETRIC_WEIGHT": volumetric_weight,
                    "LENGTH": dimensions['length'],
                    "BREADTH": dimensions['breadth'],
                    "HEIGHT": dimensions['height'],
                    "DG_SHIPMENT": "false",
                    "DECLARED_VALUE": order[27]}

                dict2 = {  "INVOICE_NUMBER": str(order[54]) if order[54] else str(last_invoice_no),
                            "INVOICE_DATE": datetime.now().strftime('%Y-%m-%d'),
                            "ITEM_CATEGORY": "ECOMMERCE",
                            "PACKING_TYPE": "Box",
                            "PICKUP_TYPE": "WH",
                            "RETURN_TYPE": "WH",
                            "CONSIGNEE_ADDRESS_TYPE": "HOME",
                            "PICKUP_LOCATION_CODE": pickup_point[9],
                           }

                json_input.update(dict2)
                if order[26].lower() == "cod":
                    json_input["COLLECTABLE_VALUE"] = order[27]
                else:
                    json_input["COLLECTABLE_VALUE"] = 0

                ecom_url = courier[16] + "/apiv3/manifest_awb/"
                req = requests.post(ecom_url, data={"username": courier[14], "password": courier[15],
                                                    "json_input": json.dumps([json_input])})
                if req.json()['shipments'][0]['reason'] == 'INCORRECT_AWB_NUMBER':
                    fetch_awb_url = courier[16] + "/apiv2/fetch_awb/"
                    fetch_awb_req = requests.post(fetch_awb_url, data={"username": courier[14], "password": courier[15],
                                                    "count": 50, "type":json_input['PRODUCT']})
                    json_input['AWB_NUMBER'] = str(fetch_awb_req.json()['awb'][0])
                    req = requests.post(ecom_url, data={"username": courier[14], "password": courier[15],
                                                        "json_input": json.dumps([json_input])})
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

                    if order[46] == 7:
                        push_awb_easyecom(order[39],order[36], return_data_raw['shipments'][0]['awb'], courier, cur, order[9])

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
                            sms_body_key] = "Received: Your order from %s. Track here: %s . Thanks!" % (
                        client_name, tracking_link_wareiq)
                    except Exception:
                        pass
                    exotel_idx += 1

                else:
                    cur.execute("select * from client_couriers where client_prefix=%s and priority=%s;",
                                (courier[1], courier[3] + 1))
                    qs = cur.fetchone()
                    if not (qs and backup_param) or force_ship:
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

        cur.execute("UPDATE client_pickups SET invoice_last=%s WHERE id=%s;", (last_invoice_no, pickup_id))

        conn.commit()

    if exotel_idx:
        logger.info("Sending messages...count:" + str(exotel_idx))
        try:
            lad = requests.post(
                'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend',
                data=exotel_sms_data)
        except Exception as e:
            logger.error("messages not sent." + "   Error: " + str(e.args[0]))


def ship_bluedart_orders(cur, courier, courier_name, order_ids, order_id_tuple, backup_param=True, force_ship=None):
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

        last_invoice_no = pickup_point[22] if pickup_point[22] else 0

        pickup_pincode = str(pickup_point[8]).rstrip() if pickup_point[8] else None
        if pickup_pincode and pickup_pincode in bluedart_area_code_mapping:
            area_code = bluedart_area_code_mapping[pickup_pincode]
        else:
            continue
        login_id = courier[15].split('|')[0]
        customer_code = courier[15].split('|')[1]
        client_profile = {
            "LoginID": login_id,
            "LicenceKey": courier[14],
            "Api_type": "S",
            "Version": "1.3"
        }
        for order in all_new_orders:
            if not order[54]:
                last_invoice_no = invoice_order(cur, last_invoice_no, pickup_point[23], order[0], pickup_id)
            if order[26].lower() == 'cod' and not order[27] and not force_ship:
                continue
            if order[26].lower() == 'pickup':
                continue
            zone = None
            try:
                zone = get_delivery_zone(pickup_point[8], order[18])
            except Exception as e:
                logger.error("couldn't find zone: " + str(order[0]) + "\nError: " + str(e))

            if courier[1] == "ZLADE" and zone in ('A', ) and not force_ship:
                continue

            if order[26].lower() == "prepaid" and courier[1] in ("ACTIFIBER", "BEHIR", "SHAHIKITCHEN", "SUKHILIFE", "ORGANICRIOT") and not force_ship:
                continue

            time_2_days = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=1)
            if order[47] and not (order[50] and order[2] < time_2_days) and not force_ship:
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

                if not (req['ApexInbound'] == 'Yes' or req['eTailCODAirInbound'] == 'Yes' or req[
                    'eTailPrePaidAirInbound'] == 'Yes'):
                    cur.execute("select * from client_couriers where client_prefix=%s and priority=%s;",
                                (courier[1], courier[3] + 1))
                    qs = cur.fetchone()
                    if not (qs and backup_param) or force_ship:
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

                shipping_phone = order[21] if order[21] else order[5]
                shipping_phone = ''.join(e for e in str(shipping_phone) if e.isalnum())
                shipping_phone = "0" + shipping_phone[-10:]

                customer_name = order[13]
                if order[14]:
                    customer_name += " " + order[14]

                customer_address = order[15]
                if order[16]:
                    customer_address += order[16]

                consignee['ConsigneeName'] = customer_name
                consignee['ConsigneeAddress1'] = customer_address
                consignee['ConsigneePincode'] = str(order[18])
                consignee['ConsigneeMobile'] = shipping_phone

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
                shipper['VendorCode'] = (6-len(str(pickup_id)))*"0" + str(pickup_id)

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
                services['CreditReferenceNo'] = str(order[0])

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
                services['Dimensions'] = {
                    "Dimension": {"Length": dimensions['length'], "Breadth": dimensions['breadth'],
                                  "Height": dimensions['height'], "Count": 1}}
                services['itemdtl'] = {
                    "ItemDetails": {"ItemID": str(order[0]), "ItemName": package_string, "ItemValue": order[27]}}

                request_data = {
                    "Request": {'Shipper': shipper, 'Consignee': consignee, 'Services': services,
                                'Returnadds': return_address},
                    "Profile": client_profile
                }

                req = waybill_client.service.GenerateWayBill(**request_data)
                insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                                                                                dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, 
                                                                                                                channel_fulfillment_id, tracking_link, zone)
                                                                                                                VALUES  %s RETURNING id;"""
                if req['AWBNo']:

                    order_status_change_ids.append(order[0])
                    routing_code = str(req['DestinationArea']) + "-" + str(req['DestinationLocation'])
                    data_tuple = tuple([(
                        req['AWBNo'], "", order[0], pickup_point[1], courier[9], json.dumps(dimensions),
                        volumetric_weight, weight,
                        "", pickup_point[2], routing_code, fulfillment_id, tracking_link, zone)])

                    if order[46] == 7:
                        push_awb_easyecom(order[39],order[36], req['AWBNo'], courier, cur, order[9])

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
                            sms_body_key] = "Received: Your order from %s. Track here: %s . Thanks!" % (
                        client_name, tracking_link_wareiq)
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

        cur.execute("UPDATE client_pickups SET invoice_last=%s WHERE id=%s;", (last_invoice_no, pickup_id))

        conn.commit()

    if exotel_idx:
        logger.info("Sending messages...count:" + str(exotel_idx))
        try:
            lad = requests.post(
                'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend',
                data=exotel_sms_data)
        except Exception as e:
            logger.error("messages not sent." + "   Error: " + str(e.args[0]))


def ship_fedex_orders(cur, courier, courier_name, order_ids, order_id_tuple, backup_param=True, force_ship=None):
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

    api_key = courier[14].split('|')[0]
    api_pass = courier[14].split('|')[1]
    account_number = courier[15].split('|')[0]
    meter_number = courier[15].split('|')[1]
    CONFIG_OBJ = FedexConfig(key=api_key,
                             password=api_pass,
                             account_number=account_number,
                             meter_number=meter_number,
                             use_test_server=True)

    for pickup_id, all_new_orders in pickup_point_order_dict.items():

        last_shipped_order_id = 0
        pickup_points_tuple = (pickup_id,)
        cur.execute(get_pickup_points_query, pickup_points_tuple)
        order_status_change_ids = list()

        pickup_point = cur.fetchone()  # change this as we get to dynamic pickups

        last_invoice_no = pickup_point[22] if pickup_point[22] else 0

        for order in all_new_orders:
            if not order[54]:
                last_invoice_no = invoice_order(cur, last_invoice_no, pickup_point[23], order[0], pickup_id)
            if order[26].lower() == 'cod' and not order[27] and not force_ship:
                continue
            if order[26].lower() == 'pickup':
                continue
            zone = None
            try:
                zone = get_delivery_zone(pickup_point[8], order[18])
            except Exception as e:
                logger.error("couldn't find zone: " + str(order[0]) + "\nError: " + str(e))

            if courier[1] == "ZLADE" and zone in ('A', ) and not force_ship:
                continue

            if order[26].lower() == "prepaid" and courier[1] in ("ACTIFIBER", "BEHIR", "SHAHIKITCHEN", "SUKHILIFE", "ORGANICRIOT") and not force_ship:
                continue

            time_2_days = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=1)
            if order[47] and not (order[50] and order[2] < time_2_days) and not force_ship:
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
                from fedex.services.availability_commitment_service import FedexAvailabilityCommitmentRequest
                avc_request = FedexAvailabilityCommitmentRequest(CONFIG_OBJ)
                avc_request.Origin.PostalCode = pickup_point[8]
                avc_request.Origin.CountryCode = 'IN'
                avc_request.Destination.PostalCode = order[18]  # 29631
                avc_request.Destination.CountryCode = 'IN'
                from fedex.services.ship_service import FedexProcessShipmentRequest
                shipment = FedexProcessShipmentRequest(CONFIG_OBJ)

                shipping_phone = order[21] if order[21] else order[5]
                shipping_phone = ''.join(e for e in str(shipping_phone) if e.isalnum())
                shipping_phone = shipping_phone[-10:]

                customer_name = order[13]
                if order[14]:
                    customer_name += " " + order[14]

                pickup_address = pickup_point[4]
                if pickup_point[5]:
                    pickup_address += pickup_point[5]

                customer_address = order[15]
                if order[16]:
                    customer_address += order[16]

                order_type = ""
                if order[26].lower() in ("cod", "cash on delivery"):
                    order_type = "COD"
                if order[26].lower() in ("prepaid", "paid"):
                    order_type = "PREPAID"

                shipment.RequestedShipment.ShipTimestamp = datetime.now().replace(microsecond=0).isoformat()
                shipment.RequestedShipment.DropoffType = 'REGULAR_PICKUP'
                shipment.RequestedShipment.ServiceType = 'STANDARD_OVERNIGHT'
                shipment.RequestedShipment.PackagingType = 'YOUR_PACKAGING'

                shipment.RequestedShipment.Shipper.Contact.PersonName = pickup_point[11]
                shipment.RequestedShipment.Shipper.Contact.CompanyName = pickup_point[9]
                shipment.RequestedShipment.Shipper.Contact.PhoneNumber = pickup_point[3][-10:]
                shipment.RequestedShipment.Shipper.Address.StreetLines = [pickup_address]
                shipment.RequestedShipment.Shipper.Address.City = pickup_point[6]
                shipment.RequestedShipment.Shipper.Address.StateOrProvinceCode = pickup_point[10]
                shipment.RequestedShipment.Shipper.Address.PostalCode = pickup_point[8]
                shipment.RequestedShipment.Shipper.Address.CountryCode = 'IN'

                shipment.RequestedShipment.Recipient.Contact.PersonName = customer_name
                shipment.RequestedShipment.Recipient.Contact.PhoneNumber = shipping_phone
                shipment.RequestedShipment.Recipient.Address.StreetLines = customer_address
                shipment.RequestedShipment.Recipient.Address.City = order[17]
                shipment.RequestedShipment.Recipient.Address.StateOrProvinceCode = order[19]
                shipment.RequestedShipment.Recipient.Address.PostalCode = order[18]
                shipment.RequestedShipment.Recipient.Address.CountryCode = 'IN'

                shipment.RequestedShipment.ShippingChargesPayment.PaymentType = "SENDER"
                shipment.RequestedShipment.ShippingChargesPayment.Payor.ResponsibleParty.AccountNumber \
                    = CONFIG_OBJ.account_number

                # if order_type=='COD':
                #     shipment.RequestedShipment.SpecialServiceTypes = 'COD'
                #     shipment.RequestedShipment.SpecialServicesRequested.CodDetail.CodCollectionAmount.Currency = 'INR'
                #     shipment.RequestedShipment.SpecialServicesRequested.CodDetail.CodCollectionAmount.Amount = order[27]
                #     shipment.RequestedShipment.SpecialServicesRequested.CodDetail.RemitToName = 'Remitter'

                package_string = ""
                package_quantity = 0
                for idx, prod in enumerate(order[40]):
                    package_string += prod + " (" + str(order[35][idx]) + ") + "
                    package_quantity += order[35][idx]

                shipment.RequestedShipment.CustomsClearanceDetail.CustomsValue.Currency = "INR"
                shipment.RequestedShipment.CustomsClearanceDetail.CustomsValue.Amount = order[27]

                commodity = shipment.create_wsdl_object_of_type('CustomsClearanceDetail.Commodities')
                commodity.NumberOfPieces=1
                commodity.Description=package_string
                commodity.CountryOfManufacture="IN"
                package1_weight = shipment.create_wsdl_object_of_type('Weight')
                package1_weight.Value = sum(order[34])
                package1_weight.Units = "KG"
                commodity.Weight = package1_weight
                commodity.Quantity=1
                commodity.QuantityUnits="EA"
                commodity.UnitPrice.Currency="INR"
                commodity.UnitPrice.Amount=order[27]
                commodity.CustomsValue.Amount=order[27]
                commodity.CustomsValue.Currency="INR"

                shipment.RequestedShipment.CustomsClearanceDetail.Commodities.append(commodity)
                shipment.RequestedShipment.CustomsClearanceDetail.CommercialInvoice.Purpose = 'SOLD'

                shipment.RequestedShipment.LabelSpecification.LabelFormatType = 'COMMON2D'
                shipment.RequestedShipment.LabelSpecification.ImageType = 'PDF'
                shipment.RequestedShipment.LabelSpecification.LabelStockType = 'PAPER_7X4.75'

                shipment.RequestedShipment.PackageCount = 1
                shipment.RequestedShipment.TotalWeight.Units = 'KG'

                package1_weight = shipment.create_wsdl_object_of_type('Weight')
                package1_weight.Value = sum(order[34])
                package1_weight.Units = "KG"
                package1 = shipment.create_wsdl_object_of_type('RequestedPackageLineItem')
                package1.Weight = package1_weight
                package1.SequenceNumber = 1
                shipment.add_package(package1)

                shipment.send_validation_request()
                shipment.send_request()

                awb_no = None
                try:
                    awb_no = shipment.response.CompletedShipmentDetail.MasterTrackingId.TrackingNumber
                except Exception as e:
                    pass

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

                insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                                                                                dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, 
                                                                                                                channel_fulfillment_id, tracking_link, zone)
                                                                                                                VALUES  %s RETURNING id;"""
                if awb_no:
                    awb_no = str(awb_no)
                    order_status_change_ids.append(order[0])
                    #routing_code = str(req['DestinationArea']) + "-" + str(req['DestinationLocation'])
                    data_tuple = tuple([(
                        awb_no, "", order[0], pickup_point[1], courier[9], json.dumps(dimensions),
                        volumetric_weight, weight,
                        "", pickup_point[2], None, None, None, zone)])

                    if order[46] == 7:
                        push_awb_easyecom(order[39],order[36], awb_no, courier, cur, order[9])

                    client_name = str(order[51])
                    customer_phone = order[5].replace(" ", "")
                    customer_phone = "0" + customer_phone[-10:]

                    sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                    sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                    exotel_sms_data[sms_to_key] = customer_phone
                    try:
                        tracking_link_wareiq = "http://webapp.wareiq.com/tracking/" + str(
                            awb_no)
                        """
                        short_url = requests.get(
                            "https://cutt.ly/api/api.php?key=f445d0bb52699d2f870e1832a1f77ef3f9078&short=%s" % tracking_link_wareiq)
                        short_url_track = short_url.json()['url']['shortLink']
                        """
                        exotel_sms_data[
                            sms_body_key] = "Received: Your order from %s. Track here: %s . Thanks!" % (
                        client_name, tracking_link_wareiq)
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

        cur.execute("UPDATE client_pickups SET invoice_last=%s WHERE id=%s;", (last_invoice_no, pickup_id))

        conn.commit()

    if exotel_idx:
        logger.info("Sending messages...count:" + str(exotel_idx))
        try:
            lad = requests.post(
                'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend',
                data=exotel_sms_data)
        except Exception as e:
            logger.error("messages not sent." + "   Error: " + str(e.args[0]))


def ship_selfshp_orders(cur, courier, courier_name, order_ids, order_id_tuple, backup_param=True, force_ship=None):
    exotel_idx = 0
    exotel_sms_data = {
        'From': 'LM-WAREIQ'
    }
    if courier_name and order_ids:
        orders_to_ship_query = get_orders_to_ship_query.replace("__ORDER_SELECT_FILTERS__",
                                                                """and aa.id in %s""" % order_id_tuple)
    else:
        orders_to_ship_query = get_orders_to_ship_query.replace("__ORDER_SELECT_FILTERS__", """and aa.status='NEW' and ll.id is null""")
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
        pickup_points_tuple = (pickup_id,)
        cur.execute(get_pickup_points_query, pickup_points_tuple)
        order_status_change_ids = list()

        pickup_point = cur.fetchone()  # change this as we get to dynamic pickups

        last_invoice_no = pickup_point[22] if pickup_point[22] else 0

        if not pickup_point[21]:
            continue

        for order in all_new_orders:
            if order[26].lower() == 'pickup':
                continue
            zone = None
            try:
                zone = get_delivery_zone(pickup_point[8], order[18])
            except Exception as e:
                logger.error("couldn't find zone: " + str(order[0]) + "\nError: " + str(e))

            time_2_days = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=1)
            if order[47] and not (order[50] and order[2] < time_2_days) and not force_ship:
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

            if zone != 'A' and not force_ship:
                continue

            # kama ayurveda assign mumbai orders pincode check
            if pickup_point[0] == 170 and order[18] not in kama_mum_sdd_pincodes:
                continue

            # kama ayurveda assign blr orders pincode check
            if pickup_point[0] == 143 and order[18] not in kama_blr_sdd_pincodes:
                continue

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

            insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                                            dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, 
                                                                            channel_fulfillment_id, tracking_link)
                                                                            VALUES  %s RETURNING id;"""

            if order[0] > last_shipped_order_id:
                last_shipped_order_id = order[0]
            order_status_change_ids.append(order[0])
            data_tuple = tuple([(
                str(order[0]), "Success", order[0], pickup_point[1],
                courier[9], json.dumps(dimensions), volumetric_weight, weight, "", pickup_point[2],
                "", None, None)])

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

            if not order[54]:
                last_invoice_no = invoice_order(cur, last_invoice_no, pickup_point[23], order[0], pickup_id)

        if last_shipped_order_id:
            last_shipped_data_tuple = (
                last_shipped_order_id, datetime.now(tz=pytz.timezone('Asia/Calcutta')), courier[1])
            cur.execute(update_last_shipped_order_query, last_shipped_data_tuple)

        if order_status_change_ids:
            if len(order_status_change_ids) == 1:
                cur.execute(update_orders_status_query % (("(%s)") % str(order_status_change_ids[0])))
            else:
                cur.execute(update_orders_status_query, (tuple(order_status_change_ids),))

        cur.execute("UPDATE client_pickups SET invoice_last=%s WHERE id=%s;", (last_invoice_no, pickup_id))

        conn.commit()

    if exotel_idx:
        logger.info("Sending messages...count:" + str(exotel_idx))
        try:
            lad = requests.post(
                'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend',
                data=exotel_sms_data)
        except Exception as e:
            logger.error("messages not sent." + "   Error: " + str(e.args[0]))


def ship_sdd_orders(cur, courier, courier_name, order_ids, order_id_tuple, backup_param=True, force_ship=None):
    exotel_idx = 0
    exotel_sms_data = {
        'From': 'LM-WAREIQ'
    }
    if courier_name and order_ids:
        orders_to_ship_query = get_orders_to_ship_query.replace("__ORDER_SELECT_FILTERS__",
                                                                """and aa.id in %s""" % order_id_tuple)
    else:
        orders_to_ship_query = get_orders_to_ship_query.replace("__ORDER_SELECT_FILTERS__", """and aa.status='NEW' and ll.id is null""")

    get_orders_data_tuple = (courier[1], courier[1])

    cur.execute(orders_to_ship_query, get_orders_data_tuple)
    all_orders = cur.fetchall()

    pickup_point_order_dict = dict()
    headers = {"Authorization": "Token " + courier[14],
               "Content-Type": "application/json"}

    for order in all_orders:
        if order[41]:
            if order[41] not in pickup_point_order_dict:
                pickup_point_order_dict[order[41]] = [order]
            else:
                pickup_point_order_dict[order[41]].append(order)

    for pickup_id, all_new_orders in pickup_point_order_dict.items():

        last_shipped_order_id = 0
        pickup_points_tuple = (pickup_id,)
        cur.execute(get_pickup_points_query, pickup_points_tuple)
        order_status_change_ids = list()

        pickup_point = cur.fetchone()  # change this as we get to dynamic pickups

        last_invoice_no = pickup_point[22] if pickup_point[22] else 0

        if not pickup_point[21]:
            continue

        for order in all_new_orders:
            if order[26].lower() == 'pickup':
                continue
            zone = None
            try:
                zone = get_delivery_zone(pickup_point[8], order[18])
            except Exception as e:
                logger.error("couldn't find zone: " + str(order[0]) + "\nError: " + str(e))

            time_2_days = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=1)
            if order[47] and not (order[50] and order[2] < time_2_days) and not force_ship:
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

            if zone != 'A' and not force_ship:
                continue

            lat, lon = order[22], order[23]

            if not (lat and lon):
                lat, lon = get_lat_lon(order, cur)

            # kama ayurveda assign mumbai orders pincode check
            if pickup_point[0] == 170 and order[18] not in kama_mum_sdd_pincodes:
                continue

            # kama ayurveda assign blr orders pincode check
            if pickup_point[0] == 143 and order[18] not in kama_blr_sdd_pincodes:
                continue

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

            package_string = ""
            for idx, prod in enumerate(order[40]):
                package_string += prod + " (" + str(order[35][idx]) + ") + "

            time_now = datetime.utcnow()+timedelta(hours=5.5)
            if time_now.hour>12:
                alloted_time = time_now + timedelta(days=1)
            else:
                alloted_time = time_now
            alloted_time = alloted_time.replace(hour=14, minute=0)
            sdd_body = {"pickup_contact_number":pickup_point[3],
                        "store_code":"wareiqstore001",
                        "order_details":{
                        "scheduled_time":alloted_time.strftime('%Y-%m-%d %X'),
                        "order_value":str(order[27]),
                        "paid":False if order[26].lower()=='cod' else True,
                        "client_order_id":order[1],
                        "delivery_instruction": {
                            "drop_instruction_text": "",
                            "take_drop_off_picture": True,
                            "drop_off_picture_mandatory": False
                            }
                        },
                        "customer_details":{
                        "address_line_1":order[15],
                        "city":order[17],
                        "contact_number":order[5],
                        "address_line_2":order[16],
                        "name":order[13],
                        "latitude": lat,
                        "longitude": lon
                        },
                        "misc":{
                        "type":"slotted",
                        "promised_delivery_time":(alloted_time+timedelta(hours=2)).strftime('%Y-%m-%d %X'),
                        "weight": weight
                        },
                        "product_details":[{
                        "weight":weight,
                        "id":str(order[0]),
                        "quantity":1,
                        "name":package_string,
                        "price":order[27] if order[27] else 1}]}

            return_data_raw = requests.post(courier[16] + "/api/v2/stores/orders/", headers=headers, data=json.dumps(sdd_body)).json()

            if return_data_raw['message'] == 'Success':
                order_status_change_ids.append(order[0])
                data_tuple = tuple([(
                    str(return_data_raw['data']['sfx_order_id']),
                    return_data_raw['message'],
                    order[0], pickup_point[1], courier[9], json.dumps(dimensions), volumetric_weight, weight,
                    "", pickup_point[2], "", None, return_data_raw['data']['track_url'], zone)])

                if order[46] == 7:
                    push_awb_easyecom(order[39], order[36], return_data_raw['AddManifestDetails'][0]['AWBNo'], courier,
                                      cur, order[9])

                client_name = str(order[51])
                customer_phone = order[5].replace(" ", "")
                customer_phone = "0" + customer_phone[-10:]

                sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                exotel_sms_data[sms_to_key] = customer_phone
                try:
                    tracking_link_wareiq = return_data_raw['data']['track_url']
                    exotel_sms_data[
                        sms_body_key] = "Received: Your order from %s. Track here: %s . Thanks!" % (
                        client_name, tracking_link_wareiq)
                except Exception:
                    pass

                exotel_idx += 1
            else:
                cur.execute("select * from client_couriers where client_prefix=%s and priority=%s;",
                            (courier[1], courier[3] + 1))
                qs = cur.fetchone()
                if not (qs and backup_param) or force_ship:
                    insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                                dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, zone)
                                                                VALUES  %s"""
                    insert_shipments_data_tuple = list()
                    insert_shipments_data_tuple.append(("", "Fail", order[0], None,
                                                        None, None, None, None, "Pincode not serviceable", None,
                                                        None, zone), )
                    cur.execute(insert_shipments_data_query, tuple(insert_shipments_data_tuple))
                continue

            insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, 
                                                channel_fulfillment_id, tracking_link, zone)
                                                VALUES  %s RETURNING id;"""

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

            if not order[54]:
                last_invoice_no = invoice_order(cur, last_invoice_no, pickup_point[23], order[0], pickup_id)

        if last_shipped_order_id:
            last_shipped_data_tuple = (
                last_shipped_order_id, datetime.now(tz=pytz.timezone('Asia/Calcutta')), courier[1])
            cur.execute(update_last_shipped_order_query, last_shipped_data_tuple)

        if order_status_change_ids:
            if len(order_status_change_ids) == 1:
                cur.execute(update_orders_status_query % (("(%s)") % str(order_status_change_ids[0])))
            else:
                cur.execute(update_orders_status_query, (tuple(order_status_change_ids),))

        cur.execute("UPDATE client_pickups SET invoice_last=%s WHERE id=%s;", (last_invoice_no, pickup_id))

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
                            sms_body_key] = "Received: Your order from %s. Track here: %s. Thanks!" % (
                        client_name, tracking_link_wareiq)
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


def push_awb_easyecom(invoice_id, api_token, awb, courier, cur, client_prefix):
    try:
        companyCarrierId = courier[8]
        if not companyCarrierId or not companyCarrierId.isdigit() or not courier[0]:
            cur.execute("""SELECT aa.id, aa.unique_parameter FROM client_couriers aa
                        LEFT JOIN master_couriers bb on aa.courier_id=bb.id 
                        WHERE bb.courier_name='%s' 
                        AND aa.client_prefix='%s'"""%(courier[10], client_prefix))

            cour = cur.fetchone()
            if not cour[1] or not cour[1].isdigit():
                add_url = "https://api.easyecom.io/Credentials/addCarrierCredentials?api_token=%s"%api_token
                post_body = {
                              "carrier_id":easyecom_carrier_id[courier[10]],
                              "username":"wareiq",
                              "password":"wareiq",
                              "token":courier[14]
                            }

                req = requests.post(add_url, data=post_body).json()
                cur.execute("UPDATE client_couriers SET unique_parameter='%s' WHERE id=%s"%(req['data']['companyCarrierId'], cour[0]))
                companyCarrierId = req['data']['companyCarrierId']
            else:
                companyCarrierId = cour[1]

        post_url = "https://api.easyecom.io/Carrier/assignAWB?api_token=%s"%api_token
        post_body = {
                      "invoiceId": invoice_id,
                      "api_token": api_token,
                      "courier": courier[10],
                      "awbNum": awb,
                      "companyCarrierId": int(companyCarrierId)
                    }
        req = requests.post(post_url, data=post_body)
    except Exception as e:
        logger.error("Easyecom not updated.")


def invoice_order(cur, last_inv_no, inv_prefix, order_id, pickup_data_id):
    try:
        if not last_inv_no:
            last_inv_no = 0
        inv_no = last_inv_no+1
        inv_text = str(inv_no)
        inv_text = inv_text.zfill(5)
        if inv_prefix:
            inv_text = inv_prefix + "-" + inv_text

        cur.execute("""INSERT INTO orders_invoice (order_id, pickup_data_id, invoice_no_text, invoice_no, date_created) 
                        VALUES (%s, %s, %s, %s, %s);""", (order_id, pickup_data_id, inv_text, inv_no, datetime.utcnow()+timedelta(hours=5.5)))
        return inv_no
    except Exception as e:
        return last_inv_no


def get_lat_lon(order, cur):
    try:
        lat, lon = None, None
        address = order[15]
        if order[16]:
            address += " " + order[16]
        if order[17]:
            address += ", " + order[17]
        if order[19]:
            address += ", " + order[19]
        if order[18]:
            address += ", " + order[18]
        res = requests.get("https://maps.googleapis.com/maps/api/geocode/json?address=%s&key=%s" % (
        address, "AIzaSyBg7syNb_e1gZgyL1lHXBHRmg3jeaXrkco"))
        loc_rank = 0
        location_rank_dict = {"ROOFTOP": 1,
                              "RANGE_INTERPOLATED": 2,
                              "GEOMETRIC_CENTER": 3,
                              "APPROXIMATE": 4}
        for result in res.json()['results']:
            if location_rank_dict[result['geometry']['location_type']] > loc_rank:
                loc_rank = location_rank_dict[result['geometry']['location_type']]
                lat, lon = result['geometry']['location']['lat'], result['geometry']['location']['lng']

        if lat and lon:
            cur.execute("UPDATE shipping_address SET latitude=%s, longitude=%s WHERE id=%s", (lat, lon, order[12]))
        return lat, lon
    except Exception as e:
        logger.error("lat lon on found for order: ." + str(order[0]) + "   Error: " + str(e.args[0]))
        return None, None


bluedart_area_code_mapping = {"110015":"DEL",
                                "110077":"DEL",
                                "110059":"DEL",
                                "110093":"DEL",
                                "160062":"MOH",
                                "121002":"FAR",
                                "122001":"GGN",
                                "131028":"SOP",
                                "132001":"KRN",
                                "132103":"PNP",
                                "134113":"PKL",
                                "143101":"ATO",
                                "160004":"ZKP",
                                "160017":"CAR",
                                "173205":"BDI",
                                "180003":"JMU",
                                "201010":"GZB",
                                "201301":"NDA",
                                "204101":"HRS",
                                "211001":"ALL",
                                "221001":"VRS",
                                "221311":"VCI",
                                "222002":"JPX",
                                "226009":"LCK",
                                "248140":"DLJ",
                                "250001":"MEE",
                                "276001":"AZG",
                                "281004":"MAT",
                                "282001":"AGR",
                                "302012":"JAI",
                                "303007":"JBG",
                                "305801":"KGH",
                                "306401":"PAL",
                                "311802":"BLW",
                                "312001":"CGR",
                                "324006":"KOT",
                                "342012":"JOD",
                                "360001":"RJK",
                                "360311":"GDL",
                                "360579":"POR",
                                "370001":"BHJ",
                                "380001":"AHD",
                                "382320":"DGM",
                                "382345":"AHD",
                                "390003":"BDQ",
                                "393110":"JGD",
                                "394210":"SUR",
                                "395001":"SUR",
                                "396445":"NVS",
                                "400001":"BOM",
                                "400064":"BOM",
                                "400097":"BOM",
                                "400705":"NBM",
                                "401107":"BOM",
                                "403001":"PNJ",
                                "410206":"NBM",
                                "411001":"PNQ",
                                "411005":"PNQ",
                                "413501":"OBD",
                                "421302":"BCT",
                                "422002":"NSK",
                                "431001":"AUR",
                                "440005":"NGP",
                                "444601":"AMT",
                                "455001":"DEW",
                                "457779":"JBU",
                                "480001":"CWD",
                                "500003":"HYD",
                                "501101":"VKB",
                                "501141":"HYD",
                                "501301":"GTF",
                                "502001":"SRD",
                                "502103":"SDP",
                                "502110":"MDQ",
                                "502220":"ZHB",
                                "502278":"GJL",
                                "502286":"NYR",
                                "503001":"NZB",
                                "503111":"KMD",
                                "503187":"BDF",
                                "503224":"ARM",
                                "504001":"ADB",
                                "504106":"NML",
                                "504201":"MNC",
                                "505001":"KIR",
                                "505122":"ELK",
                                "505172":"PDF",
                                "505184":"RGM",
                                "505301":"SCE",
                                "505325":"KRM",
                                "505327":"JGT",
                                "506001":"WAL",
                                "506101":"MBD",
                                "506167":"JGN",
                                "506169":"BPY",
                                "507001":"KHA",
                                "507002":"KHF",
                                "507101":"KUM",
                                "507111":"BDX",
                                "507115":"PVN",
                                "507303":"STP",
                                "508001":"NLG",
                                "508116":"BON",
                                "508206":"KDE",
                                "508207":"MGF",
                                "508211":"SPT",
                                "508243":"HYD",
                                "509001":"MBN",
                                "509103":"WAN",
                                "509125":"GDW",
                                "509209":"NGK",
                                "509210":"NYP",
                                "509216":"SDN",
                                "509301":"JDC",
                                "515001":"XJA",
                                "515201":"HUR",
                                "515591":"KDZ",
                                "515671":"DMV",
                                "515801":"GTX",
                                "516001":"CUX",
                                "516329":"PVD",
                                "516360":"XDP",
                                "517001":"CHT",
                                "517214":"PIU",
                                "517325":"MLE",
                                "517408":"PMN",
                                "517501":"TPT",
                                "517644":"KHT",
                                "518004":"KUR",
                                "518301":"ADZ",
                                "518501":"NAX",
                                "520002":"VIJ",
                                "521001":"MLM",
                                "521101":"GNV",
                                "521137":"PKE",
                                "521201":"NUZ",
                                "521301":"GWD",
                                "522002":"GUN",
                                "522101":"BAP",
                                "522124":"POF",
                                "522201":"TLI",
                                "522265":"REP",
                                "522601":"NAT",
                                "522616":"CPT",
                                "523001":"OGL",
                                "523155":"CGF",
                                "523316":"MRF",
                                "524001":"RLN",
                                "524101":"GUX",
                                "524121":"SUT",
                                "524126":"NVY",
                                "524132":"KHT",
                                "524201":"KVI",
                                "530005":"VZG",
                                "531001":"ANP",
                                "531116":"NRF",
                                "532001":"SKM",
                                "532127":"GRV",
                                "532221":"PLS",
                                "533001":"KAK",
                                "533101":"RJY",
                                "533201":"APM",
                                "533450":"PTM",
                                "534002":"ELU",
                                "534101":"TPQ",
                                "534201":"BMW",
                                "534211":"TAN",
                                "534260":"PLZ",
                                "535002":"VZM",
                                "535128":"GRV",
                                "535501":"PRF",
                                "560001":"BLR",
                                "560078":"BLR",
                                "560035":"BLR",
                                "560103":"BLR",
                                "560074":"XBD",
                                "561202":"PGA",
                                "561203":"DDP",
                                "561208":"GOW",
                                "562101":"CHP",
                                "562106":"BLR",
                                "562110":"DVH",
                                "562114":"HSK",
                                "562159":"RAG",
                                "563101":"KOL",
                                "563114":"BLR",
                                "563122":"KGF",
                                "563125":"CHM",
                                "563130":"MLU",
                                "563131":"MBL",
                                "570001":"MYS",
                                "571111":"NJD",
                                "571114":"HUN",
                                "571124":"TNP",
                                "571213":"GNK",
                                "571234":"KSH",
                                "571313":"CMR",
                                "571401":"MDY",
                                "571421":"MRR",
                                "571426":"KRE",
                                "571432":"NBC",
                                "571434":"SNP",
                                "571440":"KEG",
                                "571501":"CNN",
                                "571511":"RAG",
                                "572101":"TUM",
                                "572130":"BLR",
                                "572201":"GNT",
                                "573103":"ASK",
                                "573115":"HBE",
                                "573116":"CNP",
                                "573134":"SKL",
                                "573201":"HAS",
                                "574201":"PUT",
                                "574301":"MLR",
                                "575001":"MLR",
                                "576101":"MPL",
                                "576201":"KNA",
                                "577002":"DGE",
                                "577101":"CHK",
                                "577132":"MDE",
                                "577201":"SHM",
                                "577213":"CNA",
                                "577228":"HAS",
                                "577301":"BHA",
                                "577401":"GNS",
                                "577427":"SKP",
                                "577432":"TRI",
                                "577501":"CTX",
                                "577522":"CLK",
                                "577548":"KDD",
                                "577598":"HRI",
                                "577601":"HAR",
                                "580001":"DWD",
                                "580020":"HBL",
                                "581110":"HAV",
                                "581115":"RNN",
                                "581303":"KAW",
                                "581325":"DAN",
                                "581329":"HAY",
                                "581343":"HNR",
                                "581401":"SIS",
                                "582101":"GAB",
                                "583101":"YBL",
                                "583121":"SGP",
                                "583201":"HSP",
                                "583227":"GVH",
                                "583231":"KOP",
                                "584101":"RCR",
                                "584111":"SHU",
                                "584122":"LGR",
                                "584123":"SRV",
                                "584128":"SND",
                                "585101":"GUL",
                                "585202":"YDR",
                                "585222":"SDF",
                                "585223":"SHU",
                                "585327":"BSV",
                                "585401":"BID",
                                "586101":"BIJ",
                                "586128":"SDG",
                                "586209":"IDN",
                                "587101":"BAG",
                                "587125":"IKL",
                                "587301":"JAM",
                                "590001":"IXG",
                                "591102":"BLG",
                                "591304":"ATN",
                                "591307":"GKK",
                                "600001":"MAA",
                                "600052":"RHS",
                                "600100":"PKN",
                                "601201":"GPD",
                                "601204":"PNF",
                                "602001":"TVF",
                                "603002":"TKF",
                                "603103":"KLM",
                                "603202":"MRM",
                                "603306":"ACP",
                                "604001":"TNV",
                                "604408":"KPM",
                                "605001":"PON",
                                "605602":"VPR",
                                "606001":"VUD",
                                "607002":"CUD",
                                "609001":"IYL",
                                "610001":"TIZ",
                                "611001":"KKI",
                                "612001":"KMK",
                                "613001":"TJV",
                                "614602":"PKK",
                                "620001":"TRZ",
                                "621212":"PER",
                                "622001":"XKK",
                                "624001":"DND",
                                "625001":"IXM",
                                "626117":"RJP",
                                "626124":"SIV",
                                "626125":"SVI",
                                "627001":"TEE",
                                "629001":"NOI",
                                "630001":"KAD",
                                "631001":"AOF",
                                "631501":"KPM",
                                "632301":"ANE",
                                "632401":"RPT",
                                "635001":"KRG",
                                "635109":"HOR",
                                "635802":"AMU",
                                "636004":"SAL",
                                "638001":"EDE",
                                "641004":"CJB",
                                "641020":"PKM",
                                "641301":"ITT",
                                "641602":"TIM",
                                "641652":"TRP",
                                "641664":"XAD",
                                "642126":"UMP",
                                "683111":"GLY",
                                "700001":"CCU",
                                "700044":"MAH",
                                "713502":"KWT",
                                "734421":"SVM",
                                "751020":"BBN",
                                "781001":"GAU",
                                "799009":"AAR",
                                "800014":"PAT",
                                "812004":"BGL",
                                "831002":"JMD",
                                "834002":"RAN",}

easyecom_carrier_id = {"Delhivery Surface Standard": 2,
                       "Delhivery": 2,
                       "Delhivery 2 KG": 2,
                       "Delhivery 20 KG": 2,
                       "Delhivery 10 KG": 2,
                       "Ecom Express": 3,
                       "Xpressbees": 13,
                       "Xpressbees Surface": 13,
                       "Xpressbees 5 KG": 13,
                       "Bluedart": 1,
                       }

kama_blr_sdd_pincodes = ('560001','560002','560003','560004','560005','560006','560007','560008','560009','560010','560011','560012','560013','560014','560015','560016','560017','560018','560019','560020','560021','560022','560023','560024','560025','560026','560027','560028','560029','560030','560031','560032','560033','560034','560035','560036','560037','560038','560039','560040','560041','560042','560043','560044','560045','560046','560047','560048','560049','560050','560051','560052','560053','560054','560055','560056','560057','560058','560059','560060','560061','560062','560063','560064','560065','560066','560067','560068','560069','560070','560071','560072','560073','560074','560075','560076','560077','560078','560079','560080','560081','560082','560083','560084','560085','560086','560087','560088','560089','560090','560091','560092','560093','560094','560095','560096','560097','560098','560099','560100','560102','560103','560104','560105','560106','560107','560108','560109','560110','560111','560113','560114','560300','562106','562107','562125','562130','562149','562157')
kama_mum_sdd_pincodes = ('400082','400080','400081','400078','400042','400076','400083','400079','400072','400084','400086','400075','400077','400089','400070','400071','400024','400059','400053','400069','400096','400093','400099','400074','400022','400043','400088','400085','400094','400017','400029','400047','400049','400050','400051','400052','400054','400055','400056','400057','400058','400060','400062','400063','400064','400065','400067','400087','400090','400097','400098','400101','400102','400104','400601','400602','400603','400604','400605','400606','400607','400608','400610','400615','401107','406007','400016','400019','400037','400028','400014','400031','400025','400030','400018','400013','400012','400015','400033','400011','400027','400010','400035','400006','400036','400026','400034','400007','400008','400004','400009','400003','400002','400001','400020','400023','400032','400021','400039','400005')