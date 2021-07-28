import psycopg2, requests, os, json, pytz
import logging, random, string
from datetime import datetime, timedelta
from requests_oauthlib.oauth1_session import OAuth1Session
from zeep import Client
from app.db_utils import DbConnection, UrlShortner
from fedex.config import FedexConfig

from .queries import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)

conn = DbConnection.get_db_connection_instance()
conn_2 = DbConnection.get_pincode_db_connection_instance()
cur_2 = conn_2.cursor()

RAVEN_URL = "https://api.ravenapp.dev/v1/apps/ccaaf889-232e-49df-aeb8-869e3153509d/events/send"
RAVEN_HEADERS = {"Content-Type": "application/json", "Authorization": "AuthKey K4noY3GgzaW8OEedfZWAOyg+AmKZTsqO/h/8Y4LVtFA="}


def ship_orders(courier_name=None, order_ids=None, force_ship=None, client_prefix=None, cur=None):
    if not cur:
        cur = conn.cursor()
    order_id_tuple = "()"
    if courier_name and order_ids:  # creating courier details list for manual shipping
        if len(order_ids) == 1:
            order_id_tuple = "('" + str(order_ids[0]) + "')"
        else:
            order_id_tuple = str(tuple(order_ids))
        cur.execute("""DELETE FROM order_scans where shipment_id in (select id from shipments where order_id in %s);
                        DELETE FROM order_status where shipment_id in (select id from shipments where order_id in %s);
                           DELETE FROM shipments where order_id in %s;""" % (order_id_tuple, order_id_tuple, order_id_tuple))
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
        cur.execute(fetch_client_couriers_query.replace('__CLIENT_FILTER__', "and aa.client_prefix!='DHANIPHARMACY'" if not
                                                    client_prefix else "and aa.client_prefix='%s'"%client_prefix))
        all_couriers = cur.fetchall()

    for courier in all_couriers:
        try:
            if courier[10].startswith('Delhivery'):
                ship_delhivery_orders(cur, courier, courier_name, order_ids, order_id_tuple, force_ship=force_ship)

            elif courier[10] == "Shadowfax":
                ship_shadowfax_orders(cur, courier, courier_name, order_ids, order_id_tuple, force_ship=force_ship)

            elif courier[10].startswith('Xpressbees'):
                ship_expressbees_orders(cur, courier, courier_name, order_ids, order_id_tuple, force_ship=force_ship)

            elif courier[10].startswith('Expressbees'):
                ship_expressbees_orders(cur, courier, courier_name, order_ids, order_id_tuple, force_ship=force_ship)

            elif courier[10].startswith('Bluedart'):
                ship_bluedart_orders(cur, courier, courier_name, order_ids, order_id_tuple, force_ship=force_ship)

            elif courier[10].startswith('Ecom'):
                ship_ecom_orders(cur, courier, courier_name, order_ids, order_id_tuple, force_ship=force_ship)

            elif courier[10].startswith('Self Ship'):
                ship_selfshp_orders(cur, courier, courier_name, order_ids, order_id_tuple, force_ship=force_ship)

            elif courier[10].startswith('Pidge'):
                ship_pidge_orders(cur, courier, courier_name, order_ids, order_id_tuple, force_ship=force_ship)

            # elif courier[10].startswith('SDD'):
            #     ship_sdd_orders(cur, courier, courier_name, order_ids, order_id_tuple, force_ship=force_ship)

            elif courier[10].startswith('FedEx'):
                ship_fedex_orders(cur, courier, courier_name, order_ids, order_id_tuple, force_ship=force_ship)

        except Exception as e:
            logger.error("couldn't ship orders: " + str(courier[10]) + "\nError: " + str(e))
            conn.rollback()

    if not order_ids:
        cur.execute(update_same_state_query)
        conn.commit()

    cur.close()


def cod_verification_text(order, cur):
    cod_confirmation_link = "https://track.wareiq.com/core/v1/passthru/cod?CustomField=%s" % str(order[0])
    cod_confirmation_link = UrlShortner.get_short_url(cod_confirmation_link, cur)

    insert_cod_ver_tuple = (order[0], cod_confirmation_link, datetime.now())
    cur.execute("INSERT INTO cod_verification (order_id, verification_link, date_created) VALUES (%s,%s,%s);",
                insert_cod_ver_tuple)
    client_name = order[51]
    customer_phone = order[5].replace(" ", "")
    customer_phone = "0" + customer_phone[-10:]
    payload = {
        "event": "cod_verification",
        "user": {
            "mobile": customer_phone,
        },
        "data": {
            "client_name": client_name,
            "order_amount": str(order[27]),
            "verification_link": cod_confirmation_link
        }
    }

    req = requests.post(RAVEN_URL, headers=RAVEN_HEADERS, data=json.dumps(payload))


def send_received_event(client_name, customer_phone, tracking_link):
    payload = {
        "event": "received",
        "user": {
            "mobile": customer_phone,
        },
        "data": {
            "client_name": client_name,
            "tracking_link": tracking_link,
        }
    }

    req = requests.post(RAVEN_URL, headers=RAVEN_HEADERS, data=json.dumps(payload))


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

        last_shipped_order_id = 0
        pickup_points_tuple = (pickup_id,)
        cur.execute(get_pickup_points_query, pickup_points_tuple)

        pickup_point = cur.fetchone()  # change this as we get to dynamic pickups

        last_invoice_no = pickup_point[22] if pickup_point[22] else 0

        headers = {"Authorization": "Token " + courier[14],
                   "Content-Type": "application/json"}

        order_chunks = [all_new_orders[i * 15:(i + 1) * 15] for i in range((len(all_new_orders) + 15 - 1) // 15)]
        for order_chunk in order_chunks:
            shipments = list()
            for order in order_chunk:
                try:
                    if order[18] in delhivery_embargo_pincodes:
                        continue
                    zone = None
                    try:
                        zone = get_delivery_zone(pickup_point[8], order[18])
                    except Exception as e:
                        logger.error("couldn't find zone: " + str(order[0]) + "\nError: " + str(e))
                    if courier[1]=="DHANIPHARMACY" and zone not in ('A', 'B'):
                        continue
                    if not order[54]:
                        last_invoice_no = invoice_order(cur, last_invoice_no, pickup_point[23], order[0], pickup_id)
                    if order[26].lower() == 'cod' and not order[27] and not force_ship:
                        continue

                    orders_dict[str(order[0])] = (order[0], order[33], order[34], order[35],
                                                  order[36], order[37], order[38], order[39],
                                                  order[5], order[9], order[45], order[46],
                                                  order[51], order[52], zone, order[54], order[55], order[56])

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
                                cod_verification_text(order, cur)
                            except Exception as e:
                                logger.error(
                                    "Cod confirmation not sent. Order id: " + str(order[0]))
                            continue
                    if order[0] > last_shipped_order_id:
                        last_shipped_order_id = order[0]

                    # check delhivery pincode serviceability
                    # check_url = "https://track.delhivery.com/c/api/pin-codes/json/?filter_codes=%s" % str(order[18])
                    # req = requests.get(check_url, headers=headers)
                    # if not req.json()['delivery_codes']:
                    #     cur.execute("select * from client_couriers where client_prefix=%s and priority=%s;",
                    #                 (courier[1], courier[3] + 1))
                    #     qs = cur.fetchone()
                    #     if not (qs and backup_param) or force_ship:
                    #         insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id,
                    #                                                     dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, zone)
                    #                                                     VALUES  %s"""
                    #         insert_shipments_data_tuple = list()
                    #         insert_shipments_data_tuple.append(("", "Fail", order[0], None,
                    #                                             None, None, None, None, "Pincode not serviceable", None,
                    #                                             None, zone), )
                    #         cur.execute(insert_shipments_data_query, tuple(insert_shipments_data_tuple))
                    #     continue

                    package_string = ""
                    if order[40]:
                        for idx, prod in enumerate(order[40]):
                            package_string += prod + " (" + str(order[35][idx]) + ") + "
                        package_string += "Shipping essential"
                    else:
                        package_string += "WareIQ package essential"

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
                    shipment_data['category_of_goods'] = "essential"
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

            delivery_shipments_body = {
                "data": json.dumps({"shipments": shipments, "pickup_location": pickup_location}), "format": "json"}
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

            return_data = req.json()['packages']

            insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, 
                                                channel_fulfillment_id, tracking_link, zone)
                                                VALUES  """

            order_status_change_ids = list()
            insert_shipments_data_tuple = list()
            insert_order_status_dict = dict()
            for package in return_data:
                try:
                    remark = ''
                    if package['remarks']:
                        remark = package['remarks'][0]
                    fulfillment_id = None
                    tracking_link = None
                    if package['waybill']:

                        order_status_change_ids.append(orders_dict[package['refnum']][0])
                        client_name = str(orders_dict[package['refnum']][12])
                        customer_phone = orders_dict[package['refnum']][8].replace(" ", "")
                        customer_phone = "0" + customer_phone[-10:]

                        if orders_dict[package['refnum']][11]==7:
                            push_awb_easyecom(orders_dict[package['refnum']][7],
                                              orders_dict[package['refnum']][4],
                                              package['waybill'], courier, cur, orders_dict[package['refnum']][16], orders_dict[package['refnum']][17])

                        try:
                            tracking_link_wareiq = "https://webapp.wareiq.com/tracking/" + str(package['waybill'])
                            tracking_link_wareiq = UrlShortner.get_short_url(tracking_link_wareiq, cur)
                            if courier[1] != 'DHANIPHARMACY':
                                send_received_event(client_name, customer_phone, tracking_link_wareiq)
                        except Exception:
                            pass

                        if orders_dict[package['refnum']][9] == "NASHER":
                            try:
                                nasher_url = "https://www.nashermiles.com/alexandria/api/v1/shipment/create"
                                nasher_headers = {"Content-Type": "application/x-www-form-urlencoded",
                                                  "Authorization": "Basic c2VydmljZS5hcGl1c2VyOllQSGpBQXlXY3RWYzV5MWg="}
                                nasher_body = {
                                    "order_id": package['refnum'],
                                    "awb_number": str(package['waybill']),
                                    "tracking_link": "https://webapp.wareiq.com/tracking/" + str(package['waybill'])}
                                req = requests.post(nasher_url, headers=nasher_headers, data=json.dumps(nasher_body))
                            except Exception as e:
                                logger.error("Couldn't update shopify for: " + str(package['refnum'])
                                             + "\nError: " + str(e.args))

                    elif 'pincode' in remark:
                        cur.execute("select * from client_couriers where client_prefix=%s and priority=%s;",
                                    (courier[1], courier[3] + 1))
                        qs = cur.fetchone()
                        if not (qs and backup_param) or force_ship:
                            insert_shipments_data_query += "%s,"
                            insert_shipments_data_tuple.append(("", "Fail", orders_dict[package['refnum']][0], None,
                                                                None, None, None, None, "Pincode not serviceable", None,
                                                                None, orders_dict[package['refnum']][14]), )
                        continue

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

            if order_status_change_ids:
                if len(order_status_change_ids) == 1:
                    cur.execute(update_orders_status_query % (("(%s)") % str(order_status_change_ids[0])))
                else:
                    cur.execute(update_orders_status_query, (tuple(order_status_change_ids),))

            conn.commit()

        if last_shipped_order_id:
            last_shipped_data_tuple = (
                last_shipped_order_id, datetime.now(tz=pytz.timezone('Asia/Calcutta')), courier[1])
            cur.execute(update_last_shipped_order_query, last_shipped_data_tuple)

        cur.execute("UPDATE client_pickups SET invoice_last=%s WHERE id=%s;", (last_invoice_no, pickup_id))

        conn.commit()


def ship_shadowfax_orders(cur, courier, courier_name, order_ids, order_id_tuple, backup_param=True, force_ship=None):
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
                        cod_verification_text(order, cur)
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

                    try:
                        tracking_link_wareiq = "https://webapp.wareiq.com/tracking/" + str(return_data_raw['data']['awb_number'])
                        tracking_link_wareiq = UrlShortner.get_short_url(tracking_link_wareiq, cur)
                        send_received_event(client_name, customer_phone, tracking_link_wareiq)
                    except Exception:
                        pass

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


def ship_xpressbees_orders(cur, courier, courier_name, order_ids, order_id_tuple, backup_param=True, force_ship=None):
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

    last_assigned_awb = 0
    try:
        cur.execute("select max(awb) from shipments where courier_id=%s;" % str(courier[9]))
        fet_res = cur.fetchone()
        if fet_res:
            last_assigned_awb = int(fet_res[0])
    except Exception:
        pass

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
                            cod_verification_text(order, cur)
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

                if req.json()['AddManifestDetails'][0]['ReturnMessage'] == 'Invalid AWB Prefix':
                    headers['XBkey'] = courier[14]
                    batch_create_req = requests.post("http://xbclientapi.xbees.in/POSTShipmentService.svc/AWBNumberSeriesGeneration", headers=headers, json={"BusinessUnit": "ECOM", "ServiceType":"FORWARD", "DeliveryType": "COD"})
                    batch_req = requests.post("http://xbclientapi.xbees.in/TrackingService.svc/GetAWBNumberGeneratedSeries", headers=headers, json={"BusinessUnit": "ECOM", "ServiceType":"FORWARD", "BatchID": batch_create_req.json()['BatchID']})
                    xpressbees_shipment_body['ManifestDetails']['AirWayBillNO'] = str(batch_req.json()['AWBNoSeries'][0])
                    req = requests.post(xpressbees_url, headers=headers, data=json.dumps(xpressbees_shipment_body))
                    last_assigned_awb = int(batch_req.json()['AWBNoSeries'][0])

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
                        push_awb_easyecom(order[39],order[36], return_data_raw['AddManifestDetails'][0]['AWBNo'], courier, cur, order[55], order[56])

                    client_name = str(order[51])
                    customer_phone = order[5].replace(" ", "")
                    customer_phone = "0" + customer_phone[-10:]

                    try:
                        tracking_link_wareiq = "https://webapp.wareiq.com/tracking/" + str(return_data_raw['AddManifestDetails'][0]['AWBNo'])
                        tracking_link_wareiq = UrlShortner.get_short_url(tracking_link_wareiq, cur)
                        if courier[1] != 'DHANIPHARMACY':
                            send_received_event(client_name, customer_phone, tracking_link_wareiq)
                    except Exception:
                        pass

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
                cur.execute("UPDATE orders SET status='READY TO SHIP' WHERE id=%s;" % str(order[0]))
                conn.commit()

            except Exception as e:
                conn.rollback()
                print("couldn't assign order: " + str(order[1]) + "\nError: " + str(e))

        if last_shipped_order_id:
            last_shipped_data_tuple = (
                last_shipped_order_id, datetime.now(tz=pytz.timezone('Asia/Calcutta')), courier[1])
            cur.execute(update_last_shipped_order_query, last_shipped_data_tuple)

        cur.execute("UPDATE client_pickups SET invoice_last=%s WHERE id=%s;", (last_invoice_no, pickup_id))

        conn.commit()


def ship_expressbees_orders(cur, courier, courier_name, order_ids, order_id_tuple, backup_param=True, force_ship=None):
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

    last_assigned_awb = 0
    try:
        cur.execute("select max(awb) from shipments where courier_id=%s;" % str(courier[9]))
        fet_res = cur.fetchone()
        if fet_res:
            last_assigned_awb = int(fet_res[0])
    except Exception:
        pass

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
                            cod_verification_text(order, cur)
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
                username = courier[14].split("|")[0]
                password = courier[14].split("|")[1]
                bus_acc_name = courier[14].split("|")[2]
                expressbees_shipment_body = {"AirWayBillNO": str(last_assigned_awb),
                                            "BusinessAccountName": str(bus_acc_name),
                                            "OrderNo": order[1],
                                            "OrderType": order[26],
                                            "DeclaredValue": str(order[27]),
                                            "PickupType": "Warehouse",
                                            "Quantity": "1",
                                            "ServiceType": "SD",
                                            "DropDetails": {
                                                "Addresses": [
                                                    {
                                                        "Address": customer_address,
                                                        "City": order[17],
                                                        "EmailID": "",
                                                        "Name": customer_name,
                                                        "PinCode": str(order[18]),
                                                        "State": order[19],
                                                        "Type": "Primary"
                                                    }
                                                ],
                                                "ContactDetails": [
                                                    {
                                                        "PhoneNo": shipping_phone,
                                                        "Type": "Primary",
                                                    }
                                                ]
                                            },
                                            "PickupDetails": {
                                                "Addresses": [
                                                    {
                                                        "Address": pickup_address,
                                                        "City": pickup_point[6],
                                                        "EmailID": "",
                                                        "Name": pickup_point[11],
                                                        "PinCode": str(pickup_point[8]),
                                                        "State": pickup_point[10],
                                                        "Type": "Primary"
                                                    }
                                                ],
                                                "ContactDetails": [
                                                    {
                                                        "PhoneNo":  pickup_point[3],
                                                        "Type": "Primary"
                                                    }
                                                ],
                                                "PickupVendorCode": pickup_point[9]
                                            },
                                            "RTODetails": {
                                                "Addresses": [
                                                    {
                                                        "Address": rto_address,
                                                        "City": pickup_point[15],
                                                        "EmailID": "",
                                                        "Name": pickup_point[20],
                                                        "PinCode": pickup_point[17],
                                                        "State": pickup_point[19],
                                                        "Type": "Primary"
                                                    }
                                                ],
                                                "ContactDetails": [
                                                    {
                                                        "PhoneNo": pickup_point[12],
                                                        "Type": "Primary"
                                                    }
                                                ]
                                            },
                                            "ManifestID": str(order[0]),
                                            "PackageDetails": {
                                                "Dimensions": {
                                                    "Height": str(dimensions['length']),
                                                    "Length": str(dimensions['breadth']),
                                                    "Width": str(dimensions['height'])
                                                },
                                                "Weight": {
                                                    "BillableWeight": str(sum(order[34])),
                                                    "PhyWeight": str(sum(order[34])),
                                                    "VolWeight": str(volumetric_weight)
                                                }
                                            },
                                            "GSTMultiSellerInfo": [
                                                {
                                                    "SellerName": str(pickup_point[11]),
                                                    "SellerPincode": str(pickup_point[8]),
                                                    "SellerAddress": pickup_address,
                                                    "HSNDetails": [
                                                        {
                                                            "ProductCategory": "E-commerce",
                                                            "ProductDesc": package_string,
                                                            "HSNCode": ""
                                                        }
                                                    ]
                                                }
                                            ]
                                        }

                if order[26].lower() == "cod":
                    expressbees_shipment_body["CollectibleAmount"] = order[27]

                xbees_auth_url = "http://userauthapis.xbees.in/api/auth/generateToken"
                req_auth = requests.post(xbees_auth_url, headers=headers, data=json.dumps({"username": username, "password": password,
                                                                                           "secretkey": courier[15].split("|")[0]}))
                headers['token'] = req_auth.json()['token']
                headers['versionnumber'] = "v1"
                xpressbees_url = "http://api.shipmentmanifestation.xbees.in/shipmentmanifestation/Forward"
                req = requests.post(xpressbees_url, headers=headers, data=json.dumps(expressbees_shipment_body))
                while req.json()['ReturnMessage'] == 'AWB Already Exists' or req.json()['ReturnMessage']=='AirWayBillNO Already exists':
                    last_assigned_awb += 1
                    expressbees_shipment_body['AirWayBillNO'] = str(last_assigned_awb)
                    req = requests.post(xpressbees_url, headers=headers,
                                        data=json.dumps(expressbees_shipment_body))

                if req.json()['ReturnMessage'] == 'Invalid AWB Prefix' or req.json()['ReturnMessage'].startswith('Invalid AirWayBillNO'):
                    headers['XBkey'] = courier[15].split("|")[1]
                    batch_create_req = requests.post("http://xbclientapi.xbees.in/POSTShipmentService.svc/AWBNumberSeriesGeneration", headers=headers, json={"BusinessUnit": "ECOM", "ServiceType":"FORWARD", "DeliveryType": "COD"})
                    batch_req = requests.post("http://xbclientapi.xbees.in/TrackingService.svc/GetAWBNumberGeneratedSeries", headers=headers, json={"BusinessUnit": "ECOM", "ServiceType":"FORWARD", "BatchID": batch_create_req.json()['BatchID']})
                    expressbees_shipment_body['AirWayBillNO'] = str(batch_req.json()['AWBNoSeries'][0])
                    req = requests.post(xpressbees_url, headers=headers, data=json.dumps(expressbees_shipment_body))
                    last_assigned_awb = int(batch_req.json()['AWBNoSeries'][0])

                return_data_raw = req.json()
                insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                                                                    dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, 
                                                                                                    channel_fulfillment_id, tracking_link, zone)
                                                                                                    VALUES  %s RETURNING id;"""

                if return_data_raw['ReturnMessage'] == 'successful' or return_data_raw['ReturnMessage']=='Successfull':

                    order_status_change_ids.append(order[0])
                    data_tuple = tuple([(
                        return_data_raw['AWBNo'], return_data_raw['ReturnMessage'],
                        order[0], pickup_point[1], courier[9], json.dumps(dimensions), volumetric_weight, weight,
                        "", pickup_point[2], "", fulfillment_id, tracking_link, zone)])

                    if order[46] == 7:
                        push_awb_easyecom(order[39],order[36], return_data_raw['AWBNo'], courier, cur, order[55], order[56])

                    client_name = str(order[51])
                    customer_phone = order[5].replace(" ", "")
                    customer_phone = "0" + customer_phone[-10:]

                    try:
                        tracking_link_wareiq = "https://webapp.wareiq.com/tracking/" + str(return_data_raw['AWBNo'])
                        tracking_link_wareiq = UrlShortner.get_short_url(tracking_link_wareiq, cur)
                        if courier[1] != 'DHANIPHARMACY':
                            send_received_event(client_name, customer_phone, tracking_link_wareiq)
                    except Exception:
                        pass

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
                cur.execute("UPDATE orders SET status='READY TO SHIP' WHERE id=%s;" % str(order[0]))
                conn.commit()

            except Exception as e:
                conn.rollback()
                print("couldn't assign order: " + str(order[1]) + "\nError: " + str(e))

        if last_shipped_order_id:
            last_shipped_data_tuple = (
                last_shipped_order_id, datetime.now(tz=pytz.timezone('Asia/Calcutta')), courier[1])
            cur.execute(update_last_shipped_order_query, last_shipped_data_tuple)

        cur.execute("UPDATE client_pickups SET invoice_last=%s WHERE id=%s;", (last_invoice_no, pickup_id))

        conn.commit()


def ship_ecom_orders(cur, courier, courier_name, order_ids, order_id_tuple, backup_param=True, force_ship=None):
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

    last_assigned_awb_cod = 0
    last_assigned_awb_ppd = 0
    try:
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
    except Exception:
        pass

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
                            cod_verification_text(order, cur)
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

                while req.json()['shipments'][0]['reason'] == 'AIRWAYBILL_IN_USE':
                    last_assigned_awb += 1
                    json_input['AWB_NUMBER'] = str(last_assigned_awb)
                    req = requests.post(ecom_url, data={"username": courier[14], "password": courier[15],
                                                        "json_input": json.dumps([json_input])})

                if req.json()['shipments'][0]['reason'] == 'INCORRECT_AWB_NUMBER':
                    fetch_awb_url = courier[16] + "/apiv2/fetch_awb/"
                    fetch_awb_req = requests.post(fetch_awb_url, data={"username": courier[14], "password": courier[15],
                                                    "count": 50, "type":json_input['PRODUCT']})
                    json_input['AWB_NUMBER'] = str(fetch_awb_req.json()['awb'][0])
                    req = requests.post(ecom_url, data={"username": courier[14], "password": courier[15],
                                                        "json_input": json.dumps([json_input])})
                    if order[26].lower() == "cod":
                        last_assigned_awb_cod = int(fetch_awb_req.json()['awb'][0])
                    else:
                        last_assigned_awb_ppd = int(fetch_awb_req.json()['awb'][0])
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
                        push_awb_easyecom(order[39],order[36], return_data_raw['shipments'][0]['awb'], courier, cur, order[55], order[56])

                    client_name = str(order[51])
                    customer_phone = order[5].replace(" ", "")
                    customer_phone = "0" + customer_phone[-10:]

                    try:
                        tracking_link_wareiq = "https://webapp.wareiq.com/tracking/" + str(return_data_raw['shipments'][0]['awb'])
                        tracking_link_wareiq = UrlShortner.get_short_url(tracking_link_wareiq, cur)
                        if courier[1] != 'DHANIPHARMACY':
                            send_received_event(client_name, customer_phone, tracking_link_wareiq)
                    except Exception:
                        pass

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
                cur.execute("UPDATE orders SET status='READY TO SHIP' WHERE id=%s;" % str(order[0]))
                conn.commit()

            except Exception as e:
                conn.rollback()
                print("couldn't assign order: " + str(order[1]) + "\nError: " + str(e))

        if last_shipped_order_id:
            last_shipped_data_tuple = (
                last_shipped_order_id, datetime.now(tz=pytz.timezone('Asia/Calcutta')), courier[1])
            cur.execute(update_last_shipped_order_query, last_shipped_data_tuple)

        cur.execute("UPDATE client_pickups SET invoice_last=%s WHERE id=%s;", (last_invoice_no, pickup_id))

        conn.commit()


def ship_bluedart_orders(cur, courier, courier_name, order_ids, order_id_tuple, backup_param=True, force_ship=None):
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
    # check_url = "https://netconnect.bluedart.com/Ver1.9/ShippingAPI/Finder/ServiceFinderQuery.svc?wsdl"
    # pincode_client = Client(check_url)

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

            if order[26].lower() == "prepaid" and courier[1] in ("ACTIFIBER", "BEHIR", "SHAHIKITCHEN", "SUKHILIFE", "ORGANICRIOT", "SUCCESSCRAFT", "HOMELY", "BEHIR2", "BEHIR3") and not force_ship:
                continue

            time_2_days = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=1)
            if order[47] and not (order[50] and order[2] < time_2_days) and not force_ship:
                if order[26].lower() == 'cod' and not order[42] and order[43]:
                    continue  # change this to continue later
                if order[26].lower() == 'cod' and not order[43]:
                    try:  ## Cod confirmation  text
                        cod_verification_text(order, cur)
                    except Exception as e:
                        logger.error(
                            "Cod confirmation not sent. Order id: " + str(order[0]))
                    continue
            if order[0] > last_shipped_order_id:
                last_shipped_order_id = order[0]
            try:
                # check delhivery pincode serviceability

                # request_data = {
                #     'pinCode': str(order[18]),
                #     "profile": client_profile
                # }
                # req = pincode_client.service.GetServicesforPincode(**request_data)
                #
                # if not (req['ApexInbound'] == 'Yes' or req['eTailCODAirInbound'] == 'Yes' or req[
                #     'eTailPrePaidAirInbound'] == 'Yes'):
                #     cur.execute("select * from client_couriers where client_prefix=%s and priority=%s;",
                #                 (courier[1], courier[3] + 1))
                #     qs = cur.fetchone()
                #     if not (qs and backup_param) or force_ship:
                #         insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id,
                #                                             dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, zone)
                #                                             VALUES  %s"""
                #         insert_shipments_data_tuple = list()
                #         insert_shipments_data_tuple.append(("", "Fail", order[0], None,
                #                                             None, None, None, None, "Pincode not serviceable", None,
                #                                             None, zone), )
                #         cur.execute(insert_shipments_data_query, tuple(insert_shipments_data_tuple))
                #     continue

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
                shipper['OriginArea'] = "BOM" if pickup_point[18]=='AAJMUM' and courier[1]=='DHANIPHARMACY' else area_code
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
                break_loop = None
                for idx, prod in enumerate(order[40]):
                    if "tiles gap filler" in prod.lower():
                        break_loop=True
                    package_string += prod + " (" + str(order[35][idx]) + ") + "
                    package_quantity += order[35][idx]

                if break_loop:
                    continue

                package_string += "Shipping"

                services['ProductCode'] = 'A'
                services['ProductType'] = 'Dutiables'
                services['DeclaredValue'] = order[27]
                services['ItemCount'] = 1
                services['CreditReferenceNo'] = str(order[0])
                if courier[1]=='DHANIPHARMACY':
                    services['CreditReferenceNo'] = "dp" + str(order[0])
                    services['PackType'] = "L"

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
                services['RegisterPickup'] = True

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
                        push_awb_easyecom(order[39],order[36], req['AWBNo'], courier, cur, order[55], order[56])

                    client_name = str(order[51])
                    customer_phone = order[5].replace(" ", "")
                    customer_phone = "0" + customer_phone[-10:]

                    try:
                        tracking_link_wareiq = "https://webapp.wareiq.com/tracking/" + str(req['AWBNo'])
                        tracking_link_wareiq = UrlShortner.get_short_url(tracking_link_wareiq, cur)
                        if courier[1]!='DHANIPHARMACY':
                            send_received_event(client_name, customer_phone, tracking_link_wareiq)
                    except Exception:
                        pass

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
                cur.execute("UPDATE orders SET status='READY TO SHIP' WHERE id=%s;"%str(order[0]))
                conn.commit()

            except Exception as e:
                conn.rollback()
                print("couldn't assign order: " + str(order[1]) + "\nError: " + str(e))

        if last_shipped_order_id:
            last_shipped_data_tuple = (
                last_shipped_order_id, datetime.now(tz=pytz.timezone('Asia/Calcutta')), courier[1])
            cur.execute(update_last_shipped_order_query, last_shipped_data_tuple)

        cur.execute("UPDATE client_pickups SET invoice_last=%s WHERE id=%s;", (last_invoice_no, pickup_id))

        conn.commit()


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
    shipment_type = courier[15].split('|')[2]
    CONFIG_OBJ = FedexConfig(key=api_key,
                             password=api_pass,
                             account_number=account_number,
                             meter_number=meter_number)

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
                        cod_verification_text(order, cur)
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
                shipment.RequestedShipment.ServiceType = shipment_type
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

                if order_type=='COD':
                    shipment.RequestedShipment.SpecialServicesRequested.CodDetail.CodCollectionAmount.Currency = 'INR'
                    shipment.RequestedShipment.SpecialServicesRequested.CodDetail.CodCollectionAmount.Amount = order[27]
                    shipment.RequestedShipment.SpecialServicesRequested.CodDetail.RemitToName = 'Remitter'
                    shipment.RequestedShipment.SpecialServicesRequested.SpecialServiceTypes = ['COD']
                    shipment.RequestedShipment.SpecialServicesRequested.CodDetail.CollectionType.value = 'GUARANTEED_FUNDS'
                    shipment.RequestedShipment.SpecialServicesRequested.CodDetail.FinancialInstitutionContactAndAddress.Contact.CompanyName = 'WareIQ'
                    shipment.RequestedShipment.SpecialServicesRequested.CodDetail.FinancialInstitutionContactAndAddress.Address.City = 'Bengaluru'
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
                    routing_code = str(shipment.response.CompletedShipmentDetail.OperationalDetail.UrsaPrefixCode) \
                                   + " " + str(shipment.response.CompletedShipmentDetail.OperationalDetail.UrsaSuffixCode)
                    routing_code += "|"+str(shipment.response.CompletedShipmentDetail.MasterTrackingId.FormId)
                    routing_code += "|"+str(shipment.response.CompletedShipmentDetail.OperationalDetail.DestinationServiceArea) \
                                    + " " + str(shipment.response.CompletedShipmentDetail.OperationalDetail.AirportId)
                    data_tuple = tuple([(
                        awb_no, "", order[0], pickup_point[1], courier[9], json.dumps(dimensions),
                        volumetric_weight, weight,
                        "", pickup_point[2], routing_code, None, None, zone)])

                    if order[46] == 7:
                        push_awb_easyecom(order[39],order[36], awb_no, courier, cur, order[55], order[56])

                    client_name = str(order[51])
                    customer_phone = order[5].replace(" ", "")
                    customer_phone = "0" + customer_phone[-10:]

                    try:
                        tracking_link_wareiq = "https://webapp.wareiq.com/tracking/" + str(awb_no)
                        tracking_link_wareiq = UrlShortner.get_short_url(tracking_link_wareiq, cur)
                        send_received_event(client_name, customer_phone, tracking_link_wareiq)
                    except Exception:
                        pass

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

        # todo: remove this
        if int(pickup_id) in (1140, 1141, 1142, 1442):
            continue

        last_shipped_order_id = 0
        pickup_points_tuple = (pickup_id,)
        cur.execute(get_pickup_points_query, pickup_points_tuple)
        order_status_change_ids = list()

        pickup_point = cur.fetchone()  # change this as we get to dynamic pickups

        last_invoice_no = pickup_point[22] if pickup_point[22] else 0

        if not pickup_point[21] and not force_ship:
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
                        cod_verification_text(order, cur)
                    except Exception as e:
                        logger.error(
                            "Cod confirmation not sent. Order id: " + str(order[0]))
                    continue

            if zone != 'A' and not force_ship and courier[1]=='KAMAAYURVEDA':
                continue

            # kama ayurveda assign mumbai orders pincode check
            if pickup_point[0] == 170 and order[18] not in kama_mum_sdd_pincodes:
                continue

            # kama ayurveda assign blr orders pincode check
            if pickup_point[0] == 143 and order[18] not in kama_blr_sdd_pincodes:
                continue

            # kama ayurveda assign chennai orders pincode check
            if pickup_point[0] == 1182 and order[18] not in kama_chn_sdd_pincodes:
                continue

            if pickup_point[0] == 1489 and order[18] not in kama_TLLTRO_sdd_pincodes:
                continue

            if pickup_point[0] == 1492 and order[18] not in kama_MHCHRO_sdd_pincodes:
                continue

            if pickup_point[0] == 1164 and order[18] not in kama_MHJTRO_sdd_pincodes:
                continue

            if pickup_point[0] == 1194 and order[18] not in kama_HRDGRO_sdd_pincodes:
                continue

            if pickup_point[0] == 1495 and order[18] not in kama_RJMIRO_sdd_pincodes:
                continue

            if pickup_point[0] == 1526 and order[18] not in kama_UPPMRO_sdd_pincodes:
                continue

            if pickup_point[0] == 1527 and order[18] not in kama_GJAORO_sdd_pincodes:
                continue

            # kama ayurveda assign delhi orders pincode check
            if pickup_point[0] == 142 and order[18] not in pidge_del_sdd_pincodes:
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
            data_tuple = tuple([("WIQ"+str(order[0]), "Success", order[0], pickup_point[1],
                courier[9], json.dumps(dimensions), volumetric_weight, weight, "", pickup_point[2],
                "", None, None)])

            if order[46] == 7:
                push_awb_easyecom(order[39], order[36], "WIQ"+str(order[0]), courier, cur, order[55], order[56])

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


def ship_sdd_orders(cur, courier, courier_name, order_ids, order_id_tuple, backup_param=True, force_ship=None):
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
                        cod_verification_text(order, cur)
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

            # kama ayurveda assign chennai orders pincode check
            if pickup_point[0] == 1182 and order[18] not in kama_chn_sdd_pincodes:
                continue

            # kama ayurveda assign delhi orders pincode check
            if pickup_point[0] == 142 and order[18] not in pidge_del_sdd_pincodes:
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
                                      cur, order[55], order[56])

                client_name = str(order[51])
                customer_phone = order[5].replace(" ", "")
                customer_phone = "0" + customer_phone[-10:]

                try:
                    tracking_link_wareiq = return_data_raw['data']['track_url']
                    tracking_link_wareiq = UrlShortner.get_short_url(tracking_link_wareiq, cur)
                    send_received_event(client_name, customer_phone, tracking_link_wareiq)
                except Exception:
                    pass

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


def ship_pidge_orders(cur, courier, courier_name, order_ids, order_id_tuple, backup_param=True, force_ship=None):
    if courier_name and order_ids:
        orders_to_ship_query = get_orders_to_ship_query.replace("__ORDER_SELECT_FILTERS__",
                                                                """and aa.id in %s""" % order_id_tuple)
    else:
        orders_to_ship_query = get_orders_to_ship_query.replace("__ORDER_SELECT_FILTERS__", """and aa.status='NEW' and ll.id is null""")

    get_orders_data_tuple = (courier[1], courier[1])

    cur.execute(orders_to_ship_query, get_orders_data_tuple)
    all_orders = cur.fetchall()

    pickup_point_order_dict = dict()
    headers = {"Authorization": "Bearer " + courier[14],
               "Content-Type": "application/json",
               "platform": "Postman",
               "deviceId": "abc",
               "buildNumber": "123"}

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

        if str(pickup_point[8]) not in pidge_del_sdd_pincodes or pickup_point[0]==1443: #todo: remove this
            continue

        last_invoice_no = pickup_point[22] if pickup_point[22] else 0

        if not pickup_point[21]:
            continue

        pick_lat, pick_lon = pickup_point[24], pickup_point[25]

        if not (pick_lat and pick_lon):
            pick_lat, pick_lon = get_lat_lon_pickup(pickup_point, cur)

        for order in all_new_orders:
            if order[26].lower() == 'pickup':
                continue
            # kama ayurveda assign delhi orders pincode check
            if order[18] not in pidge_del_sdd_pincodes:
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
                        cod_verification_text(order, cur)
                    except Exception as e:
                        logger.error(
                            "Cod confirmation not sent. Order id: " + str(order[0]))
                    continue

            if zone != 'A' and not force_ship:
                continue

            lat, lon = order[22], order[23]

            if not (lat and lon):
                lat, lon = get_lat_lon(order, cur)

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

            if max(volumetric_weight, weight)>2:
                continue

            package_string = ""
            for idx, prod in enumerate(order[40]):
                package_string += prod + " (" + str(order[35][idx]) + ") + "

            customer_phone = order[5].replace(" ", "")
            customer_phone = customer_phone[-10:]

            pidge_body = {
                            "vendor_order_id": order[0],
                            "reference_id": order[1],
                            "volume": (int(max(volumetric_weight, weight)*2) + 1)*250,
                            "cash_to_be_collected": int(order[27]) if order[26].lower()=='cod' or order[26].lower()=='cash on delivery' else 0,
                            "originator_details": {
                                "first_name": pickup_point[11],
                                "mobile": pickup_point[3]
                            },
                            "sender_details": {
                                "name": pickup_point[11],
                                "mobile": pickup_point[3]
                            },
                            "receiver_details": {
                                "name": order[13],
                                "mobile": customer_phone
                            },
                            "from_address": {
                                "address_line1": pickup_point[4],
                                "address_line2": pickup_point[5] if pickup_point[5] else pickup_point[10],
                                "landmark": "N/A",
                                "instructions_to_reach": "ANY",
                                "google_maps_address": str(pickup_point[4])+str(pickup_point[5]),
                                "exact_location": {
                                    "latitude": pick_lat,
                                    "longitude": pick_lon
                                },
                                "state": pickup_point[10],
                                "pincode": pickup_point[8]
                            },
                            "to_address": {
                                "address_line1": order[15],
                                "address_line2": order[16] if order[16] else order[19],
                                "landmark": "N/A",
                                "instructions_to_reach": "ANY",
                                "google_maps_address": str(order[15])+str(order[16]),
                                "exact_location": {
                                    "latitude": lat,
                                    "longitude": lon
                                },
                                "state": order[19],
                                "pincode": order[18]
                            }
                        }

            return_data_raw = requests.post(courier[16] + "/v1.0/vendor/order", headers=headers, data=json.dumps(pidge_body)).json()
            logger.info(str(order[0])+": "+str(return_data_raw))
            if return_data_raw.get('success'):
                order_status_change_ids.append(order[0])
                data_tuple = tuple([(
                    str(return_data_raw['data']['PBID']),
                    return_data_raw['message'],
                    order[0], pickup_point[1], courier[9], json.dumps(dimensions), volumetric_weight, weight,
                    "", pickup_point[2], "", None, "https://t.pidge.in/?t="+return_data_raw['data']['track_code'], zone)])

                if order[46] == 7:
                    push_awb_easyecom(order[39], order[36], str(return_data_raw['data']['PBID']), courier,
                                      cur, order[55], order[56])

                client_name = str(order[51])

                try:
                    tracking_link_wareiq = "https://t.pidge.in/?t="+return_data_raw['data']['track_code']
                    tracking_link_wareiq = UrlShortner.get_short_url(tracking_link_wareiq, cur)
                    send_received_event(client_name, customer_phone, tracking_link_wareiq)
                except Exception:
                    pass

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
                    cod_confirmation_link = "https://track.wareiq.com/core/v1/passthru/cod?CustomField=%s" % str(
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
                        tracking_link_wareiq = "https://webapp.wareiq.com/tracking/" + str(return_data['awbno'])
                        tracking_link_wareiq = UrlShortner.get_short_url(tracking_link_wareiq, cur)
                        exotel_sms_data[
                            sms_body_key] = "Received: Your order from %s . Track here: %s . Powered by WareIQ." % (
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


def push_awb_easyecom(invoice_id, api_token, awb, courier, cur, companyCarrierId, client_channel_id):
    try:
        if not companyCarrierId or not companyCarrierId.isdigit():
            cur.execute("""SELECT id, unique_parameter FROM client_channel
                        WHERE id=%s;"""%str(client_channel_id))

            cour = cur.fetchone()
            if not cour[1] or not cour[1].isdigit():
                add_url = "https://api.easyecom.io/Credentials/addCarrierCredentials?api_token=%s"%api_token
                post_body = {
                              "carrier_id": 14039,
                              "username":"wareiq",
                              "password":"wareiq",
                              "token": "wareiq"
                            }

                req = requests.post(add_url, data=post_body).json()
                cur.execute("UPDATE client_channel SET unique_parameter='%s' WHERE id=%s"%(req['data']['companyCarrierId'], str(client_channel_id)))
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
        if req.status_code!=200:
            requests.post(post_url, data=post_body)
            try:
                error = str(req.json())
            except Exception:
                error = None
            if error:
                cur.execute("UPDATE orders SET status_detail=%s WHERE order_id_channel_unique=%s and client_channel_id=%s",
                            (str(req.json()), invoice_id, client_channel_id))
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

        qr_url="https://track.wareiq.com/orders/v1/invoice/%s?uid=%s"%(str(order_id), ''.join(random.choices(string.ascii_lowercase+string.ascii_uppercase + string.digits, k=6)))

        cur.execute("""INSERT INTO orders_invoice (order_id, pickup_data_id, invoice_no_text, invoice_no, date_created, qr_url) 
                        VALUES (%s, %s, %s, %s, %s, %s);""", (order_id, pickup_data_id, inv_text, inv_no, datetime.utcnow()+timedelta(hours=5.5), qr_url))
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
        if not res.json()['results']:
            address = order[18] + ", " + order[17]
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


def get_lat_lon_pickup(pickup_point, cur):
    try:
        lat, lon = None, None
        address = pickup_point[4]
        if pickup_point[5]:
            address += " " + pickup_point[5]
        if pickup_point[6]:
            address += ", " + pickup_point[6]
        if pickup_point[10]:
            address += ", " + pickup_point[10]
        if pickup_point[8]:
            address += ", " + str(pickup_point[8])
        res = requests.get("https://maps.googleapis.com/maps/api/geocode/json?address=%s&key=%s" % (
        address, "AIzaSyBg7syNb_e1gZgyL1lHXBHRmg3jeaXrkco"))
        if not res.json()['results']:
            address = str(pickup_point[8]) + ", " + pickup_point[6]
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
            cur.execute("UPDATE pickup_points SET latitude=%s, longitude=%s WHERE id=%s", (lat, lon, pickup_point[1]))
        return lat, lon
    except Exception as e:
        logger.error("lat lon on found for order: ." + str(pickup_point[1]) + "   Error: " + str(e.args[0]))
        return None, None


bluedart_area_code_mapping = {"110015":"DEL",
                                "110077":"DEL",
                                "110059":"DEL",
                                "110093":"DEL",
                                "160062":"MOH",
                                "121002":"FAR",
                                "122001":"GGN",
                                "122004":"GGN",
                                "131028":"SOP",
                                "131101":"SOP",
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
                                "440027":"BOM",
                                "400064":"BOM",
                                "400097":"BOM",
                                "400050":"BOM",
                                "400705":"NBM",
                                "401107":"BOM",
                                "403001":"PNJ",
                                "410206":"NBM",
                                "411001":"PNQ",
                                "411005":"PNQ",
                                "413501":"OBD",
                                "421302":"BOM",
                                "422002":"NSK",
                                "431001":"AUR",
                                "440005":"NGP",
                                "444601":"AMT",
                                "455001":"DEW",
                                "457779":"JBU",
                                "480001":"CWD",
                                "500003":"HYD",
                                "501101":"VKB",
                                "501218":"VKB",
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
                                "560025":"BLR",
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
                                "562123":"BLR",
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
kama_chn_sdd_pincodes = ('600001','600002','600003','600004','600005','600006','600007','600008','600009','600010','600011','600012','600013','600014','600015','600016','600017','600018','600019','600020','600021','600022','600023','600024','600025','600026','600027','600028','600029','600030','600031','600032','600033','600034','600035','600036','600037','600038','600039','600040','600041','600042','600043','600044','600045','600046','600047','600048','600049','600050','600053','600056','600058','600059','600061','600064','600070','600073','600074','600075','600076','600077','600078','600079','600080','600081','600082','600083','600084','600085','600086','600087','600088','600089','600090','600091','600092','600093','600094','600095','600096','600097','600098','600099','600100','600101','600102','600104','600106','600107','600108','600110','600112','600113','600114','600115','600116','600117','600118','600119','600122','600125','600129','600063','603210','600126')
pidge_del_sdd_pincodes = ('110001','110002','110003','110004','110005','110006','110008','110011','110012','110015','110055','110060','110069','110031','110032','110051','110091','110092','110093','110095','110096','110030','110037','110038','110047','110061','110068','110070','110074','110007','110009','110026','110034','110035','110052','110054','110056','110063','110083','110085','110087','110088','110089','110013','110014','110016','110017','110019','110020','110021','110022','110023','110024','110025','110029','110044','110048','110049','110057','110062','110065','110066','110067','110076','110080','110090','110010','110018','110027','110028','110045','110046','110058','110059','110064','110075','110077','110078','110079','122001','122002','122003','122004','122005','122007','122008','122009','122010','122011','122016','122017','122018','201301','201303','201304','201305','201307','201309','201006','201007','201009','201010','201011','201012','201014','201016','201017','201018','201019','121001','121002','121003','121005','121006','121007','121008','121009','121010','121011','110033','110041','110042','110053','110084','110086','110094','121004','121012','121013','201002')
kama_TLLTRO_sdd_pincodes = ('500003','500007','500009','500017','500020','500025','500026','500039','500040','500044','500047','500056','500061','500076','500092','500062','500087','500094','500071','500098','500015','500010','500096','500032','500075','500030','500086','500104','500107','500264','500048','500019','500073','500100','500114','500018','500082','500016','500014','500049','500050','500055','500072','500085','500090','502032','500133','500138','502325','500079','500066','500065','500064','500060','500059','500057','500035','500036','500028','500027','500023','500013','500012','500001','500006','500004','500022','500041','500029','500063','500080','500005','500002')
kama_MHCHRO_sdd_pincodes = ('400601','400602','400603','400604','400605','400606','400607','400608','400610','400615','401107','406007','400016','400019','400037','400028','400014','400031','400025','400030','400018','400013','400012','400015','400033','400011','400027','400010','400035','400006','400036','400026','400034','400007','400008','400004','400009','400003','400002','400001','400020','400023','400032','400021','400039','400005','400050','400051','400098','400052','400029','400055','400054','400057','400056','400049','400058','400047','400060','400087','400102','400065','400062','400090','400104','400063','400097','400064','400101','400067','400017','401107','400601','400602','400603','400604','400605','400606','400607','400608','400610','400615','406007')
kama_MHJTRO_sdd_pincodes = ('400082','400080','400081','400078','400042','400076','400083','400079','400072','400084','400086','400075','400077','400089','400070','400071','400024','400059','400053','400069','400096','400093','400099','400074','400022','400043','400088','400085','400094','400030','400031','400032','400033','400034','400035','400036','400037','400039','400042','400043','400047','400049','400050','400051','400052','400053','400054','400055','400056','400057','400058','400059','400060','400062','400063','400064','400065','400067','400069','400070','400071','400072','400074','400075','400076','400077','400078','400079','400080','400081','400082','400083','400084','400085','400086','400087','400088','400089','400090','400093','400094','400096','400097','400098','400099','400101','400102','400104')
kama_HRDGRO_sdd_pincodes = ('110037','122001','122002','122004','122005','122006','122007','122008','122009','122010','122015','122016','122017','122018','122021','122022','122101','122102','122003','122011')
kama_RJMIRO_sdd_pincodes = ()
kama_UPPMRO_sdd_pincodes = ('226010','226016','226014','226005','226001','226006','226024','226012','226011','226017','226004','226020','226021')
kama_GJAORO_sdd_pincodes = ('380052','380059','380054','382455','380015','380058','380009','382210','380007','380005')
delhivery_embargo_pincodes = ('796012','796004','796007','796009','796008','796001','796005','641110','680547','680004','680002','680012','680570','680007','680620','680617','680017','680016','680641','680027','680613','680614','680553','680551','680621','680003','680612','680545','680571','680563','680549','680619','680642','680618','680569','680541','680011','680555','680015','680611','785615','785612','785609','785699','845414','796321','671310','671312','671313','572214','572218','680316','680699','680721','680303','680722','680741','680311','680735','680302','680732','680312','680309','680693','680307','680697','680731','680689','680736','680308','680325','641035','641006','641004','641048','641037','641009','641014','641049','641012','641027','641044','682008','682013','682009','682006','682011','682007','682001','682004','682010','682015','682003','682031','682002','682014','682005','682035','682016','682029','682018','682507','641018','641064','641033','641036','641045','641015','641005','641408','641111','641028','641103','641016','641405','641026','641109','641002','641101','641114','641010','641007','641001','782123','782122','783130','781135','783123','781127','534452','534449','534426','679579','679583','679551','679578','679594','679589','679553','679577','679580','679581','679587','679576','679586','679573','679584','679554','676110','679575','679552','679585','679591','679582','679536','679574','737113','737111','783124','783126','781320','783382','783121','783125','783101','783120','783122','785641','785702','785621','785610','785603','785705','785613','786121','785623','785625','785622','506369','506135','506134','601202','601205','600067','601206','601101','601203','601201','601207','601204','501511','501512','501505','796571','795010','795002','795005','523105','523109','523116','523113','523115','523279','523281','523104','523101','523271','523292','671329','671315','671316','671531','671551','671124','671321','671542','671545','671121','671544','671125','671317','671122','671123','796310','796081','796082','673018','673002','673015','673013','673029','673004','673026','673003','673019','673519','673001','673301','673014','673024','673032','673027','673655','673302','673028','673025','673007','670691','670650','670643','670701','670612','670693','796891','194101','796701','673651','676509','676519','676513','676504','676505','676528','676515','679324','676503','673642','676507','676517','676506','673649','676121','796441','843129','577413','577433','845449','845450','341023','847203','341301','341302','521229','534460','534467','125052','562132','572140','562111','843313','782126','125047','507163','507183','507161','534462','534461','521214','210208','521226','521227','521230','521228','585217','585301','332712','332701','303601','587115','587207','586201','272155','507209','811316','587206','582210','582116','582112','582120','212106','586115','586142','586220','586123','586206','586202','586120','586128','583224','273403','333012','333022','333307','333304','582114','583236','583232','212301','212307','671552','274401','845434','845429','273306','273310','507158','506381','509208','509352','509353','501501','591317','591220','843349','577134','577118','577127','577120','577126','501111','501102','501101','274203','274206','274402','274403','272173','272164','272170','272178','272175','754212','754224','754216','587145','509120','274604','506164','274603','222181','222170','222149','222148','785695','785694','222142','222129','331023','331303','843128','845431','845435','784190','782411','522661','522617','577412','577421','577426','577435','843324','676122','676521','676522','676523','679326','676525','691573','691579','695311','691578','670343','670353','670507','670521','670315','670358','670501','670312','670310','670332','671221','670304','670307','670346','670305','670333','670339','670314','670330','670309','671311','670327','670308','670337','670303','673523','673527','673524','673526','673525','673513','673614','673508','502278','502301','502279','502311','502312','502281','796261','785601','785602','796181','796901','577112','577139','577123','577411','505153','505152','505185','522330','522203','522313','522325','522213','522304','522308','522306','522307','522301','522211','522201','522202','796186','676107','676510','676562','676103','676313','676551','676111','676102','676108','676307','676101','676109','676561','676556','676105','676301','676106','676302','676502','676104','676309','676306','676311','676320','676508','676305','676511','676304','676319','676315','676303','676501','676308','784525','628908','628722','628712','628902','628901','628906','628903','628907','628904','530011','530046','530005','530026','530053','530044','530030','530032','530015','530049','530031','530025','530014','530012','142047','152021','142044','100094','101401','103102','111202','112004','115003','118136','120071','121081','122098','124146','126001','132203','133307','140021','141806','145022','164145','171094','171103','171203','171224','171225','171226','172110','172111','172113','173032','173204','173217','175132','175139','175141','176316','180021','181102','182146','182203','185101','185111','190006','190011','190012','190019','190020','190023','190025','191101','191111','191112','191113','191121','191131','191201','192006','192101','192122','192123','192124','192125','192129','192201','192202','192221','192231','192232','192301','192303','192305','193101','193102','193121','193201','193222','193223','193224','193301','193303','193401','193502','194103','194104','200401','201102','201203','206010','209509','212656','214306','216221','227813','229159','230401','231211','233310','243201','243203','246177','246435','247149','249125','249145','258001','260001','261403','262523','262551','262554','262902','263132','263140','263635','271215','271308','271855','275132','278182','284093','284203','285124','291301','300008','300075','300501','301710','303108','303508','303602','304025','306105','306601','306603','310001','313329','313330','313706','320001','320004','320008','322218','322251','324015','325204','325216','326030','327031','327034','331505','331517','332746','333305','334023','334202','334305','334602','341025','341319','342306','342314','344011','344031','344034','344037','344044','344801','360081','361143','362010','362730','364265','367001','369170','370145','380038','380068','380421','382230','382465','383091','383205','383422','389172','389860','391170','392130','393050','393105','396050','400045','400201','400204','400407','400508','400600','401064','401142','401603','401701','402501','403018','410001','410038','410114','411105','411201','411207','412040','412113','412206','412301','412303','413219','413221','413227','413416','413505','413521','413532','413605','415715','416212','416630','416813','420123','421101','421205','422201','422204','422205','422208','422301','423103','423106','423111','423117','425108','425205','425414','425442','426412','431091','433301','441046','441221','441901','441916','442024','442606','442906','445102','445202','445308','445402','452672','453551','456560','458667','460005','460220','461442','462062','465686','466445','466661','471405','473770','473885','475002','475335','475675','477557','480881','486670','488442','491558','491665','491771','491888','493455','493778','494114','494115','494226','494347','494441','494444','494449','494450','494552','494553','494661','494776','495116','495445','495449','495552','495674','496224','496242','496772','497231','497333','498847','500112','500301','500454','500901','501517','502023','502031','502210','502257','502345','506071','506112','506307','507134','507307','508113','508258','509003','509991','510571','512190','512701','513203','515281','515421','515556','515872','516173','517167','517194','517213','517305','517321','517324','517569','517643','518221','518225','518468','520018','521021','521105','521501','522026','522411','522505','523304','523305','523315','523346','524224','524228','524310','524343','524403','525002','527101','530004','530013','530016','530020','531043','531051','531105','531235','533001','533003','533004','533225','534201','534202','534203','534208','534463','535552','543260','544445','560203','560978','561101','561204','561209','562001','562073','566003','567103','569020','570033','571214','571250','571320','572119','572202','572222','572227','572228','573121','574201','574203','574208','574241','574325','576212','576233','577121','577122','577129','577145','577419','577424','577514','577537','577539','577540','578301','581239','581412','582117','582207','583128','583192','583277','584120','584121','584124','584127','585035','585127','585212','585215','585218','585355','585416','586205','587131','590000','590021','590049','591052','591559','596202','597201','600302','602026','602204','605501','623301','625215','625705','631052','631206','633001','635107','635116','635501','635853','638109','638152','641075','641625','642133','670633','671344','671416','671543','673586','674001','676555','678581','679501','680684','682555','685501','685505','685561','685612','685613','685615','686510','689662','689667','690641','691564','711351','714215','732010','734018','734501','735201','735220','737001','737106','737120','743001','743370','743373','744202','744204','744205','745112','750076','750103','751029','751032','754028','754139','754910','758017','758044','759029','759117','759147','761006','761121','761132','761209','761214','762016','762020','762104','763669','765016','765026','765029','766017','766027','766028','767016','767028','767035','768206','768621','770027','770037','770048','774123','782480','782486','782624','785600','786020','788735','788737','788820','789130','790001','790003','791122','792104','793106','793108','793109','794102','794114','795001','795007','795008','795009','795103','795113','795115','795126','795128','795130','795133','795136','795141','795142','795148','795149','796017','796036','796501','797108','799045','799181','799211','799284','802132','804483','811001','815356','822112','824144','824206','825323','834019','834498','835229','840001','843001','843319','843331','843502','845455','847105','847401','848205','851129','851130','852137','852213','852217','854332','854333','883937','949661','963210')