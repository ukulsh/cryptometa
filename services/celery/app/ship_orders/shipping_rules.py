import psycopg2, requests, os, json, pytz
import logging, random, string
from datetime import datetime, timedelta
from requests_oauthlib.oauth1_session import OAuth1Session
from zeep import Client
from app.db_utils import DbConnection, UrlShortner
from fedex.config import FedexConfig
from .utils import *
from .config import *

from .queries import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)

conn = DbConnection.get_db_connection_instance()
conn_2 = DbConnection.get_pincode_db_connection_instance()
cur_2 = conn_2.cursor()


class ShippingRules:

    def __init__(self, courier_name=None, order_ids=None, force_ship=None, client_prefix=None, cur=None):
        self.courier_name = courier_name
        self.order_ids = order_ids
        self.force_ship = force_ship
        self.client_prefix = client_prefix
        self.cur = cur if cur else conn.cursor()

    @staticmethod
    def check_rule_match_for_order(condition_type, conditions, order):

        match = False
        if not conditions:
            return True
        for each_condition in conditions:
            match = check_condition_match_for_order(each_condition, order)
            if (match and condition_type=='OR') or (not match and condition_type=='AND'):
                break

        return match

    def get_ship_orders_courier_wise(self):

        if self.courier_name and self.order_ids:
            if len(self.order_ids) == 1:
                order_id_tuple = "('" + str(self.order_ids[0]) + "')"
            else:
                order_id_tuple = str(tuple(self.order_ids))
            self.cur.execute("""DELETE FROM order_scans where shipment_id in (select id from shipments where order_id in %s);
                                DELETE FROM order_status where shipment_id in (select id from shipments where order_id in %s);
                               DELETE FROM shipments where order_id in %s;""" % (order_id_tuple, order_id_tuple, order_id_tuple))
            conn.commit()
            self.cur.execute("SELECT DISTINCT(client_prefix) from orders where id in %s" % order_id_tuple)
            client_list = self.cur.fetchall()
            self.cur.execute("""SELECT bb.id,bb.courier_name,bb.logo_url,bb.date_created,bb.date_updated,bb.api_key,bb.api_password,
                            bb.api_url FROM master_couriers bb WHERE courier_name='%s'""" % self.courier_name)
            courier_details = self.cur.fetchone()
            for client in client_list:
                return_dict = dict()
                courier = (None, client[0], None, 1, None, None, None, None, "") + courier_details
                self.cur.execute(get_orders_to_ship_query.replace('__ORDER_SELECT_FILTERS__', 'and aa.id in %s'),
                                 (client,client,order_id_tuple))
                all_orders = self.cur.fetchall()
                return_dict[courier_details[0]] = {"courier": courier, "orders": all_orders}

        self.cur.execute(delete_failed_shipments_query)
        time_now = datetime.utcnow()
        if time_now.hour == 22 and 0 < time_now.minute < 30:
            time_now = time_now - timedelta(days=30)
            self.cur.execute("""delete from shipments where order_id in 
                                (select id from orders where order_date>%s and status='NEW')
                                and remark = 'Pincode not serviceable'""", (time_now,))
        conn.commit()
        self.cur.execute(fetch_client_shipping_rules_query.replace('__CLIENT_FILTER__', "and aa.client_prefix!='DHANIPHARMACY'" if not
        self.client_prefix else "and aa.client_prefix='%s'"%self.client_prefix))
        all_rules = self.cur.fetchall()
        client_dict = {}
        for rule in all_rules:
            if rule[1] not in client_dict:
                client_dict[rule[1]] = [rule]
            else:
                client_dict[rule[1]].append(rule)

        for client, rules in client_dict.items():
            return_dict = dict()
            self.cur.execute(get_orders_to_ship_query.replace('__ORDER_SELECT_FILTERS__', 'and ll.id is null'),
                             (client,client))
            all_orders = self.cur.fetchall()
            for order in all_orders:
                courier_id = None
                for rule in rules:
                    match = self.check_rule_match_for_order(rule[4], rule[5], order)
                    if match:
                        courier_id = get_courier_id_to_ship_with(rule, str(order[18]), self.cur)
                        break
                if courier_id:
                    if courier_id not in return_dict:
                        return_dict[courier_id]=[order]
                    else:
                        return_dict[courier_id].append(order)


class ShipDelhivery:

    def __init__(self, cur=None, courier=None, orders=None, backup_param=True, force_ship=None):
        self.courier = courier
        self.all_orders = orders
        self.force_ship = force_ship
        self.cur = cur if cur else conn.cursor()
        self.headers = {"Authorization": "Token " + courier[14],
                        "Content-Type": "application/json"}
        self.pickup_point_order_dict = dict()
        self.orders_dict = dict()
        self.backup_param = backup_param

    def ship_orders(self):
        for order in self.all_orders:
            if order[41]:
                if order[41] not in self.pickup_point_order_dict:
                    self.pickup_point_order_dict[order[41]] = [order]
                else:
                    self.pickup_point_order_dict[order[41]].append(order)

        for pickup_id, all_new_orders in self.pickup_point_order_dict.items():

            last_shipped_order_id = 0
            pickup_points_tuple = (pickup_id,)
            self.cur.execute(get_pickup_points_query, pickup_points_tuple)
            pickup_point = self.cur.fetchone()  # change this as we get to dynamic pickups
            last_invoice_no = pickup_point[22] if pickup_point[22] else 0

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
                        if self.courier[1]=="DHANIPHARMACY" and zone not in ('A', 'B'):
                            continue
                        if not order[54]:
                            last_invoice_no = invoice_order(self.cur, last_invoice_no, pickup_point[23], order[0], pickup_id)
                        if order[26].lower() == 'cod' and not order[27] and not self.force_ship:
                            continue

                        self.orders_dict[str(order[0])] = (order[0], order[33], order[34], order[35],
                                                      order[36], order[37], order[38], order[39],
                                                      order[5], order[9], order[45], order[46],
                                                      order[51], order[52], zone, order[54], order[55], order[56])

                        if order[17].lower() in ("bengaluru", "bangalore", "banglore") and self.courier[1] in ("SOHOMATTRESS",) and \
                                order[26].lower() != 'pickup' and not self.force_ship:
                            continue

                        if self.courier[1] == "ZLADE" and self.courier[10] == "Delhivery Surface Standard" and zone and zone not in (
                                'A', 'B') and order[26].lower() != 'pickup' and not self.force_ship:
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

                        time_2_days = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=1)
                        if order[47] and not (order[50] and order[2] < time_2_days) and not self.force_ship:
                            if order[26].lower() == 'cod' and not order[42] and order[43]:
                                continue  # change this to continue later
                            if order[26].lower() == 'cod' and not order[43]:
                                try:  ## Cod confirmation  text
                                    cod_verification_text(order, self.cur)
                                except Exception as e:
                                    logger.error(
                                        "Cod confirmation not sent. Order id: " + str(order[0]))
                                continue
                        if order[0] > last_shipped_order_id:
                            last_shipped_order_id = order[0]

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
                        shipment_data['client'] = self.courier[15]
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
                delhivery_url = self.courier[16] + "api/cmu/create.json"

                req = requests.post(delhivery_url, headers=self.headers, data=delivery_shipments_body)
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
                    create_warehouse_url = self.courier[16] + "api/backend/clientwarehouse/create/"
                    requests.post(create_warehouse_url, headers=self.headers, data=json.dumps(warehouse_creation))
                    req = requests.post(delhivery_url, headers=self.headers, data=delivery_shipments_body)

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

                            order_status_change_ids.append(self.orders_dict[package['refnum']][0])
                            client_name = str(self.orders_dict[package['refnum']][12])
                            customer_phone = self.orders_dict[package['refnum']][8].replace(" ", "")
                            customer_phone = "0" + customer_phone[-10:]

                            if self.orders_dict[package['refnum']][11]==7:
                                push_awb_easyecom(self.orders_dict[package['refnum']][7],
                                                  self.orders_dict[package['refnum']][4],
                                                  package['waybill'], self.courier, self.cur, self.orders_dict[package['refnum']][16], self.orders_dict[package['refnum']][17])

                            try:
                                tracking_link_wareiq = "https://webapp.wareiq.com/tracking/" + str(package['waybill'])
                                tracking_link_wareiq = UrlShortner.get_short_url(tracking_link_wareiq, self.cur)
                                if self.courier[1] != 'DHANIPHARMACY':
                                    send_received_event(client_name, customer_phone, tracking_link_wareiq)
                            except Exception:
                                pass

                            if self.orders_dict[package['refnum']][9] == "NASHER":
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
                            self.cur.execute("select * from client_couriers where client_prefix=%s and priority=%s;",
                                        (self.courier[1], self.courier[3] + 1))
                            qs = self.cur.fetchone()
                            if not (qs and self.backup_param) or self.force_ship:
                                insert_shipments_data_query += "%s,"
                                insert_shipments_data_tuple.append(("", "Fail", self.orders_dict[package['refnum']][0], None,
                                                                    None, None, None, None, "Pincode not serviceable", None,
                                                                    None, self.orders_dict[package['refnum']][14]), )
                            continue

                        if 'COD' in remark or 'blocked' in remark:
                            continue

                        if not self.orders_dict[package['refnum']][13]:
                            dimensions = self.orders_dict[package['refnum']][1][0]
                            weight = self.orders_dict[package['refnum']][2][0] * self.orders_dict[package['refnum']][3][0]
                            volumetric_weight = (dimensions['length'] * dimensions['breadth'] * dimensions['height']) * \
                                                self.orders_dict[package['refnum']][3][0] / 5000
                            for idx, dim in enumerate(self.orders_dict[package['refnum']][1]):
                                if idx == 0:
                                    continue
                                volumetric_weight += (dim['length'] * dim['breadth'] * dim['height']) * \
                                                     self.orders_dict[package['refnum']][3][idx] / 5000
                                weight += self.orders_dict[package['refnum']][2][idx] * (self.orders_dict[package['refnum']][3][idx])

                            if dimensions['length'] and dimensions['breadth']:
                                dimensions['height'] = round(
                                    (volumetric_weight * 5000) / (dimensions['length'] * dimensions['breadth']))
                        else:
                            dimensions = {"length": 1, "breadth": 1, "height": 1}
                            weight = float(self.orders_dict[package['refnum']][13])
                            volumetric_weight = float(self.orders_dict[package['refnum']][13])

                        data_tuple = (package['waybill'], package['status'], self.orders_dict[package['refnum']][0], pickup_point[1],
                                      self.courier[9], json.dumps(dimensions), volumetric_weight, weight, remark, pickup_point[2],
                                      package['sort_code'], fulfillment_id, tracking_link, self.orders_dict[package['refnum']][14])
                        insert_shipments_data_tuple.append(data_tuple)
                        insert_shipments_data_query += "%s,"
                        insert_order_status_dict[package['waybill']] = [self.orders_dict[package['refnum']][0], self.courier[9],
                                                                        None, "UD", "Received", "Consignment Manifested",
                                                                        pickup_point[6], pickup_point[6],
                                                                        datetime.utcnow() + timedelta(hours=5.5)]

                    except Exception as e:
                        logger.error("Order not shipped. Remarks: " + str(package['remarks']) + "\nError: " + str(e.args[0]))

                if insert_shipments_data_tuple:
                    insert_shipments_data_tuple = tuple(insert_shipments_data_tuple)
                    insert_shipments_data_query = insert_shipments_data_query.strip(",")
                    insert_shipments_data_query += " RETURNING id,awb;"
                    self.cur.execute(insert_shipments_data_query, insert_shipments_data_tuple)
                    shipment_ret = self.cur.fetchall()
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

                    self.cur.execute(order_status_add_query, tuple(order_status_tuple_list))

                if order_status_change_ids:
                    if len(order_status_change_ids) == 1:
                        self.cur.execute(update_orders_status_query % (("(%s)") % str(order_status_change_ids[0])))
                    else:
                        self.cur.execute(update_orders_status_query, (tuple(order_status_change_ids),))

                conn.commit()

            if last_shipped_order_id:
                last_shipped_data_tuple = (
                    last_shipped_order_id, datetime.now(tz=pytz.timezone('Asia/Calcutta')), self.courier[1])
                self.cur.execute(update_last_shipped_order_query, last_shipped_data_tuple)

            self.cur.execute("UPDATE client_pickups SET invoice_last=%s WHERE id=%s;", (last_invoice_no, pickup_id))

            conn.commit()