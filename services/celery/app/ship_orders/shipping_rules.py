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

    def __init__(self, courier_name=None, order_ids=None, force_ship=None, client_prefix=None, cur=None, next_priority=None):
        self.courier_name = courier_name
        self.order_ids = order_ids
        self.force_ship = force_ship
        self.client_prefix = client_prefix
        self.cur = cur if cur else conn.cursor()
        self.next_priority = next_priority

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


    def ship_orders_with_given_courier(self, courier_obj):
        if courier_obj['courier'][10].startswith('Delhivery'):
            ship_obj = ShipDelhivery(courier=courier_obj['courier'], orders=courier_obj['orders'], cur=self.cur,
                                     next_priority=courier_obj['next_priority'] if courier_obj.get('next_priority')
                                     else self.next_priority)
            ship_obj.ship_orders()

        if courier_obj['courier'][10].startswith('Xpressbees'):
            ship_obj = ShipXpressbees(courier=courier_obj['courier'], orders=courier_obj['orders'], cur=self.cur,
                                      next_priority=courier_obj['next_priority'] if courier_obj.get('next_priority')
                                      else self.next_priority)
            ship_obj.ship_orders()

        if courier_obj['courier'][10].startswith('Bluedart'):
            ship_obj = ShipBluedart(courier=courier_obj['courier'], orders=courier_obj['orders'], cur=self.cur,
                                    next_priority=courier_obj['next_priority'] if courier_obj.get('next_priority')
                                    else self.next_priority)
            ship_obj.ship_orders()

        if courier_obj['courier'][10].startswith('Ecom'):
            ship_obj = ShipEcomExpress(courier=courier_obj['courier'], orders=courier_obj['orders'], cur=self.cur,
                                       next_priority=courier_obj['next_priority'] if courier_obj.get('next_priority')
                                       else self.next_priority)
            ship_obj.ship_orders()

        if courier_obj['courier'][10].startswith('Self Ship'):
            ship_obj = ShipSelfShip(courier=courier_obj['courier'], orders=courier_obj['orders'], cur=self.cur,
                                    next_priority=courier_obj['next_priority'] if courier_obj.get('next_priority')
                                    else self.next_priority)
            ship_obj.ship_orders()

        if courier_obj['courier'][10].startswith('Pidge'):
            ship_obj = ShipPidge(courier=courier_obj['courier'], orders=courier_obj['orders'], cur=self.cur,
                                 next_priority=courier_obj['next_priority'] if courier_obj.get('next_priority')
                                 else self.next_priority)
            ship_obj.ship_orders()

        if courier_obj['courier'][10].startswith('FedEx'):
            ship_obj = ShipFedex(courier=courier_obj['courier'], orders=courier_obj['orders'], cur=self.cur,
                                 next_priority=courier_obj['next_priority'] if courier_obj.get('next_priority')
                                 else self.next_priority)
            ship_obj.ship_orders()

        if courier_obj['courier'][10].startswith('DTDC'):
            ship_obj = ShipDTDC(courier=courier_obj['courier'], orders=courier_obj['orders'], cur=self.cur,
                                 next_priority=courier_obj['next_priority'] if courier_obj.get('next_priority')
                                 else self.next_priority)
            ship_obj.ship_orders()

        if courier_obj['courier'][10].startswith('Blowhorn'):
            ship_obj = ShipBlowhorn(courier=courier_obj['courier'], orders=courier_obj['orders'], cur=self.cur,
                                next_priority=courier_obj['next_priority'] if courier_obj.get('next_priority')
                                else self.next_priority)
            ship_obj.ship_orders()

    def ship_orders_courier_wise(self):

        if self.courier_name and self.order_ids:
            if len(self.order_ids) == 1:
                order_id_tuple = "(" + str(self.order_ids[0]) + ")"
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
                courier = (None, client[0], None, 1, None, None, None, None, "") + courier_details
                self.cur.execute(get_orders_to_ship_query.replace('__ORDER_SELECT_FILTERS__', 'and aa.id in %s'),
                                 (client,client,tuple(self.order_ids)))
                all_orders = self.cur.fetchall()
                courier_obj = {"courier": courier, "orders": all_orders}
                self.ship_orders_with_given_courier(courier_obj)

            return None

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
                next_priority = None
                for rule in rules:
                    match = self.check_rule_match_for_order(rule[4], rule[5], order)
                    if match:
                        courier_id, next_priority = get_courier_id_to_ship_with(rule, str(order[18]), self.cur)
                        break
                if courier_id:
                    if courier_id not in return_dict:
                        self.cur.execute("""SELECT bb.id,bb.courier_name,bb.logo_url,bb.date_created,bb.date_updated,bb.api_key,bb.api_password,
                            bb.api_url FROM master_couriers bb WHERE id=%s""" % str(courier_id))
                        courier_details = self.cur.fetchone()
                        courier = (None, client, None, 1, None, None, None, None, "") + courier_details
                        return_dict[courier_id]={"courier": courier, "orders": [order], "next_priority": next_priority}
                    else:
                        return_dict[courier_id]['orders'].append(order)

            for courier_id, courier_obj in return_dict.items():
                self.ship_orders_with_given_courier(courier_obj)


class ShipDelhivery:

    def __init__(self, cur=None, courier=None, orders=None, force_ship=None, next_priority=None):
        self.courier = courier
        self.all_orders = orders
        self.force_ship = force_ship
        self.cur = cur if cur else conn.cursor()
        self.headers = {"Authorization": "Token " + courier[14],
                        "Content-Type": "application/json"}
        self.pickup_point_order_dict = dict()
        self.orders_dict = dict()
        self.next_priority = next_priority

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
                            if self.next_priority:
                                push_order_to_next_priority(self.next_priority, [order[0]], self.courier, self.cur)
                            continue
                        zone = None
                        try:
                            zone = get_delivery_zone(pickup_point[8], order[18])
                        except Exception as e:
                            logger.error("couldn't find zone: " + str(order[0]) + "\nError: " + str(e))

                        if not order[54]:
                            last_invoice_no = invoice_order(self.cur, last_invoice_no, pickup_point[23], order[0], pickup_id)
                        if order[26].lower() == 'cod' and not order[27] and not self.force_ship:
                            continue

                        self.orders_dict[str(order[0])] = (order[0], order[33], order[34], order[35],
                                                      order[36], order[37], order[38], order[39],
                                                      order[5], order[9], order[45], order[46],
                                                      order[51], order[52], zone, order[54], order[55], order[56], order[60])

                        weight, volumetric_weight, dimensions = calculate_order_weight_dimensions(order)

                        if self.courier[10] == "Delhivery Surface Standard" and not self.force_ship:
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
                                    self.cur.execute("""SELECT id, courier_name, logo_url, date_created, date_updated, api_key, 
                                                                                        api_password, api_url FROM master_couriers
                                                                                        WHERE courier_name='%s'""" % new_courier_name)
                                    courier_data = self.cur.fetchone()
                                    courier_new = list(self.courier)
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
                                    ship_obj = ShipDelhivery(courier=tuple(courier_new), orders=[order])
                                    ship_obj.ship_orders()
                                except Exception as e:
                                    logger.error(
                                        "Couldn't assign backup courier for: " + str(order[0]) + "\nError: " + str(e.args))
                                    pass

                                continue

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

                        if order[26].lower() == "pickup":
                            item_list = []
                            self.cur.execute("""SELECT image_url, color, reason, unique_id, quantity, name FROM
                                                 return_order_quality_check aa
                                                 left join op_association bb on aa.order_id=bb.order_id
                                                 and aa.master_product_id=bb.master_product_id
                                                 left join master_products cc on aa.master_product_id=cc.id
                                                 WHERE aa.order_id=%s"""%str(order[0]))
                            items=self.cur.fetchall()
                            for item in items:
                                item_list.append({"images": item[0],
                                                  "color": item[1],
                                                  "reason": item[2],
                                                  "ean": item[3] if item[3] else "",
                                                  "imei": item[3] if item[3] else "",
                                                  "item_quantity": item[4],
                                                  "descr": item[5]
                                                  })
                            shipment_data['qc'] = {'item': item_list}

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
                                if self.orders_dict[package['refnum']][18]:
                                    tracking_link_wareiq = "https://"+self.orders_dict[package['refnum']][18]+".wiq.app/tracking/" + str(package['waybill'])
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
                            if self.next_priority:
                                push_order_to_next_priority(self.next_priority, [self.orders_dict[package['refnum']][0]], self.courier, self.cur)
                            else:
                                insert_shipments_data_query += "%s,"
                                insert_shipments_data_tuple.append(("", "Fail", self.orders_dict[package['refnum']][0], None,
                                                                    None, None, None, None, "Pincode not serviceable", None,
                                                                    None, None, None, self.orders_dict[package['refnum']][14]), )
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
                    status_add_orders = 0
                    for ship_temp in shipment_ret:
                        if ship_temp[1]:
                            status_add_orders += 1
                            insert_order_status_dict[ship_temp[1]][2] = ship_temp[0]
                            order_status_add_query += "%s,"
                            order_status_tuple_list.append(tuple(insert_order_status_dict[ship_temp[1]]))

                    if status_add_orders:
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


class ShipXpressbees:

    def __init__(self, cur=None, courier=None, orders=None, force_ship=None, next_priority=None):
        self.courier = courier
        self.all_orders = orders
        self.force_ship = force_ship
        self.cur = cur if cur else conn.cursor()
        self.pickup_point_order_dict = dict()
        self.orders_dict = dict()
        self.last_assigned_awb = 0
        self.headers = {"Content-Type": "application/json"}
        self.next_priority = next_priority
        try:
            self.cur.execute("select max(awb) from shipments where courier_id=%s;" % str(courier[9]))
            fet_res = self.cur.fetchone()
            if fet_res:
                self.last_assigned_awb = int(fet_res[0])
        except Exception:
            pass

    def ship_orders(self):
        for order in self.all_orders:
            if order[41]:
                if order[41] not in self.pickup_point_order_dict:
                    self.pickup_point_order_dict[order[41]] = [order]
                else:
                    self.pickup_point_order_dict[order[41]].append(order)

        for pickup_id, all_new_orders in self.pickup_point_order_dict.items():
            last_shipped_order_id = 0
            headers = {"Content-Type": "application/json"}
            pickup_points_tuple = (pickup_id,)
            self.cur.execute(get_pickup_points_query, pickup_points_tuple)
            order_status_change_ids = list()

            pickup_point = self.cur.fetchone()  # change this as we get to dynamic pickups

            last_invoice_no = pickup_point[22] if pickup_point[22] else 0
            for order in all_new_orders:
                if not order[54]:
                    last_invoice_no = invoice_order(self.cur, last_invoice_no, pickup_point[23], order[0], pickup_id)
                if order[26].lower() == 'cod' and not order[27] and not self.force_ship:
                    continue
                if self.force_ship and order[26].lower() == 'pickup':
                    continue
                if order[26].lower() == 'pickup':
                    try:
                        self.cur.execute("""SELECT id, courier_name, logo_url, date_created, date_updated, api_key, 
                                                                            api_password, api_url FROM master_couriers
                                                                            WHERE courier_name='%s'""" % "Delhivery Surface Standard")
                        courier_data = self.cur.fetchone()
                        courier_new = list(self.courier)
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
                        ship_obj = ShipDelhivery(courier=tuple(courier_new), orders=[order])
                        ship_obj.ship_orders()
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
                if order[47] and not (order[50] and order[2] < time_2_days) and not self.force_ship:
                    if order[26].lower() == 'cod' and not order[42] and order[43]:
                        continue
                    if order[26].lower() == 'cod' and not order[43]:
                        if order[26].lower() == 'cod' and not order[43]:
                            try:  ## Cod confirmation  text
                                cod_verification_text(order, self.cur)
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

                    weight, volumetric_weight, dimensions = calculate_order_weight_dimensions(order)

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
                    self.last_assigned_awb += 1
                    username = self.courier[14].split("|")[0]
                    password = self.courier[14].split("|")[1]
                    bus_acc_name = self.courier[14].split("|")[2]
                    expressbees_shipment_body = {"AirWayBillNO": str(self.last_assigned_awb),
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
                                                                                               "secretkey": self.courier[15].split("|")[0]}))
                    headers['token'] = req_auth.json()['token']
                    headers['versionnumber'] = "v1"
                    xpressbees_url = "http://api.shipmentmanifestation.xbees.in/shipmentmanifestation/Forward"
                    req = requests.post(xpressbees_url, headers=headers, data=json.dumps(expressbees_shipment_body))
                    while req.json()['ReturnMessage'] == 'AWB Already Exists' or req.json()['ReturnMessage']=='AirWayBillNO Already exists':
                        self.last_assigned_awb += 1
                        expressbees_shipment_body['AirWayBillNO'] = str(self.last_assigned_awb)
                        req = requests.post(xpressbees_url, headers=headers,
                                            data=json.dumps(expressbees_shipment_body))

                    if req.json()['ReturnMessage'] == 'Invalid AWB Prefix' or req.json()['ReturnMessage'].startswith('Invalid AirWayBillNO'):
                        headers['XBkey'] = self.courier[15].split("|")[1]
                        batch_create_req = requests.post("http://xbclientapi.xbees.in/POSTShipmentService.svc/AWBNumberSeriesGeneration", headers=headers, json={"BusinessUnit": "ECOM", "ServiceType":"FORWARD", "DeliveryType": "COD"})
                        batch_req = requests.post("http://xbclientapi.xbees.in/TrackingService.svc/GetAWBNumberGeneratedSeries", headers=headers, json={"BusinessUnit": "ECOM", "ServiceType":"FORWARD", "BatchID": batch_create_req.json()['BatchID']})
                        expressbees_shipment_body['AirWayBillNO'] = str(batch_req.json()['AWBNoSeries'][0])
                        req = requests.post(xpressbees_url, headers=headers, data=json.dumps(expressbees_shipment_body))
                        self.last_assigned_awb = int(batch_req.json()['AWBNoSeries'][0])

                    return_data_raw = req.json()
                    insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                                                                        dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, 
                                                                                                        channel_fulfillment_id, tracking_link, zone)
                                                                                                        VALUES  %s RETURNING id;"""

                    if return_data_raw['ReturnMessage'] == 'successful' or return_data_raw['ReturnMessage']=='Successfull':

                        order_status_change_ids.append(order[0])
                        data_tuple = tuple([(
                            return_data_raw['AWBNo'], return_data_raw['ReturnMessage'],
                            order[0], pickup_point[1], self.courier[9], json.dumps(dimensions), volumetric_weight, weight,
                            "", pickup_point[2], "", fulfillment_id, tracking_link, zone)])

                        if order[46] == 7:
                            push_awb_easyecom(order[39],order[36], return_data_raw['AWBNo'], self.courier, self.cur, order[55], order[56])

                        client_name = str(order[51])
                        customer_phone = order[5].replace(" ", "")
                        customer_phone = "0" + customer_phone[-10:]

                        try:
                            tracking_link_wareiq = "https://webapp.wareiq.com/tracking/" + str(return_data_raw['AWBNo'])
                            if order[60]:
                                tracking_link_wareiq = "https://"+order[60]+".wiq.app/tracking/" + str(return_data_raw['AWBNo'])
                            tracking_link_wareiq = UrlShortner.get_short_url(tracking_link_wareiq, self.cur)
                            if self.courier[1] != 'DHANIPHARMACY':
                                send_received_event(client_name, customer_phone, tracking_link_wareiq)
                        except Exception:
                            pass

                    else:
                        if self.next_priority:
                            push_order_to_next_priority(self.next_priority, [order[0]], self.courier, self.cur)
                        else:
                            insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id,
                                                                dimensions, volumetric_weight, weight, remark, return_point_id, routing_code,
                                                                channel_fulfillment_id, tracking_link, zone)
                                                                        VALUES  %s"""
                            insert_shipments_data_tuple = list()
                            insert_shipments_data_tuple.append(("", "Fail", order[0], None,
                                                                None, None, None, None, "Pincode not serviceable", None,
                                                                None, None, None, zone), )
                            self.cur.execute(insert_shipments_data_query, tuple(insert_shipments_data_tuple))
                        continue

                    self.cur.execute(insert_shipments_data_query, data_tuple)
                    ship_temp = self.cur.fetchone()
                    order_status_add_query = """INSERT INTO order_status (order_id, courier_id, shipment_id, 
                                                    status_code, status, status_text, location, location_city, 
                                                    status_time) VALUES %s"""

                    order_status_add_tuple = [(order[0], self.courier[9],
                                               ship_temp[0], "UD", "Received", "Consignment Manifested",
                                               pickup_point[6], pickup_point[6], datetime.utcnow() + timedelta(hours=5.5))]

                    self.cur.execute(order_status_add_query, tuple(order_status_add_tuple))
                    self.cur.execute("UPDATE orders SET status='READY TO SHIP' WHERE id=%s;" % str(order[0]))
                    conn.commit()

                except Exception as e:
                    conn.rollback()
                    print("couldn't assign order: " + str(order[1]) + "\nError: " + str(e))

            if last_shipped_order_id:
                last_shipped_data_tuple = (
                    last_shipped_order_id, datetime.now(tz=pytz.timezone('Asia/Calcutta')), self.courier[1])
                self.cur.execute(update_last_shipped_order_query, last_shipped_data_tuple)

            self.cur.execute("UPDATE client_pickups SET invoice_last=%s WHERE id=%s;", (last_invoice_no, pickup_id))

            conn.commit()


class ShipBluedart:

    def __init__(self, cur=None, courier=None, orders=None, force_ship=None, next_priority=None):
        self.courier = courier
        self.all_orders = orders
        self.force_ship = force_ship
        self.cur = cur if cur else conn.cursor()
        self.pickup_point_order_dict = dict()
        self.orders_dict = dict()
        self.headers = {"Content-Type": "application/json"}
        self.bluedart_url = courier[16] + "/Ver1.9/ShippingAPI/WayBill/WayBillGeneration.svc?wsdl"
        self.waybill_client = Client(self.bluedart_url)
        self.login_id = self.courier[15].split('|')[0]
        self.customer_code = self.courier[15].split('|')[1]
        self.next_priority = next_priority
        self.client_profile = {
            "LoginID": self.login_id,
            "LicenceKey": self.courier[14],
            "Api_type": "S",
            "Version": "1.3"
        }

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
            order_status_change_ids = list()

            pickup_point = self.cur.fetchone()  # change this as we get to dynamic pickups

            last_invoice_no = pickup_point[22] if pickup_point[22] else 0

            pickup_pincode = str(pickup_point[8]).rstrip() if pickup_point[8] else None
            if pickup_pincode and pickup_pincode in bluedart_area_code_mapping:
                area_code = bluedart_area_code_mapping[pickup_pincode]
            else:
                order_ids = [order[0] for order in all_new_orders]
                if self.next_priority:
                    push_order_to_next_priority(self.next_priority, order_ids, self.courier, self.cur)
                continue

            for order in all_new_orders:
                if not order[54]:
                    last_invoice_no = invoice_order(self.cur, last_invoice_no, pickup_point[23], order[0], pickup_id)
                if order[26].lower() == 'cod' and not order[27] and not self.force_ship:
                    continue
                if order[26].lower() == 'pickup':
                    continue
                zone = None
                try:
                    zone = get_delivery_zone(pickup_point[8], order[18])
                except Exception as e:
                    logger.error("couldn't find zone: " + str(order[0]) + "\nError: " + str(e))

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
                try:

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

                    shipper['CustomerCode'] = self.customer_code
                    shipper['OriginArea'] = "BOM" if pickup_point[18]=='AAJMUM' and self.courier[1]=='DHANIPHARMACY' else area_code
                    shipper['CustomerName'] = self.courier[1]

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
                    if self.courier[1]=='DHANIPHARMACY':
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

                    weight, volumetric_weight, dimensions = calculate_order_weight_dimensions(order)

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
                        "Profile": self.client_profile
                    }

                    req = self.waybill_client.service.GenerateWayBill(**request_data)
                    insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, 
                                                channel_fulfillment_id, tracking_link, zone)
                                                VALUES  %s RETURNING id;"""
                    if req['AWBNo']:
                        order_status_change_ids.append(order[0])
                        routing_code = str(req['DestinationArea']) + "-" + str(req['DestinationLocation'])
                        data_tuple = tuple([(
                            req['AWBNo'], "", order[0], pickup_point[1], self.courier[9], json.dumps(dimensions),
                            volumetric_weight, weight,
                            "", pickup_point[2], routing_code, fulfillment_id, tracking_link, zone)])

                        if order[46] == 7:
                            push_awb_easyecom(order[39],order[36], req['AWBNo'], self.courier, self.cur, order[55], order[56])

                        client_name = str(order[51])
                        customer_phone = order[5].replace(" ", "")
                        customer_phone = "0" + customer_phone[-10:]

                        try:
                            tracking_link_wareiq = "https://webapp.wareiq.com/tracking/" + str(req['AWBNo'])
                            if order[60]:
                                tracking_link_wareiq = "https://"+order[60]+".wiq.app/tracking/" + str(req['AWBNo'])
                            tracking_link_wareiq = UrlShortner.get_short_url(tracking_link_wareiq, self.cur)
                            if self.courier[1]!='DHANIPHARMACY':
                                send_received_event(client_name, customer_phone, tracking_link_wareiq)
                        except Exception:
                            pass

                    else:
                        if self.next_priority:
                            push_order_to_next_priority(self.next_priority, [order[0]], self.courier, self.cur)
                        else:
                            insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id,
                                                                dimensions, volumetric_weight, weight, remark, return_point_id, routing_code,
                                                                channel_fulfillment_id, tracking_link, zone)
                                                                                    VALUES  %s"""
                            insert_shipments_data_tuple = list()
                            insert_shipments_data_tuple.append(("", "Fail", order[0], None,
                                                                None, None, None, None, "Pincode not serviceable", None,
                                                                None, None, None, zone), )
                            self.cur.execute(insert_shipments_data_query, tuple(insert_shipments_data_tuple))
                        continue

                    self.cur.execute(insert_shipments_data_query, data_tuple)
                    ship_temp = self.cur.fetchone()
                    order_status_add_query = """INSERT INTO order_status (order_id, courier_id, shipment_id, 
                                                                status_code, status, status_text, location, location_city, 
                                                                status_time) VALUES %s"""

                    order_status_add_tuple = [(order[0], self.courier[9],
                                               ship_temp[0], "UD", "Received", "Consignment Manifested",
                                               pickup_point[6], pickup_point[6],
                                               datetime.utcnow() + timedelta(hours=5.5))]

                    self.cur.execute(order_status_add_query, tuple(order_status_add_tuple))
                    self.cur.execute("UPDATE orders SET status='READY TO SHIP' WHERE id=%s;"%str(order[0]))
                    conn.commit()

                except Exception as e:
                    conn.rollback()
                    print("couldn't assign order: " + str(order[1]) + "\nError: " + str(e))

            if last_shipped_order_id:
                last_shipped_data_tuple = (
                    last_shipped_order_id, datetime.now(tz=pytz.timezone('Asia/Calcutta')), self.courier[1])
                self.cur.execute(update_last_shipped_order_query, last_shipped_data_tuple)

            self.cur.execute("UPDATE client_pickups SET invoice_last=%s WHERE id=%s;", (last_invoice_no, pickup_id))

            conn.commit()


class ShipEcomExpress:

    def __init__(self, cur=None, courier=None, orders=None, force_ship=None, next_priority=None):
        self.courier = courier
        self.all_orders = orders
        self.force_ship = force_ship
        self.cur = cur if cur else conn.cursor()
        self.headers = {"Authorization": "Token " + courier[14],
                        "Content-Type": "application/json"}
        self.pickup_point_order_dict = dict()
        self.orders_dict = dict()
        self.last_assigned_awb_cod = 0
        self.last_assigned_awb_ppd = 0
        self.next_priority = next_priority
        try:
            cur.execute("""select awb from shipments aa
                                left join orders bb on aa.order_id=bb.id
                                left join orders_payments cc on cc.order_id=bb.id
                                where courier_id=%s
                                and payment_mode ilike 'cod'
                                order by aa.id DESC
                                LIMIT 1;""" % str(courier[9]))

            self.last_assigned_awb_cod = cur.fetchone()[0]
            self.last_assigned_awb_cod = int(self.last_assigned_awb_cod)

            cur.execute("""select awb from shipments aa
                                left join orders bb on aa.order_id=bb.id
                                left join orders_payments cc on cc.order_id=bb.id
                                where courier_id=%s
                                and (payment_mode ilike 'prepaid' or payment_mode ilike 'paid')
                                order by aa.id DESC
                                LIMIT 1;""" % str(courier[9]))

            self.last_assigned_awb_ppd = cur.fetchone()[0]
            self.last_assigned_awb_ppd = int(self.last_assigned_awb_ppd)
        except Exception:
            pass

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
            order_status_change_ids = list()

            pickup_point = self.cur.fetchone()  # change this as we get to dynamic pickups

            last_invoice_no = pickup_point[22] if pickup_point[22] else 0

            for order in all_new_orders:
                if not order[54]:
                    last_invoice_no = invoice_order(self.cur, last_invoice_no, pickup_point[23], order[0], pickup_id)
                if order[26].lower() == 'cod' and not order[27] and not self.force_ship:
                    continue
                if self.force_ship and order[26].lower() == 'pickup':
                    continue
                if order[26].lower() == 'pickup':
                    try:
                        self.cur.execute("""SELECT id, courier_name, logo_url, date_created, date_updated, api_key, 
                                                                            api_password, api_url FROM master_couriers
                                                                            WHERE courier_name='%s'""" % "Delhivery Surface Standard")
                        courier_data = self.cur.fetchone()
                        courier_new = list(self.courier)
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
                        ship_obj = ShipDelhivery(courier=tuple(courier_new), orders=[order])
                        ship_obj.ship_orders()
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
                if order[47] and not (order[50] and order[2] < time_2_days) and not self.force_ship:
                    if order[26].lower() == 'cod' and not order[42] and order[43]:
                        continue
                    if order[26].lower() == 'cod' and not order[43]:
                        if order[26].lower() == 'cod' and not order[43]:
                            try:  ## Cod confirmation  text
                                cod_verification_text(order, self.cur)
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

                    weight, volumetric_weight, dimensions = calculate_order_weight_dimensions(order)

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
                        self.last_assigned_awb_cod += 1
                        last_assigned_awb = self.last_assigned_awb_cod
                    else:
                        self.last_assigned_awb_ppd += 1
                        last_assigned_awb = self.last_assigned_awb_ppd
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
                        "LENGTH": dimensions['length'] if dimensions['length']<150 else 149,
                        "BREADTH": dimensions['breadth'] if dimensions['breadth']<150 else 149,
                        "HEIGHT": dimensions['height'] if dimensions['height']<150 else 149,
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

                    ecom_url = self.courier[16] + "/apiv3/manifest_awb/"
                    req = requests.post(ecom_url, data={"username": self.courier[14], "password": self.courier[15],
                                                        "json_input": json.dumps([json_input])})

                    while req.json()['shipments'][0]['reason'] == 'AIRWAYBILL_IN_USE':
                        last_assigned_awb += 1
                        json_input['AWB_NUMBER'] = str(last_assigned_awb)
                        req = requests.post(ecom_url, data={"username": self.courier[14], "password": self.courier[15],
                                                            "json_input": json.dumps([json_input])})

                    if req.json()['shipments'][0]['reason'] == 'INCORRECT_AWB_NUMBER':
                        fetch_awb_url = self.courier[16] + "/apiv2/fetch_awb/"
                        fetch_awb_req = requests.post(fetch_awb_url, data={"username": self.courier[14], "password": self.courier[15],
                                                                           "count": 50, "type":json_input['PRODUCT']})
                        json_input['AWB_NUMBER'] = str(fetch_awb_req.json()['awb'][0])
                        req = requests.post(ecom_url, data={"username": self.courier[14], "password": self.courier[15],
                                                            "json_input": json.dumps([json_input])})
                        if order[26].lower() == "cod":
                            self.last_assigned_awb_cod = int(fetch_awb_req.json()['awb'][0])
                        else:
                            self.last_assigned_awb_ppd = int(fetch_awb_req.json()['awb'][0])
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
                            order[0], pickup_point[1], self.courier[9], json.dumps(dimensions), volumetric_weight, weight,
                            "", pickup_point[2], "", fulfillment_id, tracking_link, zone)])

                        if order[46] == 7:
                            push_awb_easyecom(order[39],order[36], return_data_raw['shipments'][0]['awb'], self.courier,
                                              self.cur, order[55], order[56])

                        client_name = str(order[51])
                        customer_phone = order[5].replace(" ", "")
                        customer_phone = "0" + customer_phone[-10:]

                        try:
                            tracking_link_wareiq = "https://webapp.wareiq.com/tracking/" + str(return_data_raw['shipments'][0]['awb'])
                            if order[60]:
                                tracking_link_wareiq = "https://"+order[60]+".wiq.app/tracking/" + str(return_data_raw['shipments'][0]['awb'])
                            tracking_link_wareiq = UrlShortner.get_short_url(tracking_link_wareiq, self.cur)
                            if self.courier[1] != 'DHANIPHARMACY':
                                send_received_event(client_name, customer_phone, tracking_link_wareiq)
                        except Exception:
                            pass

                    else:
                        if self.next_priority:
                            push_order_to_next_priority(self.next_priority, [order[0]], self.courier, self.cur)
                        else:
                            insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id,
                                                                dimensions, volumetric_weight, weight, remark, return_point_id, routing_code,
                                                                channel_fulfillment_id, tracking_link, zone)
                                                                        VALUES  %s"""
                            insert_shipments_data_tuple = list()
                            insert_shipments_data_tuple.append(("", "Fail", order[0], None,
                                                                None, None, None, None, "Pincode not serviceable", None,
                                                                None, None, None, zone), )
                            self.cur.execute(insert_shipments_data_query, tuple(insert_shipments_data_tuple))
                        continue

                    self.cur.execute(insert_shipments_data_query, data_tuple)
                    ship_temp = self.cur.fetchone()
                    order_status_add_query = """INSERT INTO order_status (order_id, courier_id, shipment_id, 
                                                    status_code, status, status_text, location, location_city, 
                                                    status_time) VALUES %s"""

                    order_status_add_tuple = [(order[0], self.courier[9],
                                               ship_temp[0], "UD", "Received", "Consignment Manifested",
                                               pickup_point[6], pickup_point[6], datetime.utcnow() + timedelta(hours=5.5))]

                    self.cur.execute(order_status_add_query, tuple(order_status_add_tuple))
                    self.cur.execute("UPDATE orders SET status='READY TO SHIP' WHERE id=%s;" % str(order[0]))
                    conn.commit()

                except Exception as e:
                    conn.rollback()
                    print("couldn't assign order: " + str(order[1]) + "\nError: " + str(e))

            if last_shipped_order_id:
                last_shipped_data_tuple = (
                    last_shipped_order_id, datetime.now(tz=pytz.timezone('Asia/Calcutta')), self.courier[1])
                self.cur.execute(update_last_shipped_order_query, last_shipped_data_tuple)

            self.cur.execute("UPDATE client_pickups SET invoice_last=%s WHERE id=%s;", (last_invoice_no, pickup_id))

            conn.commit()


class ShipFedex:

    def __init__(self, cur=None, courier=None, orders=None, force_ship=None, next_priority=None):
        self.courier = courier
        self.all_orders = orders
        self.force_ship = force_ship
        self.cur = cur if cur else conn.cursor()
        self.pickup_point_order_dict = dict()
        self.orders_dict = dict()
        self.api_key = courier[14].split('|')[0]
        self.api_pass = courier[14].split('|')[1]
        self.account_number = courier[15].split('|')[0]
        self.meter_number = courier[15].split('|')[1]
        self.shipment_type = courier[15].split('|')[2]
        self.next_priority = next_priority
        self.CONFIG_OBJ = FedexConfig(key=self.api_key,
                                 password=self.api_pass,
                                 account_number=self.account_number,
                                 meter_number=self.meter_number)

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
            order_status_change_ids = list()

            pickup_point = self.cur.fetchone()  # change this as we get to dynamic pickups

            last_invoice_no = pickup_point[22] if pickup_point[22] else 0

            for order in all_new_orders:
                if not order[54]:
                    last_invoice_no = invoice_order(self.cur, last_invoice_no, pickup_point[23], order[0], pickup_id)
                if order[26].lower() == 'cod' and not order[27] and not self.force_ship:
                    continue
                if order[26].lower() == 'pickup':
                    continue
                zone = None
                try:
                    zone = get_delivery_zone(pickup_point[8], order[18])
                except Exception as e:
                    logger.error("couldn't find zone: " + str(order[0]) + "\nError: " + str(e))

                if self.courier[1] == "ZLADE" and zone in ('A', ) and not self.force_ship:
                    continue

                if order[26].lower() == "prepaid" and self.courier[1] in ("ACTIFIBER", "BEHIR", "SHAHIKITCHEN", "SUKHILIFE", "ORGANICRIOT") and not self.force_ship:
                    continue

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
                try:
                    from fedex.services.availability_commitment_service import FedexAvailabilityCommitmentRequest
                    avc_request = FedexAvailabilityCommitmentRequest(self.CONFIG_OBJ)
                    avc_request.Origin.PostalCode = pickup_point[8]
                    avc_request.Origin.CountryCode = 'IN'
                    avc_request.Destination.PostalCode = order[18]  # 29631
                    avc_request.Destination.CountryCode = 'IN'
                    from fedex.services.ship_service import FedexProcessShipmentRequest
                    shipment = FedexProcessShipmentRequest(self.CONFIG_OBJ)

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
                    shipment.RequestedShipment.ServiceType = self.shipment_type
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
                        = self.CONFIG_OBJ.account_number

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

                    weight, volumetric_weight, dimensions = calculate_order_weight_dimensions(order)

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
                            awb_no, "", order[0], pickup_point[1], self.courier[9], json.dumps(dimensions),
                            volumetric_weight, weight,
                            "", pickup_point[2], routing_code, None, None, zone)])

                        client_name = str(order[51])
                        customer_phone = order[5].replace(" ", "")
                        customer_phone = "0" + customer_phone[-10:]

                        try:
                            tracking_link_wareiq = "https://webapp.wareiq.com/tracking/" + str(awb_no)
                            if order[60]:
                                tracking_link_wareiq = "https://"+order[60]+".wiq.app/tracking/" + str(awb_no)
                            tracking_link_wareiq = UrlShortner.get_short_url(tracking_link_wareiq, self.cur)
                            send_received_event(client_name, customer_phone, tracking_link_wareiq)
                        except Exception:
                            pass

                    else:
                        if self.next_priority:
                            push_order_to_next_priority(self.next_priority, [order[0]], self.courier, self.cur)
                        else:
                            insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id,
                                                                    dimensions, volumetric_weight, weight, remark, return_point_id, routing_code,
                                                                    channel_fulfillment_id, tracking_link, zone)
                                                                                    VALUES  %s"""
                            insert_shipments_data_tuple = list()
                            insert_shipments_data_tuple.append(("", "Fail", order[0], None,
                                                                None, None, None, None, "Pincode not serviceable", None,
                                                                None, None, None, zone), )
                            self.cur.execute(insert_shipments_data_query, tuple(insert_shipments_data_tuple))
                        continue

                    self.cur.execute(insert_shipments_data_query, data_tuple)
                    ship_temp = self.cur.fetchone()
                    order_status_add_query = """INSERT INTO order_status (order_id, courier_id, shipment_id, 
                                                                status_code, status, status_text, location, location_city, 
                                                                status_time) VALUES %s"""

                    order_status_add_tuple = [(order[0], self.courier[9],
                                               ship_temp[0], "UD", "Received", "Consignment Manifested",
                                               pickup_point[6], pickup_point[6],
                                               datetime.utcnow() + timedelta(hours=5.5))]

                    self.cur.execute(order_status_add_query, tuple(order_status_add_tuple))
                    conn.commit()
                    if awb_no and order[46] == 7:
                        push_awb_easyecom(order[39],order[36], awb_no, self.courier, self.cur, order[55], order[56],
                                          pushLabel=True, order_id=order[0])

                except Exception as e:
                    print("couldn't assign order: " + str(order[1]) + "\nError: " + str(e))

            if last_shipped_order_id:
                last_shipped_data_tuple = (
                    last_shipped_order_id, datetime.now(tz=pytz.timezone('Asia/Calcutta')), self.courier[1])
                self.cur.execute(update_last_shipped_order_query, last_shipped_data_tuple)

            if order_status_change_ids:
                if len(order_status_change_ids) == 1:
                    self.cur.execute(update_orders_status_query % (("(%s)") % str(order_status_change_ids[0])))
                else:
                    self.cur.execute(update_orders_status_query, (tuple(order_status_change_ids),))

            self.cur.execute("UPDATE client_pickups SET invoice_last=%s WHERE id=%s;", (last_invoice_no, pickup_id))

            conn.commit()


class ShipSelfShip:

    def __init__(self, cur=None, courier=None, orders=None, force_ship=None, next_priority=None):
        self.courier = courier
        self.all_orders = orders
        self.force_ship = force_ship
        self.cur = cur if cur else conn.cursor()
        self.pickup_point_order_dict = dict()
        self.orders_dict = dict()
        self.next_priority = next_priority

    def ship_orders(self):
        for order in self.all_orders:
            if order[41]:
                if order[41] not in self.pickup_point_order_dict:
                    self.pickup_point_order_dict[order[41]] = [order]
                else:
                    self.pickup_point_order_dict[order[41]].append(order)

        for pickup_id, all_new_orders in self.pickup_point_order_dict.items():

            # todo: remove this
            if int(pickup_id) in (1140, 1141, 1142, 1442):
                continue

            last_shipped_order_id = 0
            pickup_points_tuple = (pickup_id,)
            self.cur.execute(get_pickup_points_query, pickup_points_tuple)
            order_status_change_ids = list()

            pickup_point = self.cur.fetchone()  # change this as we get to dynamic pickups

            last_invoice_no = pickup_point[22] if pickup_point[22] else 0

            for order in all_new_orders:
                if order[26].lower() == 'pickup':
                    continue
                zone = None
                try:
                    zone = get_delivery_zone(pickup_point[8], order[18])
                except Exception as e:
                    logger.error("couldn't find zone: " + str(order[0]) + "\nError: " + str(e))

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

                if zone != 'A' and not self.force_ship and self.courier[1]=='KAMAAYURVEDA':
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

                weight, volumetric_weight, dimensions = calculate_order_weight_dimensions(order)

                insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                                                dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, 
                                                                                channel_fulfillment_id, tracking_link)
                                                                                VALUES  %s RETURNING id;"""

                if order[0] > last_shipped_order_id:
                    last_shipped_order_id = order[0]
                order_status_change_ids.append(order[0])
                data_tuple = tuple([("WIQ"+str(order[0]), "Success", order[0], pickup_point[1],
                                     self.courier[9], json.dumps(dimensions), volumetric_weight, weight, "", pickup_point[2],
                                     "", None, None)])

                if order[46] == 7:
                    push_awb_easyecom(order[39], order[36], "WIQ"+str(order[0]), self.courier, self.cur, order[55], order[56])

                self.cur.execute(insert_shipments_data_query, data_tuple)
                ship_temp = self.cur.fetchone()
                order_status_add_query = """INSERT INTO order_status (order_id, courier_id, shipment_id, 
                                                                            status_code, status, status_text, location, location_city, 
                                                                            status_time) VALUES %s"""

                order_status_add_tuple = [(order[0], self.courier[9],
                                           ship_temp[0], "UD", "Received", "Consignment Manifested",
                                           pickup_point[6], pickup_point[6],
                                           datetime.utcnow() + timedelta(hours=5.5))]

                self.cur.execute(order_status_add_query, tuple(order_status_add_tuple))

                if not order[54]:
                    last_invoice_no = invoice_order(self.cur, last_invoice_no, pickup_point[23], order[0], pickup_id)

            if last_shipped_order_id:
                last_shipped_data_tuple = (
                    last_shipped_order_id, datetime.now(tz=pytz.timezone('Asia/Calcutta')), self.courier[1])
                self.cur.execute(update_last_shipped_order_query, last_shipped_data_tuple)

            if order_status_change_ids:
                if len(order_status_change_ids) == 1:
                    self.cur.execute(update_orders_status_query % (("(%s)") % str(order_status_change_ids[0])))
                else:
                    self.cur.execute(update_orders_status_query, (tuple(order_status_change_ids),))

            self.cur.execute("UPDATE client_pickups SET invoice_last=%s WHERE id=%s;", (last_invoice_no, pickup_id))

            conn.commit()


class ShipPidge:

    def __init__(self, cur=None, courier=None, orders=None, force_ship=None, next_priority=None):
        self.courier = courier
        self.all_orders = orders
        self.force_ship = force_ship
        self.cur = cur if cur else conn.cursor()
        self.pickup_point_order_dict = dict()
        self.orders_dict = dict()
        self.next_priority = next_priority
        self.headers = {"Authorization": "Bearer " + courier[14],
                        "Content-Type": "application/json",
                        "platform": "Postman",
                        "deviceId": "abc",
                        "buildNumber": "123"}

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
            order_status_change_ids = list()

            pickup_point = self.cur.fetchone()  # change this as we get to dynamic pickups

            if str(pickup_point[8]) not in pidge_del_sdd_pincodes or pickup_point[0]==1443: #todo: remove this
                continue

            last_invoice_no = pickup_point[22] if pickup_point[22] else 0

            if not pickup_point[21]:
                continue

            pick_lat, pick_lon = pickup_point[24], pickup_point[25]

            if not (pick_lat and pick_lon):
                pick_lat, pick_lon = get_lat_lon_pickup(pickup_point, self.cur)

            for order in all_new_orders:
                if order[26].lower() == 'pickup':
                    try:
                        self.cur.execute("""SELECT id, courier_name, logo_url, date_created, date_updated, api_key, 
                                                                            api_password, api_url FROM master_couriers
                                                                            WHERE courier_name='%s'""" % "Delhivery Surface Standard")
                        courier_data = self.cur.fetchone()
                        courier_new = list(self.courier)
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
                        ship_obj = ShipDelhivery(courier=tuple(courier_new), orders=[order])
                        ship_obj.ship_orders()
                    except Exception as e:
                        logger.error("Couldn't assign backup courier for: " + str(order[0]) + "\nError: " + str(e.args))
                        pass
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

                if zone != 'A' and not self.force_ship:
                    continue

                lat, lon = order[22], order[23]

                if not (lat and lon):
                    lat, lon = get_lat_lon(order, self.cur)

                weight, volumetric_weight, dimensions = calculate_order_weight_dimensions(order)

                if max(volumetric_weight, weight)>2:
                    if self.next_priority:
                        push_order_to_next_priority(self.next_priority, [order[0]], self.courier, self.cur)
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

                return_data_raw = requests.post(self.courier[16] + "/v1.0/vendor/order", headers=self.headers, data=json.dumps(pidge_body)).json()
                logger.info(str(order[0])+": "+str(return_data_raw))
                if return_data_raw.get('success'):
                    order_status_change_ids.append(order[0])
                    data_tuple = tuple([(
                        str(return_data_raw['data']['PBID']),
                        return_data_raw['message'],
                        order[0], pickup_point[1], self.courier[9], json.dumps(dimensions), volumetric_weight, weight,
                        "", pickup_point[2], "", None, "https://t.pidge.in/?t="+return_data_raw['data']['track_code'], zone)])

                    if order[46] == 7:
                        push_awb_easyecom(order[39], order[36], str(return_data_raw['data']['PBID']), self.courier,
                                          self.cur, order[55], order[56])

                    client_name = str(order[51])

                    try:
                        tracking_link_wareiq = "https://t.pidge.in/?t="+return_data_raw['data']['track_code']
                        tracking_link_wareiq = UrlShortner.get_short_url(tracking_link_wareiq, self.cur)
                        send_received_event(client_name, customer_phone, tracking_link_wareiq)
                    except Exception:
                        pass

                else:
                    if self.next_priority:
                        push_order_to_next_priority(self.next_priority, [order[0]], self.courier, self.cur)
                    else:
                        insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                        dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, 
                                                        channel_fulfillment_id, tracking_link, zone)
                                                                    VALUES  %s"""
                        insert_shipments_data_tuple = list()
                        insert_shipments_data_tuple.append(("", "Fail", order[0], None,
                                                            None, None, None, None, "Pincode not serviceable", None,
                                                            None, zone, None, None), )
                        self.cur.execute(insert_shipments_data_query, tuple(insert_shipments_data_tuple))
                    continue

                insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                    dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, 
                                                    channel_fulfillment_id, tracking_link, zone)
                                                    VALUES  %s RETURNING id;"""

                self.cur.execute(insert_shipments_data_query, data_tuple)
                ship_temp = self.cur.fetchone()
                order_status_add_query = """INSERT INTO order_status (order_id, courier_id, shipment_id, 
                                                                            status_code, status, status_text, location, location_city, 
                                                                            status_time) VALUES %s"""

                order_status_add_tuple = [(order[0], self.courier[9],
                                           ship_temp[0], "UD", "Received", "Consignment Manifested",
                                           pickup_point[6], pickup_point[6],
                                           datetime.utcnow() + timedelta(hours=5.5))]

                self.cur.execute(order_status_add_query, tuple(order_status_add_tuple))

                if not order[54]:
                    last_invoice_no = invoice_order(self.cur, last_invoice_no, pickup_point[23], order[0], pickup_id)

            if last_shipped_order_id:
                last_shipped_data_tuple = (
                    last_shipped_order_id, datetime.now(tz=pytz.timezone('Asia/Calcutta')), self.courier[1])
                self.cur.execute(update_last_shipped_order_query, last_shipped_data_tuple)

            if order_status_change_ids:
                if len(order_status_change_ids) == 1:
                    self.cur.execute(update_orders_status_query % (("(%s)") % str(order_status_change_ids[0])))
                else:
                    self.cur.execute(update_orders_status_query, (tuple(order_status_change_ids),))

            self.cur.execute("UPDATE client_pickups SET invoice_last=%s WHERE id=%s;", (last_invoice_no, pickup_id))

            conn.commit()


class ShipBlowhorn:

    def __init__(self, cur=None, courier=None, orders=None, force_ship=None, next_priority=None):
        self.courier = courier
        self.all_orders = orders
        self.force_ship = force_ship
        self.cur = cur if cur else conn.cursor()
        self.pickup_point_order_dict = dict()
        self.orders_dict = dict()
        self.next_priority = next_priority
        self.headers = {"API_KEY": courier[14],
                        "Content-Type": "application/json"}

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
            order_status_change_ids = list()
            pickup_point = self.cur.fetchone()  # change this as we get to dynamic pickups
            last_invoice_no = pickup_point[22] if pickup_point[22] else 0
            pick_lat, pick_lon = pickup_point[24], pickup_point[25]

            if not (pick_lat and pick_lon):
                pick_lat, pick_lon = get_lat_lon_pickup(pickup_point, self.cur)

            for order in all_new_orders:
                if order[26].lower() == 'pickup':
                    try:
                        self.cur.execute("""SELECT id, courier_name, logo_url, date_created, date_updated, api_key, 
                                                                            api_password, api_url FROM master_couriers
                                                                            WHERE courier_name='%s'""" % "Delhivery Surface Standard")
                        courier_data = self.cur.fetchone()
                        courier_new = list(self.courier)
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
                        ship_obj = ShipDelhivery(courier=tuple(courier_new), orders=[order])
                        ship_obj.ship_orders()
                    except Exception as e:
                        logger.error("Couldn't assign backup courier for: " + str(order[0]) + "\nError: " + str(e.args))
                        pass
                    continue
                # kama ayurveda assign delhi orders pincode check
                zone = None
                try:
                    zone = get_delivery_zone(pickup_point[8], order[18])
                except Exception as e:
                    logger.error("couldn't find zone: " + str(order[0]) + "\nError: " + str(e))

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

                if zone != 'A' and not self.force_ship:
                    continue

                lat, lon = order[22], order[23]

                if not (lat and lon):
                    lat, lon = get_lat_lon(order, self.cur)

                weight, volumetric_weight, dimensions = calculate_order_weight_dimensions(order)

                package_string = ""
                for idx, prod in enumerate(order[40]):
                    package_string += prod + " (" + str(order[35][idx]) + ") + "

                customer_phone = order[5].replace(" ", "")
                customer_phone = customer_phone[-10:]

                pickup_time = datetime.utcnow()+timedelta(hours=5.5)
                if pickup_time.hour>=12:
                    pickup_time = pickup_time + timedelta(days=1)
                pickup_time = pickup_time.strftime("%Y-%m-%d")
                deliver_time = pickup_time+"T16:00:00.000000"
                pickup_time = pickup_time+"T14:00:00.000000"

                delivery_address = order[15]
                delivery_address += order[16] if order[16] else ""
                pickup_address = pickup_point[4]
                pickup_address += pickup_point[5] if pickup_point[5] else ""
                blowhorn_body = {
                    "customer_name": order[13],
                    "customer_mobile": customer_phone,
                    "customer_email": order[4] if order[4] else "noemail@example.com",
                    "delivery_address": delivery_address,
                    "delivery_postal_code": order[18],
                    "reference_number": str(order[0]),
                    "customer_reference_number": order[1],
                    "delivery_lat": str(lat),
                    "delivery_lon": str(lon),
                    "pickup_address": pickup_address,
                    "pickup_postal_code": str(pickup_point[8]),
                    "pickup_lat": str(pick_lat),
                    "pickup_lon": str(pick_lon),
                    "pickup_customer_name": pickup_point[11],
                    "pickup_customer_mobile": pickup_point[3],
                    "weight": str(weight),
                    "volume": str(volumetric_weight),
                    "pickup_datetime": pickup_time,
                    "expected_delivery_time": deliver_time,
                    "is_cod": False,
                    "item_details": [
                        {
                            "item_name": package_string,
                            "item_quantity": 1
                        }
                    ]
                }

                if order[26].lower()=='cod' or order[26].lower()=='cash on delivery':
                    blowhorn_body['is_cod'] = True
                    blowhorn_body['cash_on_delivery'] = str(order[27])

                return_data_raw = requests.post(self.courier[16] + "/api/orders/shipment", headers=self.headers, data=json.dumps(blowhorn_body)).json()
                logger.info(str(order[0])+": "+str(return_data_raw))
                if return_data_raw.get('status')=='PASS':
                    order_status_change_ids.append(order[0])
                    data_tuple = tuple([(
                        str(return_data_raw['message']['awb_number']),
                        return_data_raw['status'],
                        order[0], pickup_point[1], self.courier[9], json.dumps(dimensions), volumetric_weight, weight,
                        "", pickup_point[2], "", None, None, zone)])

                    if order[46] == 7:
                        push_awb_easyecom(order[39], order[36], str(return_data_raw['message']['awb_number']), self.courier,
                                          self.cur, order[55], order[56])

                    client_name = str(order[51])

                    try:
                        tracking_link_wareiq = "https://webapp.wareiq.com/tracking/" + str(return_data_raw['message']['awb_number'])
                        if order[60]:
                            tracking_link_wareiq = "https://"+order[60]+".wiq.app/tracking/" + str(return_data_raw['message']['awb_number'])
                        tracking_link_wareiq = UrlShortner.get_short_url(tracking_link_wareiq, self.cur)
                        send_received_event(client_name, customer_phone, tracking_link_wareiq)
                    except Exception:
                        pass

                else:
                    if self.next_priority:
                        push_order_to_next_priority(self.next_priority, [order[0]], self.courier, self.cur)
                    else:
                        insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                        dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, 
                                                        channel_fulfillment_id, tracking_link, zone)
                                                                    VALUES  %s"""
                        insert_shipments_data_tuple = list()
                        insert_shipments_data_tuple.append(("", "Fail", order[0], None,
                                                            None, None, None, None, "Pincode not serviceable", None,
                                                            None, zone, None, None), )
                        self.cur.execute(insert_shipments_data_query, tuple(insert_shipments_data_tuple))
                    continue

                insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                    dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, 
                                                    channel_fulfillment_id, tracking_link, zone)
                                                    VALUES  %s RETURNING id;"""

                self.cur.execute(insert_shipments_data_query, data_tuple)
                ship_temp = self.cur.fetchone()
                order_status_add_query = """INSERT INTO order_status (order_id, courier_id, shipment_id, 
                                                                            status_code, status, status_text, location, location_city, 
                                                                            status_time) VALUES %s"""

                order_status_add_tuple = [(order[0], self.courier[9],
                                           ship_temp[0], "UD", "Received", "Consignment Manifested",
                                           pickup_point[6], pickup_point[6],
                                           datetime.utcnow() + timedelta(hours=5.5))]

                self.cur.execute(order_status_add_query, tuple(order_status_add_tuple))

                if not order[54]:
                    last_invoice_no = invoice_order(self.cur, last_invoice_no, pickup_point[23], order[0], pickup_id)

            if last_shipped_order_id:
                last_shipped_data_tuple = (
                    last_shipped_order_id, datetime.now(tz=pytz.timezone('Asia/Calcutta')), self.courier[1])
                self.cur.execute(update_last_shipped_order_query, last_shipped_data_tuple)

            if order_status_change_ids:
                if len(order_status_change_ids) == 1:
                    self.cur.execute(update_orders_status_query % (("(%s)") % str(order_status_change_ids[0])))
                else:
                    self.cur.execute(update_orders_status_query, (tuple(order_status_change_ids),))

            self.cur.execute("UPDATE client_pickups SET invoice_last=%s WHERE id=%s;", (last_invoice_no, pickup_id))

            conn.commit()


class ShipDTDC:

    def __init__(self, cur=None, courier=None, orders=None, force_ship=None, next_priority=None):
        self.courier = courier
        self.all_orders = orders
        self.force_ship = force_ship
        self.cur = cur if cur else conn.cursor()
        self.pickup_point_order_dict = dict()
        self.orders_dict = dict()
        self.headers = {"api-key": courier[14],
                        "Content-Type": "application/json"}
        self.client_code = courier[15].split("|")[0]
        self.service_type_id = courier[15].split("|")[1]
        self.next_priority = next_priority

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
            order_status_change_ids = list()

            pickup_point = self.cur.fetchone()  # change this as we get to dynamic pickups

            last_invoice_no = pickup_point[22] if pickup_point[22] else 0
            for order in all_new_orders:
                if not order[54]:
                    last_invoice_no = invoice_order(self.cur, last_invoice_no, pickup_point[23], order[0], pickup_id)
                if order[26].lower() == 'cod' and not order[27] and not self.force_ship:
                    continue
                if self.force_ship and order[26].lower() == 'pickup':
                    continue
                if order[26].lower() == 'pickup':
                    try:
                        self.cur.execute("""SELECT id, courier_name, logo_url, date_created, date_updated, api_key, 
                                                                            api_password, api_url FROM master_couriers
                                                                            WHERE courier_name='%s'""" % "Delhivery Surface Standard")
                        courier_data = self.cur.fetchone()
                        courier_new = list(self.courier)
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
                        ship_obj = ShipDelhivery(courier=tuple(courier_new), orders=[order])
                        ship_obj.ship_orders()
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
                if order[47] and not (order[50] and order[2] < time_2_days) and not self.force_ship:
                    if order[26].lower() == 'cod' and not order[42] and order[43]:
                        continue
                    if order[26].lower() == 'cod' and not order[43]:
                        if order[26].lower() == 'cod' and not order[43]:
                            try:  ## Cod confirmation  text
                                cod_verification_text(order, self.cur)
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

                    weight, volumetric_weight, dimensions = calculate_order_weight_dimensions(order)

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
                    dtdc_body = {
                        "consignments": [
                            {
                                "customer_code": self.client_code,
                                "reference_number": "",
                                "service_type_id": self.service_type_id,
                                "load_type": "NON-DOCUMENT",
                                "description": package_string,
                                "num_pieces": "1",
                                "dimension_unit": "cm",
                                "length": str(dimensions['length']),
                                "width": str(dimensions['breadth']),
                                "height": str(dimensions['height']),
                                "weight_unit": "kg",
                                "weight": str(sum(order[34])),
                                "declared_value": str(order[27]),
                                "customer_reference_number": order[1],
                                "commodity_id": "Laptop",
                                "consignment_type": "Forward",
                                "origin_details": {
                                    "name": pickup_point[11],
                                    "phone": str(pickup_point[3]),
                                    "alternate_phone": "",
                                    "address_line_1": pickup_address,
                                    "address_line_2": "",
                                    "pincode": str(pickup_point[8]),
                                    "city": pickup_point[6],
                                    "state": pickup_point[10]
                                },
                                "destination_details": {
                                    "name": customer_name,
                                    "alternate_phone": "",
                                    "phone": shipping_phone,
                                    "address_line_1": customer_address,
                                    "address_line_2": "",
                                    "pincode": str(order[18]),
                                    "city": order[17],
                                    "state": order[19]
                                },
                                "pieces_detail": [
                                    {
                                        "description": package_string,
                                        "declared_value": str(order[27]),
                                        "weight": str(sum(order[34])),
                                        "height": str(dimensions['height']),
                                        "length": str(dimensions['length']),
                                        "width": str(dimensions['breadth'])
                                    }
                                ]
                            }
                        ]
                    }
                    if order[26].lower() == "cod":
                        dtdc_body["consignments"][0]['cod_amount'] = str(order[27])
                        dtdc_body["consignments"][0]['cod_collection_mode'] = "cash"

                    dtdc_manifest_url = self.courier[16]
                    req = requests.post(dtdc_manifest_url, headers=self.headers, data=json.dumps(dtdc_body))
                    return_data_raw = req.json()
                    insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                                                                        dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, 
                                                                                                        channel_fulfillment_id, tracking_link, zone)
                                                                                                        VALUES  %s RETURNING id;"""

                    if return_data_raw['data'][0]['success']:

                        order_status_change_ids.append(order[0])
                        data_tuple = tuple([(
                            return_data_raw['data'][0]['reference_number'], return_data_raw['status'],
                            order[0], pickup_point[1], self.courier[9], json.dumps(dimensions), volumetric_weight, weight,
                            "", pickup_point[2], "", fulfillment_id, tracking_link, zone)])

                        if order[46] == 7:
                            push_awb_easyecom(order[39],order[36], return_data_raw['data'][0]['reference_number'], self.courier, self.cur, order[55], order[56])

                        client_name = str(order[51])
                        customer_phone = order[5].replace(" ", "")
                        customer_phone = "0" + customer_phone[-10:]

                        try:
                            tracking_link_wareiq = "https://webapp.wareiq.com/tracking/" + str(return_data_raw['data'][0]['reference_number'])
                            if order[60]:
                                tracking_link_wareiq = "https://"+order[60]+".wiq.app/tracking/" + str(return_data_raw['data'][0]['reference_number'])
                            tracking_link_wareiq = UrlShortner.get_short_url(tracking_link_wareiq, self.cur)
                            if self.courier[1] != 'DHANIPHARMACY':
                                send_received_event(client_name, customer_phone, tracking_link_wareiq)
                        except Exception:
                            pass

                    else:
                        if self.next_priority:
                            push_order_to_next_priority(self.next_priority, [order[0]], self.courier, self.cur)
                        else:
                            insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id,
                                                                dimensions, volumetric_weight, weight, remark, return_point_id, routing_code,
                                                                channel_fulfillment_id, tracking_link, zone)
                                                                        VALUES  %s"""
                            insert_shipments_data_tuple = list()
                            insert_shipments_data_tuple.append(("", "Fail", order[0], None,
                                                                None, None, None, None, "Pincode not serviceable", None,
                                                                None, None, None, zone), )
                            self.cur.execute(insert_shipments_data_query, tuple(insert_shipments_data_tuple))
                        continue

                    self.cur.execute(insert_shipments_data_query, data_tuple)
                    ship_temp = self.cur.fetchone()
                    order_status_add_query = """INSERT INTO order_status (order_id, courier_id, shipment_id, 
                                                    status_code, status, status_text, location, location_city, 
                                                    status_time) VALUES %s"""

                    order_status_add_tuple = [(order[0], self.courier[9],
                                               ship_temp[0], "UD", "Received", "Consignment Manifested",
                                               pickup_point[6], pickup_point[6], datetime.utcnow() + timedelta(hours=5.5))]

                    self.cur.execute(order_status_add_query, tuple(order_status_add_tuple))
                    self.cur.execute("UPDATE orders SET status='READY TO SHIP' WHERE id=%s;" % str(order[0]))
                    conn.commit()

                except Exception as e:
                    conn.rollback()
                    print("couldn't assign order: " + str(order[1]) + "\nError: " + str(e))

            if last_shipped_order_id:
                last_shipped_data_tuple = (
                    last_shipped_order_id, datetime.now(tz=pytz.timezone('Asia/Calcutta')), self.courier[1])
                self.cur.execute(update_last_shipped_order_query, last_shipped_data_tuple)

            self.cur.execute("UPDATE client_pickups SET invoice_last=%s WHERE id=%s;", (last_invoice_no, pickup_id))

            conn.commit()


def push_order_to_next_priority(next_priority, order_ids, courier, cur):
    try:
        cur.execute("""SELECT id, courier_name, logo_url, date_created, date_updated, api_key, 
                                                                                        api_password, api_url FROM master_couriers
                                                                                        WHERE id=%s""" % str(next_priority[0]))
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
        next_priority = next_priority.copy()
        next_priority.pop(0)
        ship_obj = ShippingRules(courier_name=courier_data[1], order_ids=order_ids, next_priority=next_priority, cur=cur)
        ship_obj.ship_orders_courier_wise()
    except Exception as e:
        logger.error(
            "Couldn't assign backup courier for: " + str(order[0]) + "\nError: " + str(e.args))
        pass

    return None