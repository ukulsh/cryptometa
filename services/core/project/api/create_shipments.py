import psycopg2, requests, os, json, pytz
import logging
from datetime import datetime, timedelta
from requests_oauthlib.oauth1_session import OAuth1Session

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
conn_2 = psycopg2.connect(host="wareiq-core-prod2.cvqssxsqruyc.us-east-1.rds.amazonaws.com", database="users_prod", user="postgres", password="aSderRFgd23")


def lambda_handler(courier_name=None, order_ids=None):
    cur = conn.cursor()
    cur_2 = conn_2.cursor()
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
        conn.commit()
        cur.execute(fetch_client_couriers_query)
        all_couriers=cur.fetchall()

    exotel_idx = 0
    exotel_sms_data = {
        'From': 'LM-WAREIQ'
    }
    for courier in all_couriers:
        if courier[10] in ("Delhivery", "Delhivery Surface Standard", "Delhivery Bulk", "Delhivery Heavy", "Delhivery Heavy 2"):
            if courier_name and order_ids:
                orders_to_ship_query = get_orders_to_ship_query.replace("__ORDER_SELECT_FILTERS__", """and aa.id in %s"""%order_id_tuple)
            else:
                orders_to_ship_query = get_orders_to_ship_query.replace("__ORDER_SELECT_FILTERS__", """and aa.status='NEW'
                                                                                                    and ll.id is null""")
            get_orders_data_tuple = (courier[1], courier[1])
            if courier[3]==2:
                orders_to_ship_query = orders_to_ship_query.replace('__PRODUCT_FILTER__', "and ship_courier[1]='%s'"%courier[10])
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

            for pickup_id, all_new_orders in pickup_point_order_dict.items():

                shipments = list()
                last_shipped_order_id = 0
                pickup_points_tuple = (pickup_id,)
                cur.execute(get_pickup_points_query, pickup_points_tuple)

                pickup_point = cur.fetchone()  # change this as we get to dynamic pickups

                headers = {"Authorization": "Token " + courier[14],
                           "Content-Type": "application/json"}
                for order in all_new_orders:
                    """
                    if order[17].lower() in ("bengaluru", "bangalore", "banglore") and courier[1] == "MIRAKKI":
                        continue
                    """
                    time_2_days = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=2)
                    if order[47] and not (order[50] and order[2] < time_2_days):
                        if order[26].lower()=='cod' and not order[42] and order[43]:
                            continue #change this to continue later
                        if order[26].lower()=='cod' and not order[43]:
                            cod_confirmation_link = "http://track.wareiq.com/core/v1/passthru/cod?CustomField=%s"%str(order[0])
                            """
                            short_url = requests.get(
                                "https://cutt.ly/api/api.php?key=f445d0bb52699d2f870e1832a1f77ef3f9078&short=%s" % cod_confirmation_link)
                            short_url_track = short_url.json()['url']['shortLink']
                            """
                            insert_cod_ver_tuple = (order[0], cod_confirmation_link, datetime.now())
                            cur.execute("INSERT INTO cod_verification (order_id, verification_link, date_created) VALUES (%s,%s,%s);", insert_cod_ver_tuple)
                            cur_2.execute("select client_name from clients where client_prefix='%s'" % order[9])
                            client_name = cur_2.fetchone()
                            customer_phone = order[5].replace(" ", "")
                            customer_phone = "0" + customer_phone[-10:]

                            sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                            sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                            exotel_sms_data[sms_to_key] = customer_phone

                            exotel_sms_data[
                                sms_body_key] = "Dear Customer, You recently placed an order from %s with order id %s. " \
                                                "Please click on the link (%s) to verify. " \
                                                "Your order will be shipped soon after confirmation." % (
                                                    client_name[0], str(order[1]), cod_confirmation_link)

                            exotel_idx += 1
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
                        """
                        elif 'covid_zone' in req.json()['delivery_codes'][0]['postal_code']:
                            if not order[48] and req.json()['delivery_codes'][0]['postal_code']['covid_zone'].upper()=='R':
                                insert_shipments_data_query = ""INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                                                                    dimensions, volumetric_weight, weight, remark, return_point_id, routing_code)
                                                                                                    VALUES  %s""
                                insert_shipments_data_tuple = list()
                                insert_shipments_data_tuple.append(("", "Fail", order[0], None,
                                                                    None, None, None, None, "Non essential in Red Zone(COVID)",
                                                                    None, None), )
                                cur.execute(insert_shipments_data_query, tuple(insert_shipments_data_tuple))
                                continue
                        """

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
                        if order[49] and order[49][0]:
                            shipment_data['category_of_goods'] = order[49][0]
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

                insert_shipments_data_query += " RETURNING id,awb;"

                orders_dict = dict()
                for prev_order in all_orders:
                    orders_dict[prev_order[1]] = (prev_order[0], prev_order[33], prev_order[34], prev_order[35],
                                                  prev_order[36], prev_order[37], prev_order[38], prev_order[39],
                                                  prev_order[5], prev_order[9], prev_order[45], prev_order[46])

                order_status_change_ids = list()
                insert_shipments_data_tuple = list()
                insert_order_status_dict = dict()
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
                        try:
                            tracking_link_wareiq = "http://webapp.wareiq.com/tracking/"+str(package['waybill'])
                            """
                            short_url = requests.get("https://cutt.ly/api/api.php?key=f445d0bb52699d2f870e1832a1f77ef3f9078&short=%s"%tracking_link_wareiq)
                            short_url_track = short_url.json()['url']['shortLink']
                            """
                            exotel_sms_data[
                                sms_body_key] = "Dear Customer, thank you for ordering from %s. " \
                                                "Your order will be shipped by Delhivery with AWB number %s. " \
                                                "You can track your order on this ( %s ) link." % (
                                                client_name[0], str(package['waybill']), tracking_link_wareiq)
                        except Exception:
                            exotel_sms_data[
                                sms_body_key] = "Dear Customer, thank you for ordering from %s. Your order will be shipped by Delhivery with AWB number %s. " \
                                                "You can track your order using this AWB number." % (client_name[0], str(package['waybill']))
                        exotel_idx += 1

                        if orders_dict[package['refnum']][10] and orders_dict[package['refnum']][11]==1: #shopify
                            try:
                                order_ls = [orders_dict[package['refnum']][4],orders_dict[package['refnum']][5],
                                            orders_dict[package['refnum']][6],orders_dict[package['refnum']][7]]
                                fulfillment_id, tracking_link = None, None #shopify_fulfillment(order_ls, str(package['waybill']), orders_dict[package['refnum']][10])
                            except Exception as e:
                                logger.error("Couldn't update shopify for: " + str(package['refnum'])
                                             + "\nError: " + str(e.args))
                        """
                        if orders_dict[package['refnum']][11] == 5: #woocommerce
                            try:
                                order_ls = [orders_dict[package['refnum']][4],orders_dict[package['refnum']][5],
                                            orders_dict[package['refnum']][6],orders_dict[package['refnum']][7]]
                                woocommerce_fulfillment(order_ls)
                            except Exception as e:
                                logger.error("Couldn't update woocommerce for: " + str(package['refnum'])
                                             + "\nError: " + str(e.args))
                        """
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

                    dimensions = orders_dict[package['refnum']][1][0]
                    dimensions['length'] = dimensions['length']*orders_dict[package['refnum']][3][0]
                    weight = orders_dict[package['refnum']][2][0]*orders_dict[package['refnum']][3][0]
                    volumetric_weight = (dimensions['length']*dimensions['breadth']*dimensions['height'])/5000
                    for idx, dim in enumerate(orders_dict[package['refnum']][1]):
                        if idx==0:
                            continue
                        dim['length'] = dim['length']*(orders_dict[package['refnum']][3][idx])
                        volumetric_weight += (dim['length']*dim['breadth']*dim['height'])/5000
                        weight += orders_dict[package['refnum']][2][idx]*(orders_dict[package['refnum']][3][idx])

                    dimensions['height'] = round((volumetric_weight*5000)/(dimensions['length']*dimensions['breadth']))

                    data_tuple = (package['waybill'], package['status'], orders_dict[package['refnum']][0], pickup_point[1],
                                  courier[9], json.dumps(dimensions), volumetric_weight, weight, remark, pickup_point[2],
                                  package['sort_code'], fulfillment_id, tracking_link)
                    insert_shipments_data_tuple.append(data_tuple)
                    insert_order_status_dict[package['waybill']] = [orders_dict[package['refnum']][0], courier[9],
                                                                    None, "UD", "Received", "Consignment Manifested",
                                                                    pickup_point[6], pickup_point[6],datetime.utcnow()+timedelta(hours=5.5)]

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
                    last_shipped_data_tuple = (last_shipped_order_id, datetime.now(tz=pytz.timezone('Asia/Calcutta')), courier[1])
                    cur.execute(update_last_shipped_order_query, last_shipped_data_tuple)

                if order_status_change_ids:
                    if len(order_status_change_ids) == 1:
                        cur.execute(update_orders_status_query % (("(%s)")%str(order_status_change_ids[0])))
                    else:
                        cur.execute(update_orders_status_query, (tuple(order_status_change_ids),))

                conn.commit()

        elif courier[10] == "Delhivery" and courier[1] in ('BEYONDUW'):
            get_orders_data_tuple = (courier[1], courier[1])
            if courier[3]==2:
                orders_to_ship_query = get_orders_to_ship_query.replace('__PRODUCT_FILTER__', "and ship_courier[1]='%s'"%courier[10])
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
                        if order[26].lower()=='cod' and not order[43]:
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
                            cur_2.execute("select client_name from clients where client_prefix='%s'" % order[9])
                            client_name = cur_2.fetchone()
                            customer_phone = order[5].replace(" ", "")
                            customer_phone = "0" + customer_phone[-10:]

                            sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                            sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                            exotel_sms_data[sms_to_key] = customer_phone

                            exotel_sms_data[
                                sms_body_key] = "Dear Customer, You recently placed an order from %s with order id %s. " \
                                                "Please click on the link (%s) to verify. " \
                                                "Your order will be shipped soon after confirmation." % (
                                                    client_name[0], str(order[1]), cod_confirmation_link)

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
                                          headers=headers,                    data=form_data)
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
                            customer_name += " "+ order[14]

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
                            cur_2.execute("select client_name from clients where client_prefix='%s'" % order[9])
                            client_name = cur_2.fetchone()
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
                                    sms_body_key] = "Dear Customer, thank you for ordering from %s. " \
                                                    "Your order will be shipped by Delhivery with AWB number %s. " \
                                                    "You can track your order on this ( %s ) link." % (
                                                        client_name[0], str(return_data['awbno']), tracking_link_wareiq)
                            except Exception:
                                exotel_sms_data[
                                    sms_body_key] = "Dear Customer, thank you for ordering from %s. Your order will be shipped by Delhivery with AWB number %s. " \
                                                    "You can track your order using this AWB number." % (
                                                    client_name[0], str(return_data['awbno']))
                            exotel_idx += 1

                            cur.execute(insert_shipments_data_query, data_tuple)
                            ship_temp = cur.fetchone()
                            order_status_add_query = """INSERT INTO order_status (order_id, courier_id, shipment_id, 
                                                        status_code, status, status_text, location, location_city, 
                                                        status_time) VALUES %s"""

                            order_status_add_tuple = [(order[0], courier[9],
                                                     ship_temp[0], "UD", "Received", "Consignment Manifested",
                                                     pickup_point[6], pickup_point[6],datetime.utcnow()+timedelta(hours=5.5))]

                            cur.execute(order_status_add_query, tuple(order_status_add_tuple))

                    except Exception as e:
                        logger.error("couldn't assign order: " + str(order[1]) + "\nError: " + str(e))

                if order_status_change_ids:
                    if len(order_status_change_ids) == 1:
                        cur.execute(update_orders_status_query % (("(%s)") % str(order_status_change_ids[0])))
                    else:
                        cur.execute(update_orders_status_query, (tuple(order_status_change_ids),))

                conn.commit()

        elif courier[10] == "Shadowfax":
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
                    time_2_days = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=2)
                    if order[47] and not (order[50] and order[2]<time_2_days):
                        if order[26].lower()=='cod' and not order[42] and order[43]:
                            continue
                        if order[26].lower()=='cod' and not order[43]:
                            cod_confirmation_link = "http://track.wareiq.com/core/v1/passthru/cod?CustomField=%s"%str(order[0])
                            """
                            short_url = requests.get(
                                "https://cutt.ly/api/api.php?key=f445d0bb52699d2f870e1832a1f77ef3f9078&short=%s" % cod_confirmation_link)
                            short_url_track = short_url.json()['url']['shortLink']
                            """
                            insert_cod_ver_tuple = (order[0], cod_confirmation_link, datetime.now())
                            cur.execute(
                                "INSERT INTO cod_verification (order_id, verification_link, date_created) VALUES (%s,%s,%s);",
                                insert_cod_ver_tuple)
                            cur_2.execute("select client_name from clients where client_prefix='%s'" % order[9])
                            client_name = cur_2.fetchone()
                            customer_phone = order[5].replace(" ", "")
                            customer_phone = "0" + customer_phone[-10:]

                            sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                            sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                            exotel_sms_data[sms_to_key] = customer_phone

                            exotel_sms_data[
                                sms_body_key] = "Dear Customer, You recently placed an order from %s with order id %s. " \
                                                "Please click on the link (%s) to verify. " \
                                                "Your order will be shipped soon after confirmation." % (
                                                    client_name[0], str(order[1]), cod_confirmation_link)

                            exotel_idx += 1
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
                        volumetric_weight = (dimensions['length'] * dimensions['breadth'] * dimensions['height']) / 5000
                        for idx, dim in enumerate(order[33]):
                            if idx == 0:
                                continue
                            dim['length'] += dim['length'] * (order[35][idx])
                            volumetric_weight += (dim['length'] * dim['breadth'] * dim['height']) / 5000
                            weight += order[34][idx] * (order[35][idx])

                        dimensions['height'] = round(
                            (volumetric_weight * 5000) / (dimensions['length'] * dimensions['breadth']))

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
                                                                                                        VALUES  %s RETURNING id;"""
                        if not return_data_raw['errors']:
                            order_status_change_ids.append(order[0])
                            return_data = return_data_raw['data']
                            cur_2.execute("select client_name from clients where client_prefix='%s'" % order[9])
                            client_name = cur_2.fetchone()
                            customer_phone = order[5].replace(" ", "")
                            customer_phone = "0" + customer_phone[-10:]

                            sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                            sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                            exotel_sms_data[sms_to_key] = customer_phone
                            try:
                                tracking_link_wareiq = "http://webapp.wareiq.com/tracking/" + str(return_data_raw['data']['awb_number'])
                                """
                                short_url = requests.get(
                                    "https://cutt.ly/api/api.php?key=f445d0bb52699d2f870e1832a1f77ef3f9078&short=%s" % tracking_link_wareiq)
                                short_url_track = short_url.json()['url']['shortLink']
                                """
                                exotel_sms_data[
                                    sms_body_key] = "Dear Customer, thank you for ordering from %s. " \
                                                    "Your order will be shipped by Shadowfax with AWB number %s. " \
                                                    "You can track your order on this ( %s ) link." % (
                                                        client_name[0], str(return_data_raw['data']['awb_number']), tracking_link_wareiq)
                            except Exception:
                                exotel_sms_data[
                                    sms_body_key] = "Dear Customer, thank you for ordering from %s. Your order will be shipped by Shadowfax with AWB number %s. " \
                                                    "You can track your order using this AWB number." % (
                                                    client_name[0], str(return_data_raw['data']['awb_number']))
                            exotel_idx += 1

                            if order[45] and order[46] == 1:  # shopify
                                try:
                                    order_ls = [order[36], order[37], order[38], order[39]]
                                    fulfillment_id, tracking_link = None, None #shopify_fulfillment(order_ls,
                                    #                                      str(return_data_raw['data']['awb_number']),
                                    #                                      order[45])
                                except Exception as e:
                                    logger.error("Couldn't update shopify for: " + str(order[1])
                                                 + "\nError: " + str(e.args))
                            """
                            if order[46] == 5:  # Woocommerce
                                try:
                                    order_ls = [order[36], order[37], order[38], order[39]]
                                    woocommerce_fulfillment(order_ls)
                                except Exception as e:
                                    logger.error("Couldn't update woocommerce for: " + str(order[1])
                                                 + "\nError: " + str(e.args))
                            """

                            data_tuple = tuple([(
                                return_data['awb_number'], return_data_raw['message'], order[0], pickup_point[1],
                                courier[9], json.dumps(dimensions), volumetric_weight, weight, "", pickup_point[2],
                                "", fulfillment_id, tracking_link)])

                        else:
                            data_tuple = tuple([(
                                "", return_data_raw['message'], order[0], pickup_point[1],
                                courier[9], json.dumps(dimensions), volumetric_weight, weight, return_data_raw['errors'], pickup_point[2],
                                "", fulfillment_id, tracking_link)])

                        cur.execute(insert_shipments_data_query, data_tuple)
                        ship_temp = cur.fetchone()
                        order_status_add_query = """INSERT INTO order_status (order_id, courier_id, shipment_id, 
                                                                                status_code, status, status_text, location, location_city, 
                                                                                status_time) VALUES %s"""

                        order_status_add_tuple = [(order[0], courier[9],
                                                   ship_temp[0], "UD", "Received", "Consignment Manifested",
                                                   pickup_point[6], pickup_point[6],datetime.utcnow()+timedelta(hours=5.5))]

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

        elif courier[10] in ("Xpressbees", "Xpressbees Surface"):
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

            cur.execute("select max(awb) from shipments where courier_id=%s;"%str(courier[9]))
            last_assigned_awb = cur.fetchone()[0]
            last_assigned_awb =int(last_assigned_awb)

            for pickup_id, all_new_orders in pickup_point_order_dict.items():
                last_shipped_order_id = 0
                headers = {"Content-Type": "application/json"}
                pickup_points_tuple = (pickup_id,)
                cur.execute(get_pickup_points_query, pickup_points_tuple)
                order_status_change_ids = list()

                pickup_point = cur.fetchone()  # change this as we get to dynamic pickups

                for order in all_new_orders:
                    time_2_days = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=2)
                    if order[47] and not (order[50] and order[2] < time_2_days):
                        if order[26].lower()=='cod' and not order[42] and order[43]:
                            continue
                        if order[26].lower()=='cod' and not order[43]:
                            cod_confirmation_link = "http://track.wareiq.com/core/v1/passthru/cod?CustomField=%s"%str(order[0])
                            """
                            short_url = requests.get(
                                "https://cutt.ly/api/api.php?key=f445d0bb52699d2f870e1832a1f77ef3f9078&short=%s" % cod_confirmation_link)
                            short_url_track = short_url.json()['url']['shortLink']
                            """
                            insert_cod_ver_tuple = (order[0], cod_confirmation_link, datetime.now())
                            cur.execute(
                                "INSERT INTO cod_verification (order_id, verification_link, date_created) VALUES (%s,%s,%s);",
                                insert_cod_ver_tuple)
                            cur_2.execute("select client_name from clients where client_prefix='%s'" % order[9])
                            client_name = cur_2.fetchone()
                            customer_phone = order[5].replace(" ", "")
                            customer_phone = "0" + customer_phone[-10:]

                            sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                            sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                            exotel_sms_data[sms_to_key] = customer_phone

                            exotel_sms_data[
                                sms_body_key] = "Dear Customer, You recently placed an order from %s with order id %s. " \
                                                "Please click on the link (%s) to verify. " \
                                                "Your order will be shipped soon after confirmation." % (
                                                    client_name[0], str(order[1]), cod_confirmation_link)

                            exotel_idx += 1
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

                        dimensions['height'] = round(
                            (volumetric_weight * 5000) / (dimensions['length'] * dimensions['breadth']))

                        customer_phone = order[21].replace(" ", "")
                        customer_phone = "0" + customer_phone[-10:]

                        customer_name = order[13]
                        if order[14]:
                            customer_name += " "+ order[14]

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
                            xpressbees_shipment_body["ManifestDetails"]["CollectibleAmount"]= order[27]
                        xpressbees_url = courier[16] + "POSTShipmentService.svc/AddManifestDetails"
                        req = requests.post(xpressbees_url, headers=headers, data=json.dumps(xpressbees_shipment_body))
                        while req.json()['AddManifestDetails'][0]['ReturnMessage']=='AWB Already Exists':
                            last_assigned_awb += 1
                            xpressbees_shipment_body['ManifestDetails']['AirWayBillNO'] = str(last_assigned_awb)
                            req = requests.post(xpressbees_url, headers=headers,
                                                data=json.dumps(xpressbees_shipment_body))
                        return_data_raw = req.json()
                        insert_shipments_data_query = """INSERT INTO SHIPMENTS (awb, status, order_id, pickup_id, courier_id, 
                                                                                                        dimensions, volumetric_weight, weight, remark, return_point_id, routing_code, 
                                                                                                        channel_fulfillment_id, tracking_link)
                                                                                                        VALUES  %s RETURNING id;"""
                        if return_data_raw['AddManifestDetails'][0]['ReturnMessage']=='successful':
                            order_status_change_ids.append(order[0])
                            if order[45] and order[46] == 1:  # shopify
                                try:
                                    order_ls = [order[36], order[37], order[38], order[39]]
                                    fulfillment_id, tracking_link = None, None #shopify_fulfillment(order_ls,
                                                                         #str(return_data_raw['AddManifestDetails'][0]['AWBNo']),
                                                                         #order[45])
                                except Exception as e:
                                    logger.error("Couldn't update shopify for: " + str(order[1])
                                                 + "\nError: " + str(e.args))

                            """
                            if order[46] == 1:  # Woocommerce
                                try:
                                    order_ls = [order[36], order[37], order[38], order[39]]
                                    woocommerce_fulfillment(order_ls)
                                except Exception as e:
                                    logger.error("Couldn't update Woocommerce for: " + str(order[1])
                                                 + "\nError: " + str(e.args))
                            """
                            data_tuple = tuple([(
                            return_data_raw['AddManifestDetails'][0]['AWBNo'], return_data_raw['AddManifestDetails'][0]['ReturnMessage'],
                            order[0], pickup_point[1], courier[9], json.dumps(dimensions), volumetric_weight, weight,
                            "", pickup_point[2], "", fulfillment_id, tracking_link)])
                            cur_2.execute("select client_name from clients where client_prefix='%s'" % order[9])
                            client_name = cur_2.fetchone()
                            customer_phone = order[5].replace(" ", "")
                            customer_phone = "0" + customer_phone[-10:]

                            sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                            sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                            exotel_sms_data[sms_to_key] = customer_phone
                            try:
                                tracking_link_wareiq = "http://webapp.wareiq.com/tracking/" + str(return_data_raw['AddManifestDetails'][0]['AWBNo'])
                                """
                                short_url = requests.get(
                                    "https://cutt.ly/api/api.php?key=f445d0bb52699d2f870e1832a1f77ef3f9078&short=%s" % tracking_link_wareiq)
                                short_url_track = short_url.json()['url']['shortLink']
                                """
                                exotel_sms_data[
                                    sms_body_key] = "Dear Customer, thank you for ordering from %s. " \
                                                    "Your order will be shipped by Xpressbees with AWB number %s. " \
                                                    "You can track your order on this ( %s ) link." % (
                                                        client_name[0], str(return_data_raw['AddManifestDetails'][0]['AWBNo']), tracking_link_wareiq)
                            except Exception:
                                exotel_sms_data[
                                    sms_body_key] = "Dear Customer, thank you for ordering from %s. Your order will be shipped by Xpressbees with AWB number %s. " \
                                                    "You can track your order using this AWB number." % (
                                                    client_name[0], str(return_data_raw['AddManifestDetails'][0]['AWBNo']))
                            exotel_idx += 1


                        else:
                            data_tuple = tuple([(
                                "", return_data_raw['AddManifestDetails'][0]['ReturnMessage'], order[0], pickup_point[1],
                                courier[9], json.dumps(dimensions), volumetric_weight, weight, return_data_raw['AddManifestDetails'][0]['ReturnMessage'], pickup_point[2],
                                "", fulfillment_id, tracking_link)])

                        cur.execute(insert_shipments_data_query, data_tuple)
                        ship_temp = cur.fetchone()
                        order_status_add_query = """INSERT INTO order_status (order_id, courier_id, shipment_id, 
                                                    status_code, status, status_text, location, location_city, 
                                                    status_time) VALUES %s"""

                        order_status_add_tuple = [(order[0], courier[9],
                                                   ship_temp[0], "UD", "Received", "Consignment Manifested",
                                                   pickup_point[6], pickup_point[6],datetime.utcnow()+timedelta(hours=5.5))]

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

    cur.close()


def shopify_fulfillment(order, awb_no, location_id):
    create_fulfillment_url = "https://%s:%s@%s/admin/api/2019-10/orders/%s/fulfillments.json" % (
        order[0], order[1],
        order[2], order[3])
    tracking_link = "http://webapp.wareiq.com/tracking/%s" % str(awb_no)
    ful_header = {'Content-Type': 'application/json'}
    fulfil_data = {
        "fulfillment": {
            "tracking_number": str(awb_no),
            "tracking_urls": [
                tracking_link
            ],
            "tracking_company": "WareIQ",
            "location_id": int(location_id),
            "notify_customer": False
        }
    }
    req_ful = requests.post(create_fulfillment_url, data=json.dumps(fulfil_data),
                                headers=ful_header)
    fulfillment_id = str(req_ful.json()['fulfillment']['id'])
    return fulfillment_id, tracking_link


def woocommerce_fulfillment(order):
    auth_session = OAuth1Session(order[0],
                                 client_secret=order[1])
    url = '%s/wp-json/wc/v3/orders/%s' % (order[2], str(order[3]))
    r = auth_session.post(url, data={"status": "shipped"})
    if r.status_code == 400:
        r = auth_session.post(url, data={"status": "completed"})
