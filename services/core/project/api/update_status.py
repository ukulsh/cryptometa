import psycopg2, requests, os, json
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
conn = psycopg2.connect(host="wareiq-core-prod2.cvqssxsqruyc.us-east-1.rds.amazonaws.com", database="core_prod", user="postgres", password="aSderRFgd23")
conn_2 = psycopg2.connect(host="wareiq-core-prod2.cvqssxsqruyc.us-east-1.rds.amazonaws.com", database="users_prod", user="postgres", password="aSderRFgd23")


def lambda_handler():
    cur = conn.cursor()
    cur_2 =conn_2.cursor()
    cur.execute(get_courier_id_and_key_query)
    for courier in cur.fetchall():
        try:
            if courier[1] in ("Delhivery", "Delhivery Surface Standard", "Delhivery Bulk", "Delhivery Heavy", "Delhivery Heavy 2"):
                cur.execute(get_status_update_orders_query%str(courier[0]))
                all_orders = cur.fetchall()
                pickup_count = 0
                exotel_idx = 0
                exotel_sms_data = {
                    'From': 'LM-WAREIQ'
                }
                orders_dict = dict()
                pickup_dict = dict()
                req_ship_data = list()
                chunks = [all_orders[x:x + 500] for x in range(0, len(all_orders), 500)]
                for some_orders in chunks:
                    awb_string = ""
                    for order in some_orders:
                        orders_dict[order[1]] = order
                        awb_string += order[1]+","

                    awb_string = awb_string.rstrip(',')

                    check_status_url = "https://track.delhivery.com/api/status/packages/json/?waybill=%s&token=%s" % (awb_string, courier[2])
                    req = requests.get(check_status_url).json()
                    req_ship_data += req['ShipmentData']
                logger.info("Count of delhivery packages: "+str(len(req_ship_data)))
                for ret_order in req_ship_data:
                    try:
                        new_status = ret_order['Shipment']['Status']['Status']
                        current_awb = ret_order['Shipment']['AWB']

                        try:
                            order_status_tuple = (orders_dict[current_awb][0], orders_dict[current_awb][10], courier[0])
                            cur.execute(select_statuses_query, order_status_tuple)
                            all_scans = cur.fetchall()
                            all_scans_dict = dict()
                            for temp_scan in all_scans:
                                all_scans_dict[temp_scan[2]] = temp_scan
                            new_status_dict = dict()
                            for each_scan in ret_order['Shipment']['Scans']:
                                status_time = each_scan['ScanDetail']['StatusDateTime']
                                if status_time:
                                    if len(status_time) == 19:
                                        status_time = datetime.strptime(status_time, '%Y-%m-%dT%H:%M:%S')
                                    else:
                                        status_time = datetime.strptime(status_time, '%Y-%m-%dT%H:%M:%S.%f')

                                to_record_status = ""
                                if each_scan['ScanDetail']['Scan'] == "Manifested" \
                                    and each_scan['ScanDetail']['Instructions'] == "Consignment Manifested":
                                    to_record_status = "Received"
                                elif each_scan['ScanDetail']['Scan'] == "In Transit" \
                                        and each_scan['ScanDetail']['Instructions'] == "Shipment Picked Up from Client Location":
                                    to_record_status = "Picked"
                                elif each_scan['ScanDetail']['Scan'] == "In Transit" \
                                        and each_scan['ScanDetail']['ScanType'] == "UD":
                                    to_record_status = "In Transit"
                                elif each_scan['ScanDetail']['Scan'] == "Dispatched" \
                                        and each_scan['ScanDetail']['Instructions'] == "Out for delivery":
                                    to_record_status = "Out for delivery"
                                elif each_scan['ScanDetail']['Scan'] == "Delivered":
                                    to_record_status = "Delivered"
                                elif each_scan['ScanDetail']['Scan'] == "Pending" \
                                     and each_scan['ScanDetail']['Instructions'] == "Customer Refused to accept/Order Cancelled":
                                    to_record_status = "Cancelled"
                                elif each_scan['ScanDetail']['ScanType'] == "RT":
                                    to_record_status = "Returned"
                                elif each_scan['ScanDetail']['Scan'] == "RTO":
                                    to_record_status = "RTO"

                                if not to_record_status:
                                    continue

                                if to_record_status not in new_status_dict:
                                    new_status_dict[to_record_status] = (orders_dict[current_awb][0], courier[0],
                                                                         orders_dict[current_awb][10],
                                                                         each_scan['ScanDetail']['ScanType'],
                                                                         to_record_status,
                                                                         each_scan['ScanDetail']['Instructions'],
                                                                         each_scan['ScanDetail']['ScannedLocation'],
                                                                         each_scan['ScanDetail']['CityLocation'],
                                                                         status_time)
                                elif to_record_status=='In Transit' and new_status_dict[to_record_status][8]<status_time:
                                    new_status_dict[to_record_status] = (orders_dict[current_awb][0], courier[0],
                                                                         orders_dict[current_awb][10],
                                                                         each_scan['ScanDetail']['ScanType'],
                                                                         to_record_status,
                                                                         each_scan['ScanDetail']['Instructions'],
                                                                         each_scan['ScanDetail']['ScannedLocation'],
                                                                         each_scan['ScanDetail']['CityLocation'],
                                                                         status_time)

                            for status_key, status_value in new_status_dict.items():
                                if status_key not in all_scans_dict:
                                    cur.execute("INSERT INTO order_status (order_id, courier_id, shipment_id, "
                                                "status_code, status, status_text, location, location_city, "
                                                "status_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);",
                                                status_value)

                                elif status_key=='In Transit' and status_value[8]>all_scans_dict[status_key][5]:
                                    cur.execute("UPDATE order_status SET location=%s, location_city=%s, status_time=%s"
                                                " WHERE id=%s;",(status_value[6], status_value[7], status_value[8], all_scans_dict[status_key][0]))

                        except Exception as e:
                            logger.error("Open status failed for id: " + str(orders_dict[current_awb][0])+ "\nErr: " + str(e.args[0]))

                        if new_status=="Manifested":
                            continue

                        new_status = new_status.upper()
                        status_type = ret_order['Shipment']['Status']['StatusType']
                        status_detail = None
                        status_code = None
                        if new_status == "PENDING":
                            status_code = ret_order['Shipment']['Scans'][-1]['ScanDetail']['StatusCode']
                            if status_code in delhivery_status_code_mapping_dict:
                                status_detail = delhivery_status_code_mapping_dict[status_code]
                            else:
                                status_detail = ret_order['Shipment']['Scans'][-1]['ScanDetail']['Instructions']

                        edd = ret_order['Shipment']['expectedDate']
                        if edd:
                            edd = datetime.strptime(edd, '%Y-%m-%dT%H:%M:%S')
                            cur.execute("UPDATE shipments SET edd=%s WHERE awb=%s", (edd, current_awb))

                        if new_status=='DELIVERED' and orders_dict[current_awb][13] and str(orders_dict[current_awb][13]).lower()=='prepaid':
                            try:  ## Delivery check text
                                sms_to_key, sms_body_key, customer_phone, sms_body_key_data = verification_text(orders_dict[current_awb], exotel_idx, cur, cur_2)
                                exotel_sms_data[sms_to_key] = customer_phone
                                exotel_sms_data[sms_body_key] = sms_body_key_data
                                exotel_idx += 1
                            except Exception as e:
                                logger.error("Delivery confirmation not sent. Order id: "+str(orders_dict[current_awb][0]))
                        """
                            if orders_dict[current_awb][6] and orders_dict[current_awb][5]:
                                complete_fulfillment_url = "https://%s:%s@%s/admin/api/2019-10/orders/%s/fulfillments/%s/complete.json" % (
                                    orders_dict[current_awb][7], orders_dict[current_awb][8],
                                    orders_dict[current_awb][9], orders_dict[current_awb][5], orders_dict[current_awb][6])
                                ful_header = {'Content-Type': 'application/json'}
                                fulfil_data = {}
                                try:
                                    req_ful = requests.post(complete_fulfillment_url, data=json.dumps(fulfil_data),
                                                            headers=ful_header)
                                except Exception as e:
                                    print("Couldn't update shopify for: " + str(orders_dict[current_awb][0]))
                        """

                        if orders_dict[current_awb][2] in ('READY TO SHIP', 'PICKUP REQUESTED', 'NOT PICKED') and new_status=='IN TRANSIT':
                            pickup_count += 1
                            if orders_dict[current_awb][11] not in pickup_dict:
                                pickup_dict[orders_dict[current_awb][11]] = 1
                            else:
                                pickup_dict[orders_dict[current_awb][11]] += 1
                            cur.execute(update_prod_quantity_query_pickup%str(orders_dict[current_awb][0]))
                            """
                            if orders_dict[current_awb][5] and not orders_dict[current_awb][6]:
                                create_fulfillment_url = "https://%s:%s@%s/admin/api/2019-10/orders/%s/fulfillments.json"  % (
                                    orders_dict[current_awb][7], orders_dict[current_awb][8],
                                    orders_dict[current_awb][9], orders_dict[current_awb][5])
                                tracking_link = "https://www.delhivery.com/track/package/%s" % str(current_awb)
                                ful_header = {'Content-Type': 'application/json'}
                                fulfil_data = {
                                    "fulfillment": {
                                        "tracking_number": str(current_awb),
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
                                    cur.execute("UPDATE shipments SET channel_fulfillment_id=%s, tracking_link=%s WHERE awb=%s;",
                                        (fulfillment_id, tracking_link, current_awb))
                                except Exception as e:
                                    logger.error("Couldn't update shopify for: " + str(orders_dict[current_awb][0])
                                                 + "\nError: " + str(e.args))
                            """
                            if edd:
                                edd = edd.strftime('%-d %b')
                                cur_2.execute("select client_name from clients where client_prefix='%s'"%orders_dict[current_awb][3])
                                client_name = cur_2.fetchone()
                                customer_phone = orders_dict[current_awb][4].replace(" ","")
                                customer_phone = "0"+customer_phone[-10:]

                                sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                                sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                                exotel_sms_data[sms_to_key] = customer_phone
                                try:
                                    tracking_link_wareiq = "http://webapp.wareiq.com/tracking/" + str(
                                        orders_dict[current_awb][1])
                                    short_url = requests.get(
                                        "https://cutt.ly/api/api.php?key=f445d0bb52699d2f870e1832a1f77ef3f9078&short=%s" % tracking_link_wareiq)
                                    short_url_track = short_url.json()['url']['shortLink']
                                    exotel_sms_data[
                                        sms_body_key] = "Dear Customer, your %s order has been shipped via Delhivery with AWB number %s. " \
                                                        "It is expected to arrive by %s. You can track your order on this (%s) link." % (
                                                            client_name[0], str(orders_dict[current_awb][1]), edd,
                                                            short_url_track)
                                except Exception:
                                    exotel_sms_data[sms_body_key] = "Dear Customer, your %s order has been shipped via Delhivery with AWB number %s. It is expected to arrive by %s. Thank you for Ordering." % (
                                    client_name[0], orders_dict[current_awb][1], edd)
                                exotel_idx += 1

                        if orders_dict[current_awb][2] != new_status:
                            status_update_tuple = (new_status, status_type, status_detail, orders_dict[current_awb][0])
                            cur.execute(order_status_update_query, status_update_tuple)
                            if new_status=="RTO" and ret_order['Shipment']['Status']['StatusType']=="DL":
                                cur.execute(update_prod_quantity_query_rto%str(orders_dict[current_awb][0]))

                            if new_status=='PENDING' and status_code in ('EOD-111','EOD-6','FMEOD-118','EOD-69','EOD-11'):
                                try:  # NDR check text
                                    sms_to_key, sms_body_key, customer_phone, sms_body_key_data = verification_text(
                                        orders_dict[current_awb], exotel_idx, cur, cur_2, ndr=True)
                                    exotel_sms_data[sms_to_key] = customer_phone
                                    exotel_sms_data[sms_body_key] = sms_body_key_data
                                    exotel_idx += 1
                                except Exception as e:
                                    logger.error(
                                        "NDR confirmation not sent. Order id: " + str(orders_dict[current_awb][0]))

                    except Exception as e:
                        logger.error("status update failed for " + "    err:" + str(e.args[0]))

                if exotel_idx:
                    logger.info("Sending messages...count:" + str(exotel_idx))
                    try:
                        lad = requests.post(
                            'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend',
                            data=exotel_sms_data)
                    except Exception as e:
                        logger.error("messages not sent." + "   Error: " + str(e.args[0]))
                if pickup_count:
                    logger.info("Total Picked: " + str(pickup_count) + "  Time: " + str(datetime.utcnow()))
                    try:
                        for key, value in pickup_dict.items():
                            logger.info("picked for pickup_id "+str(key)+": " +str(value))
                            date_today = datetime.now().strftime('%Y-%m-%d')
                            pickup_count_tuple = (value, courier[0], key, date_today)
                            cur.execute(update_pickup_count_query, pickup_count_tuple)
                    except Exception as e:
                        logger.error("Couldn't update pickup count for : " + str(e.args[0]))

                conn.commit()
            elif courier[1] == "Shadowfax":
                pickup_count = 0
                cur.execute(get_status_update_orders_query % str(courier[0]))
                all_orders = cur.fetchall()
                exotel_idx = 0
                exotel_sms_data = {
                    'From': 'LM-WAREIQ'
                }
                orders_dict = dict()
                awb_list = list()
                pickup_dict = dict()

                for order in all_orders:
                    orders_dict[order[1]] = order
                    awb_list.append(order[1])

                headers = {"Authorization": "Token " + courier[2],
                           "Content-Type": "application/json"}
                shadowfax_body = {"awb_numbers": awb_list}
                check_status_url = "http://dale.shadowfax.in/api/v2/clients/bulk_track/?format=json"
                req = requests.post(check_status_url, headers=headers, data=json.dumps(shadowfax_body)).json()
                logger.info("Count of Shadowfax packages: " + str(len(req['data'])))
                for ret_order in req['data']:
                    try:
                        new_status = ret_order['status']
                        current_awb = ret_order['awb_number']

                        try:
                            order_status_tuple = (orders_dict[current_awb][0], orders_dict[current_awb][10], courier[0])
                            cur.execute(select_statuses_query, order_status_tuple)
                            all_scans = cur.fetchall()
                            all_scans_dict = dict()
                            for temp_scan in all_scans:
                                all_scans_dict[temp_scan[2]] = temp_scan
                            new_status_dict = dict()
                            for each_scan in ret_order['tracking_details']:
                                if not each_scan.get('location'):
                                    continue
                                status_time = each_scan['created']
                                if status_time:
                                    status_time = datetime.strptime(status_time, '%Y-%m-%dT%H:%M:%SZ')

                                to_record_status = ""
                                if each_scan['status'] == "New" \
                                    and each_scan['status_id'] == "new":
                                    to_record_status = "Received"
                                elif each_scan['status'] == "Picked" \
                                        and each_scan['status_id'] == "picked":
                                    to_record_status = "Picked"
                                elif each_scan['status'] == "Received at Forward Hub" \
                                        and each_scan['status_id'] == "recd_at_fwd_hub":
                                    to_record_status = "In Transit"
                                elif each_scan['status'] == "Out For Delivery" \
                                        and each_scan['status_id'] == "ofd":
                                    to_record_status = "Out for delivery"
                                elif each_scan['status'] == "Delivered" \
                                     and each_scan['status_id'] == "delivered":
                                    to_record_status = "Delivered"
                                elif each_scan['status'] == "Cancelled":
                                    to_record_status = "Cancelled"
                                elif each_scan['status_id'] == "rts_d":
                                    to_record_status = "RTO"

                                if not to_record_status:
                                    continue

                                if to_record_status not in new_status_dict:
                                    new_status_dict[to_record_status] = (orders_dict[current_awb][0], courier[0],
                                                                         orders_dict[current_awb][10],
                                                                         shadowfax_status_mapping[each_scan['status_id']][1],
                                                                         to_record_status,
                                                                         each_scan['remarks'],
                                                                         each_scan['location'],
                                                                         each_scan['location'],
                                                                         status_time)
                                elif to_record_status=='In Transit' and new_status_dict[to_record_status][8]<status_time:
                                    new_status_dict[to_record_status] = (orders_dict[current_awb][0], courier[0],
                                                                         orders_dict[current_awb][10],
                                                                         shadowfax_status_mapping[each_scan['status_id']][1],
                                                                         to_record_status,
                                                                         each_scan['remarks'],
                                                                         each_scan['location'],
                                                                         each_scan['location'],
                                                                         status_time)

                            for status_key, status_value in new_status_dict.items():
                                if status_key not in all_scans_dict:
                                    cur.execute("INSERT INTO order_status (order_id, courier_id, shipment_id, "
                                                "status_code, status, status_text, location, location_city, "
                                                "status_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);",
                                                status_value)

                                elif status_key=='In Transit' and status_value[8]>all_scans_dict[status_key][5]:
                                    cur.execute("UPDATE order_status SET location=%s, location_city=%s, status_time=%s"
                                                " WHERE id=%s;",(status_value[6], status_value[7], status_value[8], all_scans_dict[status_key][0]))

                        except Exception as e:
                            logger.error("Open status failed for id: " + str(orders_dict[current_awb][0])+ "\nErr: " + str(e.args[0]))

                        try:
                            status_type = shadowfax_status_mapping[new_status][1]
                            new_status_temp = shadowfax_status_mapping[new_status][0]
                            status_detail = None
                            if new_status_temp == "PENDING":
                                if new_status in shadowfax_status_mapping:
                                    status_detail = shadowfax_status_mapping[new_status][2]
                                else:
                                    status_detail = ret_order['tracking_details'][-1]['status']
                        except KeyError:
                            if new_status=='seller_initiated_delay':
                                continue
                            else:
                                status_type = "UD"
                                new_status_temp = new_status_temp.upper()
                                status_detail = None
                        if new_status_temp == "READY TO SHIP" and orders_dict[current_awb][2] == new_status:
                            continue
                        new_status = new_status_temp
                        edd = ret_order['promised_delivery_date']
                        if edd:
                            edd = datetime.strptime(edd, '%Y-%m-%dT%H:%M:%SZ')
                            cur.execute("UPDATE shipments SET edd=%s WHERE awb=%s", (edd, current_awb))

                        if new_status == 'DELIVERED' and orders_dict[current_awb][13] and str(
                                orders_dict[current_awb][13]).lower() == 'prepaid':
                            try:  ## Delivery check text
                                sms_to_key, sms_body_key, customer_phone, sms_body_key_data = verification_text(
                                    orders_dict[current_awb], exotel_idx, cur, cur_2)
                                exotel_sms_data[sms_to_key] = customer_phone
                                exotel_sms_data[sms_body_key] = sms_body_key_data
                                exotel_idx += 1
                            except Exception as e:
                                logger.error(
                                    "Delivery confirmation not sent. Order id: " + str(orders_dict[current_awb][0]))
                        """
                            if orders_dict[current_awb][6] and orders_dict[current_awb][5]:
                                complete_fulfillment_url = "https://%s:%s@%s/admin/api/2019-10/orders/%s/fulfillments/%s/complete.json" % (
                                    orders_dict[current_awb][7], orders_dict[current_awb][8],
                                    orders_dict[current_awb][9], orders_dict[current_awb][5], orders_dict[current_awb][6])
                                ful_header = {'Content-Type': 'application/json'}
                                fulfil_data = {}
                                try:
                                    req_ful = requests.post(complete_fulfillment_url, data=json.dumps(fulfil_data),
                                                            headers=ful_header)
                                except Exception as e:
                                    print("Couldn't update shopify for: " + str(orders_dict[current_awb][0]))
                        """

                        if orders_dict[current_awb][2] in (
                        'READY TO SHIP', 'PICKUP REQUESTED', 'NOT PICKED') and new_status == 'IN TRANSIT':
                            pickup_count += 1
                            if orders_dict[current_awb][11] not in pickup_dict:
                                pickup_dict[orders_dict[current_awb][11]] = 1
                            else:
                                pickup_dict[orders_dict[current_awb][11]] += 1
                            cur.execute(update_prod_quantity_query_pickup%str(orders_dict[current_awb][0]))
                            """
                            if orders_dict[current_awb][5] and not orders_dict[current_awb][6]:
                                create_fulfillment_url = "https://%s:%s@%s/admin/api/2019-10/orders/%s/fulfillments.json" % (
                                    orders_dict[current_awb][7], orders_dict[current_awb][8],
                                    orders_dict[current_awb][9], orders_dict[current_awb][5])
                                tracking_link = "https://saruman.shadowfax.in/awb/awb_to_unique_code/?order_id=%s" % str(current_awb)
                                ful_header = {'Content-Type': 'application/json'}
                                fulfil_data = {
                                    "fulfillment": {
                                        "tracking_number": str(current_awb),
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
                                    cur.execute(
                                        "UPDATE shipments SET channel_fulfillment_id=%s, tracking_link=%s WHERE awb=%s;",
                                        (fulfillment_id, tracking_link, current_awb))
                                except Exception as e:
                                    logger.error("Couldn't update shopify for: " + str(orders_dict[current_awb][0])
                                                 + "\nError: " + str(e.args))
                            """
                            if edd:
                                edd = edd.strftime('%-d %b')
                                cur_2.execute(
                                    "select client_name from clients where client_prefix='%s'" % orders_dict[current_awb][
                                        3])
                                client_name = cur_2.fetchone()
                                customer_phone = orders_dict[current_awb][4].replace(" ", "")
                                customer_phone = "0" + customer_phone[-10:]

                                sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                                sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                                exotel_sms_data[sms_to_key] = customer_phone
                                exotel_sms_data[
                                    sms_body_key] = "Dear Customer, your %s order has been shipped via Shadowfax with AWB number %s. It is expected to arrive by %s. Thank you for ordering." % (
                                    client_name[0], orders_dict[current_awb][1], edd)
                                exotel_idx += 1

                        if orders_dict[current_awb][2] != new_status:
                            status_update_tuple = (new_status, status_type, status_detail, orders_dict[current_awb][0])
                            cur.execute(order_status_update_query, status_update_tuple)
                            if ret_order['status']=="rts_d":
                                cur.execute(update_prod_quantity_query_rto%str(orders_dict[current_awb][0]))

                            if ret_order['status'] in ('cancelled_by_customer', 'nc'):
                                try:  # NDR check text
                                    sms_to_key, sms_body_key, customer_phone, sms_body_key_data = verification_text(
                                        orders_dict[current_awb], exotel_idx, cur, cur_2, ndr=True)
                                    exotel_sms_data[sms_to_key] = customer_phone
                                    exotel_sms_data[sms_body_key] = sms_body_key_data
                                    exotel_idx += 1
                                except Exception as e:
                                    logger.error(
                                        "NDR confirmation not sent. Order id: " + str(orders_dict[current_awb][0]))

                    except Exception as e:
                        logger.error("status update failed for " + "    err:" + str(e.args[0]))

                if exotel_idx:
                    logger.info("Sending messages...count:" + str(exotel_idx))
                    logger.info("Total Picked: " + str(exotel_idx) + "  Time: " + str(datetime.utcnow()))
                    try:
                        lad = requests.post(
                            'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend',
                            data=exotel_sms_data)
                    except Exception as e:
                        logger.error("messages not sent." + "   Error: " + str(e.args[0]))

                if pickup_count:
                    logger.info("Total Picked: " + str(pickup_count) + "  Time: " + str(datetime.utcnow()))
                    try:
                        for key, value in pickup_dict.items():
                            logger.info("picked for pickup_id "+str(key)+": " +str(value))
                            date_today = datetime.now().strftime('%Y-%m-%d')
                            pickup_count_tuple = (value, courier[0], key, date_today)
                            cur.execute(update_pickup_count_query, pickup_count_tuple)
                    except Exception as e:
                        logger.error("Couldn't update pickup count for : " + str(e.args[0]))

                conn.commit()

            elif courier[1] in ("Xpressbees", "Xpressbees Surface"):
                pickup_count = 0
                cur.execute(get_status_update_orders_query % str(courier[0]))
                all_orders = cur.fetchall()
                exotel_idx = 0
                exotel_sms_data = {
                    'From': 'LM-WAREIQ'
                }
                orders_dict = dict()
                pickup_dict = dict()
                req_ship_data = list()
                headers = {"Content-Type": "application/json"}
                chunks = [all_orders[x:x + 20] for x in range(0, len(all_orders), 20)]
                for some_orders in chunks:
                    awb_string = ""
                    for order in some_orders:
                        orders_dict[order[1]] = order
                        awb_string += order[1] + ","

                    xpressbees_body = {"AWBNo": awb_string.rstrip(","), "XBkey": courier[2]}

                    check_status_url = "http://xbclientapi.xbees.in/TrackingService.svc/GetShipmentSummaryDetails"
                    req = requests.post(check_status_url, headers=headers, data=json.dumps(xpressbees_body)).json()
                    req_ship_data += req

                logger.info("Count of Xpressbees packages: " + str(len(req_ship_data)))
                for ret_order in req_ship_data:
                    try:
                        if not ret_order['ShipmentSummary']:
                            continue
                        new_status = ret_order['ShipmentSummary'][0]['StatusCode']
                        current_awb = ret_order['AWBNo']

                        try:
                            order_status_tuple = (orders_dict[current_awb][0], orders_dict[current_awb][10], courier[0])
                            cur.execute(select_statuses_query, order_status_tuple)
                            all_scans = cur.fetchall()
                            all_scans_dict = dict()
                            for temp_scan in all_scans:
                                all_scans_dict[temp_scan[2]] = temp_scan
                            new_status_dict = dict()
                            for each_scan in ret_order['ShipmentSummary']:
                                if not each_scan.get('Location'):
                                    continue
                                status_time = each_scan['StatusDate']+"T"+each_scan['StatusTime']
                                if status_time:
                                    status_time = datetime.strptime(status_time, '%d-%m-%YT%H%M')

                                to_record_status = ""
                                if each_scan['StatusCode'] == "DRC":
                                    to_record_status = "Received"
                                elif each_scan['StatusCode'] == "PUD":
                                    to_record_status = "Picked"
                                elif each_scan['StatusCode'] in ("IT","RAD"):
                                    to_record_status = "In Transit"
                                elif each_scan['StatusCode'] == "OFD":
                                    to_record_status = "Out for delivery"
                                elif each_scan['StatusCode'] == "DLVD":
                                    to_record_status = "Delivered"
                                elif each_scan['StatusCode'] == "UD" and each_scan['Status'] in \
                                        ("Consignee Refused To Accept", "Consignee Refused to Pay COD Amount"):
                                    to_record_status = "Cancelled"
                                elif each_scan['StatusCode'] == "RTO":
                                    to_record_status = "Returned"
                                elif each_scan['StatusCode'] == "RTD":
                                    to_record_status = "RTO"

                                if not to_record_status:
                                    continue

                                if to_record_status not in new_status_dict:
                                    new_status_dict[to_record_status] = (orders_dict[current_awb][0], courier[0],
                                                                         orders_dict[current_awb][10],
                                                                         xpressbees_status_mapping[
                                                                             each_scan['StatusCode']][1],
                                                                         to_record_status,
                                                                         each_scan['Status'],
                                                                         each_scan['Location'],
                                                                         each_scan['Location'].split(', ')[1],
                                                                         status_time)
                                elif to_record_status == 'In Transit' and new_status_dict[to_record_status][
                                    8] < status_time:
                                    new_status_dict[to_record_status] = (orders_dict[current_awb][0], courier[0],
                                                                         orders_dict[current_awb][10],
                                                                         xpressbees_status_mapping[
                                                                             each_scan['StatusCode']][1],
                                                                         to_record_status,
                                                                         each_scan['Status'],
                                                                         each_scan['Location'],
                                                                         each_scan['Location'].split(', ')[1],
                                                                         status_time)

                            for status_key, status_value in new_status_dict.items():
                                if status_key not in all_scans_dict:
                                    cur.execute("INSERT INTO order_status (order_id, courier_id, shipment_id, "
                                                "status_code, status, status_text, location, location_city, "
                                                "status_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);",
                                                status_value)

                                elif status_key == 'In Transit' and status_value[8] > all_scans_dict[status_key][5]:
                                    cur.execute("UPDATE order_status SET location=%s, location_city=%s, status_time=%s"
                                                " WHERE id=%s;", (status_value[6], status_value[7], status_value[8],
                                                                  all_scans_dict[status_key][0]))

                        except Exception as e:
                            logger.error(
                                "Open status failed for id: " + str(orders_dict[current_awb][0]) + "\nErr: " + str(
                                    e.args[0]))

                        try:
                            status_type = xpressbees_status_mapping[new_status][1]
                            new_status_temp = xpressbees_status_mapping[new_status][0]
                            status_detail = None
                            if new_status_temp == "PENDING":
                                status_detail = ret_order['ShipmentSummary'][0]['Status']
                        except KeyError:
                            new_status_temp = new_status_temp.upper()
                            status_type = None
                            status_detail = None
                        if new_status_temp == "READY TO SHIP" and orders_dict[current_awb][2] == new_status:
                            continue
                        new_status = new_status_temp

                        edd = ret_order['ShipmentSummary'][0].get('ExpectedDeliveryDate')
                        if edd:
                            edd = datetime.strptime(ret_order['ShipmentSummary'][0]['ExpectedDeliveryDate'], '%m/%d/%Y %I:%M:%S %p')
                            cur.execute("UPDATE shipments SET edd=%s WHERE awb=%s", (edd, current_awb))

                        if new_status == 'DELIVERED' and orders_dict[current_awb][13] and str(
                                orders_dict[current_awb][13]).lower() == 'prepaid':
                            try:  ## Delivery check text
                                sms_to_key, sms_body_key, customer_phone, sms_body_key_data = verification_text(
                                    orders_dict[current_awb], exotel_idx, cur, cur_2)
                                exotel_sms_data[sms_to_key] = customer_phone
                                exotel_sms_data[sms_body_key] = sms_body_key_data
                                exotel_idx += 1
                            except Exception as e:
                                logger.error(
                                    "Delivery confirmation not sent. Order id: " + str(orders_dict[current_awb][0]))
                        if orders_dict[current_awb][2] in (
                                'READY TO SHIP', 'PICKUP REQUESTED', 'NOT PICKED') and new_status == 'IN TRANSIT':
                            pickup_count += 1
                            if orders_dict[current_awb][11] not in pickup_dict:
                                pickup_dict[orders_dict[current_awb][11]] = 1
                            else:
                                pickup_dict[orders_dict[current_awb][11]] += 1
                            cur.execute(update_prod_quantity_query_pickup % str(orders_dict[current_awb][0]))

                            if edd:
                                edd = edd.strftime('%-d %b')
                                cur_2.execute(
                                    "select client_name from clients where client_prefix='%s'" % orders_dict[current_awb][
                                        3])
                                client_name = cur_2.fetchone()
                                customer_phone = orders_dict[current_awb][4].replace(" ", "")
                                customer_phone = "0" + customer_phone[-10:]

                                sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                                sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                                exotel_sms_data[sms_to_key] = customer_phone
                                try:
                                    tracking_link_wareiq = "http://webapp.wareiq.com/tracking/" + str(
                                        orders_dict[current_awb][1])
                                    short_url = requests.get(
                                        "https://cutt.ly/api/api.php?key=f445d0bb52699d2f870e1832a1f77ef3f9078&short=%s" % tracking_link_wareiq)
                                    short_url_track = short_url.json()['url']['shortLink']
                                    exotel_sms_data[
                                        sms_body_key] = "Dear Customer, your %s order has been shipped via Xpressbees with AWB number %s. " \
                                                        "It is expected to arrive by %s. You can track your order on this (%s) link." % (
                                                            client_name[0], str(orders_dict[current_awb][1]), edd,
                                                            short_url_track)
                                except Exception:
                                    exotel_sms_data[
                                        sms_body_key] = "Dear Customer, your %s order has been shipped via Xpressbees with AWB number %s. It is expected to arrive by %s. Thank you for Ordering." % (
                                        client_name[0], orders_dict[current_awb][1], edd)
                                exotel_idx += 1

                        if orders_dict[current_awb][2] != new_status:
                            status_update_tuple = (new_status, status_type, status_detail, orders_dict[current_awb][0])
                            cur.execute(order_status_update_query, status_update_tuple)
                            if ret_order['ShipmentSummary'][0]['StatusCode'] == "RTD":
                                cur.execute(update_prod_quantity_query_rto % str(orders_dict[current_awb][0]))

                            if ret_order['ShipmentSummary'][0]['StatusCode'] == 'UD' \
                                    and ret_order['ShipmentSummary'][0]['Status'] in \
                                    ("Customer Refused To Accept", "Customer Refused to Pay COD Amount",
                                     "Add Incomplete/ Incorrect & Mobile Not Reachable", "Customer Not Available & Mobile Not Reachable"):
                                try:  # NDR check text
                                    sms_to_key, sms_body_key, customer_phone, sms_body_key_data = verification_text(
                                        orders_dict[current_awb], exotel_idx, cur, cur_2, ndr=True)
                                    exotel_sms_data[sms_to_key] = customer_phone
                                    exotel_sms_data[sms_body_key] = sms_body_key_data
                                    exotel_idx += 1
                                except Exception as e:
                                    logger.error(
                                        "NDR confirmation not sent. Order id: " + str(orders_dict[current_awb][0]))

                    except Exception as e:
                        logger.error("status update failed for " + "    err:" + str(e.args[0]))

                if exotel_idx:
                    logger.info("Sending messages...count:" + str(exotel_idx))
                    logger.info("Total Picked: " + str(exotel_idx) + "  Time: " + str(datetime.utcnow()))
                    try:
                        lad = requests.post(
                            'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend',
                            data=exotel_sms_data)
                    except Exception as e:
                        logger.error("messages not sent." + "   Error: " + str(e.args[0]))

                if pickup_count:
                    logger.info("Total Picked: " + str(pickup_count) + "  Time: " + str(datetime.utcnow()))
                    try:
                        for key, value in pickup_dict.items():
                            logger.info("picked for pickup_id " + str(key) + ": " + str(value))
                            date_today = datetime.now().strftime('%Y-%m-%d')
                            pickup_count_tuple = (value, courier[0], key, date_today)
                            cur.execute(update_pickup_count_query, pickup_count_tuple)
                    except Exception as e:
                        logger.error("Couldn't update pickup count for : " + str(e.args[0]))

                conn.commit()

        except Exception as e:
            logger.error("Status update failed: "+str(e.args[0]))

    cur.close()


def verification_text(current_order, exotel_idx, cur, cur_2, ndr=None):
    if not ndr:
        del_confirmation_link = "http://track.wareiq.com/core/v1/passthru/delivery?CustomField=%s&digits=1&verified_via=text" % str(
            current_order[0])
    else:
        del_confirmation_link = "http://track.wareiq.com/core/v1/passthru/ndr?CustomField=%s&digits=0&verified_via=text" % str(
            current_order[0])
    short_url = requests.get(
        "https://cutt.ly/api/api.php?key=f445d0bb52699d2f870e1832a1f77ef3f9078&short=%s" % del_confirmation_link)
    short_url_track = short_url.json()['url']['shortLink']
    insert_cod_ver_tuple = (current_order[0], short_url_track, datetime.now())
    if not ndr:
        cur.execute(
            "INSERT INTO delivery_check (order_id, verification_link, date_created) VALUES (%s,%s,%s);",
            insert_cod_ver_tuple)
    else:
        cur.execute(
            "INSERT INTO ndr_verification (order_id, verification_link, date_created) VALUES (%s,%s,%s);",
            insert_cod_ver_tuple)
    cur_2.execute("select client_name from clients where client_prefix='%s'" % current_order[3])
    client_name = cur_2.fetchone()
    customer_phone = current_order[4].replace(" ", "")
    customer_phone = "0" + customer_phone[-10:]

    sms_to_key = "Messages[%s][To]" % str(exotel_idx)
    sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

    if not ndr:
        sms_body_key_data = "Dear Customer, your order from %s with order id %s was delivered today." \
                            " Please click on the link (%s) to report any issue. We'll call you back shortly." % (
                                client_name[0], str(current_order[12]),
                                short_url_track)
    else:
        sms_body_key_data = "Dear Customer, Delivery for your order from %s with order id %s was attempted today." \
                            " If you didn't cancel, please click on the link (%s). We'll call you shortly." % (
                                client_name[0], str(current_order[12]),
                                short_url_track)

    return sms_to_key, sms_body_key, customer_phone, sms_body_key_data


delhivery_status_code_mapping_dict = {
                            "DLYDC-107": "Office/Institute closed",
                            "DLYDC-110": "Delivery rescheduled",
                            "DLYDC-132": "Out of Delivery Area (ODA)",
                            "EOD-104": "Entry restricted area",
                            "EOD-11": "Consignee unavailable",
                            "EOD-111": "Consignee opened the package and refused to accept",
                            "EOD-3": "Delivery rescheduled",
                            "EOD-40": "Payment Mode/Amount Dispute",
                            "EOD-6": "Cancelled the order",
                            "EOD-69": "Customer asked for open delivery",
                            "EOD-74": "Bad Address",
                            "EOD-86": "Not attempted",
                            "FMEOD-103": "Shipper is closed",
                            "FMEOD-106": "Not attempted",
                            "FMEOD-118": "Cancelled the order",
                            "FMEOD-152": "Shipment not ready-Partial Pickup",
                            "FMOFP-101": "Out for Pickup",
                            "RDPD-17": "Not attempted",
                            "RDPD-3": "Returned",
                            "RT-101": "Not attempted",
                            "RT-108": "Returned",
                            "RT-113": "Returned",
                            "ST-105": "Reattempt requested",
                            "ST-107": "Reattempt requested",
                            "ST-108": "Reached maximum attempt count",
                            }

shadowfax_status_mapping = {"new":("READY TO SHIP", "UD", ""),
                            "sent_to_rev":("READY TO SHIP", "UD", ""),
                            "assigned_for_pickup":("READY TO SHIP", "UD", ""),
                            "ofp":("READY TO SHIP", "UD", ""),
                            "picked":("IN TRANSIT", "UD", ""),
                            "recd_at_rev_hub":("IN TRANSIT", "UD", ""),
                            "sent_to_fwd":("IN TRANSIT", "UD", ""),
                            "recd_at_fwd_hub":("IN TRANSIT", "UD", ""),
                            "recd_at_fwd_dc":("IN TRANSIT", "UD", ""),
                            "assigned_for_delivery":("IN TRANSIT", "UD", ""),
                            "ofd":("DISPATCHED", "UD", ""),
                            "cid":("PENDING", "UD", "Delivery rescheduled"),
                            "nc":("PENDING", "UD", "Consignee unavailable"),
                            "na":("PENDING", "UD", "Not attempted"),
                            "reopen_ndr":("PENDING", "UD", "Delivery rescheduled"),
                            "delivered":("DELIVERED", "DL", ""),
                            "cancelled_by_customer":("PENDING", "UD", "Cancelled the order"),
                            "rts":("PENDING", "RT", "Pending for rts"),
                            "rts_d":("RTO", "DL", ""),
                            "lost":("LOST", "UD", ""),
                            "on_hold":("ON HOLD", "UD", ""),
                            "pickup_on_hold":("NOT PICKED", "UD", ""),
                            }

xpressbees_status_mapping = {"DRC":("READY TO SHIP", "UD", ""),
                            "PUC":("PICKUP REQUESTED", "UD", ""),
                            "OFP":("PICKUP REQUESTED", "UD", ""),
                            "PUD":("IN TRANSIT", "UD", ""),
                            "PND":("NOT PICKED", "UD", ""),
                            "PKD":("IN TRANSIT", "UD", ""),
                            "IT":("IN TRANSIT", "UD", ""),
                            "RAD":("IN TRANSIT", "UD", ""),
                            "OFD":("DISPATCHED", "UD", ""),
                            "RTON":("IN TRANSIT", "RT", ""),
                            "RTO":("IN TRANSIT", "RT", ""),
                            "RTO-IT":("IN TRANSIT", "RT", ""),
                            "RAO":("IN TRANSIT", "RT", ""),
                            "RTU":("IN TRANSIT", "RT", ""),
                            "RTO-OFD":("DISPATCHED", "RT", ""),
                            "STD":("DAMAGED", "UD", ""),
                            "STG":("SHORTAGE", "UD", ""),
                            "RTO-STG":("SHORTAGE", "RT", ""),
                            "DLVD":("DELIVERED", "DL", ""),
                            "RTD":("RTO", "DL", ""),
                            "LOST":("LOST", "UD", ""),
                            "UD":("PENDING", "UD", "")
                            }