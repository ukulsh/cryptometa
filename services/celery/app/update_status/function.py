import psycopg2, requests, os, json
import logging
from datetime import datetime, timedelta
from .queries import *
from .update_status_utils import *
from woocommerce import API


logger = logging.getLogger()
logger.setLevel(logging.INFO)
"""
host = os.environ('DTATBASE_HOST')
database = os.environ('DTATBASE_NAME')
user = os.environ('DTATBASE_USER')
password = os.environ('DTATBASE_PASSWORD')
conn = psycopg2.connect(host=host, database=database, user=user, password=password)
"""
conn = psycopg2.connect(host="wareiq-core-prod2.cvqssxsqruyc.us-east-1.rds.amazonaws.com", database="core_prod",
                        user="postgres", password="aSderRFgd23")
conn_2 = psycopg2.connect(host="wareiq-core-prod2.cvqssxsqruyc.us-east-1.rds.amazonaws.com", database="users_prod",
                          user="postgres", password="aSderRFgd23")


def update_status():
    cur = conn.cursor()
    cur_2 = conn_2.cursor()
    cur.execute(get_courier_id_and_key_query)
    for courier in cur.fetchall():
        try:
            if courier[1] in (
                    "Delhivery", "Delhivery Surface Standard", "Delhivery Bulk", "Delhivery Heavy",
                    "Delhivery Heavy 2"):
                cur.execute(get_status_update_orders_query % str(courier[0]))
                all_orders = cur.fetchall()
                pickup_count = 0
                exotel_idx = 0
                exotel_sms_data = {
                    'From': 'LM-WAREIQ'
                }
                orders_dict = dict()
                pickup_dict = dict()
                emails_list = list()
                req_ship_data = list()
                chunks = [all_orders[x:x + 500] for x in range(0, len(all_orders), 500)]
                for some_orders in chunks:
                    awb_string = ""
                    for order in some_orders:
                        orders_dict[order[1]] = order
                        awb_string += order[1] + ","

                    awb_string = awb_string.rstrip(',')

                    check_status_url = "https://track.delhivery.com/api/status/packages/json/?waybill=%s&token=%s" % (
                        awb_string, courier[2])
                    req = requests.get(check_status_url)
                    try:
                        req_ship_data += req.json()['ShipmentData']
                    except Exception as e:
                        logger.error("Status Tracking Failed for: " + awb_string + "\nError: " + str(e.args[0]))
                        if e.args[0] == 'ShipmentData':
                            sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                            sms_body_key = "Messages[%s][Body]" % str(exotel_idx)
                            sms_body_key_data = "Status Update Fail Alert"
                            customer_phone = "08750108744"
                            exotel_sms_data[sms_to_key] = customer_phone
                            exotel_sms_data[sms_body_key] = sms_body_key_data
                            exotel_idx += 1
                        continue
                logger.info("Count of delhivery packages: " + str(len(req_ship_data)))
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
                                        and "Picked Up" in each_scan['ScanDetail']['Instructions']:
                                    to_record_status = "Picked"
                                elif each_scan['ScanDetail']['Scan'] == "In Transit" \
                                        and "Pick Up Completed" in each_scan['ScanDetail']['Instructions']:
                                    to_record_status = "Picked RVP"
                                elif each_scan['ScanDetail']['Scan'] == "In Transit" \
                                        and each_scan['ScanDetail']['ScanType'] == "UD":
                                    to_record_status = "In Transit"
                                elif each_scan['ScanDetail']['Scan'] == "In Transit" \
                                        and each_scan['ScanDetail']['ScanType'] == "PU":
                                    to_record_status = "In Transit"
                                elif each_scan['ScanDetail']['Scan'] == "Dispatched" \
                                        and each_scan['ScanDetail']['Instructions'] == "Out for delivery":
                                    to_record_status = "Out for delivery"
                                elif each_scan['ScanDetail']['Scan'] == "Dispatched" \
                                        and each_scan['ScanDetail']['ScanType'] == "PU":
                                    to_record_status = "Dispatched for DTO"
                                elif each_scan['ScanDetail']['Scan'] == "Delivered":
                                    to_record_status = "Delivered"
                                elif each_scan['ScanDetail']['Scan'] == "Pending" \
                                        and each_scan['ScanDetail'][
                                    'Instructions'] == "Customer Refused to accept/Order Cancelled":
                                    to_record_status = "Cancelled"
                                elif each_scan['ScanDetail']['ScanType'] == "RT":
                                    to_record_status = "Returned"
                                elif each_scan['ScanDetail']['Scan'] == "RTO":
                                    to_record_status = "RTO"
                                elif each_scan['ScanDetail']['Scan'] == "DTO":
                                    to_record_status = "DTO"
                                elif each_scan['ScanDetail']['Scan'] == "Canceled":
                                    to_record_status = "Canceled"

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
                                elif to_record_status == 'In Transit' and new_status_dict[to_record_status][
                                    8] < status_time:
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

                                elif status_key == 'In Transit' and status_value[8] > all_scans_dict[status_key][5]:
                                    cur.execute("UPDATE order_status SET location=%s, location_city=%s, status_time=%s"
                                                " WHERE id=%s;", (status_value[6], status_value[7], status_value[8],
                                                                  all_scans_dict[status_key][0]))

                        except Exception as e:
                            logger.error(
                                "Open status failed for id: " + str(orders_dict[current_awb][0]) + "\nErr: " + str(
                                    e.args[0]))

                        if new_status == "Manifested":
                            continue

                        new_status = new_status.upper()
                        status_type = ret_order['Shipment']['Status']['StatusType']
                        if new_status == 'NOT PICKED':
                            new_status = "PICKUP REQUESTED"
                        status_detail = None
                        status_code = None
                        if new_status == "PENDING":
                            status_code = ret_order['Shipment']['Scans'][-1]['ScanDetail']['StatusCode']

                        edd = ret_order['Shipment']['expectedDate']
                        if edd:
                            edd = datetime.strptime(edd, '%Y-%m-%dT%H:%M:%S')
                            cur.execute("UPDATE shipments SET edd=%s WHERE awb=%s", (edd, current_awb))

                        if new_status == 'DELIVERED':
                            if orders_dict[current_awb][30] != False:
                                if orders_dict[current_awb][14] == 6:  # Magento complete
                                    try:
                                        magento_complete_order(orders_dict[current_awb])
                                    except Exception as e:
                                        logger.error(
                                            "Couldn't complete Magento for: " + str(orders_dict[current_awb][0])
                                            + "\nError: " + str(e.args))

                            if orders_dict[current_awb][28] != False and str(
                                    orders_dict[current_awb][13]).lower() == 'cod' and orders_dict[current_awb][
                                14] == 1:  # mark paid on shopify
                                try:
                                    shopify_markpaid(orders_dict[current_awb])
                                except Exception as e:
                                    logger.error(
                                        "Couldn't mark paid Shopify for: " + str(orders_dict[current_awb][0])
                                        + "\nError: " + str(e.args))

                            """
                            if orders_dict[current_awb][13] and str(orders_dict[current_awb][13]).lower() == 'prepaid':
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
                        if new_status == 'RTO':
                            if orders_dict[current_awb][32] != False:
                                if orders_dict[current_awb][14] == 6:  # Magento return
                                    try:
                                        magento_return_order(orders_dict[current_awb])
                                    except Exception as e:
                                        logger.error("Couldn't return Magento for: " + str(orders_dict[current_awb][0])
                                                     + "\nError: " + str(e.args))
                                elif orders_dict[current_awb][14] == 5:  # Woocommerce Cancelled
                                    try:
                                        woocommerce_returned(orders_dict[current_awb])
                                    except Exception as e:
                                        logger.error(
                                            "Couldn't cancel on woocommerce for: " + str(orders_dict[current_awb][0])
                                            + "\nError: " + str(e.args))

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
                            # cur.execute(update_prod_quantity_query_pickup%str(orders_dict[current_awb][0]))
                            if orders_dict[current_awb][26] != False:
                                if orders_dict[current_awb][14] == 5:
                                    try:
                                        woocommerce_fulfillment(orders_dict[current_awb])
                                    except Exception as e:
                                        logger.error(
                                            "Couldn't update woocommerce for: " + str(orders_dict[current_awb][0])
                                            + "\nError: " + str(e.args))
                                elif orders_dict[current_awb][14] == 1:
                                    try:
                                        shopify_fulfillment(orders_dict[current_awb], cur)
                                    except Exception as e:
                                        logger.error("Couldn't update shopify for: " + str(orders_dict[current_awb][0])
                                                     + "\nError: " + str(e.args))
                                elif orders_dict[current_awb][14] == 6:  # Magento fulfilment
                                    try:
                                        if orders_dict[current_awb][28] != False:
                                            magento_invoice(orders_dict[current_awb])
                                        magento_fulfillment(orders_dict[current_awb], cur)
                                    except Exception as e:
                                        logger.error("Couldn't update Magento for: " + str(orders_dict[current_awb][0])
                                                     + "\nError: " + str(e.args))

                            if orders_dict[current_awb][19]:
                                email = create_email(orders_dict[current_awb], edd.strftime('%-d %b') if edd else "",
                                                     orders_dict[current_awb][19])
                                if email:
                                    emails_list.append((email, [orders_dict[current_awb][19]]))
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
                                cur_2.execute("select client_name from clients where client_prefix='%s'" %
                                              orders_dict[current_awb][3])
                                client_name = cur_2.fetchone()
                                customer_phone = orders_dict[current_awb][4].replace(" ", "")
                                customer_phone = "0" + customer_phone[-10:]

                                sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                                sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                                exotel_sms_data[sms_to_key] = customer_phone
                                try:
                                    tracking_link_wareiq = "http://webapp.wareiq.com/tracking/" + str(
                                        orders_dict[current_awb][1])
                                    """
                                    short_url = requests.get(
                                        "https://cutt.ly/api/api.php?key=f445d0bb52699d2f870e1832a1f77ef3f9078&short=%s" % tracking_link_wareiq)
                                    short_url_track = short_url.json()['url']['shortLink']
                                    """
                                    exotel_sms_data[
                                        sms_body_key] = "Dear Customer, your %s order has been shipped via Delhivery with AWB number %s. " \
                                                        "It is expected to arrive by %s. You can track your order on this (%s) link." % (
                                                            client_name[0], str(orders_dict[current_awb][1]), edd,
                                                            tracking_link_wareiq)
                                except Exception:
                                    exotel_sms_data[
                                        sms_body_key] = "Dear Customer, your %s order has been shipped via Delhivery with AWB number %s. It is expected to arrive by %s. Thank you for Ordering." % (
                                        client_name[0], orders_dict[current_awb][1], edd)
                                exotel_idx += 1

                        if orders_dict[current_awb][2] != new_status:
                            status_update_tuple = (new_status, status_type, status_detail, orders_dict[current_awb][0])
                            cur.execute(order_status_update_query, status_update_tuple)

                            if new_status == 'PENDING' and status_code in delhivery_status_code_mapping_dict:
                                try:  # NDR check text
                                    ndr_reason = delhivery_status_code_mapping_dict[status_code]
                                    sms_to_key, sms_body_key, customer_phone, sms_body_key_data = verification_text(
                                        orders_dict[current_awb], exotel_idx, cur, cur_2, ndr=True,
                                        ndr_reason=ndr_reason)
                                    if sms_body_key_data:
                                        exotel_sms_data[sms_to_key] = customer_phone
                                        exotel_sms_data[sms_body_key] = sms_body_key_data
                                        exotel_idx += 1
                                except Exception as e:
                                    logger.error(
                                        "NDR confirmation not sent. Order id: " + str(orders_dict[current_awb][0]))

                    except Exception as e:
                        logger.error("status update failed for " + str(orders_dict[current_awb][0]) + "    err:" + str(
                            e.args[0]))

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
                            logger.info("picked for pickup_id " + str(key) + ": " + str(value))
                            date_today = datetime.now().strftime('%Y-%m-%d')
                            pickup_count_tuple = (value, courier[0], key, date_today)
                            cur.execute(update_pickup_count_query, pickup_count_tuple)
                    except Exception as e:
                        logger.error("Couldn't update pickup count for : " + str(e.args[0]))

                """
                if emails_list:
                    send_bulk_emails(emails_list)
                """

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
                emails_list = list()
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
                                                                         shadowfax_status_mapping[
                                                                             each_scan['status_id']][1],
                                                                         to_record_status,
                                                                         each_scan['remarks'],
                                                                         each_scan['location'],
                                                                         each_scan['location'],
                                                                         status_time)
                                elif to_record_status == 'In Transit' and new_status_dict[to_record_status][
                                    8] < status_time:
                                    new_status_dict[to_record_status] = (orders_dict[current_awb][0], courier[0],
                                                                         orders_dict[current_awb][10],
                                                                         shadowfax_status_mapping[
                                                                             each_scan['status_id']][1],
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

                                elif status_key == 'In Transit' and status_value[8] > all_scans_dict[status_key][5]:
                                    cur.execute("UPDATE order_status SET location=%s, location_city=%s, status_time=%s"
                                                " WHERE id=%s;", (status_value[6], status_value[7], status_value[8],
                                                                  all_scans_dict[status_key][0]))

                        except Exception as e:
                            logger.error(
                                "Open status failed for id: " + str(orders_dict[current_awb][0]) + "\nErr: " + str(
                                    e.args[0]))

                        try:
                            status_type = shadowfax_status_mapping[new_status][1]
                            new_status_temp = shadowfax_status_mapping[new_status][0]
                            status_detail = None
                        except KeyError:
                            if new_status == 'seller_initiated_delay':
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

                        if new_status == 'DELIVERED':
                            if orders_dict[current_awb][30] != False:
                                if orders_dict[current_awb][14] == 6:  # Magento complete
                                    try:
                                        magento_complete_order(orders_dict[current_awb])
                                    except Exception as e:
                                        logger.error(
                                            "Couldn't complete Magento for: " + str(orders_dict[current_awb][0])
                                            + "\nError: " + str(e.args))

                            if orders_dict[current_awb][28] != False and str(
                                    orders_dict[current_awb][13]).lower() == 'cod' and orders_dict[current_awb][
                                14] == 1:  # mark paid on shopify
                                try:
                                    shopify_markpaid(orders_dict[current_awb])
                                except Exception as e:
                                    logger.error(
                                        "Couldn't mark paid Shopify for: " + str(orders_dict[current_awb][0])
                                        + "\nError: " + str(e.args))
                            """
                            if orders_dict[current_awb][13] and str(orders_dict[current_awb][13]).lower() == 'prepaid':
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
                        if new_status == 'RTO':
                            if orders_dict[current_awb][32] != False:
                                if orders_dict[current_awb][14] == 6:  # Magento return
                                    try:
                                        magento_return_order(orders_dict[current_awb])
                                    except Exception as e:
                                        logger.error("Couldn't return Magento for: " + str(orders_dict[current_awb][0])
                                                     + "\nError: " + str(e.args))
                                elif orders_dict[current_awb][14] == 5:  # Woocommerce Cancelled
                                    try:
                                        woocommerce_returned(orders_dict[current_awb])
                                    except Exception as e:
                                        logger.error(
                                            "Couldn't cancel on woocommerce for: " + str(orders_dict[current_awb][0])
                                            + "\nError: " + str(e.args))
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
                            # cur.execute(update_prod_quantity_query_pickup%str(orders_dict[current_awb][0]))
                            if orders_dict[current_awb][26] != False:
                                if orders_dict[current_awb][14] == 5:
                                    try:
                                        woocommerce_fulfillment(orders_dict[current_awb])
                                    except Exception as e:
                                        logger.error(
                                            "Couldn't update woocommerce for: " + str(orders_dict[current_awb][0])
                                            + "\nError: " + str(e.args))
                                elif orders_dict[current_awb][14] == 1:
                                    try:
                                        shopify_fulfillment(orders_dict[current_awb], cur)
                                    except Exception as e:
                                        logger.error("Couldn't update shopify for: " + str(orders_dict[current_awb][0])
                                                     + "\nError: " + str(e.args))
                                elif orders_dict[current_awb][14] == 6:  # Magento fulfilment
                                    try:
                                        if orders_dict[current_awb][28] != False:
                                            magento_invoice(orders_dict[current_awb])
                                        magento_fulfillment(orders_dict[current_awb], cur)
                                    except Exception as e:
                                        logger.error("Couldn't update Magento for: " + str(orders_dict[current_awb][0])
                                                     + "\nError: " + str(e.args))
                            if orders_dict[current_awb][19]:
                                email = create_email(orders_dict[current_awb], edd.strftime('%-d %b') if edd else "",
                                                     orders_dict[current_awb][19])
                                if email:
                                    emails_list.append((email, [orders_dict[current_awb][19]]))
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
                                    "select client_name from clients where client_prefix='%s'" %
                                    orders_dict[current_awb][
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
                            if new_status == "PENDING" and ret_order['status'] in shadowfax_status_mapping \
                                    and shadowfax_status_mapping[new_status][2]:
                                try:  # NDR check text
                                    sms_to_key, sms_body_key, customer_phone, sms_body_key_data = verification_text(
                                        orders_dict[current_awb], exotel_idx, cur, cur_2, ndr=True,
                                        ndr_reason=shadowfax_status_mapping[new_status][2])
                                    if sms_body_key_data:
                                        exotel_sms_data[sms_to_key] = customer_phone
                                        exotel_sms_data[sms_body_key] = sms_body_key_data
                                        exotel_idx += 1
                                except Exception as e:
                                    logger.error(
                                        "NDR confirmation not sent. Order id: " + str(orders_dict[current_awb][0]))

                    except Exception as e:
                        logger.error("status update failed for " + str(orders_dict[current_awb][0]) + "    err:" + str(
                            e.args[0]))

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

                """
                if emails_list:
                    send_bulk_emails(emails_list)
                """

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
                emails_list = list()
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
                        order_picked_check = False

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
                                status_time = each_scan['StatusDate'] + "T" + each_scan['StatusTime']
                                if status_time:
                                    status_time = datetime.strptime(status_time, '%d-%m-%YT%H%M')

                                to_record_status = ""
                                if each_scan['StatusCode'] == "DRC":
                                    to_record_status = "Received"
                                elif each_scan['StatusCode'] == "PUD":
                                    to_record_status = "Picked"
                                    order_picked_check = True
                                elif each_scan['StatusCode'] in ("IT", "RAD"):
                                    to_record_status = "In Transit"
                                    order_picked_check = True
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
                            edd = datetime.strptime(ret_order['ShipmentSummary'][0]['ExpectedDeliveryDate'],
                                                    '%m/%d/%Y %I:%M:%S %p')
                            cur.execute("UPDATE shipments SET edd=%s WHERE awb=%s", (edd, current_awb))

                        if new_status == 'DELIVERED':
                            if orders_dict[current_awb][30] != False:
                                if orders_dict[current_awb][14] == 6:  # Magento complete
                                    try:
                                        magento_complete_order(orders_dict[current_awb])
                                    except Exception as e:
                                        logger.error(
                                            "Couldn't complete Magento for: " + str(orders_dict[current_awb][0])
                                            + "\nError: " + str(e.args))

                            if orders_dict[current_awb][28] != False and str(
                                    orders_dict[current_awb][13]).lower() == 'cod' and orders_dict[current_awb][
                                14] == 1:  # mark paid on shopify
                                try:
                                    shopify_markpaid(orders_dict[current_awb])
                                except Exception as e:
                                    logger.error(
                                        "Couldn't mark paid Shopify for: " + str(orders_dict[current_awb][0])
                                        + "\nError: " + str(e.args))
                            """
                            if orders_dict[current_awb][13] and str(orders_dict[current_awb][13]).lower() == 'prepaid':
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
                        if new_status == 'RTO':
                            if orders_dict[current_awb][32] != False:
                                if orders_dict[current_awb][14] == 6:  # Magento return
                                    try:
                                        magento_return_order(orders_dict[current_awb])
                                    except Exception as e:
                                        logger.error("Couldn't return Magento for: " + str(orders_dict[current_awb][0])
                                                     + "\nError: " + str(e.args))
                                elif orders_dict[current_awb][14] == 5:  # Woocommerce Cancelled
                                    try:
                                        woocommerce_returned(orders_dict[current_awb])
                                    except Exception as e:
                                        logger.error(
                                            "Couldn't cancel on woocommerce for: " + str(orders_dict[current_awb][0])
                                            + "\nError: " + str(e.args))

                        if orders_dict[current_awb][2] in (
                                'READY TO SHIP', 'PICKUP REQUESTED', 'NOT PICKED') and new_status == 'IN TRANSIT':

                            cur_2.execute(
                                "select client_name from clients where client_prefix='%s'" %
                                orders_dict[current_awb][
                                    3])
                            client_name = cur_2.fetchone()
                            customer_phone = orders_dict[current_awb][4].replace(" ", "")
                            customer_phone = "0" + customer_phone[-10:]

                            sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                            sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                            exotel_sms_data[sms_to_key] = customer_phone
                            tracking_link_wareiq = "http://webapp.wareiq.com/tracking/" + str(
                                orders_dict[current_awb][1])
                            if edd:
                                edd = edd.strftime('%-d %b')
                                """
                                short_url = requests.get(
                                    "https://cutt.ly/api/api.php?key=f445d0bb52699d2f870e1832a1f77ef3f9078&short=%s" % tracking_link_wareiq)
                                short_url_track = short_url.json()['url']['shortLink']
                                """
                                exotel_sms_data[
                                    sms_body_key] = "Dear Customer, your %s order has been shipped via Xpressbees with AWB number %s. " \
                                                    "It is expected to arrive by %s. You can track your order on this (%s) link." % (
                                                        client_name[0], str(orders_dict[current_awb][1]), edd,
                                                        tracking_link_wareiq)
                            else:
                                exotel_sms_data[
                                    sms_body_key] = "Dear Customer, your %s order has been shipped via Xpressbees with AWB number %s. You can track your order on this (%s) link." % (
                                    client_name[0], str(orders_dict[current_awb][1]),
                                    tracking_link_wareiq)
                            exotel_idx += 1
                            if order_picked_check:
                                pickup_count += 1
                                if orders_dict[current_awb][11] not in pickup_dict:
                                    pickup_dict[orders_dict[current_awb][11]] = 1
                                else:
                                    pickup_dict[orders_dict[current_awb][11]] += 1
                                # cur.execute(update_prod_quantity_query_pickup % str(orders_dict[current_awb][0]))
                                if orders_dict[current_awb][26] != False:
                                    if orders_dict[current_awb][14] == 5:
                                        try:
                                            woocommerce_fulfillment(orders_dict[current_awb])
                                        except Exception as e:
                                            logger.error(
                                                "Couldn't update woocommerce for: " + str(orders_dict[current_awb][0])
                                                + "\nError: " + str(e.args))
                                    elif orders_dict[current_awb][14] == 1:
                                        try:
                                            shopify_fulfillment(orders_dict[current_awb], cur)
                                        except Exception as e:
                                            logger.error(
                                                "Couldn't update shopify for: " + str(orders_dict[current_awb][0])
                                                + "\nError: " + str(e.args))
                                    elif orders_dict[current_awb][14] == 6:  # Magento fulfilment
                                        try:
                                            if orders_dict[current_awb][28] != False:
                                                magento_invoice(orders_dict[current_awb])
                                            magento_fulfillment(orders_dict[current_awb], cur)
                                        except Exception as e:
                                            logger.error(
                                                "Couldn't update Magento for: " + str(orders_dict[current_awb][0])
                                                + "\nError: " + str(e.args))

                                if orders_dict[current_awb][19]:
                                    """
                                    email = create_email(orders_dict[current_awb],
                                                         edd.strftime('%-d %b') if edd else "",
                                                         orders_dict[current_awb][19])
                                    if email:
                                        emails_list.append((email, [orders_dict[current_awb][19]]))
                                    """
                            else:
                                continue

                        if orders_dict[current_awb][2] != new_status:
                            status_update_tuple = (new_status, status_type, status_detail, orders_dict[current_awb][0])
                            cur.execute(order_status_update_query, status_update_tuple)

                            if ret_order['ShipmentSummary'][0]['StatusCode'] == 'UD' \
                                    and ret_order['ShipmentSummary'][0]['Status'] in \
                                    ("Customer Refused To Accept", "Customer Refused to Pay COD Amount",
                                     "Add Incomplete/Incorrect & Mobile Not Reachable",
                                     "Customer Not Available & Mobile Not Reachable"):
                                try:  # NDR check text
                                    ndr_reason = None
                                    if ret_order['ShipmentSummary'][0]['Status'] in Xpressbees_ndr_reasons:
                                        ndr_reason = Xpressbees_ndr_reasons[ret_order['ShipmentSummary'][0]['Status']]
                                    sms_to_key, sms_body_key, customer_phone, sms_body_key_data = verification_text(
                                        orders_dict[current_awb], exotel_idx, cur, cur_2, ndr=True,
                                        ndr_reason=ndr_reason)
                                    if sms_body_key_data:
                                        exotel_sms_data[sms_to_key] = customer_phone
                                        exotel_sms_data[sms_body_key] = sms_body_key_data
                                        exotel_idx += 1
                                except Exception as e:
                                    logger.error(
                                        "NDR confirmation not sent. Order id: " + str(orders_dict[current_awb][0]))

                    except Exception as e:
                        logger.error("status update failed for " + str(orders_dict[current_awb][0]) + "    err:" + str(
                            e.args[0]))

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
                """
                if emails_list:
                    send_bulk_emails(emails_list)
                """
                conn.commit()

        except Exception as e:
            logger.error("Status update failed: " + str(e.args[0]))

    cur.close()


def verification_text(current_order, exotel_idx, cur, cur_2, ndr=None, ndr_reason=None):
    if not ndr:
        del_confirmation_link = "http://track.wareiq.com/core/v1/passthru/delivery?CustomField=%s" % str(
            current_order[0])
    else:
        del_confirmation_link = "http://track.wareiq.com/core/v1/passthru/ndr?CustomField=%s" % str(
            current_order[0])
    """
    short_url = requests.get(
        "https://cutt.ly/api/api.php?key=f445d0bb52699d2f870e1832a1f77ef3f9078&short=%s" % del_confirmation_link)
    short_url_track = short_url.json()['url']['shortLink']
    """
    insert_cod_ver_tuple = (current_order[0], del_confirmation_link, datetime.now())
    if not ndr:
        cur.execute(
            "INSERT INTO delivery_check (order_id, verification_link, date_created) VALUES (%s,%s,%s);",
            insert_cod_ver_tuple)
    else:
        cur.execute("SELECT * from ndr_shipments WHERE shipment_id=%s" % str(current_order[10]))
        if not cur.fetchone():
            ndr_ship_tuple = (
                current_order[0], current_order[10], ndr_reason, "required", datetime.utcnow() + timedelta(hours=5.5))
            cur.execute(
                "INSERT INTO ndr_shipments (order_id, shipment_id, reason_id, current_status, date_created) VALUES (%s,%s,%s,%s,%s);",
                ndr_ship_tuple)
            if ndr_reason in (1, 3, 9, 11):
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
                                del_confirmation_link)
    elif ndr_reason in (1, 3, 9, 11):
        sms_body_key_data = "Dear Customer, Delivery for your order from %s with order id %s was attempted today." \
                            " If you didn't cancel, please click on the link (%s). We'll call you shortly." % (
                                client_name[0], str(current_order[12]),
                                del_confirmation_link)
    else:
        sms_body_key_data = None

    return sms_to_key, sms_body_key, customer_phone, sms_body_key_data


delhivery_status_code_mapping_dict = {
    "DLYDC-107": 6,
    "DLYDC-110": 4,
    "DLYDC-132": 8,
    "EOD-104": 7,
    "EOD-11": 1,
    "EOD-111": 11,
    "EOD-3": 4,
    "EOD-40": 9,
    "EOD-6": 3,
    "EOD-69": 11,
    "EOD-74": 2,
    "EOD-86": 12,
    "FMEOD-106": 12,
    "FMEOD-118": 3,
    "RDPD-17": 12,
    "RT-101": 12,
    "ST-108": 13,
}

shadowfax_status_mapping = {"new": ("READY TO SHIP", "UD", None),
                            "sent_to_rev": ("READY TO SHIP", "UD", None),
                            "assigned_for_pickup": ("READY TO SHIP", "UD", None),
                            "ofp": ("READY TO SHIP", "UD", None),
                            "picked": ("IN TRANSIT", "UD", None),
                            "recd_at_rev_hub": ("IN TRANSIT", "UD", None),
                            "sent_to_fwd": ("IN TRANSIT", "UD", None),
                            "recd_at_fwd_hub": ("IN TRANSIT", "UD", None),
                            "recd_at_fwd_dc": ("IN TRANSIT", "UD", None),
                            "assigned_for_delivery": ("IN TRANSIT", "UD", None),
                            "ofd": ("DISPATCHED", "UD", None),
                            "cid": ("PENDING", "UD", 4),
                            "nc": ("PENDING", "UD", 1),
                            "na": ("PENDING", "UD", 12),
                            "reopen_ndr": ("PENDING", "UD", 4),
                            "delivered": ("DELIVERED", "DL", None),
                            "cancelled_by_customer": ("PENDING", "UD", 3),
                            "rts": ("PENDING", "RT", None),
                            "rts_d": ("RTO", "DL", None),
                            "lost": ("LOST", "UD", None),
                            "on_hold": ("ON HOLD", "UD", None),
                            "pickup_on_hold": ("PICKUP REQUESTED", "UD", None),
                            }

xpressbees_status_mapping = {"DRC": ("READY TO SHIP", "UD", ""),
                             "PUC": ("PICKUP REQUESTED", "UD", ""),
                             "OFP": ("PICKUP REQUESTED", "UD", ""),
                             "PUD": ("IN TRANSIT", "UD", ""),
                             "PND": ("PICKUP REQUESTED", "UD", ""),
                             "PKD": ("IN TRANSIT", "UD", ""),
                             "IT": ("IN TRANSIT", "UD", ""),
                             "RAD": ("IN TRANSIT", "UD", ""),
                             "OFD": ("DISPATCHED", "UD", ""),
                             "RTON": ("IN TRANSIT", "RT", ""),
                             "RTO": ("IN TRANSIT", "RT", ""),
                             "RTO-IT": ("IN TRANSIT", "RT", ""),
                             "RAO": ("IN TRANSIT", "RT", ""),
                             "RTU": ("IN TRANSIT", "RT", ""),
                             "RTO-OFD": ("DISPATCHED", "RT", ""),
                             "STD": ("DAMAGED", "UD", ""),
                             "STG": ("SHORTAGE", "UD", ""),
                             "RTO-STG": ("SHORTAGE", "RT", ""),
                             "DLVD": ("DELIVERED", "DL", ""),
                             "RTD": ("RTO", "DL", ""),
                             "LOST": ("LOST", "UD", ""),
                             "UD": ("PENDING", "UD", "")
                             }

Xpressbees_ndr_reasons = {"Customer Refused To Accept": 3,
                          "Customer Refused to Pay COD Amount": 9,
                          "Add Incomplete/Incorrect & Mobile Not Reachable": 1,
                          "Customer Not Available & Mobile Not Reachable": 1}


def woocommerce_fulfillment(order):
    wcapi = API(
        url=order[9],
        consumer_key=order[7],
        consumer_secret=order[8],
        version="wc/v3"
    )
    status_mark = order[27]
    if not status_mark:
        status_mark = "completed"
    r = wcapi.post('orders/%s' % str(order[5]), data={"status": status_mark})


def woocommerce_returned(order):
    wcapi = API(
        url=order[9],
        consumer_key=order[7],
        consumer_secret=order[8],
        version="wc/v3"
    )
    status_mark = order[33]
    if not status_mark:
        status_mark = "cancelled"
    r = wcapi.post('orders/%s' % str(order[5]), data={"status": status_mark})


def shopify_fulfillment(order, cur):
    if not order[25]:
        get_locations_url = "https://%s:%s@%s/admin/api/2019-10/locations.json" % (order[7], order[8], order[9])
        req = requests.get(get_locations_url).json()
        location_id = str(req['locations'][0]['id'])
        cur.execute("UPDATE client_channel set unique_parameter=%s where id=%s" % (location_id, order[34]))
    else:
        location_id = str(order[25])

    create_fulfillment_url = "https://%s:%s@%s/admin/api/2019-10/orders/%s/fulfillments.json" % (
        order[7], order[8],
        order[9], order[5])
    tracking_link = "http://webapp.wareiq.com/tracking/%s" % str(order[1])
    ful_header = {'Content-Type': 'application/json'}
    fulfil_data = {
        "fulfillment": {
            "tracking_number": str(order[1]),
            "tracking_urls": [
                tracking_link
            ],
            "tracking_company": "WareIQ",
            "location_id": int(location_id),
            "notify_customer": True
        }
    }
    req_ful = requests.post(create_fulfillment_url, data=json.dumps(fulfil_data),
                            headers=ful_header)
    fulfillment_id = str(req_ful.json()['fulfillment']['id'])
    if fulfillment_id and tracking_link:
        cur.execute("UPDATE shipments SET channel_fulfillment_id=%s, tracking_link=%s WHERE id=%s",
                    (fulfillment_id, tracking_link, order[10]))
    return fulfillment_id, tracking_link


def shopify_markpaid(order):
    get_transactions_url = "https://%s:%s@%s/admin/api/2019-10/orders/%s/transactions.json" % (
        order[7], order[8],
        order[9], order[5])

    tra_header = {'Content-Type': 'application/json'}
    transaction_data = {
        "transaction": {
            "kind": "sale",
            "source": "external",
            "amount": str(order[35]),
            "currency": "INR"
        }
    }
    req_ful = requests.post(get_transactions_url, data=json.dumps(transaction_data),
                            headers=tra_header)


def magento_fulfillment(order, cur):
    create_fulfillment_url = "%s/V1/order/%s/ship" % (order[9], order[5])
    tracking_link = "http://webapp.wareiq.com/tracking/%s" % str(order[1])
    ful_header = {'Content-Type': 'application/json',
                  'Authorization': 'Bearer ' + order[7]}

    items_list = list()
    for idx, sku in enumerate(order[16]):
        if sku:
            items_list.append({
                "extension_attributes": {},
                "order_item_id": int(sku),
                "qty": int(order[17][idx])
            })
    fulfil_data = {
        "items": items_list,
        "notify": False,
        "tracks": [
            {
                "extension_attributes": {"warehouse_name": "HydShip3"},
                "track_number": str(order[1]),
                "title": "WareIQ",
                "carrier_code": "WareIQ"
            }
        ]
    }
    req_ful = requests.post(create_fulfillment_url, data=json.dumps(fulfil_data),
                            headers=ful_header)

    if type(req_ful.json()) == str:
        cur.execute("UPDATE shipments SET channel_fulfillment_id=%s, tracking_link=%s WHERE id=%s",
                    (req_ful.json(), tracking_link, order[10]))

    shipped_comment_url = "%s/V1/orders/%s/comments" % (order[9], order[5])

    status_mark = order[27]
    if not status_mark:
        status_mark = "shipped"
    time_now = datetime.utcnow() + timedelta(hours=5.5)
    time_now = time_now.strftime('%Y-%m-%d %H:%M:%S')
    complete_data = {
        "statusHistory": {
            "comment": "Shipment Created",
            "created_at": time_now,
            "parent_id": int(order[5]),
            "is_customer_notified": 0,
            "is_visible_on_front": 0,
            "status": status_mark
        }
    }
    req_ful = requests.post(shipped_comment_url, data=json.dumps(complete_data),
                            headers=ful_header)
    return req_ful.json(), tracking_link


def magento_invoice(order):
    create_invoice_url = "%s/V1/order/%s/invoice" % (order[9], order[5])
    ful_header = {'Content-Type': 'application/json',
                  'Authorization': 'Bearer ' + order[7]}

    items_list = list()
    for idx, sku in enumerate(order[16]):
        if sku:
            items_list.append({
                "extension_attributes": {},
                "order_item_id": int(sku),
                "qty": int(order[17][idx])
            })

    invoice_data = {
        "capture": False,
        "notify": False
    }
    req_ful = requests.post(create_invoice_url, data=json.dumps(invoice_data),
                            headers=ful_header)

    invoice_comment_url = "%s/V1/orders/%s/comments" % (order[9], order[5])

    status_mark = order[29]
    if not status_mark:
        status_mark = "invoiced"
    time_now = datetime.utcnow() + timedelta(hours=5.5)
    time_now = time_now.strftime('%Y-%m-%d %H:%M:%S')
    complete_data = {
        "statusHistory": {
            "comment": "Invoice Created",
            "created_at": time_now,
            "parent_id": int(order[5]),
            "is_customer_notified": 0,
            "is_visible_on_front": 0,
            "status": status_mark
        }
    }
    req_ful = requests.post(invoice_comment_url, data=json.dumps(complete_data),
                            headers=ful_header)


def magento_complete_order(order):
    complete_order_url = "%s/V1/orders/%s/comments" % (order[9], order[5])
    ful_header = {'Content-Type': 'application/json',
                  'Authorization': 'Bearer ' + order[7]}

    status_mark = order[31]
    if not status_mark:
        status_mark = "delivered"
    time_now = datetime.utcnow() + timedelta(hours=5.5)
    time_now = time_now.strftime('%Y-%m-%d %H:%M:%S')
    complete_data = {
        "statusHistory": {
            "comment": "Order Delivered",
            "created_at": time_now,
            "parent_id": int(order[5]),
            "is_customer_notified": 0,
            "is_visible_on_front": 0,
            "status": "delivered"
        }
    }
    req_ful = requests.post(complete_order_url, data=json.dumps(complete_data),
                            headers=ful_header)


def magento_return_order(order):
    complete_order_url = "%s/V1/orders/%s/comments" % (order[9], order[5])
    ful_header = {'Content-Type': 'application/json',
                  'Authorization': 'Bearer ' + order[7]}

    status_mark = order[33]
    if not status_mark:
        status_mark = "returned"
    time_now = datetime.utcnow() + timedelta(hours=5.5)
    time_now = time_now.strftime('%Y-%m-%d %H:%M:%S')
    complete_data = {
        "statusHistory": {
            "comment": "Order Returned",
            "created_at": time_now,
            "parent_id": int(order[5]),
            "is_customer_notified": 0,
            "is_visible_on_front": 0,
            "status": status_mark
        }
    }
    req_ful = requests.post(complete_order_url, data=json.dumps(complete_data),
                            headers=ful_header)