import psycopg2, requests, os, json, hmac, hashlib, base64
import logging, xmltodict
from datetime import datetime, timedelta
from .queries import *
from .update_status_utils import *
from woocommerce import API
from app.db_utils import DbConnection

logger = logging.getLogger()
logger.setLevel(logging.INFO)
"""
host = os.environ('DTATBASE_HOST')
database = os.environ('DTATBASE_NAME')
user = os.environ('DTATBASE_USER')
password = os.environ('DTATBASE_PASSWORD')
conn = psycopg2.connect(host=host, database=database, user=user, password=password)
"""

conn = DbConnection.get_db_connection_instance()


def update_status():
    cur = conn.cursor()
    cur.execute(get_courier_id_and_key_query)
    for courier in cur.fetchall():
        try:
            if courier[1].startswith('Delhivery'):
                track_delhivery_orders(courier, cur)

            elif courier[1] == "Shadowfax":
                track_shadowfax_orders(courier, cur)

            elif courier[1].startswith('Xpressbees'):
                track_xpressbees_orders(courier, cur)

            elif courier[1].startswith('Bluedart'):
                track_bluedart_orders(courier, cur)

            elif courier[1].startswith('Ecom'):
                track_ecomxp_orders(courier, cur)

        except Exception as e:
            logger.error("Status update failed: " + str(e.args[0]))

    cur.close()


def track_delhivery_orders(courier, cur):
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
                            and "picked" in str(each_scan['ScanDetail']['Instructions']).lower():
                        to_record_status = "Picked"
                    elif each_scan['ScanDetail']['Scan'] == "In Transit" \
                            and each_scan['ScanDetail']['StatusCode']=='EOD-77':
                        to_record_status = "Picked RVP"
                    elif each_scan['ScanDetail']['Scan'] == "In Transit" \
                            and each_scan['ScanDetail']['ScanType'] == "UD":
                        to_record_status = "In Transit"
                    elif each_scan['ScanDetail']['Scan'] == "In Transit" \
                            and each_scan['ScanDetail']['ScanType'] == "PU":
                        to_record_status = "In Transit"
                    elif each_scan['ScanDetail']['Scan'] == "Dispatched" \
                            and each_scan['ScanDetail']['ScanType'] == "PU":
                        to_record_status = "Dispatched for DTO"
                    elif each_scan['ScanDetail']['Scan'] == "Dispatched" \
                            and each_scan['ScanDetail']['Instructions'] == "Out for delivery":
                        to_record_status = "Out for delivery"
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

            if orders_dict[current_awb][2]=='CANCELED' and new_status!='IN TRANSIT':
                continue

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
                if datetime.utcnow().hour < 4:
                    cur.execute("UPDATE shipments SET edd=%s WHERE awb=%s", (edd, current_awb))
                    cur.execute("UPDATE shipments SET pdd=%s WHERE awb=%s and pdd is null", (edd, current_awb))

            client_name = orders_dict[current_awb][20]
            customer_phone = orders_dict[current_awb][4].replace(" ", "")
            customer_phone = "0" + customer_phone[-10:]

            if new_status == 'DELIVERED':

                update_delivered_on_channels(orders_dict[current_awb])

                sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                exotel_sms_data[sms_to_key] = customer_phone

                exotel_sms_data[sms_body_key] = "Delivered: Your %s order via Delhivery - https://webapp.wareiq.com/tracking/%s . Powered by WareIQ" % (
                    client_name, current_awb)

                exotel_idx += 1

            if new_status == 'DTO':
                sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                sms_body_key = "Messages[%s][Body]" % str(exotel_idx)
                exotel_sms_data[sms_to_key] = customer_phone
                exotel_sms_data[sms_body_key] = "Delivered: Your %s order via Delhivery to seller - https://webapp.wareiq.com/tracking/%s . Powered by WareIQ" % (client_name, current_awb)
                exotel_idx += 1

            if orders_dict[current_awb][2] in ('SCHEDULED', 'DISPATCHED') and new_status == 'IN TRANSIT' and orders_dict[current_awb][13].lower() == 'pickup':
                sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                sms_body_key = "Messages[%s][Body]" % str(exotel_idx)
                exotel_sms_data[sms_to_key] = customer_phone
                exotel_sms_data[sms_body_key] = "Picked: Your %s order via Delhivery - https://webapp.wareiq.com/tracking/%s . Powered by WareIQ" % (client_name, current_awb)
                exotel_idx += 1

            if new_status == 'RTO':
                update_rto_on_channels(orders_dict[current_awb])

            if orders_dict[current_awb][2] in ('READY TO SHIP', 'PICKUP REQUESTED', 'NOT PICKED') and new_status == 'IN TRANSIT':
                pickup_count += 1
                if orders_dict[current_awb][11] not in pickup_dict:
                    pickup_dict[orders_dict[current_awb][11]] = 1
                else:
                    pickup_dict[orders_dict[current_awb][11]] += 1
                time_now = datetime.utcnow() + timedelta(hours=5.5)
                cur.execute("UPDATE order_pickups SET picked=%s, pickup_time=%s WHERE order_id=%s",
                            (True, time_now, orders_dict[current_awb][0]))

                update_picked_on_channels(orders_dict[current_awb], cur)

                if orders_dict[current_awb][19]:
                    email = create_email(orders_dict[current_awb], edd.strftime('%-d %b') if edd else "",
                                         orders_dict[current_awb][19])
                    if email:
                        emails_list.append((email, [orders_dict[current_awb][19]]))

                cur.execute("UPDATE shipments SET pdd=%s WHERE awb=%s", (edd, current_awb))
                sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                exotel_sms_data[sms_to_key] = customer_phone

                tracking_link_wareiq = "https://webapp.wareiq.com/tracking/" + str(orders_dict[current_awb][1])

                exotel_sms_data[sms_body_key] = "Shipped: Your %s order via Delhivery . Track here: %s . Powered by WareIQ." % (
                client_name, tracking_link_wareiq)

                exotel_idx += 1

            if orders_dict[current_awb][2] != new_status:

                status_update_tuple = (new_status, status_type, status_detail, orders_dict[current_awb][0])
                cur.execute(order_status_update_query, status_update_tuple)

                if new_status == 'PENDING' and status_code in delhivery_status_code_mapping_dict:
                    try:  # NDR check text
                        ndr_reason = delhivery_status_code_mapping_dict[status_code]
                        sms_to_key, sms_body_key, customer_phone, sms_body_key_data = verification_text(
                            orders_dict[current_awb], exotel_idx, cur, ndr=True,
                            ndr_reason=ndr_reason)
                        if sms_body_key_data:
                            exotel_sms_data[sms_to_key] = customer_phone
                            exotel_sms_data[sms_body_key] = sms_body_key_data
                            exotel_idx += 1
                    except Exception as e:
                        logger.error(
                            "NDR confirmation not sent. Order id: " + str(orders_dict[current_awb][0]))

            conn.commit()

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

    conn.commit()

    if emails_list:
        send_bulk_emails(emails_list)


def track_shadowfax_orders(courier, cur):
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

            if orders_dict[current_awb][2]=='CANCELED' and new_status!='IN TRANSIT':
                continue

            edd = ret_order['promised_delivery_date']
            if edd:
                edd = datetime.strptime(edd, '%Y-%m-%dT%H:%M:%SZ')
                if datetime.utcnow().hour < 4:
                    cur.execute("UPDATE shipments SET edd=%s WHERE awb=%s", (edd, current_awb))
                    cur.execute("UPDATE shipments SET pdd=%s WHERE awb=%s and pdd is null", (edd, current_awb))

            client_name = orders_dict[current_awb][20]
            customer_phone = orders_dict[current_awb][4].replace(" ", "")
            customer_phone = "0" + customer_phone[-10:]

            if new_status == 'DELIVERED':

                update_delivered_on_channels(orders_dict[current_awb])

                sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                exotel_sms_data[sms_to_key] = customer_phone

                exotel_sms_data[sms_body_key] = "Delivered: Your %s order via Shadowfax - https://webapp.wareiq.com/tracking/%s . Powered by WareIQ" % (
                    client_name, current_awb)

                exotel_idx += 1

            if new_status == 'RTO':
                update_rto_on_channels(orders_dict[current_awb])

            if orders_dict[current_awb][2] in (
                    'READY TO SHIP', 'PICKUP REQUESTED', 'NOT PICKED') and new_status == 'IN TRANSIT':
                pickup_count += 1
                if orders_dict[current_awb][11] not in pickup_dict:
                    pickup_dict[orders_dict[current_awb][11]] = 1
                else:
                    pickup_dict[orders_dict[current_awb][11]] += 1
                time_now = datetime.utcnow() + timedelta(hours=5.5)
                cur.execute("UPDATE order_pickups SET picked=%s, pickup_time=%s WHERE order_id=%s",
                            (True, time_now, orders_dict[current_awb][0]))

                update_picked_on_channels(orders_dict[current_awb], cur)

                if orders_dict[current_awb][19]:
                    email = create_email(orders_dict[current_awb], edd.strftime('%-d %b') if edd else "",
                                         orders_dict[current_awb][19])
                    if email:
                        emails_list.append((email, [orders_dict[current_awb][19]]))

                if edd:
                    cur.execute("UPDATE shipments SET pdd=%s WHERE awb=%s", (edd, current_awb))
                    edd = edd.strftime('%-d %b')

                sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                exotel_sms_data[sms_to_key] = customer_phone
                exotel_sms_data[
                    sms_body_key] = "Shipped: Your %s order via Shadowfax . Track here: https://webapp.wareiq.com/tracking/%s . Powered by WareIQ." % (
                    client_name, orders_dict[current_awb][1])
                exotel_idx += 1

            if orders_dict[current_awb][2] != new_status:
                status_update_tuple = (new_status, status_type, status_detail, orders_dict[current_awb][0])
                cur.execute(order_status_update_query, status_update_tuple)
                if new_status == "PENDING" and ret_order['status'] in shadowfax_status_mapping \
                        and shadowfax_status_mapping[new_status][2]:
                    try:  # NDR check text
                        sms_to_key, sms_body_key, customer_phone, sms_body_key_data = verification_text(
                            orders_dict[current_awb], exotel_idx, cur, ndr=True,
                            ndr_reason=shadowfax_status_mapping[new_status][2])
                        if sms_body_key_data:
                            exotel_sms_data[sms_to_key] = customer_phone
                            exotel_sms_data[sms_body_key] = sms_body_key_data
                            exotel_idx += 1
                    except Exception as e:
                        logger.error(
                            "NDR confirmation not sent. Order id: " + str(orders_dict[current_awb][0]))

            conn.commit()

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

    conn.commit()

    if emails_list:
        send_bulk_emails(emails_list)


def track_xpressbees_orders(courier, cur):
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

            status_detail = None
            try:
                status_type = xpressbees_status_mapping[new_status][1]
                new_status_temp = xpressbees_status_mapping[new_status][0]
            except KeyError:
                new_status_temp = new_status_temp.upper()
                status_type = None
            if new_status_temp == "READY TO SHIP" and orders_dict[current_awb][2] == new_status:
                continue
            new_status = new_status_temp

            if orders_dict[current_awb][2]=='CANCELED' and new_status!='IN TRANSIT':
                continue

            edd = ret_order['ShipmentSummary'][0].get('ExpectedDeliveryDate')
            if edd:
                edd = datetime.strptime(ret_order['ShipmentSummary'][0]['ExpectedDeliveryDate'],
                                        '%m/%d/%Y %I:%M:%S %p')
                if datetime.utcnow().hour < 4:
                    cur.execute("UPDATE shipments SET edd=%s WHERE awb=%s", (edd, current_awb))
                    cur.execute("UPDATE shipments SET pdd=%s WHERE awb=%s and pdd is null", (edd, current_awb))

            client_name = orders_dict[current_awb][20]
            customer_phone = orders_dict[current_awb][4].replace(" ", "")
            customer_phone = "0" + customer_phone[-10:]

            if orders_dict[current_awb][2]=='PICKUP REQUESTED' and new_status=='READY TO SHIP':
                continue

            if new_status == 'DELIVERED':

                update_delivered_on_channels(orders_dict[current_awb])

                sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                exotel_sms_data[sms_to_key] = customer_phone

                exotel_sms_data[sms_body_key] = "Delivered: Your %s order via Xpressbees - https://webapp.wareiq.com/tracking/%s . Powered by WareIQ" % (
                    client_name, current_awb)

                exotel_idx += 1

            if new_status == 'RTO':
                update_rto_on_channels(orders_dict[current_awb])

            if orders_dict[current_awb][2] in (
                    'READY TO SHIP', 'PICKUP REQUESTED',
                    'NOT PICKED') and new_status == 'IN TRANSIT' and order_picked_check:

                sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                exotel_sms_data[sms_to_key] = customer_phone
                if edd:
                    cur.execute("UPDATE shipments SET pdd=%s WHERE awb=%s", (edd, current_awb))
                    edd = edd.strftime('%-d %b')
                    """
                    short_url = requests.get(
                        "https://cutt.ly/api/api.php?key=f445d0bb52699d2f870e1832a1f77ef3f9078&short=%s" % tracking_link_wareiq)
                    short_url_track = short_url.json()['url']['shortLink']
                    """
                if order_picked_check:
                    exotel_sms_data[
                        sms_body_key] = "Shipped: Your %s order via Xpressbees . Track here: https://webapp.wareiq.com/tracking/%s . Powered by WareIQ." % (
                        client_name, str(orders_dict[current_awb][1]))
                    exotel_idx += 1
                    pickup_count += 1
                    if orders_dict[current_awb][11] not in pickup_dict:
                        pickup_dict[orders_dict[current_awb][11]] = 1
                    else:
                        pickup_dict[orders_dict[current_awb][11]] += 1
                    time_now = datetime.utcnow() + timedelta(hours=5.5)
                    cur.execute("UPDATE order_pickups SET picked=%s, pickup_time=%s WHERE order_id=%s",
                                (True, time_now, orders_dict[current_awb][0]))

                    update_picked_on_channels(orders_dict[current_awb], cur)

                    if orders_dict[current_awb][19]:

                        email = create_email(orders_dict[current_awb], "", orders_dict[current_awb][19])
                        if email:
                            emails_list.append((email, [orders_dict[current_awb][19]]))
                else:
                    continue

            if orders_dict[current_awb][2] != new_status:
                status_update_tuple = (new_status, status_type, status_detail, orders_dict[current_awb][0])
                cur.execute(order_status_update_query, status_update_tuple)

                if ret_order['ShipmentSummary'][0]['StatusCode'] == 'UD':
                    try:  # NDR check text
                        ndr_reason = None
                        if ret_order['ShipmentSummary'][0]['Status'].lower() in Xpressbees_ndr_reasons:
                            ndr_reason = Xpressbees_ndr_reasons[ret_order['ShipmentSummary'][0]['Status'].lower()]
                        elif "future delivery" in ret_order['ShipmentSummary'][0]['Status'].lower():
                            ndr_reason = 4
                        elif "open delivery" in ret_order['ShipmentSummary'][0]['Status'].lower():
                            ndr_reason = 10
                        elif "address incomplete" in ret_order['ShipmentSummary'][0]['Status'].lower():
                            ndr_reason = 2
                        elif "amount not ready" in ret_order['ShipmentSummary'][0]['Status'].lower():
                            ndr_reason = 15
                        else:
                            ndr_reason = 14
                        sms_to_key, sms_body_key, customer_phone, sms_body_key_data = verification_text(
                            orders_dict[current_awb], exotel_idx, cur, ndr=True,
                            ndr_reason=ndr_reason)
                        if sms_body_key_data:
                            exotel_sms_data[sms_to_key] = customer_phone
                            exotel_sms_data[sms_body_key] = sms_body_key_data
                            exotel_idx += 1
                    except Exception as e:
                        logger.error(
                            "NDR confirmation not sent. Order id: " + str(orders_dict[current_awb][0]))

            conn.commit()

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

    conn.commit()

    if emails_list:
        send_bulk_emails(emails_list)


def track_bluedart_orders(courier, cur):
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
    chunks = [all_orders[x:x + 200] for x in range(0, len(all_orders), 200)]
    for some_orders in chunks:
        awb_string = ""
        for order in some_orders:
            orders_dict[order[1]] = order
            awb_string += order[1] + ","

        awb_string = awb_string.rstrip(',')

        check_status_url = "https://api.bluedart.com/servlet/RoutingServlet?handler=tnt&action=custawbquery&loginid=HYD50082&awb=awb&numbers=%s&format=xml&lickey=eguvjeknglfgmlsi5ko5hn3vvnhoddfs&verno=1.3&scan=1" % awb_string
        req = requests.get(check_status_url)
        try:
            req = xmltodict.parse(req.content)
            if type(req['ShipmentData']['Shipment'])==list:
                req_ship_data += req['ShipmentData']['Shipment']
            else:
                req_ship_data += [req['ShipmentData']['Shipment']]

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
    logger.info("Count of Bluedart packages: " + str(len(req_ship_data)))
    for ret_order in req_ship_data:
        try:
            if ret_order['StatusType']=='NF':
                continue
            try:
                scan_group = ret_order['Scans']['ScanDetail'][0]['ScanGroupType']
                scan_code = ret_order['Scans']['ScanDetail'][0]['ScanCode']
                scan_list = ret_order['Scans']['ScanDetail']
            except Exception as e:
                scan_group = ret_order['Scans']['ScanDetail']['ScanGroupType']
                scan_code = ret_order['Scans']['ScanDetail']['ScanCode']
                scan_list = [ret_order['Scans']['ScanDetail']]

            if scan_group not in bluedart_status_mapping or scan_code not in bluedart_status_mapping[scan_group]:
                continue

            new_status = bluedart_status_mapping[scan_group][scan_code][0]
            current_awb = ret_order['@WaybillNo']
            is_return = False
            if '@RefNo' in ret_order and str(ret_order['@RefNo']).startswith("074"):
                current_awb = str(str(ret_order['@RefNo']).split("-")[1]).strip()
                is_return = True

            if is_return and new_status!='DELIVERED':
                continue

            try:
                order_status_tuple = (orders_dict[current_awb][0], orders_dict[current_awb][10], courier[0])
                cur.execute(select_statuses_query, order_status_tuple)
                all_scans = cur.fetchall()
                all_scans_dict = dict()
                for temp_scan in all_scans:
                    all_scans_dict[temp_scan[2]] = temp_scan
                new_status_dict = dict()
                for each_scan in scan_list:
                    status_time = each_scan['ScanDate']+"T"+each_scan['ScanTime']
                    if status_time:
                        status_time = datetime.strptime(status_time, '%d-%b-%YT%H:%M')

                    to_record_status = ""
                    if each_scan['ScanCode']=="015" and not is_return:
                        to_record_status = "Picked"
                    elif new_status=="IN TRANSIT" and each_scan['ScanType'] == "UD" and not is_return:
                        to_record_status = "In Transit"
                    elif each_scan['ScanCode'] in ("002", "092") and not is_return:
                        to_record_status = "Out for delivery"
                    elif each_scan['ScanCode'] in ("000", "090", "099") and not is_return:
                        to_record_status = "Delivered"
                    elif each_scan['ScanType'] == "RT" and not is_return:
                        to_record_status = "Returned"
                    elif each_scan['ScanCode'] == '000' and is_return:
                        to_record_status = "RTO"

                    if not to_record_status:
                        continue

                    if to_record_status not in new_status_dict:
                        new_status_dict[to_record_status] = (orders_dict[current_awb][0], courier[0],
                                                             orders_dict[current_awb][10],
                                                             each_scan['ScanType'],
                                                             to_record_status,
                                                             each_scan['Scan'],
                                                             each_scan['ScannedLocation'],
                                                             each_scan['ScannedLocation'],
                                                             status_time)
                    elif to_record_status == 'In Transit' and new_status_dict[to_record_status][
                        8] < status_time and not is_return:
                        new_status_dict[to_record_status] = (orders_dict[current_awb][0], courier[0],
                                                             orders_dict[current_awb][10],
                                                             each_scan['ScanType'],
                                                             to_record_status,
                                                             each_scan['Scan'],
                                                             each_scan['ScannedLocation'],
                                                             each_scan['ScannedLocation'],
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

            if is_return and new_status=='DELIVERED':
                new_status='RTO'

            status_type = ret_order['StatusType']
            if new_status == 'NOT PICKED':
                new_status = "PICKUP REQUESTED"
            status_detail = None
            status_code = scan_code

            if orders_dict[current_awb][2]=='CANCELED' and new_status!='IN TRANSIT':
                continue

            edd = ret_order['ExpectedDeliveryDate'] if 'ExpectedDeliveryDate' in ret_order else None
            if edd:
                edd = datetime.strptime(edd, '%d %B %Y')
                if datetime.utcnow().hour < 4:
                    cur.execute("UPDATE shipments SET edd=%s WHERE awb=%s", (edd, current_awb))
                    cur.execute("UPDATE shipments SET pdd=%s WHERE awb=%s and pdd is null", (edd, current_awb))

            client_name = orders_dict[current_awb][20]
            customer_phone = orders_dict[current_awb][4].replace(" ", "")
            customer_phone = "0" + customer_phone[-10:]

            if new_status == 'DELIVERED':

                update_delivered_on_channels(orders_dict[current_awb])

                sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                exotel_sms_data[sms_to_key] = customer_phone

                exotel_sms_data[sms_body_key] = "Delivered: Your %s order via Bluedart - https://webapp.wareiq.com/tracking/%s . Powered by WareIQ" % (
                    client_name, current_awb)

                exotel_idx += 1

            if new_status == 'RTO':
                update_rto_on_channels(orders_dict[current_awb])

            if orders_dict[current_awb][2] in (
                    'READY TO SHIP', 'PICKUP REQUESTED', 'NOT PICKED') and new_status == 'IN TRANSIT':
                pickup_count += 1
                if orders_dict[current_awb][11] not in pickup_dict:
                    pickup_dict[orders_dict[current_awb][11]] = 1
                else:
                    pickup_dict[orders_dict[current_awb][11]] += 1
                time_now = datetime.utcnow() + timedelta(hours=5.5)
                cur.execute("UPDATE order_pickups SET picked=%s, pickup_time=%s WHERE order_id=%s",
                            (True, time_now, orders_dict[current_awb][0]))
                update_picked_on_channels(orders_dict[current_awb], cur)

                if orders_dict[current_awb][19]:
                    email = create_email(orders_dict[current_awb], edd.strftime('%-d %b') if edd else "",
                                         orders_dict[current_awb][19])
                    if email:
                        emails_list.append((email, [orders_dict[current_awb][19]]))

                cur.execute("UPDATE shipments SET pdd=%s WHERE awb=%s", (edd, current_awb))
                sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                exotel_sms_data[sms_to_key] = customer_phone

                tracking_link_wareiq = "https://webapp.wareiq.com/tracking/" + str(orders_dict[current_awb][1])

                exotel_sms_data[sms_body_key] = "Shipped: Your %s order via Bluedart . Track here: %s . Powered by WareIQ." % (
                    client_name, tracking_link_wareiq)

                exotel_idx += 1

            if orders_dict[current_awb][2] != new_status:
                status_update_tuple = (new_status, status_type, status_detail, orders_dict[current_awb][0])
                cur.execute(order_status_update_query, status_update_tuple)

                if new_status == 'PENDING' and status_code in bluedart_status_mapping[scan_group]:
                    try:  # NDR check text
                        ndr_reason = bluedart_status_mapping[scan_group][status_code][3]
                        sms_to_key, sms_body_key, customer_phone, sms_body_key_data = verification_text(
                            orders_dict[current_awb], exotel_idx, cur, ndr=True,
                            ndr_reason=ndr_reason)
                        if sms_body_key_data:
                            exotel_sms_data[sms_to_key] = customer_phone
                            exotel_sms_data[sms_body_key] = sms_body_key_data
                            exotel_idx += 1
                    except Exception as e:
                        logger.error(
                            "NDR confirmation not sent. Order id: " + str(orders_dict[current_awb][0]))

            conn.commit()

        except Exception as e:
            logger.error("status update failed for " + str(current_awb) + "    err:" + str(
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

    conn.commit()

    if emails_list:
        send_bulk_emails(emails_list)


def track_ecomxp_orders(courier, cur):
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
    chunks = [all_orders[x:x + 100] for x in range(0, len(all_orders), 100)]
    for some_orders in chunks:
        awb_string = ""
        for order in some_orders:
            orders_dict[order[1]] = order
            awb_string += order[1] + ","

        awb_string = awb_string.rstrip(',')

        check_status_url = "https://plapi.ecomexpress.in/track_me/api/mawbd/?awb=%s&username=%s&password=%s" % (awb_string, courier[2], courier[3])
        req = requests.get(check_status_url)
        try:
            req = xmltodict.parse(req.content)
            if type(req['ecomexpress-objects']['object'])==list:
                req_data = list()
                for elem in req['ecomexpress-objects']['object']:
                    req_obj = ecom_express_convert_xml_dict(elem)
                    req_data.append(req_obj)
            else:
                req_data = [ecom_express_convert_xml_dict(req['ecomexpress-objects']['object'])]

            req_ship_data += req_data

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
    logger.info("Count of Ecom Express packages: " + str(len(req_ship_data)))
    for ret_order in req_ship_data:
        try:

            scan_code = ret_order['reason_code_number']
            scan_list = ret_order['scans']

            if scan_code not in ecom_express_status_mapping:
                continue

            new_status = ecom_express_status_mapping[scan_code][0]
            current_awb = ret_order['awb_number']
            status_type = ecom_express_status_mapping[scan_code][1]
            status_detail = None
            status_code = scan_code

            if orders_dict[current_awb][2]=='CANCELED' and new_status!='IN TRANSIT':
                continue

            try:
                order_status_tuple = (orders_dict[current_awb][0], orders_dict[current_awb][10], courier[0])
                cur.execute(select_statuses_query, order_status_tuple)
                all_scans = cur.fetchall()
                all_scans_dict = dict()
                for temp_scan in all_scans:
                    all_scans_dict[temp_scan[2]] = temp_scan
                new_status_dict = dict()
                for each_scan in scan_list:
                    status_time = each_scan['updated_on']
                    if status_time:
                        status_time = datetime.strptime(status_time, '%d %b, %Y, %H:%M')

                    to_record_status = ""
                    if each_scan['reason_code_number']=="0011":
                        to_record_status = "Picked"
                    elif each_scan['reason_code_number']=="003":
                        to_record_status = "In Transit"
                    elif each_scan['reason_code_number']=="006":
                        to_record_status = "Out for delivery"
                    elif each_scan['reason_code_number']=="999":
                        to_record_status = "Delivered"
                    elif each_scan['reason_code_number']=="777":
                        to_record_status = "Returned"
                    elif ret_order.get('rts_reason_code_number') and ret_order.get('rts_last_update') and ret_order.get('rts_reason_code_number')=='999':
                        to_record_status = "RTO"
                        if ret_order['rts_last_update']:
                            status_time = ret_order['rts_last_update']
                            status_time = datetime.strptime(status_time, '%d %b, %Y, %H:%M')
                        else:
                            status_time = datetime.utcnow()+timedelta(hours=5.5)
                        new_status='RTO'
                        status_type='DL'

                    if not to_record_status:
                        continue

                    if to_record_status not in new_status_dict:
                        new_status_dict[to_record_status] = (orders_dict[current_awb][0], courier[0],
                                                             orders_dict[current_awb][10],
                                                             each_scan['reason_code_number'],
                                                             to_record_status,
                                                             each_scan['status'],
                                                             each_scan['location_city'],
                                                             each_scan['city_name'],
                                                             status_time)
                    elif to_record_status == 'In Transit' and new_status_dict[to_record_status][
                        8] < status_time:
                        new_status_dict[to_record_status] = (orders_dict[current_awb][0], courier[0],
                                                             orders_dict[current_awb][10],
                                                             each_scan['reason_code_number'],
                                                             to_record_status,
                                                             each_scan['status'],
                                                             each_scan['location_city'],
                                                             each_scan['city_name'],
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

            edd = ret_order['expected_date'] if 'expected_date' in ret_order else None
            if edd:
                edd = datetime.strptime(edd, '%d-%b-%Y')
                if datetime.utcnow().hour < 4:
                    cur.execute("UPDATE shipments SET edd=%s WHERE awb=%s", (edd, current_awb))
                    cur.execute("UPDATE shipments SET pdd=%s WHERE awb=%s and pdd is null", (edd, current_awb))

            client_name = orders_dict[current_awb][20]
            customer_phone = orders_dict[current_awb][4].replace(" ", "")
            customer_phone = "0" + customer_phone[-10:]

            if new_status == 'DELIVERED':

                update_delivered_on_channels(orders_dict[current_awb])

                sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                exotel_sms_data[sms_to_key] = customer_phone

                exotel_sms_data[sms_body_key] = "Delivered: Your %s order via Ecom Express - https://webapp.wareiq.com/tracking/%s . Powered by WareIQ" % (
                    client_name, current_awb)

                exotel_idx += 1

            if new_status == 'RTO':
                update_rto_on_channels(orders_dict[current_awb])

            if orders_dict[current_awb][2] in (
                    'READY TO SHIP', 'PICKUP REQUESTED', 'NOT PICKED') and new_status == 'IN TRANSIT':
                pickup_count += 1
                if orders_dict[current_awb][11] not in pickup_dict:
                    pickup_dict[orders_dict[current_awb][11]] = 1
                else:
                    pickup_dict[orders_dict[current_awb][11]] += 1
                time_now = datetime.utcnow() + timedelta(hours=5.5)
                cur.execute("UPDATE order_pickups SET picked=%s, pickup_time=%s WHERE order_id=%s",
                            (True, time_now, orders_dict[current_awb][0]))

                update_picked_on_channels(orders_dict[current_awb], cur)

                if orders_dict[current_awb][19]:
                    email = create_email(orders_dict[current_awb], edd.strftime('%-d %b') if edd else "",
                                         orders_dict[current_awb][19])
                    if email:
                        emails_list.append((email, [orders_dict[current_awb][19]]))

                cur.execute("UPDATE shipments SET pdd=%s WHERE awb=%s", (edd, current_awb))
                sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                exotel_sms_data[sms_to_key] = customer_phone

                tracking_link_wareiq = "https://webapp.wareiq.com/tracking/" + str(orders_dict[current_awb][1])

                exotel_sms_data[sms_body_key] = "Shipped: Your %s order via Ecom Express . Track here: %s . Powered by WareIQ." % (
                    client_name, tracking_link_wareiq)

                exotel_idx += 1

            if orders_dict[current_awb][2] != new_status:
                status_update_tuple = (new_status, status_type, status_detail, orders_dict[current_awb][0])
                cur.execute(order_status_update_query, status_update_tuple)

                if new_status == 'PENDING' and status_code in ecom_express_ndr_reasons:
                    try:  # NDR check text
                        ndr_reason = ecom_express_ndr_reasons[status_code]
                        sms_to_key, sms_body_key, customer_phone, sms_body_key_data = verification_text(
                            orders_dict[current_awb], exotel_idx, cur, ndr=True,
                            ndr_reason=ndr_reason)
                        if sms_body_key_data:
                            exotel_sms_data[sms_to_key] = customer_phone
                            exotel_sms_data[sms_body_key] = sms_body_key_data
                            exotel_idx += 1
                    except Exception as e:
                        logger.error(
                            "NDR confirmation not sent. Order id: " + str(orders_dict[current_awb][0]))

            conn.commit()

        except Exception as e:
            logger.error("status update failed for " + str(current_awb) + "    err:" + str(
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

    conn.commit()

    if emails_list:
        send_bulk_emails(emails_list)


def verification_text(current_order, exotel_idx, cur, ndr=None, ndr_reason=None):
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
        date_today = (datetime.utcnow()+timedelta(hours=5.5)).strftime('%Y-%m-%d')
        cur.execute("SELECT * from ndr_shipments WHERE shipment_id=%s and date_created::date='%s';" % (str(current_order[10]), date_today))
        if not cur.fetchone():
            ndr_ship_tuple = (
                current_order[0], current_order[10], ndr_reason, "required", datetime.utcnow() + timedelta(hours=5.5))
            cur.execute(
                "INSERT INTO ndr_shipments (order_id, shipment_id, reason_id, current_status, date_created) VALUES (%s,%s,%s,%s,%s);",
                ndr_ship_tuple)
            if current_order[37] != False:
                cur.execute("SELECT * FROM ndr_verification where order_id=%s;"%str(current_order[0]))
                if not cur.fetchone():
                    cur.execute(
                        "INSERT INTO ndr_verification (order_id, verification_link, date_created) VALUES (%s,%s,%s);",
                        insert_cod_ver_tuple)

    client_name = current_order[20]
    customer_phone = current_order[4].replace(" ", "")
    customer_phone = "0" + customer_phone[-10:]

    sms_to_key = "Messages[%s][To]" % str(exotel_idx)
    sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

    if not ndr:
        sms_body_key_data = "Dear Customer, your order from %s with order id %s was delivered today." \
                            " Please click on the link (%s) to report any issue. We'll call you back shortly." % (
                                client_name, str(current_order[12]),
                                del_confirmation_link)
    elif ndr_reason in (1, 3, 9, 11) and current_order[37] != False:
        sms_body_key_data = "Dear Customer, your order from %s was attempted today but could not be delivered. Click on the link (%s) to re-attempt." % (
                                client_name, del_confirmation_link)
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

Xpressbees_ndr_reasons = {"customer refused to accept": 3,
                          "consignee refused to accept": 3,
                          "customer refused to pay cod amount": 9,
                          "add incomplete/incorrect & mobile not reachable": 1,
                          "add incomplete/incorrect": 2,
                          "customer not available & mobile not reachable": 1,
                          "customer not available": 1,
                          "consignee not available": 1,
                          "oda (out of delivery area)": 8}


bluedart_status_mapping = {'S': {'002':('DISPATCHED','UD','SHIPMENT OUTSCAN',),
'001':('IN TRANSIT','UD','SHIPMENT INSCAN',),
'003':('IN TRANSIT','UD','SHIPMENT OUTSCANNED TO NETWORK',),
'004':('IN TRANSIT','UD','PLASTIC BAG INSCAN',),
'005':('IN TRANSIT','UD','POD SLIP INSCAN',),
'006':('IN TRANSIT','UD','UNDELIVERED INSCAN',),
'007':('IN TRANSIT','UD','CANVAS BAG CONSOLIDATED SCAN',),
'008':('IN TRANSIT','UD','OVERAGE DELETED',),
'009':('IN TRANSIT','UD','SHIPMENT-AUTOSCAN',),
'010':('IN TRANSIT','UD','SHIPMENT SHORTAGE',),
'011':('IN TRANSIT','UD','TRANSIT CONNECTION SCAN',),
'012':('IN TRANSIT','UD','PLASTIC BAG - AUTO TALLY',),
'013':('IN TRANSIT','UD','SHIPMENT CONNECTED THRU SCL APPLICATION',),
'014':('IN TRANSIT','UD','PAPER WORK INSCAN',),
'015':('IN TRANSIT','UD','PICK UP SCAN ON FIELD',),
'020':('IN TRANSIT','UD','DIRECT CANVAS BAG SCANNED',),
'021':('IN TRANSIT','UD','MIXED CANVAS BAG SCANNED',),
'022':('IN TRANSIT','UD','CANVAS BAG IN SCAN AT DESTINATION LOC',),
'023':('IN TRANSIT','UD','CLUBBED CANVAS BAG SCAN',),
'024':('IN TRANSIT','UD','UNDELIVERED SHIPMENT HELD AT LOCATION',),
'025':('IN TRANSIT','UD','SHIPMENT SCAN TALLIED/ SLAH TALLY',),
'026':('IN TRANSIT','UD','TRANSIT SCAN',),
'027':('IN TRANSIT','UD','LOAD/VEHICLE ARRIVED AT DELIVERY LOC',),
'100':('IN TRANSIT','UD','CANVAS BAG RECEIVED AS OVERAGE',),
'106':('IN TRANSIT','UD','CHANGE IN WEIGHT EFFECTED',)},
                           "T":{'098':('CANCELED','DL','TCL PICKUP CANCELLED',),
'135':('CONFISCATED','DL','SHPT.CONFISCATED,CASE CLOSED',),
'129':('DAMAGED','UD','DAMAGED SHIPMENT, CASE CLOSED',),
'130':('DAMAGED','UD','SHPT/PACKAGE RECD.IN DAMAGED CONDITION',),
'178':('DAMAGED','DL','SHIPMENT SPOILED-SHIPPER RECONSTRUCTING',),
'000':('DELIVERED','DL','SHIPMENT DELIVERED',),
'090':('DELIVERED','DL','FORWARDED TO 3RD PARTY-NO POD AVAILABLE',),
'099':('DELIVERED','DL','MOVED TO HISTORY FILES',),
'025':('DESTROYED','DL','SHIPMENT DESTROYED/ABANDONED',),
'070':('DESTROYED','DL','ABANDONED/FORFEITED;DISPOSAL POLICY',),
'141':('DESTROYED','DL','SHIPMENT  DESTROYED/SENT FOR DISPOSAL',),
'092':('DISPATCHED','UD','SHIPMENT OUT FOR DELIVERY',),
'027':('IN TRANSIT','RD','SHIPMENT REDIRECTED ON FRESH AWB',),
'028':('IN TRANSIT','UD','RELEASED FROM CUSTOMS',),
'029':('IN TRANSIT','UD','DELIVERY  SCHEDULED FOR NEXT WORKING DAY',),
'030':('IN TRANSIT','UD','PKG HELD FOR TAXES',),
'031':('IN TRANSIT','UD','PACKAGE INTERCHANGED AT ORIGIN',),
'032':('IN TRANSIT','UD','PROCEDURAL DELAY IN DELIVERY EXPECTED',),
'033':('IN TRANSIT','UD','APX/SFC AWB RECD,SHIPMENT NOT RECEIVED',),
'034':('IN TRANSIT','UD','RTO SHPT HAL AS PER CUSTOMERS REQUEST',),
'035':('IN TRANSIT','UD','HANDED OVER TO AD-HOC/AGENT/SUB-COURIER',),
'036':('IN TRANSIT','UD','LATE ARRIVAL/SCHED. FOR NEXT WORKING DAY',),
'037':('IN TRANSIT','UD','PACKAGE WRONGLY ROUTED IN NETWORK',),
'038':('IN TRANSIT','UD','CLEARANCE PROCESS DELAYED',),
'039':('IN TRANSIT','UD','SHIPMENT INSPECTED FOR SECURITY PURPOSES',),
'040':('IN TRANSIT','UD','CNEE CUSTOMS BROKER NOTIFIED FOR CLRNCE',),
'041':('IN TRANSIT','UD','SHPT/PAPERWORK HANDED OVER TO CNEE BRKR',),
'042':('IN TRANSIT','UD','CNEE NAME / SURNAME MIS-MATCH',),
'043':('IN TRANSIT','UD','SHIPMENT RETURNED TO SHIPPER/ORIGIN',),
'044':('IN TRANSIT','UD','CNEE REFUSING TO PAY OCTROI/TAX/DEMURRAG',),
'045':('IN TRANSIT','UD','HELD FOR CLARITY ON HANDLING CHARGES',),
'046':('IN TRANSIT','UD','HELD AT PUD/HUB;REGULATORY PAPERWORK REQ',),
'047':('IN TRANSIT','UD','CONTENTS MISSING',),
'048':('IN TRANSIT','UD','MISROUTE DUE TO SHIPPER FAULT/WRONG PIN',),
'049':('IN TRANSIT','UD','MISROUTE DUE TO BDE FAULT',),
'050':('IN TRANSIT','RD','SHPT REDIRECTED ON SAME AWB',),
'051':('IN TRANSIT','UD','CHANGE IN MODE - AIR SHPT. BY SFC',),
'052':('IN TRANSIT','UD','MISSED CONNECTION',),
'053':('IN TRANSIT','UD','SHIPMENT SUB-COURIERED',),
'054':('IN TRANSIT','UD','NOT CONNECTED AS PER CUTOFF',),
'055':('IN TRANSIT','UD','SHIPMENT OFF-LOADED BY AIRLINE',),
'056':('IN TRANSIT','UD','P.O. BOX ADDRESS,UNABLE TO DELIVER',),
'057':('IN TRANSIT','UD','FLIGHT CANCELLED',),
'058':('IN TRANSIT','UD','MISROUTE;WRONG PIN/ZIP BY SHIPPER',),
'059':('IN TRANSIT','UD','COMM FLIGHT,VEH/TRAIN; DELAYED/CANCELLED',),
'060':('IN TRANSIT','UD','REDIRECTED ON SAME AWB TO SHIPPER',),
'061':('IN TRANSIT','UD','CMENT WITHOUT PINCODE;SHPR FAILURE',),
'062':('IN TRANSIT','UD','OCTROI/TAXES/CHEQUE/DD/COD AMT NOT READY',),
'063':('IN TRANSIT','UD','INCOMPLETE ST WAYBILL;DELIVERY DELAYED',),
'064':('IN TRANSIT','UD','HELD FOR DUTY/TAXES/FEES PAYMENT',),
'065':('IN TRANSIT','UD','IN TRANSIT',),
'066':('IN TRANSIT','UD','TIME CONSTRAINT;UNABLE TO DELIVER',),
'067':('IN TRANSIT','UD','TRANSPORT STRIKE',),
'068':('IN TRANSIT','UD','MISROUTE IN NETWORK',),
'069':('IN TRANSIT','UD','CNEE OFFICE CLOSED;UNABLE TO DELIVER',),
'071':('IN TRANSIT','UD','UNABLE TO DELIVER:DUE NATURAL DISASTER',),
'072':('IN TRANSIT','UD','FREIGHT SHIPMENT:RECD AT BOMBAY',),
'073':('IN TRANSIT','UD','CCU HUB;TRANSHIPMENT PERMIT AWAITED',),
'075':('IN TRANSIT','UD','SHIPMENT TRANSITED THRU DHL FACILITY',),
'076':('IN TRANSIT','UD','CREDIT CARD;CNEE REFUSING IDENTIFICATION',),
'077':('IN TRANSIT','UD','PACKAGE INTERCHANGED',),
'078':('IN TRANSIT','UD','SHP IMPOUNDED BY REGULATORY AUTHORITY',),
'079':('IN TRANSIT','UD','DELIVERY NOT ATTEMPTED AT DESTINATION',),
'080':('IN TRANSIT','UD','NOT CONNECTED, SPACE CONSTRAINT',),
'081':('IN TRANSIT','UD','INCOMPLETE CREDIT CARD POD',),
'082':('IN TRANSIT','UD','DELAY AT DESTINATION;POD AWAITED',),
'083':('IN TRANSIT','UD','SHIPMENT HELD IN NETWORK',),
'084':('IN TRANSIT','UD','ALL/PART/PACKAGING OF SHIPMENT DAMAGED',),
'085':('IN TRANSIT','UD','SCHEDULED FOR MOVEMENT IN NETWORK',),
'086':('IN TRANSIT','UD','SHPT DELIVERED/CNEE CONSIDERS DAMAGED',),
'087':('IN TRANSIT','UD','SHPT PROCESSED AT LOCATION',),
'088':('IN TRANSIT','UD','SHPT DEPARTED FM DHL FACILITY',),
'089':('IN TRANSIT','UD','SHPT REACHED DHL TRANSIT FACILITY',),
'091':('IN TRANSIT','UD','CONSIGNMENT PARTIALLY DELIVERED',),
'093':('IN TRANSIT','UD','DELIVERED TO WRONG ADDRESS AND RETRIEVED',),
'094':('IN TRANSIT','UD','SHIPMENT/PIECE MISSING',),
'095':('IN TRANSIT','UD','ADMIN OVERRIDE ON NSL FAILURES',),
'096':('IN TRANSIT','UD','LATE POD/STATUS UPDATE',),
'097':('IN TRANSIT','UD','DOD SHIPMENT DELIVERED, DD PENDING DELY.',),
'100':('IN TRANSIT','UD','SHIPMENT CANT TRAVEL ON DESIRED MODE',),
'101':('IN TRANSIT','UD','APEX CONNECTED ON COMMERCIAL FLIGHT',),
'102':('IN TRANSIT','UD','DUTS IN DOX SHIPMENT',),
'103':('IN TRANSIT','UD','SHPT CANT TRAVEL ON DESIRED MODE',),
'104':('IN TRANSIT','RT','RETURN TO SHIPPER',),
'105':('IN TRANSIT','RT','SHIPMENT RETURNED BACK TO SHIPPER',),
'106':('IN TRANSIT','UD','LINEHAUL DELAYED; ACCIDENT/TRAFFIC-JAM',),
'107':('IN TRANSIT','UD','LINEHAUL DELAYED;TRAFFICJAM ENROUTE',),
'110':('IN TRANSIT','UD','DETAINED AT ORIGIN',),
'111':('IN TRANSIT','UD','SECURITY CLEARED',),
'120':('IN TRANSIT','UD','DELIVERY BY APPOINTMENT',),
'121':('IN TRANSIT','UD','SHIPMENT BOOKED FOR EMBARGO LOCATION',),
'123':('IN TRANSIT','RT','RTO FROM HUB ON FRESH AWB',),
'132':('IN TRANSIT','RD','CHANGE IN MODE/NEW AWB CUT',),
'133':('IN TRANSIT','UD','AWB INFORMATION MODIFIED',),
'136':('IN TRANSIT','UD','APEX TRANSIT ON COMM FLT;CCU HUB',),
'140':('IN TRANSIT','UD','SHIPMENT UNDER COOLING BY AIRLINE',),
'142':('IN TRANSIT','UD','SHIPMENT PARTIALLY DELIVERED',),
'143':('IN TRANSIT','UD','SPECIAL SHIPPER ODA DELV-DELAY EXPECTED',),
'145':('IN TRANSIT','UD','AWB WRONGLY INSCANNED',),
'146':('IN TRANSIT','UD','UNDER SECURITY INVESTIGATION',),
'147':('IN TRANSIT','UD','DP DUTS HELD AT CCU W/H',),
'148':('IN TRANSIT','UD','PLEASE CONTACT CUSTOMER SERVICE',),
'149':('IN TRANSIT','UD','CMENT WITHOUT PINCODE/DELIVERY DELAYED',),
'150':('IN TRANSIT','UD','CORRECTION OF WRONG POD DETAILS',),
'151':('IN TRANSIT','UD','AWAITING CNEE FEEDBACK TO SORRY CARD',),
'152':('IN TRANSIT','UD','ATTEMPT AT SECONDARY ADDRESS',),
'154':('IN TRANSIT','UD','SHPT DETAINED/SEIZED BY REGULATORY',),
'155':('IN TRANSIT','UD','CHECK IN SCAN',),
'156':('IN TRANSIT','UD','SHPT REACHED DHL DESTINATION LOCATION',),
'157':('IN TRANSIT','UD','MISCODE;DELIVERY DELAYED',),
'159':('IN TRANSIT','UD','SERVICE CHANGE;SHPT IN TRANSIT',),
'160':('IN TRANSIT','UD','SHPT U/D:NO SERVICE INCIDNET REPORTED',),
'161':('IN TRANSIT','UD','AWAITING CONX ON SCHEDULED FLT:IN TRANST',),
'162':('IN TRANSIT','UD','TRACE INITIATED',),
'163':('IN TRANSIT','UD','DHL TRACE CLOSED',),
'166':('IN TRANSIT','UD','CAPACITY CONSTRAINT; BULK DESPATCH',),
'169':('IN TRANSIT','UD','FLFM SHIPMENT;APEX/SFC MODE',),
'170':('IN TRANSIT','UD','FREIGHT SHIPMENT:AWAITING CUSTOMS P/W',),
'171':('IN TRANSIT','UD','FREIGHT SHPT:CUSTOMS CLEARANCE ON DATE',),
'172':('IN TRANSIT','UD','FREIGHT SHIPMENT:CLEARED CUSTOMS',),
'173':('IN TRANSIT','UD','SHIPMENT NOT LOCATED',),
'174':('IN TRANSIT','UD','SHIPMENT RECEIVED;PAPERWORK NOT RECEIVED',),
'175':('IN TRANSIT','UD','CONSIGNEE NOT AVAILABLE; CANT DELIVER',),
'176':('IN TRANSIT','UD','ATA/TP SHIPMENTS;DAY DEFERRED DELIVERY',),
'177':('IN TRANSIT','UD','SHIPMENT  DESTROYED/SENT FOR DISPOSAL',),
'179':('IN TRANSIT','UD','DC DESCREPANCY',),
'180':('IN TRANSIT','UD','DC RECEIVED FROM CNEE',),
'181':('IN TRANSIT','UD','ADMIN OVER-RIDE OF DC COUNT',),
'182':('IN TRANSIT','UD','POD/DC COPY SENT',),
'183':('IN TRANSIT','UD','POD/DC ACCURACY',),
'184':('IN TRANSIT','UD','SHIPMENT HANDEDOVER TO DHL',),
'185':('IN TRANSIT','UD','APEX / SFC SHPT OVERCARRIED IN NETWORK',),
'186':('IN TRANSIT','UD','APX/SFC SHPT MISPLACED AT DST/WAREHOUSE',),
'187':('IN TRANSIT','UD','DEMURRAGE CHARGES NOT READY',),
'189':('IN TRANSIT','UD','GSTN SITE NOT WORKING',),
'190':('IN TRANSIT','UD','SHIPMENT UNTRACEABLE AT DESTINATION',),
'206':('IN TRANSIT','UD','SHIPMENT KEPT IN PARCEL LOCKER',),
'207':('IN TRANSIT','UD','SHIPMENT RETRIEVED FROM PARCEL LOCKER',),
'208':('IN TRANSIT','UD','SHIPMENT KEPT IN PARCEL SHOP FOR COLLECT',),
'209':('IN TRANSIT','UD','SHPT RETRIEVED FROM PARCEL SHOP FOR RTO',),
'210':('IN TRANSIT','UD','DG SHIPMENT SCAN IN LOCATION',),
'211':('IN TRANSIT','UD','LOAD ON HOLD;SPACE CONSTRAINT-DELVRY LOC',),
'212':('IN TRANSIT','UD','LOAD ON HOLD;SPACE CONSTRAINT IN NET VEH',),
'213':('IN TRANSIT','UD','LOAD ON HOLD;SPACE CONSTRAINT-COMML FLT',),
'214':('IN TRANSIT','UD','LOAD ON HOLD; EMBARGO ON COMML UPLIFT',),
'215':('IN TRANSIT','UD','LOAD ON HOLD; SPACE CONSTRAINT IN TRAIN',),
'216':('IN TRANSIT','UD','HELD IN DHLe NETWORK DPS CHECK',),
'220':('IN TRANSIT','UD','SHIPMENT HANDED OVER TO ASSOCIATE',),
'221':('IN TRANSIT','UD','SHPT RCD IN TRANSIT LOC; BEING CONNECTED',),
'222':('IN TRANSIT','UD','SHPT RCVD AT DESTN LOC FOR DLVRY ATTEMPT',),
'223':('IN TRANSIT','UD','UD SHPT SENDING BACK TO BDE FOR PROCESS',),
'224':('IN TRANSIT','UD','UD SHPT RCVD FRM ASSOCIATE FOR PROCESSNG',),
'301':('IN TRANSIT','UD','TRAFFIC JAM ENROUTE',),
'302':('IN TRANSIT','UD','ACCIDENT ENROUTE',),
'303':('IN TRANSIT','UD','DETAINED AT CHECK-POST',),
'304':('IN TRANSIT','UD','POLITICAL DISTURBANCE',),
'305':('IN TRANSIT','UD','HEAVY RAIN',),
'306':('IN TRANSIT','UD','VEHICLE BREAK-DOWN ENROUTE',),
'307':('IN TRANSIT','UD','HEAVY FOG',),
'309':('IN TRANSIT','UD','DETAINED BY RTO',),
'310':('IN TRANSIT','UD','VENDOR FAULT',),
'311':('IN TRANSIT','UD','ENDORSEMENT NOT DONE AT CHECK-POST',),
'312':('IN TRANSIT','UD','CAUGHT FIRE INSIDE VEHICLE',),
'313':('IN TRANSIT','UD','DELAYED BY ENROUTE SECTOR',),
'314':('IN TRANSIT','UD','DETAINED BY SALES TAX',),
'315':('IN TRANSIT','UD','ANY OTHER CONTROLABLE REASON',),
'316':('IN TRANSIT','UD','ANY OTHER NON-CONTROLABLE REASON',),
'021':('LOST','DL','LOST SHIPMENT',),
'001':('PENDING','UD','CUSTOMER ASKED FUTURE DELIVERY: HAL',4),
'002':('PENDING','UD','OUT OF DELIVERY AREA',8),
'003':('PENDING','UD','RESIDENCE/OFFICE CLOSED;CANT DELIVER',6),
'004':('PENDING','UD','COMPANY ON STRIKE, CANNOT DELIVER',7),
'005':('PENDING','UD','HOLIDAY:DELIVERY ON NEXT WORKING DAY',4),
'006':('PENDING','UD','SHIPPER PKGNG/MRKNG IMPROPER;SHPT HELD',2),
'007':('IN TRANSIT','UD','SHIPT MANIFESTED;NOT RECD BY DESTINATION',),
'008':('PENDING','UD','ADDRESS UNLOCATABLE; CANNOT DELIVER',2),
'009':('PENDING','UD','ADDRESS INCOMPLETE, CANNOT DELIVER',2),
'010':('PENDING','UD','ADDRESS INCORRECT; CANNOT DELIVER',2),
'011':('PENDING','UD','CONSIGNEE REFUSED TO ACCEPT',3),
'012':('PENDING','UD','NO SUCH CO./CNEE AT GIVEN ADDRESS',2),
'013':('PENDING','UD','CONSIGNEE NOT AVAILABLE;CANT DELIVER',1),
'014':('PENDING','UD','CNEE SHIFTED FROM THE GIVEN ADDRESS',2),
'016':('IN TRANSIT','RT','RTO FROM ORIGIN S.C. ON SAME AWB',),
'017':('PENDING','UD','DISTURBANCE/NATURAL DISASTER/STRIKE',12),
'019':('PENDING','UD','CONSIGNEE NOT YET CHECKED IN',4),
'020':('PENDING','UD','CONSIGNEE OUT OF STATION',4),
'022':('IN TRANSIT','UD','BEING PROCESSED AT CUSTOMS',),
'024':('IN TRANSIT','UD','BD FLIGHT DELAYED; BAD WEATHER/TECH SNAG',),
'137':('PENDING','UD','DELIVERY AREA NOT ACCESSIBLE',7),
'139':('PENDING','UD','NEED DEPT NAME/EXTN.NO:UNABLE TO DELIVER',2),
'201':('PENDING','UD','E-TAIL; REFUSED TO ACCEPT SHIPMENT',3),
'202':('IN TRANSIT','UD','E-TAIL; REFUSED - SHPTS ORDERED IN BULK',),
'203':('PENDING','UD','E-TAIL; REFUSED-OPEN DELIVERY REQUEST',10),
'204':('PENDING','UD','E-TAIL; REFUSED-WRONG PROD DESP/NOT ORDE',3),
'205':('PENDING','UD','E-TAIL: FAKE  BOOKING/FAKE ADDRESS',2),
'217':('PENDING','UD','CONSIGNEE HAS GIVEN BDE HAL ADDRESS',2),
'218':('PENDING','UD','CONSIGNEE ADD IS EDUCATIONAL INSTITUTION',7),
'219':('IN TRANSIT','UD','SHIPMENT MOVED TO MOBILE OFFICE',),
'308':('PENDING','UD','NO ENTRY',7),
'777':('PENDING','UD','CONSIGNEE REFUSED SHIPMENT DUE TO GST',2),
'026':('POSTED','DL','SHIPMENT POSTED',),
'074':('IN TRANSIT','RT','RETURNED (SHIPPER REQUEST)',),
'118':('RTO','RT','DELIVERED BACK TO SHIPPER',),
'188':('RTO','RT','DELIVERED BACK TO SHIPPER',)}
                           }

ecom_express_status_mapping = {"303": ("IN TRANSIT", "UD", "In Transit", "Shipment In Transit"),
                             "400": ("IN TRANSIT", "UD", "Picked", "Shipment picked up"),
                             "003": ("IN TRANSIT", "UD", "In Transit", "Bag scanned at DC"),
                             "002": ("IN TRANSIT", "UD", "In Transit", "Shipment in-scan"),
                             "004": ("IN TRANSIT", "UD", "In Transit", "Shipment in-scan"),
                             "005": ("IN TRANSIT", "UD", "In Transit", "Shipment in-scan at DC"),
                             "0011": ("IN TRANSIT", "UD", "Picked", "Shipment picked up"),
                             "21601": ("IN TRANSIT", "UD", "In Transit", "Late arrival-Misconnection/After cut off"),
                             "006": ("DISPATCHED", "UD", "Out for delivery", "Shipment out for delivery"),
                             "888": ("DAMAGED", "UD", "", "Transit Damage"),
                             "302": ("DAMAGED", "UD", "", "Transit Damage"),
                             "555": ("DESTROYED", "UD", "", "Destroyed Red Bus Shipment"),
                             "88802": ("DESTROYED", "UD", "", "Shipment destroyed - contains liquid item"),
                             "88803": ("DESTROYED", "UD", "", "Shipment destroyed - contains fragile item"),
                             "88804": ("DESTROYED", "UD", "", "Shipment destroyed - empty packet"),
                             "31701": ("DESTROYED", "UD", "", "Shipment destroyed - food item"),
                             "311": ("SHORTAGE", "UD", "", "Shortage"),
                             "313": ("SHORTAGE", "UD", "", "Shortage"),
                             "314": ("DAMAGED", "UD", "", "DMG Lock - Damage"),
                             "999": ("DELIVERED", "DL", "Delivered", "Shipment delivered"),
                             "204": ("DELIVERED", "DL", "Delivered", "Shipment delivered"),
                             "777": ("IN TRANSIT", "RT", "Returned", "Returned"),
                             "333": ("LOST", "UD", "", "Shipment Lost"),
                             "33306": ("LOST", "UD", "", "Shipment Lost"),
                             "33307": ("LOST", "UD", "", "Shipment Lost"),
                             "228": ("PENDING", "UD", "In Transit", "Out of Delivery Area"),
                             "227": ("PENDING", "UD", "In Transit", "Residence/Office Closed"),
                             "226": ("PENDING", "UD", "In Transit", "Holiday/Weekly off - Delivery on Next Working Day"),
                             "224": ("PENDING", "UD", "In Transit", "Address Unlocatable"),
                             "223": ("PENDING", "UD", "In Transit", "Address Incomplete"),
                             "222": ("PENDING", "UD", "In Transit", "Address Incorrect"),
                             "220": ("PENDING", "UD", "In Transit", "No Such Consignee At Given Address"),
                             "418": ("PENDING", "UD", "In Transit", "Consignee Shifted, phone num wrong"),
                             "417": ("PENDING", "UD", "In Transit", "PHONE NUMBER NOT ANSWERING/ADDRESS NOT LOCATABLE"),
                             "219": ("PENDING", "UD", "In Transit", "Consignee Not Available"),
                             "218": ("PENDING", "UD", "In Transit", "Consignee Shifted from the Given Address"),
                             "231": ("PENDING", "UD", "In Transit", "Shipment attempted - Customer not available"),
                             "212": ("PENDING", "UD", "In Transit", "Consignee Out Of Station"),
                             "217": ("PENDING", "UD", "In Transit", "Delivery Area Not Accessible"),
                             "213": ("PENDING", "UD", "In Transit", "Scheduled for Next Day Delivery"),
                             "331": ("PENDING", "UD", "In Transit", "Consignee requested for future delivery "),
                             "210": ("PENDING", "UD", "Cancelled", "Shipment attempted - Customer refused to accept"),
                             "209": ("PENDING", "UD", "In Transit", "Consignee Refusing to Pay COD Amount"),
                             "419": ("PENDING", "UD", "In Transit", "Three attempts made, follow up closed"),
                             "401": ("PENDING", "UD", "In Transit", "CUSTOMER RES/OFF CLOSED"),
                             "421": ("PENDING", "UD", "In Transit", "Customer Number not reachable/Switched off"),
                             "23101": ("PENDING", "UD", "In Transit", "Customer out of station"),
                             "23102": ("PENDING", "UD", "In Transit", "Customer not in office"),
                             "23103": ("PENDING", "UD", "In Transit", "Customer not in residence"),
                             "22701": ("PENDING", "UD", "In Transit", "Case with Legal team"),
                             "20002": ("PENDING", "UD", "In Transit", "Forcefully opened by customer and returned"),
                             "21002": ("PENDING", "UD", "Cancelled", "Order already cancelled"),
                             "22301": ("PENDING", "UD", "In Transit", "Customer out of station"),
                             "22303": ("PENDING", "UD", "In Transit", "No Such Consignee At Given Address"),
                             "23401": ("PENDING", "UD", "In Transit", "Address pincode mismatch - Serviceable area"),
                             "23402": ("PENDING", "UD", "In Transit", "Address pincode mismatch - Non Serviceable area"),
                             "22702": ("PENDING", "UD", "In Transit", "Shipment attempted - Office closed"),
                             "22801": ("PENDING", "UD", "In Transit", "Customer Address out of delivery area"),
                             "22901": ("PENDING", "UD", "In Transit", "Customer requested for self collection"),
                             "2447": ("PENDING", "UD", "In Transit", "No such addressee in the given address"),
                             "2445": ("PENDING", "UD", "In Transit", "Cash amount Mismatch"),
                             "12247": ("PENDING", "UD", "In Transit", "Delivery Attempt to be made - Escalations"),
                             "12245": ("PENDING", "UD", "In Transit", "Delivery attempt to be made - FE Instructions"),
                             "20701": ("PENDING", "UD", "In Transit", "Misroute due to wrong pincode given by customer"),
                             }

ecom_express_ndr_reasons = {
                              "228": 8,
                              "227": 6,
                              "226": 4,
                              "224": 2,
                              "223": 2,
                              "222": 2,
                              "220": 2,
                              "418": 2,
                              "417": 2,
                              "219": 1,
                              "218": 1,
                              "231": 1,
                              "212": 1,
                              "217": 7,
                              "213": 4,
                              "331": 4,
                              "210": 3,
                              "209": 9,
                              "419": 13,
                              "401": 6,
                              "421": 1,
                              "23101": 1,
                              "23102": 1,
                              "23103": 1,
                              "232": 2,
                              "234": 2,
                              "22701": 6,
                              "20002": 11,
                              "21002": 3,
                              "22301": 2,
                              "22303": 2,
                              "23401": 2,
                              "23402": 2,
                              "2447": 2,
                              "22702": 6,
                              "22801": 8,
                              "22901": 5,
                              "2445": 9,
                            }


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
    r = wcapi.post('orders/%s?consumer_key=%s&consumer_secret=%s' % (str(order[5]), order[7], order[8]), data={"status": status_mark})
    try:
        r = wcapi.post('orders/%s/shipment-trackings' % str(order[5]), data={"tracking_provider": "WareIQ", "tracking_number":order[1]})
    except Exception:
        pass


def lotus_organics_update(order, status):
    url = "https://www.lotus-organics.com/api/v1/order/wareiq/update"
    headers = {"Content-Type": "application/json",
               "x-api-key": "901192e41675e1b908d26a7e95c77ddc"}
    data = {
        "id": int(order[5]),
        "ware_iq_id": order[0],
        "awb_number": str(order[1]),
        "status_information": status
    }

    req = requests.put(url, headers=headers, data=json.dumps(data))


def lotus_botanicals_shipped(order):
    try:
        url = "http://webapps.lotusbotanicals.com/orders/update/shipping/"+str(order[0])
        headers = {"Content-Type": "application/json",
                   "Authorization": "Ae76eH239jla*fgna#q6fG&5Khswq_kpaj$#1a"}
        tracking_link = "http://webapp.wareiq.com/tracking/%s" % str(order[1])
        data = {"tracking_service": "WareIQ",
                "tracking_number": str(order[1]),
                "url" : tracking_link}
        req = requests.post(url, headers=headers, data=json.dumps(data))

    except Exception as e:
        logger.error("Couldn't update lotus for: " + str(order[0])
                     + "\nError: " + str(e.args))


def lotus_botanicals_delivered(order):
    try:
        url = "http://webapps.lotusbotanicals.com/orders/update/delivered/"+str(order[0])
        headers = {"Content-Type": "application/json",
                   "Authorization": "Ae76eH239jla*fgna#q6fG&5Khswq_kpaj$#1a"}
        data = {}
        req = requests.post(url, headers=headers, data=json.dumps(data))
    except Exception as e:
        logger.error("Couldn't update lotus for: " + str(order[0])
                     + "\nError: " + str(e.args))


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
    fulfillment_id = None
    try:
        fulfillment_id = str(req_ful.json()['fulfillment']['id'])
    except KeyError:
        if req_ful.json().get('errors') and req_ful.json().get('errors')=='Not Found':
            get_locations_url = "https://%s:%s@%s/admin/api/2019-10/locations.json" % (order[7], order[8], order[9])
            req = requests.get(get_locations_url).json()
            location_id = str(req['locations'][0]['id'])
            cur.execute("UPDATE client_channel set unique_parameter=%s where id=%s" % (location_id, order[34]))
            fulfil_data['fulfillment']['location_id'] = int(location_id)
            req_ful = requests.post(create_fulfillment_url, data=json.dumps(fulfil_data),
                                    headers=ful_header)
            fulfillment_id = str(req_ful.json()['fulfillment']['id'])
    if fulfillment_id and tracking_link:
        cur.execute("UPDATE shipments SET channel_fulfillment_id=%s, tracking_link=%s WHERE id=%s",
                    (fulfillment_id, tracking_link, order[10]))
    return fulfillment_id, tracking_link


def hepta_fulfilment(order):
    headers = {"Content-Type": "application/x-www-form-urlencoded",
               "Authorization": "Basic c2VydmljZS5hcGl1c2VyOllQSGpBQXlXY3RWYzV5MWg="}
    hepta_url = "https://www.nashermiles.com/alexandria/api/v1/shipment/create"
    hepta_body = {
        "order_id": str(order[5]),
        "awb_number": str(order[1]),
        "tracking_link": "http://webapp.wareiq.com/tracking/%s" % str(order[1])
    }
    req_ful = requests.post(hepta_url, headers=headers, data=json.dumps(hepta_body))


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


def shopify_cancel(order):
    get_cancel_url = "https://%s:%s@%s/admin/api/2019-10/orders/%s/cancel.json" % (
        order[7], order[8],
        order[9], order[5])

    tra_header = {'Content-Type': 'application/json'}
    cancel_data = {"restock": False}
    if order[3] in ("BEHIR", "SHAHIKITCHEN", "SUKHILIFE", "SUCCESSCRAFT", "NEWYOURCHOICE"):
        cancel_data = {"restock": True}
    req_ful = requests.post(get_cancel_url, data=json.dumps(cancel_data),
                            headers=tra_header)


def magento_fulfillment(order, cur):
    create_fulfillment_url = "%s/V1/order/%s/ship" % (order[9], order[5])
    tracking_link = "http://webapp.wareiq.com/tracking/%s" % str(order[1])
    ful_header = {'Content-Type': 'application/json',
                  'Authorization': 'Bearer ' + order[7],
                  'User-Agent': 'WareIQ server'}

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
                "extension_attributes": {"warehouse_name": str(order[36])},
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
                  'Authorization': 'Bearer ' + order[7],
                  'User-Agent': 'WareIQ server'}

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
                  'Authorization': 'Bearer ' + order[7],
                  'User-Agent': 'WareIQ server'}

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


def update_picked_on_channels(order, cur):
    if order[3] == "NASHER" and order[5]:
        hepta_fulfilment(order)
    if order[26] != False:
        if order[14] == 5:
            try:
                woocommerce_fulfillment(order)
            except Exception as e:
                logger.error(
                    "Couldn't update woocommerce for: " + str(order[0])
                    + "\nError: " + str(e.args))
        elif order[14] == 1:
            try:
                shopify_fulfillment(order, cur)
            except Exception as e:
                logger.error("Couldn't update shopify for: " + str(order[0])
                             + "\nError: " + str(e.args))
        elif order[14] == 6:  # Magento fulfilment
            try:
                if order[28] != False:
                    magento_invoice(order)
                magento_fulfillment(order, cur)
            except Exception as e:
                logger.error("Couldn't update Magento for: " + str(order[0])
                             + "\nError: " + str(e.args))
        elif order[14] == 8:  # Bikayi fulfilment
            try:
                update_bikayi_status(order, "IN TRANSIT")
            except Exception as e:
                logger.error("Couldn't update Bikayi for: " + str(order[0])
                             + "\nError: " + str(e.args))
        elif order[3] == 'LOTUSBOTANICALS':
            lotus_botanicals_shipped(order)
        elif order[3] == 'LOTUSORGANICS':
            try:
                lotus_organics_update(order, "Orders Shipped")
            except Exception as e:
                pass
        elif order[14] == 7: #Easyecom fulfilment
            try:
                update_easyecom_status(order, 2)
            except Exception as e:
                logger.error("Couldn't update Easyecom for: " + str(order[0])
                             + "\nError: " + str(e.args))


def update_delivered_on_channels(order):
    if order[30] != False:
        if order[14] == 6:  # Magento complete
            try:
                magento_complete_order(order)
            except Exception as e:
                logger.error(
                    "Couldn't complete Magento for: " + str(order[0])
                    + "\nError: " + str(e.args))

    if order[28] != False and str(
            order[13]).lower() == 'cod' and order[
        14] == 1:  # mark paid on shopify
        try:
            shopify_markpaid(order)
        except Exception as e:
            logger.error(
                "Couldn't mark paid Shopify for: " + str(order[0])
                + "\nError: " + str(e.args))

    elif order[3] == 'LOTUSBOTANICALS':
        lotus_botanicals_delivered(order)

    elif order[3] == 'LOTUSORGANICS':
        try:
            lotus_organics_update(order, "Orders Delivered")
        except Exception as e:
            pass

    elif order[14] == 7:  # Easyecom Delivered
        try:
            update_easyecom_status(order, 3)
        except Exception as e:
            logger.error("Couldn't update Easyecom for: " + str(order[0])
                         + "\nError: " + str(e.args))
    elif order[14] == 8:  # Bikayi delivered
        try:
            update_bikayi_status(order, "DELIVERED")
        except Exception as e:
            logger.error("Couldn't update Bikayi for: " + str(order[0])
                         + "\nError: " + str(e.args))


def update_rto_on_channels(order):
    if order[32] != False:
        if order[14] == 6:  # Magento return
            try:
                magento_return_order(order)
            except Exception as e:
                logger.error("Couldn't return Magento for: " + str(order[0])
                             + "\nError: " + str(e.args))
        elif order[14] == 5:  # Woocommerce Cancelled
            try:
                woocommerce_returned(order)
            except Exception as e:
                logger.error(
                    "Couldn't cancel on woocommerce for: " + str(order[0])
                    + "\nError: " + str(e.args))

        elif order[14] == 1:  # Shopify Cancelled
            try:
                shopify_cancel(order)
            except Exception as e:
                logger.error(
                    "Couldn't cancel on Shopify for: " + str(order[0])
                    + "\nError: " + str(e.args))

        elif order[3] == 'LOTUSORGANICS':
            try:
                lotus_organics_update(order, "RTO")
            except Exception as e:
                pass

        elif order[14] == 7:  # Easyecom RTO
            try:
                update_easyecom_status(order, 9)
            except Exception as e:
                logger.error("Couldn't update Easyecom for: " + str(order[0])
                             + "\nError: " + str(e.args))

        elif order[14] == 8:  # Bikayi RTO
            try:
                update_bikayi_status(order, "RTO")
            except Exception as e:
                logger.error("Couldn't update Bikayi for: " + str(order[0])
                             + "\nError: " + str(e.args))


def update_easyecom_status(order, status_id):
    create_fulfillment_url = "%s/Carrier/updateTrackingStatus?api_token=%s" % (order[9], order[7])
    ful_header = {'Content-Type': 'application/json'}
    fulfil_data = {
        "api_token": order[7],
        "current_shipment_status_id": status_id,
        "awb": order[1],
    }
    req_ful = requests.post(create_fulfillment_url, data=json.dumps(fulfil_data),
                            headers=ful_header)


def update_bikayi_status(order, status):
    bikayi_update_url = """https://asia-south1-bikai-d5ee5.cloudfunctions.net/platformPartnerFunctions-updateOrder"""
    key = "3f638d4ff80defb82109951b9638fae3fe0ff8a2d6dc20ed8c493783"
    secret = "6e130520777eb175c300aefdfc1270a4f9a57f2309451311ad3fdcfb"
    timestamp = (datetime.utcnow()+timedelta(hours=5.5)).strftime("%s")
    req_body = {"appId": "WAREIQ",
                "merchantId": order[3].split("_")[1],
                "timestamp": timestamp,
                "orderId": str(order[12]),
                "status": status,
                "trackingLink":"https://webapp.wareiq.com/tracking/"+order[1],
                "notes": status,
                "wayBill": order[1]
                }
    signature = hmac.new(bytes(secret.encode()),
                         (key.encode() + "|".encode() + base64.b64encode(
                             json.dumps(req_body).replace(" ", "").encode())),
                         hashlib.sha256).hexdigest()
    headers = {"Content-Type": "application/json",
               "authorization": signature}
    data = requests.post(bikayi_update_url, headers=headers, data=json.dumps(req_body)).json()


def ecom_express_convert_xml_dict(elem):
    req_obj = dict()
    for elem2 in elem['field']:
        req_obj[elem2['@name']] = None
        if '#text' in elem2:
            req_obj[elem2['@name']] = elem2['#text']
        elif 'object' in elem2:
            if type(elem2['object']) == list:
                scan_list = list()
                for obj in elem2['object']:
                    scan_obj = dict()
                    for newobj in obj['field']:
                        scan_obj[newobj['@name']] = None
                        if '#text' in newobj:
                            scan_obj[newobj['@name']] = newobj['#text']
                    scan_list.append(scan_obj)
                req_obj[elem2['@name']] = scan_list
            else:
                req_obj[elem2['@name']] = elem2['object']

    return req_obj