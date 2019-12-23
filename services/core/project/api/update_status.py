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
conn = psycopg2.connect(host="wareiq-core-prod.cvqssxsqruyc.us-east-1.rds.amazonaws.com", database="core_prod", user="postgres", password="postgres")
conn_2 = psycopg2.connect(host="wareiq-core-prod.cvqssxsqruyc.us-east-1.rds.amazonaws.com", database="users_prod", user="postgres", password="postgres")

def lambda_handler():
    cur = conn.cursor()
    cur_2 =conn_2.cursor()
    cur.execute(get_courier_id_and_key_query)
    for courier in cur.fetchall():
        try:
            if courier[1] in ("Delhivery", "Delhivery Surface Standard"):
                cur.execute(get_status_update_orders_query%str(courier[0]))
                all_orders = cur.fetchall()
                pickup_count = 0
                exotel_idx = 0
                exotel_sms_data = {
                    'From': 'LM-WAREIQ'
                }
                orders_dict = dict()
                awb_string = ""
                for order in all_orders:
                    orders_dict[order[1]] = order
                    awb_string += order[1]+","

                awb_string = awb_string.rstrip(',')

                check_status_url = "https://track.delhivery.com/api/status/packages/json/?waybill=%s&token=%s" % (awb_string, courier[2])
                req = requests.get(check_status_url).json()
                logger.info("Count of delhivery packages: "+str(len(req['ShipmentData'])))
                for ret_order in req['ShipmentData']:
                    try:
                        new_status = ret_order['Shipment']['Status']['Status']
                        current_awb = ret_order['Shipment']['AWB']
                        if new_status=="Manifested":
                            continue

                        new_status = new_status.upper()
                        status_type = ret_order['Shipment']['Status']['StatusType']
                        status_detail = None
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

                        """
                        if new_status=='DELIVERED':
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
                                exotel_sms_data[sms_body_key] = "Dear Customer, your %s order has been shipped via Delhivery with AWB number %s. It is expected to arrive by %s. Thank you for Ordering." % (
                                client_name[0], orders_dict[current_awb][1], edd)
                                exotel_idx += 1

                        if orders_dict[current_awb][2] != new_status:
                            status_update_tuple = (new_status, status_type, status_detail, orders_dict[current_awb][0])
                            cur.execute(order_status_update_query, status_update_tuple)

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
                        if new_status_temp == "READY TO SHIP":
                            continue
                        new_status = new_status_temp
                        edd = ret_order['promised_delivery_date']
                        if edd:
                            edd = datetime.strptime(edd, '%Y-%m-%dT%H:%M:%SZ')
                            cur.execute("UPDATE shipments SET edd=%s WHERE awb=%s", (edd, current_awb))

                        """
                        if new_status=='DELIVERED':
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

                conn.commit()
        except Exception as e:
            logger.error("Status update failed: "+str(e.args[0]))

    cur.close()


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
                            }