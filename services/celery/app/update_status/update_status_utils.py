import logging, boto3, requests, json
from datetime import datetime, timedelta
from time import sleep
from .queries import *
from courier_config import config 
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from .order_shipped import order_shipped
from .function import update_delivered_on_channels, update_rto_on_channels, update_picked_on_channels, verification_text
from app.db_utils import UrlShortner

logger = logging.getLogger()
logger.setLevel(logging.INFO)

RAVEN_URL = "https://api.ravenapp.dev/v1/apps/ccaaf889-232e-49df-aeb8-869e3153509d/events/send"
RAVEN_HEADERS = {"Content-Type": "application/json", "Authorization": "AuthKey K4noY3GgzaW8OEedfZWAOyg+AmKZTsqO/h/8Y4LVtFA="}

email_client = boto3.client('ses', region_name="us-east-1", aws_access_key_id='AKIAWRT2R3KC3YZUBFXY',
    aws_secret_access_key='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')

class OrderUpdateCourier:
    def __init__(self, courier, connection):
        self.id = courier.id
        self.name = courier.courier_name
        self.api_key = courier.api_key
        self.api_password = courier.api_password
        self.connection = connection,
        self.cursor = connection.cursor()
    
    def get_dict(self):
        return {"id": self.id, "name": self.name, "api_key": self.api_key, "api_password": self.api_password}
    
    def request_status_from_courier(self, orders):
        orders_dict = dict()
        requested_ship_data = list()
        exotel_idx = 0
        exotel_sms_data = {
            'From': 'LM-WAREIQ'
        }

        if self.id == 8:
            #Delhivery Flow
            chunks = [orders[x:x + 500] for x in range(0, len(orders), 500)]
            for some_orders in chunks:
                awb_string = ""
                for order in some_orders:
                    orders_dict[order[1]] = order
                    awb_string += order[1] + ","
                
                awb_string = awb_string.rstrip(',')
                check_status_url = "https://track.delhivery.com/api/status/packages/json/?waybill=%s&token=%s" % (awb_string, self.api_key)
                req = requests.get(check_status_url)

                try:
                    requested_ship_data += req.json()['ShipmentData']
                except Exception as e:
                    logger.error("Status Tracking Failed for: " + awb_string + "\nError: " + str(e.args[0]))
                    if e.args[0] == 'ShipmentData':
                        if len(some_orders)>25:
                            smaller_chunks = [some_orders[x:x + 20] for x in range(0, len(some_orders), 20)]
                            chunks += smaller_chunks
                        sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                        sms_body_key = "Messages[%s][Body]" % str(exotel_idx)
                        sms_body_key_data = "Status Update Fail Alert"
                        customer_phone = "08750108744"
                        exotel_sms_data[sms_to_key] = customer_phone
                        exotel_sms_data[sms_body_key] = sms_body_key_data
                        exotel_idx += 1
                    continue
            
            return requested_ship_data, orders_dict, exotel_idx, exotel_sms_data
    
    def get_courier_specific_order_data(self, order):
        if self.id == 8:
            new_status = order['Shipment']['Status']['Status']
            current_awb = order['Shipment']['AWB']
            return new_status, current_awb
    
    def convert_corier_status_to_wareiq_status(self, requested_order, existing_order):
        new_status = dict()
        if self.id == 8:
            #Delhivery map
            for each_scan in requested_order['Shipment']['Scans']:
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

                if to_record_status not in new_status:
                    new_status[to_record_status] = (existing_order[0], self.id,
                                                        existing_order[10],
                                                        each_scan['ScanDetail']['ScanType'],
                                                        to_record_status,
                                                        each_scan['ScanDetail']['Instructions'],
                                                        each_scan['ScanDetail']['ScannedLocation'],
                                                        each_scan['ScanDetail']['CityLocation'],
                                                        status_time)
                elif to_record_status == 'In Transit' and new_status[to_record_status][
                    8] < status_time:
                    new_status[to_record_status] = (existing_order[0], self.id,
                                                        existing_order[10],
                                                        each_scan['ScanDetail']['ScanType'],
                                                        to_record_status,
                                                        each_scan['ScanDetail']['Instructions'],
                                                        each_scan['ScanDetail']['ScannedLocation'],
                                                        each_scan['ScanDetail']['CityLocation'],
                                                        status_time)
            return new_status
    
    def update_shipment_data(self, requested_order, new_status, existing_order, current_awb):
        return_object = {"type": "continue", "data": {}}
        if self.id == 8:
            #Delhivery logic
            if new_status == "Manifested":
                return return_object

            new_status = new_status.upper()
            if (existing_order[2]=='CANCELED' and new_status!='IN TRANSIT') or new_status in ('READY TO SHIP', 'NOT PICKED', 'PICKUP REQUESTED'):
                return return_object

            status_type = requested_order['Shipment']['Status']['StatusType']
            status_detail = None
            status_code = None
            if new_status == "PENDING":
                status_code = requested_order['Shipment']['Scans'][-1]['ScanDetail']['StatusCode']

            edd = requested_order['Shipment']['expectedDate']
            if edd:
                edd = datetime.strptime(edd, '%Y-%m-%dT%H:%M:%S')
                if datetime.utcnow().hour < 4:
                    self.cursor.execute("UPDATE shipments SET edd=%s WHERE awb=%s", (edd, current_awb))
                    self.cursor.execute("UPDATE shipments SET pdd=%s WHERE awb=%s and pdd is null", (edd, current_awb))
            
            return_object["type"] = None
            return_object["data"] = {"status_type": status_type, "status_detail": status_detail, "status_code": status_code, "edd": edd}
            return return_object
    
    def send_delivered_update(self, new_status, existing_order, current_awb, customer_phone):
        if self.id == 8:
            update_delivered_on_channels(existing_order)
            webhook_updates(existing_order, self.cursor, new_status, "Shipment Delivered", "", (datetime.utcnow()+timedelta(hours=5.5)).strftime("%Y-%m-%d %H:%M:%S"))
            tracking_link = "https://webapp.wareiq.com/tracking/" + current_awb
            tracking_link = UrlShortner.get_short_url(tracking_link, self.cursor)
            send_delivered_event(customer_phone, existing_order, self.name, tracking_link)
    
    def send_rto_update(self, new_status, existing_order):
        if self.id == 8:
            update_rto_on_channels(existing_order)
            webhook_updates(existing_order, self.cursor, new_status, "Shipment RTO", "", (datetime.utcnow()+timedelta(hours=5.5)).strftime("%Y-%m-%d %H:%M:%S"))
    
    def send_new_to_transit_update(self, pickup_count, pickup_dict, new_status, existing_order, current_awb, status_obj, customer_phone):
        if self.id == 8:
            pickup_count += 1
            if existing_order[11] not in pickup_dict:
                pickup_dict[existing_order[11]] = 1
            else:
                pickup_dict[existing_order[11]] += 1
            
            time_now = datetime.utcnow() + timedelta(hours=5.5)
            self.cursor.execute("UPDATE order_pickups SET picked=%s, pickup_time=%s WHERE order_id=%s",
                        (True, time_now, existing_order[0]))

            update_picked_on_channels(existing_order, self.cursor, courier=self.get_dict())
            webhook_updates(existing_order, self.cursor, new_status, "Shipment Picked Up", "", (datetime.utcnow()+timedelta(hours=5.5)).strftime("%Y-%m-%d %H:%M:%S"))

            if status_obj['data']['edd']:
                self.cursor.execute("UPDATE shipments SET pdd=%s WHERE awb=%s", (status_obj['data']['edd'], current_awb))

            tracking_link = "https://webapp.wareiq.com/tracking/" + current_awb
            tracking_link = UrlShortner.get_short_url(tracking_link, self.cursor)
            send_shipped_event(customer_phone, existing_order[19], existing_order,
                            status_obj['data']['edd'].strftime('%-d %b') if status_obj['data']['edd'] else "", self.name, tracking_link)
            
            return pickup_count, pickup_dict

    def send_pending_update(self, new_status, status_code, existing_order):
        if self.id == 8:
            #Delhivery logic
            if new_status == 'PENDING' and status_code in config[self.id]['status_mapping']:
                try:  # NDR check text
                    ndr_reason = config[self.id]['status_mapping'][status_code]
                    verification_text(existing_order, self.cursor, ndr_reason=ndr_reason)
                    webhook_updates(existing_order, self.cursor, new_status, "", "",(datetime.utcnow() + timedelta(hours=5.5)).strftime("%Y-%m-%d %H:%M:%S"), ndr_id=ndr_reason)
                except Exception as e:
                    logger.error(
                        "NDR confirmation not sent. Order id: " + str(existing_order[0]))
    
    def courier_specific_status_updates(self, new_status, existing_order, exotel_idx, exotel_sms_data, customer_phone, current_awb):
        if self.id == 8:
            #Delhivery logic
            if new_status == 'DTO':
                webhook_updates(existing_order, self.cursor, new_status, "Shipment delivered to origin", "", (datetime.utcnow()+timedelta(hours=5.5)).strftime("%Y-%m-%d %H:%M:%S"))
                sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                sms_body_key = "Messages[%s][Body]" % str(exotel_idx)
                exotel_sms_data[sms_to_key] = customer_phone
                exotel_sms_data[sms_body_key] = "Delivered: Your %s order via Delhivery to seller - https://webapp.wareiq.com/tracking/%s . Powered by WareIQ" % (existing_order[[20]], current_awb)
                exotel_idx += 1

            if existing_order[2] in ('SCHEDULED', 'DISPATCHED') and new_status == 'IN TRANSIT' and existing_order[13].lower() == 'pickup':
                sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                sms_body_key = "Messages[%s][Body]" % str(exotel_idx)
                exotel_sms_data[sms_to_key] = customer_phone
                exotel_sms_data[sms_body_key] = "Picked: Your %s order via Delhivery - https://webapp.wareiq.com/tracking/%s . Powered by WareIQ" % (existing_order[[20]], current_awb)
                exotel_idx += 1
                webhook_updates(existing_order, self.cursor, "DTO "+new_status, "Shipment picked from customer", "", (datetime.utcnow()+timedelta(hours=5.5)).strftime("%Y-%m-%d %H:%M:%S"))
    
    def update_status(self):
        pickup_count = 0 
        pickup_dict = dict()

        self.cursor.execute(get_status_update_orders_query % str(self.id))
        #Fech orders objects - [id (orders), awb, status, client_prefix, customer_phone, order_id_channel_unique, 
        #                       channel_fulfillment_id, api_key (client_channel), api_password, (client_channel), 
        #                       shop_url, id (shipments), pickup_data_id, channel_order_id, payment_mode, channel_id, 
        #                       location_id, item_list, sku_quan_list, customer_name, customer_email, client_name, 
        #                       client_logo, custom_email_subject, courier_id, theme_color, unique_parameter, mark_shipped, 
        #                       shipped_status, mark_invoiced, invoiced_status, mark_delivered, delivered_status, 
        #                       mark_returned, returned_status, id (client_channel), amount, warehouse_prefix, 
        #                       verify_ndr, webhook_id]
        active_orders = self.cursor.fetchall()

        requested_ship_data, orders_dict, exotel_idx, exotel_sms_data = self.request_status_from_courier(self.id, active_orders)
        logger.info("Count of {0} packages: ".format(self.name) + str(len(requested_ship_data)))

        for requested_order in requested_ship_data:
            try:
                new_status, current_awb = self.getcourier_specific_order_data(requested_order)
                try:
                    #Tuple of (id (orders), id (shipments), id (courier))
                    order_status_tuple = (orders_dict[current_awb][0], orders_dict[current_awb][10], self.id)
                    self.cursor.execute(select_statuses_query, order_status_tuple)
                    #Fetch status objects - [id, status_code, status, status_text, location, status_time, location_city]
                    all_scans = self.cursor.fetchall()
                    all_scans = dict()
                    for scan in all_scans:
                        all_scans[scan[2]] = scan

                    new_status = self.convert_corier_status_to_wareiq_status(requested_order, orders_dict[current_awb])
                    for status_key, status_value in new_status.items():
                        if status_key not in all_scans:
                            self.cursor.execute("INSERT INTO order_status (order_id, courier_id, shipment_id, "
                                        "status_code, status, status_text, location, location_city, "
                                        "status_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);",
                                        status_value)

                        elif status_key == 'In Transit' and status_value[8] > all_scans[status_key][5]:
                            self.cursor.execute("UPDATE order_status SET location=%s, location_city=%s, status_time=%s"
                                        " WHERE id=%s;", (status_value[6], status_value[7], status_value[8],
                                                        all_scans[status_key][0]))
                except Exception as e:
                    logger.error(
                        "Open status failed for id: " + str(orders_dict[current_awb][0]) + "\nErr: " + str(e.args[0]))
                
                status_obj = self.update_shipment_data(requested_order, new_status, orders_dict[current_awb], current_awb)
                if status_obj['continue']:
                    continue

                client_name = orders_dict[current_awb][20]
                customer_phone = orders_dict[current_awb][4].replace(" ", "")
                customer_phone = "0" + customer_phone[-10:]

                if new_status == 'DELIVERED':
                    self.send_delivered_update(new_status, orders_dict[current_awb], current_awb, customer_phone)
                
                if new_status == 'RTO':
                    self.send_rto_update(new_status, orders_dict[current_awb])
                
                if orders_dict[current_awb][2] in ('READY TO SHIP', 'PICKUP REQUESTED', 'NOT PICKED') and new_status == 'IN TRANSIT':
                    pickup_count, pickup_dict = self.send_new_to_transit_update(pickup_count, pickup_dict, new_status, orders_dict[current_awb], current_awb, status_obj, customer_phone)
                    
                if orders_dict[current_awb][2] != new_status:
                    status_update_tuple = (new_status, status_obj['data']['status_type'], status_obj['data']['status_detail'], orders_dict[current_awb][0])
                    self.cursor.execute(order_status_update_query, status_update_tuple)
                    self.send_pending_update(new_status, status_obj['data']['status_code'], orders_dict[current_awb])
                
                self.courier_specific_status_updates(new_status, orders_dict[current_awb], exotel_idx, exotel_sms_data, customer_phone, current_awb)
                self.connection.commit()
            except Exception as e:
                logger.error("status update failed for " + str(orders_dict[current_awb][0]) + "    err:" + str(
                    e.args[0]))
        
        if pickup_count:
            logger.info("Total Picked: " + str(pickup_count) + "  Time: " + str(datetime.utcnow()))
            try:
                for key, value in pickup_dict.items():
                    logger.info("picked for pickup_id " + str(key) + ": " + str(value))
                    date_today = datetime.now().strftime('%Y-%m-%d')
                    pickup_count_tuple = (value, self.id, key, date_today)
                    self.cursor.execute(update_pickup_count_query, pickup_count_tuple)
            except Exception as e:
                logger.error("Couldn't update pickup count for : " + str(e.args[0]))
        
        self.connection.commit()

def send_shipped_event(mobile, email, order, edd, courier_name, tracking_link=None):
    background_color = str(order[24]) if order[24] else "#B5D0EC"
    client_logo = str(order[21]) if order[21] else "https://logourls.s3.amazonaws.com/client_logos/logo_ane.png"
    client_name = str(order[20]) if order[20] else "WareIQ"
    email_title = str(order[22]) if order[22] else "Your order has been shipped!"
    order_id = str(order[12]) if order[12] else ""
    customer_name = str(order[18]) if order[18] else "Customer"

    edd = edd if edd else ""
    awb_number = str(order[1]) if order[1] else ""
    if not tracking_link:
        tracking_link = "http://webapp.wareiq.com/tracking/" + str(order[1])

    payload = {
        "event": "shipped",
        "user": {
            "mobile": mobile,
            "email": email if email else ""
        },
        "data": {
            "client_name": client_name,
            "customer_name": customer_name,
            "courier_name": courier_name,
            "tracking_link": tracking_link,
            "email_title": email_title,
            "order_id": order_id,
            "edd": edd,
            "awb_number": awb_number,
            "background_color": background_color,
            "client_logo": client_logo
        },
        "override": {
            "email": {
                "from": {
                    "name": client_name
                }
            }
        }
    }

    req = requests.post(RAVEN_URL, headers=RAVEN_HEADERS, data=json.dumps(payload))


def send_delivered_event(mobile, order, courier_name, tracking_link=None):
    client_name = str(order[20]) if order[20] else "WareIQ"
    if not tracking_link:
        tracking_link = "http://webapp.wareiq.com/tracking/" + str(order[1])
    payload = {
        "event": "delivered",
        "user": {
            "mobile": mobile,
        },
        "data": {
            "client_name": client_name,
            "courier_name": courier_name,
            "tracking_link": tracking_link
        }
    }

    req = requests.post(RAVEN_URL, headers=RAVEN_HEADERS, data=json.dumps(payload))


def send_picked_rvp_event(mobile, order, courier_name, tracking_link=None):
    client_name = str(order[20]) if order[20] else "WareIQ"
    if not tracking_link:
        tracking_link = "http://webapp.wareiq.com/tracking/" + str(order[1])
    payload = {
        "event": "picked_rvp",
        "user": {
            "mobile": mobile,
        },
        "data": {
            "client_name": client_name,
            "courier_name": courier_name,
            "tracking_link": tracking_link
        }
    }

    req = requests.post(RAVEN_URL, headers=RAVEN_HEADERS, data=json.dumps(payload))


def send_delivered_rvp_event(mobile, order, courier_name, tracking_link=None):
    client_name = str(order[20]) if order[20] else "WareIQ"
    if not tracking_link:
        tracking_link = "http://webapp.wareiq.com/tracking/" + str(order[1])
    payload = {
        "event": "delivered_rvp",
        "user": {
            "mobile": mobile,
        },
        "data": {
            "client_name": client_name,
            "courier_name": courier_name,
            "tracking_link": tracking_link
        }
    }

    req = requests.post(RAVEN_URL, headers=RAVEN_HEADERS, data=json.dumps(payload))


def send_ndr_event(mobile, order, verification_link):
    client_name = str(order[20]) if order[20] else "WareIQ"
    payload = {
        "event": "ndr_verification",
        "user": {
            "mobile": mobile,
        },
        "data": {
            "client_name": client_name,
            "verification_link": verification_link,
        }
    }

    req = requests.post(RAVEN_URL, headers=RAVEN_HEADERS, data=json.dumps(payload))


def send_bulk_emails(emails):
    logger.info("Sending Emails....count: " + str(len(emails)) + "  Time: " + str(datetime.utcnow()))
    for email in emails:
        try:
            response = email_client.send_raw_email(
                Source=email[0]['From'],
                Destinations=email[1],
                RawMessage={
                    'Data': email[0].as_string(),
                },
            )
            sleep(0.08)
        except Exception as e:
            logger.error("Couldn't send email: " + str(email['TO'])+"\nError: "+str(e.args[0]))


def create_email(order, edd, email):
    try:
        background_color = str(order[24]) if order[24] else "#B5D0EC"
        client_logo = str(order[21]) if order[21] else "https://logourls.s3.amazonaws.com/client_logos/logo_ane.png"
        client_name = str(order[20]) if order[20] else "WareIQ"
        email_title = str(order[22]) if order[22] else "Your order has been shipped!"
        order_id = str(order[12]) if order[12] else ""
        customer_name = str(order[18]) if order[18] else "Customer"
        courier_name = "WareIQ"
        if order[23] in (1,2,8,11,12):
            courier_name = "Delhivery"
        elif order[23] in (5,13):
            courier_name = "Xpressbees"
        elif order[23] in (4,):
            courier_name = "Shadowfax"
        elif order[23] in (9,):
            courier_name = "Bluedart"

        edd = edd if edd else ""
        awb_number = str(order[1]) if order[1] else ""
        tracking_link = "http://webapp.wareiq.com/tracking/" + str(order[1])

        html = order_shipped.replace('__CLIENT_LOGO__', client_logo)\
            .replace('__CLIENT_NAME__',  client_name)\
            .replace('__BACKGROUND_COLOR__', background_color)\
            .replace('__EMAIL_TITLE__', email_title)\
            .replace('__CUSTOMER_NAME__', customer_name)\
            .replace('__ORDER_ID__', order_id)\
            .replace('__COURIER_NAME__', courier_name)\
            .replace('__EDD__', edd)\
            .replace('__AWB_NUMBER__', awb_number).replace('__TRACKING_LINK__', tracking_link)

        # create message object instance
        msg = MIMEMultipart('alternative')

        recipients = [email]
        msg['From'] = "%s <noreply@wareiq.com>"%client_name
        msg['To'] = ", ".join(recipients)
        msg['Subject'] = email_title

        # write the HTML part

        part2 = MIMEText(html, "html")
        msg.attach(part2)
        return msg
    except Exception as e:
        logger.error("Couldn't send email: " + str(order[1]) + "\nError: " + str(e.args))
        return None


def webhook_updates(order, cur, status, status_text, location, status_time, ndr_id=None):
    if order[38]:
        try:
            if ndr_id:
                cur.execute("SELECT reason FROM ndr_reasons WHERE id=%s"%str(ndr_id))
                status_text = cur.fetchone()[0]
            cur.execute("SELECT webhook_url, header_key, header_value, webhook_secret, id FROM webhooks WHERE status='active' and client_prefix='%s'"%order[3])
            all_webhooks = cur.fetchall()
            for webhook in all_webhooks:
                try:
                    req_body = {"awb": order[1],
                                "status": status,
                                "event_time": status_time,
                                "location": location,
                                "order_id": order[12],
                                "status_text": status_text}

                    headers = {"Content-Type": "application/json"}
                    if webhook[1] and webhook[2]:
                        headers[webhook[1]] = webhook[2]

                    req = requests.post(webhook[0], headers=headers, json=req_body, timeout=5)
                    if not str(req.status_code).startswith('2'):
                        cur.execute("UPDATE webhooks SET fail_count=fail_count+1 WHERE id=%s" % str(webhook[4]))
                except Exception:
                    cur.execute("UPDATE webhooks SET fail_count=fail_count+1 WHERE id=%s"%str(webhook[4]))
                    pass
        except Exception:
            pass
