from hmac import new
import logging, boto3, requests, json, xmltodict
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
    
    def update_status(self):
        pickup_count = 0 
        pickup_dict = dict()

        self.cursor.execute(get_status_update_orders_query % str(self.id))
        active_orders = self.cursor.fetchall()

        requested_ship_data, orders_dict, exotel_idx, exotel_sms_data = self.request_status_from_courier(self.id, active_orders)
        logger.info("Count of {0} packages: ".format(self.name) + str(len(requested_ship_data)))

        for requested_order in requested_ship_data:
            try:
                check_obj = self.check_if_data_exists(requested_order)
                if check_obj['type'] == 'continue':
                    continue
                
                new_data = self.get_courier_specific_order_data(requested_order)
                if new_data['type'] == 'continue':
                    continue
                
                flags, new_data = self.set_courier_specific_flags(requested_order, new_data)
                if flags['type'] == 'continue':
                    continue

                try:
                    #Tuple of (id (orders), id (shipments), id (courier))
                    order_status_tuple = (orders_dict[new_data['current_awb']][0], orders_dict[new_data['current_awb']][10], self.id)
                    self.cursor.execute(select_statuses_query, order_status_tuple)

                    #Fetch status objects - [id, status_code, status, status_text, location, status_time, location_city]
                    all_scans = self.cursor.fetchall()
                    all_scans_dict = dict()
                    for scan in all_scans:
                        all_scans_dict[scan[2]] = scan

                    new_status_dict, flags = self.convert_courier_status_to_wareiq_status(requested_order, new_data, orders_dict[new_data['current_awb']], flags)
                    for status_key, status_value in new_status_dict.items():
                        if status_key not in all_scans_dict:
                            self.cursor.execute("INSERT INTO order_status (order_id, courier_id, shipment_id, "
                                        "status_code, status, status_text, location, location_city, "
                                        "status_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);",
                                        status_value)

                        elif status_key == 'In Transit' and status_value[8] > all_scans_dict[status_key][5]:
                            self.cursor.execute("UPDATE order_status SET location=%s, location_city=%s, status_time=%s"
                                        " WHERE id=%s;", (status_value[6], status_value[7], status_value[8],
                                                        all_scans_dict[status_key][0]))
                except Exception as e:
                    logger.error(
                        "Open status failed for id: " + str(orders_dict[new_data['current_awb']][0]) + "\nErr: " + str(e.args[0]))
                
                status_obj, new_data = self.update_shipment_data(requested_order, new_data, orders_dict[new_data['current_awb']], new_data['current_awb'], flags)
                if status_obj['type'] == 'continue':
                    continue

                client_name = orders_dict[new_data['current_awb']][20]
                customer_phone = orders_dict[new_data['current_awb']][4].replace(" ", "")
                customer_phone = "0" + customer_phone[-10:]

                if new_data['new_status'] == 'DELIVERED':
                    self.send_delivered_update(new_data, orders_dict[new_data['current_awb']], customer_phone)
                
                if new_data['new_status'] == 'RTO':
                    self.send_rto_update(new_data['new_status'], orders_dict[new_data['current_awb']])
                
                if orders_dict[new_data['current_awb']][2] in ('READY TO SHIP', 'PICKUP REQUESTED', 'NOT PICKED') and new_data['new_status'] == 'IN TRANSIT':
                    pickup_count, pickup_dict, status_obj = self.send_new_to_transit_update(pickup_count, pickup_dict, new_data, orders_dict[new_data['current_awb']], status_obj, customer_phone, flags)
                    if status_obj["type"] == "continue":
                        continue
                    
                if orders_dict[new_data['current_awb']][2] != new_data['new_status']:
                    status_update_tuple = (new_data['new_status'], status_obj['data']['status_type'], status_obj['data']['status_detail'], orders_dict[new_data['current_awb']][0])
                    self.cursor.execute(order_status_update_query, status_update_tuple)
                    self.send_pending_update(new_data, status_obj['data']['status_code'], orders_dict[new_data['current_awb']], requested_order)
                
                self.courier_specific_status_updates(new_data['new_status'], orders_dict[new_data['current_awb']], exotel_idx, exotel_sms_data, customer_phone, new_data['current_awb'])
                self.connection.commit()
            except Exception as e:
                logger.error("Status update failed for " + str(orders_dict[new_data['current_awb']][0]) + "    err:" + str(
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
    
    def request_status_from_courier(self, orders):
        orders_dict = dict()
        requested_ship_data = list()
        exotel_idx = 0
        exotel_sms_data = {
            'From': 'LM-WAREIQ'
        }

        if self.name == 'Delhivery':
            chunks = [orders[x:x + 500] for x in range(0, len(orders), 500)]
            for some_orders in chunks:
                awb_string = ""
                for order in some_orders:
                    orders_dict[order[1]] = order
                    awb_string += order[1] + ","
                
                awb_string = awb_string.rstrip(',')
                check_status_url = config[self.name].status_url % (awb_string, self.api_key)
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
        
        if self.name == 'Xpressbees':
            headers = {"Content-Type": "application/json"}
            chunks = [orders[x:x + 10] for x in range(0, len(orders), 10)]
            for some_orders in chunks:
                awb_string = ""
                for order in some_orders:
                    orders_dict[order[1]] = order
                    awb_string += order[1] + ","

                xpressbees_body = {"AWBNo": awb_string.rstrip(","), "XBkey": self.api_password.split("|")[1]}

                check_status_url = config[self.name].status_url
                req = requests.post(check_status_url, headers=headers, data=json.dumps(xpressbees_body)).json()
                requested_ship_data += req
        
        if self.name == 'Bluedart':
            chunks = [orders[x:x + 200] for x in range(0, len(orders), 200)]
            for some_orders in chunks:
                awb_string = ""
                for order in some_orders:
                    orders_dict[order[1]] = order
                    awb_string += order[1] + ","

                awb_string = awb_string.rstrip(',')
                req = None
                check_status_url = config[self.name].status_url % awb_string
                try:
                    req = requests.get(check_status_url)
                except Exception:
                    sleep(10)
                    try:
                        req = requests.get(check_status_url)
                    except Exception as e:
                        logger.error("Bluedart connection issue: " + "\nError: " + str(e.args[0]))
                        pass
                
                if req:
                    try:
                        req = xmltodict.parse(req.content)
                        if type(req['ShipmentData']['Shipment'])==list:
                            requested_ship_data += req['ShipmentData']['Shipment']
                        else:
                            requested_ship_data += [req['ShipmentData']['Shipment']]
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
                
        return requested_ship_data, orders_dict, exotel_idx, exotel_sms_data

    def check_if_data_exists(self, requested_order):
        return_object = {"type": None}
        if self.name == 'Xpressbees':
            if not requested_order['ShipmentSummary']:
                return_object["type"] = "continue"
                return return_object
        
        return return_object
    
    def get_courier_specific_order_data(self, order):
        new_data = {'type': 'continue'}
        if self.name == 'Delhivery':
            new_data['new_status'] = order['Shipment']['Status']['Status']
            new_data['current_awb'] = order['Shipment']['AWB']
        if self.name == 'Xpressbees':
            new_data['new_status'] = order['ShipmentSummary'][0]['StatusCode']
            new_data['current_awb'] = order['AWBNo']
        if self.name == 'Bluedart':
            new_data['current_awb'] = order['@WaybillNo'] if '@WaybillNo' in order else ""
            if order['StatusType']=='NF':
                return new_data
            try:
                new_data['scan_group'] = order['Scans']['ScanDetail'][0]['ScanGroupType']
                new_data['scan_code'] = order['Scans']['ScanDetail'][0]['ScanCode']
            except Exception as e:
                new_data['scan_group'] = order['Scans']['ScanDetail']['ScanGroupType']
                new_data['scan_code'] = order['Scans']['ScanDetail']['ScanCode']

            if new_data['scan_group'] not in config[self.name]['status_mapping'] or new_data['scan_code'] not in config[self.name]['status_mapping'][new_data['scan_group']]:
                return new_data

            new_data['new_status'] = config[self.name]['status_mapping'][new_data['scan_group']][new_data['scan_code']][0]
            new_data['current_awb'] = order['@WaybillNo']
        
        new_data['type'] = None
        return new_data
    
    def set_courier_specific_flags(self, requested_order, new_data):
        flags = {'type': 'continue'}
        if self.name == 'Xpressbees':
            flags['order_picked_check'] = False
        if self.name == 'Bluedart':
            flags['is_return'] = False
            if '@RefNo' in requested_order and str(requested_order['@RefNo']).startswith("074"):
                new_data['current_awb'] = str(str(requested_order['@RefNo']).split("-")[1]).strip()
                flags['is_return'] = True

            if flags['is_return'] and new_data['new_status']!='DELIVERED':
                return flags
        
        flags['type'] = None
        return flags, new_data
    
    def convert_courier_status_to_wareiq_status(self, requested_order, new_data, existing_order, flags):
        new_status = dict()
        if self.name == 'Delhivery':
            #Delhivery map
            for each_scan in requested_order['Shipment']['Scans']:
                status_time = each_scan['ScanDetail']['StatusDateTime']
                if status_time:
                    if len(status_time) == 19:
                        status_time = datetime.strptime(status_time, config[self.name]['status_time_format'])
                    else:
                        status_time = datetime.strptime(status_time, config[self.name]['status_time_format'] + '.%f')

                to_record_status = config[self.name]['status_mapper_fn'](each_scan)
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
        
        if self.name == 'Xpressbees':
            for each_scan in requested_order['ShipmentSummary']:
                if not each_scan.get('Location'):
                    continue
                status_time = each_scan['StatusDate'] + "T" + each_scan['StatusTime']
                if status_time:
                    status_time = datetime.strptime(status_time, config[self.name]['status_time_format'])

                to_record_status, flags = config[self.name]['status_mapper_fn'](each_scan, flags)
                if not to_record_status:
                    continue

                if to_record_status not in new_status:
                    new_status[to_record_status] = (existing_order[0], self.id,
                                                            existing_order[10],
                                                            config[self.name]['status_mapping'][
                                                                each_scan['StatusCode']][1],
                                                            to_record_status,
                                                            each_scan['Status'],
                                                            each_scan['Location'],
                                                            each_scan['Location'].split(', ')[1],
                                                            status_time)
                elif to_record_status == 'In Transit' and new_status[to_record_status][
                    8] < status_time:
                    new_status[to_record_status] = (existing_order[0], self.id,
                                                            existing_order[10],
                                                            config[self.name]['status_mapping'][
                                                                each_scan['StatusCode']][1],
                                                            to_record_status,
                                                            each_scan['Status'],
                                                            each_scan['Location'],
                                                            each_scan['Location'].split(', ')[1],
                                                            status_time)
        
        if self.name == 'Bluedart':
            if isinstance(requested_order['Scans']['ScanDetail'], list):
                scan_list = requested_order['Scans']['ScanDetail']
            else:
                scan_list = [requested_order['Scans']['ScanDetail']]
            for each_scan in scan_list:
                status_time = each_scan['ScanDate']+"T"+each_scan['ScanTime']
                if status_time:
                    status_time = datetime.strptime(status_time, '%d-%b-%YT%H:%M')
                
                to_record_status, flags = config[self.name]['status_mapper_fn'](each_scan, new_data['new_status'], flags)
                if not to_record_status:
                    continue

                if to_record_status not in new_status:
                    new_status[to_record_status] = (existing_order[0], self.id,
                                                            existing_order[10],
                                                            each_scan['ScanType'],
                                                            to_record_status,
                                                            each_scan['Scan'],
                                                            each_scan['ScannedLocation'],
                                                            each_scan['ScannedLocation'],
                                                            status_time)
                elif to_record_status == 'In Transit' and new_status[to_record_status][
                    8] < status_time and not flags['is_return']:
                    new_status[to_record_status] = (existing_order[0], self.id,
                                                            existing_order[10],
                                                            each_scan['ScanType'],
                                                            to_record_status,
                                                            each_scan['Scan'],
                                                            each_scan['ScannedLocation'],
                                                            each_scan['ScannedLocation'],
                                                            status_time)
        
        return new_status, flags
    
    def update_shipment_data(self, requested_order, new_data, existing_order, current_awb, flags):
        return_object = {"type": "continue", "data": {"status_type": None, "status_detail": None, "status_code": None, "edd": None}}
        if self.name == 'Delhivery':
            if new_data['new_status'] == "Manifested":
                return return_object

            new_data['new_status'] = new_data['new_status'].upper()
            if (existing_order[2]=='CANCELED' and new_data['new_status']!='IN TRANSIT') or new_data['new_status'] in ('READY TO SHIP', 'NOT PICKED', 'PICKUP REQUESTED'):
                return return_object

            return_object['status_type'] = requested_order['Shipment']['Status']['StatusType']
            if new_data['new_status'] == "PENDING":
                return_object['status_code'] = requested_order['Shipment']['Scans'][-1]['ScanDetail']['StatusCode']

            return_object['edd'] = requested_order['Shipment']['expectedDate']
        
        if self.name == 'Xpressbees':
            new_status_temp = config[self.name]['status_mapping'][new_data['new_status']][0].upper()
            try:
                return_object['status_type'] = config[self.name]['status_mapping'][new_data['new_status']][1]
            except KeyError:
                return_object['status_type'] = None
            
            if new_status_temp in ("READY TO SHIP", "PICKUP REQUESTED"):
                return return_object
            
            new_data['new_status'] = new_status_temp

            if existing_order[2]=='CANCELED' and new_data['new_status']!='IN TRANSIT':
                return return_object

            return_object['edd'] = requested_order['ShipmentSummary'][0].get('ExpectedDeliveryDate')
        
        if self.name == 'Bluedart':
            if flags['is_return'] and new_data['new_status']=='DELIVERED':
                new_data['new_status']='RTO'

            return_object['status_type'] = requested_order['StatusType']
            if new_data['new_status'] in ('NOT PICKED', 'READY TO SHIP', 'PICKUP REQUESTED'):
                return return_object
            return_object['status_detail'] = None
            return_object['status_code'] = new_data['scan_code']

            if existing_order[2]=='CANCELED' and new_data['new_status']!='IN TRANSIT':
                return return_object

            return_object['edd'] = requested_order['ExpectedDeliveryDate'] if 'ExpectedDeliveryDate' in requested_order else None
        
        if return_object['edd']:
            try:
                return_object['edd'] = datetime.strptime(return_object['edd'], config[self.name]['edd_time_format'])
                if datetime.utcnow().hour < 4:
                    self.cursor.execute("UPDATE shipments SET edd=%s WHERE awb=%s", (return_object['edd'], current_awb))
                    self.cursor.execute("UPDATE shipments SET pdd=%s WHERE awb=%s and pdd is null", (return_object['edd'], current_awb))
            except Exception as e:
                logger.error(str(e.args))
            
            return_object["type"] = None
        
        return return_object, new_data
    
    def send_delivered_update(self, new_data, existing_order, customer_phone):
        update_delivered_on_channels(existing_order)
        webhook_updates(existing_order, self.cursor, new_data['new_status'], "Shipment Delivered", "", (datetime.utcnow()+timedelta(hours=5.5)).strftime("%Y-%m-%d %H:%M:%S"))
        tracking_link = "https://webapp.wareiq.com/tracking/" + new_data['current_awb']
        tracking_link = UrlShortner.get_short_url(tracking_link, self.cursor)
        send_delivered_event(customer_phone, existing_order, self.name, tracking_link)
    
    def send_rto_update(self, new_status, existing_order):
        update_rto_on_channels(existing_order)
        webhook_updates(existing_order, self.cursor, new_status, "Shipment RTO", "", (datetime.utcnow()+timedelta(hours=5.5)).strftime("%Y-%m-%d %H:%M:%S"))
    
    def send_new_to_transit_update(self, pickup_count, pickup_dict, new_data, existing_order, status_obj, customer_phone, flags):
        if self.name == 'Xpressbees':
            if not flags['order_picked_check']:
                status_obj["type"] = 'continue'
                return pickup_count, pickup_dict, status_obj
        
        pickup_count += 1
        if existing_order[11] not in pickup_dict:
            pickup_dict[existing_order[11]] = 1
        else:
            pickup_dict[existing_order[11]] += 1
        
        time_now = datetime.utcnow() + timedelta(hours=5.5)
        self.cursor.execute("UPDATE order_pickups SET picked=%s, pickup_time=%s WHERE order_id=%s",
                    (True, time_now, existing_order[0]))

        update_picked_on_channels(existing_order, self.cursor, courier=self.get_dict())
        webhook_updates(existing_order, self.cursor, new_data['new_status'], "Shipment Picked Up", "", (datetime.utcnow()+timedelta(hours=5.5)).strftime("%Y-%m-%d %H:%M:%S"))

        if status_obj['data']['edd']:
            self.cursor.execute("UPDATE shipments SET pdd=%s WHERE awb=%s", (status_obj['data']['edd'], new_data['current_awb']))

        tracking_link = "https://webapp.wareiq.com/tracking/" + new_data['current_awb']
        tracking_link = UrlShortner.get_short_url(tracking_link, self.cursor)
        send_shipped_event(customer_phone, existing_order[19], existing_order,
                        status_obj['data']['edd'].strftime('%-d %b') if status_obj['data']['edd'] else "", self.name, tracking_link)
            
        return pickup_count, pickup_dict, status_obj

    def send_pending_update(self, new_data, status_code, existing_order, requested_order):
        try:
            ndr_reason = None
            if self.name == 'Delhivery':
                if new_data['new_status'] == 'PENDING' and status_code in config[self.name]['status_mapping']:
                    ndr_reason = config[self.name]['status_mapping'][status_code]
            if self.name == 'Xpressbees':
                if requested_order['ShipmentSummary'][0]['StatusCode'] == 'UD':
                    ndr_reason = config[self.name]['ndr_mapper_fn'](requested_order)
            if self.name == 'Bluedart':
                if new_data['new_status'] == 'PENDING' and status_code in config[self.name]['status_mapping'][new_data['scan_group']]:
                    ndr_reason = config[self.name]['status_mapping'][new_data['scan_group']][status_code][3]

            if ndr_reason:
                verification_text(existing_order, self.cursor, ndr_reason=ndr_reason)
                webhook_updates(existing_order, self.cursor, new_data['new_status'], "", "",(datetime.utcnow() + timedelta(hours=5.5)).strftime("%Y-%m-%d %H:%M:%S"), ndr_id=ndr_reason)
        except Exception as e:
            logger.error(
                "NDR confirmation not sent. Order id: " + str(existing_order[0]))
    
    def courier_specific_status_updates(self, new_status, existing_order, exotel_idx, exotel_sms_data, customer_phone, current_awb):
        if self.name == 'Delhivery':
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
