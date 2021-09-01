import logging, requests, json, xmltodict
import psycopg2
from datetime import datetime, timedelta
from time import sleep

from .queries import *
from .courier_config import config
from .update_status_utils import (
    update_delivered_on_channels,
    update_rto_on_channels,
    update_picked_on_channels,
    verification_text,
    ecom_express_convert_xml_dict,
    webhook_updates,
    send_delivered_event,
    send_shipped_event,
)
from ..db_utils import DbConnection, UrlShortner


logger = logging.getLogger()
logger.setLevel(logging.INFO)

# conn = DbConnection.get_db_connection_instance()
conn = psycopg2.connect(
    host="localhost", database="core_prod", user="read_only_user", password="Wsdg546hu66", port="5431"
)


def update_status(sync_ext=None):
    cur = conn.cursor()
    # Fetch courier objects - [id, courier_name, api_key, api_password]
    if not sync_ext:
        cur.execute(get_courier_id_and_key_query + " where integrated is true and id=42;")
    else:
        cur.execute(get_courier_id_and_key_query + " where integrated is not true;")

    for courier in cur.fetchall():
        try:
            update_obj = OrderUpdateCourier(courier, cur)
            update_obj.update_status()
        except Exception as e:
            logger.error("Status update failed: " + str(e.args[0]))

    cur.close()


class OrderUpdateCourier:
    """
    This class takes care of updating the status of active orders for a courier.
    """

    def __init__(self, courier, cursor):
        self.id = courier[0]
        if courier[1].startswith("Delhivery"):
            self.name = "Delhivery"
        elif courier[1].startswith("Xpressbees"):
            self.name = "Xpressbees"
        elif courier[1].startswith("Bluedart"):
            self.name = "Bluedart"
        elif courier[1].startswith("Ecom Express"):
            self.name = "Ecom Express"
        elif courier[1].startswith("Pidge"):
            self.name = "Pidge"
        elif courier[1].startswith("DTDC"):
            self.name = "DTDC"
        self.api_key = courier[2]
        self.api_password = courier[3]
        self.cursor = cursor

    def get_dict(self):
        return {"id": self.id, "name": self.name, "api_key": self.api_key, "api_password": self.api_password}

    def update_status(self):
        """
        Main function that checks and updates status of all active orders for a courier
        """
        pickup_count = 0
        pickup_dict = dict()

        self.cursor.execute(get_status_update_orders_query % str(self.id))
        active_orders = self.cursor.fetchall()

        # 1. In case the courier APIs need bulk AWBs for update, use this logic
        if config[self.name]["api_type"] == "bulk":
            requested_ship_data, orders_dict, exotel_idx, exotel_sms_data = self.request_bulk_status_from_courier(
                active_orders
            )
            logger.info("Count of {0} packages: ".format(self.name) + str(len(requested_ship_data)))
        else:
            access_obj = self.get_access_credentials()
            if access_obj["type"] == "continue":
                return
            requested_ship_data, orders_dict, exotel_idx, exotel_sms_data = active_orders, {}, 0, {"From": "LM-WAREIQ"}

        for requested_order in requested_ship_data:
            try:
                # 2. In case the courier APIs need individual AWBs for update, use this logic
                if config[self.name]["api_type"] == "individual":
                    (
                        requested_individual_order,
                        exotel_idx,
                        exotel_sms_data,
                    ) = self.request_individual_status_from_courier(requested_order, access_obj)
                else:
                    requested_individual_order = requested_order

                # 3. Perform any courier specific sanity checks on data recieved in this section
                check_obj = self.check_if_data_exists(requested_individual_order)
                if check_obj["type"] == "continue":
                    continue

                # 4. Based on the API design, extract relevant information from API response
                order_new_status = self.get_courier_specific_order_data(requested_individual_order)
                if order_new_status["type"] == "continue":
                    continue

                # 5. In this section, generate any courier specific flags to be used later in their corresponding logic
                flags, order_new_status = self.set_courier_specific_flags(
                    requested_individual_order, order_new_status, orders_dict, check_obj
                )
                if flags["type"] == "continue":
                    continue

                if config[self.name]["api_type"] == "individual":
                    order = requested_order
                else:
                    order = orders_dict[order_new_status["current_awb"]]

                try:
                    # 6. Get current scans information for an order
                    # Tuple of (id (orders), id (shipments), id (courier))
                    order_status_tuple = (order[0], order[10], self.id)
                    self.cursor.execute(select_statuses_query, order_status_tuple)

                    # Fetch status objects - [id, status_code, status, status_text, location, status_time, location_city]
                    all_scans = self.cursor.fetchall()
                    all_scans_dict = dict()
                    for scan in all_scans:
                        all_scans_dict[scan[2]] = scan

                    # 7. Convert courier specific order status into a uniform WareIQ standard
                    new_status_dict, flags = self.convert_courier_status_to_wareiq_status(
                        requested_individual_order, order_new_status, order, flags
                    )

                    # 8. Update the status of the orders under certain conditions only
                    if new_status_dict:
                        for status_key, status_value in new_status_dict.items():
                            if status_key not in all_scans_dict:
                                self.cursor.execute(
                                    "INSERT INTO order_status (order_id, courier_id, shipment_id, "
                                    "status_code, status, status_text, location, location_city, "
                                    "status_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);",
                                    status_value,
                                )

                            elif status_key == "In Transit" and status_value[8] > all_scans_dict[status_key][5]:
                                self.cursor.execute(
                                    "UPDATE order_status SET location=%s, location_city=%s, status_time=%s"
                                    " WHERE id=%s;",
                                    (status_value[6], status_value[7], status_value[8], all_scans_dict[status_key][0]),
                                )
                    else:
                        continue
                except Exception as e:
                    logger.error("Open status failed for id: " + str(order[0]) + "\nErr: " + str(e.args[0]))

                # 9. Update shipment status of the order
                status_obj, order_new_status = self.update_shipment_data(
                    requested_individual_order,
                    order_new_status,
                    order,
                    order_new_status["current_awb"],
                    flags,
                    check_obj,
                )
                if status_obj["type"] == "continue":
                    continue

                customer_phone = order[4].replace(" ", "")
                customer_phone = "0" + customer_phone[-10:]

                # 10. Update webhooks if the new status is delivered
                # if order_new_status["new_status"] == "DELIVERED":
                #     self.send_delivered_update(order_new_status, order, customer_phone)

                # 11. Update webhooks if the new status is RTO
                # if order_new_status["new_status"] == "RTO":
                #     self.send_rto_update(order_new_status["new_status"], order)

                # 12. Update webhooks if the new status is in transit and previous status is not in transit
                if (
                    order[2].upper() in ("READY TO SHIP", "PICKUP REQUESTED", "NOT PICKED")
                    and order_new_status["new_status"] == "IN TRANSIT"
                ):
                    pickup_count, pickup_dict, status_obj = self.send_new_to_transit_update(
                        pickup_count, pickup_dict, order_new_status, order, status_obj, customer_phone, flags
                    )
                    if status_obj["type"] == "continue":
                        continue

                # 13. Update webhooks if the new status is pending
                if order[2].upper() != order_new_status["new_status"]:
                    status_update_tuple = (
                        order_new_status["new_status"],
                        status_obj["data"]["status_type"],
                        status_obj["data"]["status_detail"],
                        order[0],
                    )
                    self.cursor.execute(order_status_update_query, status_update_tuple)
                    # self.send_pending_update(order_new_status, status_obj["data"]["status_code"], order, requested_order)

                # 14. Update webhooks for courier specific updates
                # exotel_idx, exotel_sms_data = self.courier_specific_status_updates(
                #     order_new_status["new_status"], order, exotel_idx, exotel_sms_data, customer_phone, order_new_status["current_awb"]
                # )
                # conn.commit()
            except Exception as e:
                logger.error("Status update failed for " + str(requested_order[0]) + "    err:" + str(e.args[0]))

        # 15. Send SMS if necessary
        # self.send_exotel_messages(exotel_idx, exotel_sms_data)
        if pickup_count:
            logger.info("Total Picked: " + str(pickup_count) + "  Time: " + str(datetime.utcnow()))
            try:
                for key, value in pickup_dict.items():
                    logger.info("picked for pickup_id " + str(key) + ": " + str(value))
                    date_today = datetime.now().strftime("%Y-%m-%d")
                    pickup_count_tuple = (value, self.id, key, date_today)
                    self.cursor.execute(update_pickup_count_query, pickup_count_tuple)
            except Exception as e:
                logger.error("Couldn't update pickup count for : " + str(e.args[0]))

        # conn.commit()

    def get_access_credentials(self):
        """
        This function generates additional auth requirements to access status API
        """
        access_obj = {"type": None}

        if self.name == "DTDC":
            self.cursor.execute(
                "SELECT api_credential_1 api_credential_2 FROM master_couriers WHERE id={0}".format(self.id)
            )
            [username, password] = self.cursor.fetchall()[0]

            url = config[self.name]["auth_token_api"] + "?username={0}&password={1}".format(username, password)
            response = requests.get(url, payload={})
            try:
                access_obj["auth-token"] = response.text
            except Exception as e:
                access_obj["type"] = "continue"
                logger.error("Error: " + str(e.args[0]))

        return access_obj

    def try_check_status_url(self, check_status_url, api_type, headers, data):
        """
        This function executes courier API dependent request pattern
        """
        try:
            req = requests.request(api_type, check_status_url, headers=headers, data=data)
        except Exception:
            sleep(10)
            try:
                req = requests.request(api_type, check_status_url, headers=headers, data=data)
            except Exception as e:
                logger.error("{0} connection issue: ".format(self.id) + "\nError: " + str(e.args[0]))
                pass

        return req

    def alert_wareiq_team(self, exotel_idx, exotel_sms_data):
        """
        Alert WareIQ team on errors with usage of courier APIs
        """
        sms_to_key = "Messages[%s][To]" % str(exotel_idx)
        sms_body_key = "Messages[%s][Body]" % str(exotel_idx)
        sms_body_key_data = "Status Update Fail Alert"
        customer_phone = "08750108744"
        exotel_sms_data[sms_to_key] = customer_phone
        exotel_sms_data[sms_body_key] = sms_body_key_data
        exotel_idx += 1

        return exotel_idx, exotel_sms_data

    def request_bulk_status_from_courier(self, orders):
        """
        Function for couriers who require sending AWBs in bulk to their API.
        Depending on the API design of each courier, logic is segregated.
        """
        orders_dict = dict()
        requested_ship_data = list()
        exotel_idx = 0
        exotel_sms_data = {"From": "LM-WAREIQ"}

        if self.name == "Delhivery":
            chunks = [orders[x : x + 500] for x in range(0, len(orders), 500)]
            for some_orders in chunks:
                awb_string = ""
                for order in some_orders:
                    orders_dict[order[1]] = order
                    awb_string += order[1] + ","

                awb_string = awb_string.rstrip(",")
                check_status_url = config[self.name]["status_url"] % (awb_string, self.api_key)
                req = self.try_check_status_url(check_status_url, "GET", {}, {})

                if req:
                    try:
                        requested_ship_data += req.json()["ShipmentData"]
                    except Exception as e:
                        logger.error("Status Tracking Failed for: " + awb_string + "\nError: " + str(e.args[0]))
                        if e.args[0] == "ShipmentData":
                            if len(some_orders) > 25:
                                smaller_chunks = [some_orders[x : x + 20] for x in range(0, len(some_orders), 20)]
                                chunks += smaller_chunks
                            exotel_idx, exotel_sms_data = self.alert_wareiq_team(exotel_idx, exotel_sms_data)

        if self.name == "Xpressbees":
            headers = {"Content-Type": "application/json"}
            chunks = [orders[x : x + 10] for x in range(0, len(orders), 10)]
            for some_orders in chunks:
                awb_string = ""
                for order in some_orders:
                    orders_dict[order[1]] = order
                    awb_string += order[1] + ","

                xpressbees_body = {"AWBNo": awb_string.rstrip(","), "XBkey": self.api_password.split("|")[1]}

                check_status_url = config[self.name]["status_url"]
                req = self.try_check_status_url(check_status_url, "POST", headers, json.dumps(xpressbees_body))
                req = requests.post(check_status_url, headers=headers, data=json.dumps(xpressbees_body)).json()
                requested_ship_data += req

        if self.name == "Bluedart":
            chunks = [orders[x : x + 200] for x in range(0, len(orders), 200)]
            for some_orders in chunks:
                awb_string = ""
                for order in some_orders:
                    orders_dict[order[1]] = order
                    awb_string += order[1] + ","

                awb_string = awb_string.rstrip(",")
                req = None
                check_status_url = config[self.name]["status_url"] % awb_string
                req = self.try_check_status_url(check_status_url, "GET", {}, {})

                if req:
                    try:
                        req = xmltodict.parse(req.content)
                        if type(req["ShipmentData"]["Shipment"]) == list:
                            requested_ship_data += req["ShipmentData"]["Shipment"]
                        else:
                            requested_ship_data += [req["ShipmentData"]["Shipment"]]
                    except Exception as e:
                        logger.error("Status Tracking Failed for: " + awb_string + "\nError: " + str(e.args[0]))
                        if e.args[0] == "ShipmentData":
                            exotel_idx, exotel_sms_data = self.alert_wareiq_team(exotel_idx, exotel_sms_data)

        if self.name == "Ecom Express":
            chunks = [orders[x : x + 100] for x in range(0, len(orders), 100)]
            for some_orders in chunks:
                awb_string = ""
                for order in some_orders:
                    orders_dict[order[1]] = order
                    awb_string += order[1] + ","

                awb_string = awb_string.rstrip(",")

                check_status_url = config[self.name]["status_url"] % (awb_string, self.api_key, self.api_password)
                req = self.try_check_status_url(check_status_url, "GET", {}, {})
                try:
                    req = xmltodict.parse(req.content)
                    if type(req["ecomexpress-objects"]["object"]) == list:
                        req_data = list()
                        for elem in req["ecomexpress-objects"]["object"]:
                            req_obj = ecom_express_convert_xml_dict(elem)
                            req_data.append(req_obj)
                    else:
                        req_data = [ecom_express_convert_xml_dict(req["ecomexpress-objects"]["object"])]

                    requested_ship_data += req_data

                except Exception as e:
                    logger.error("Status Tracking Failed for: " + awb_string + "\nError: " + str(e.args[0]))
                    if e.args[0] == "ShipmentData":
                        exotel_idx, exotel_sms_data = self.alert_wareiq_team(exotel_idx, exotel_sms_data)

        return requested_ship_data, orders_dict, exotel_idx, exotel_sms_data

    def request_individual_status_from_courier(self, order, access_obj):
        """
        Function for couriers who require sending AWBs individually to their API.
        Depending on the API design of each courier, logic is segregated.
        """
        exotel_idx = 0
        exotel_sms_data = {"From": "LM-WAREIQ"}

        if self.name == "Pidge":
            headers = {
                "Authorization": "Bearer " + self.api_key,
                "Content-Type": "application/json",
                "platform": "Postman",
                "deviceId": "abc",
                "buildNumber": "123",
            }
            requested_order = self.try_check_status_url(
                config[self.name]["status_url"] + str(order[0]), "GET", headers, {}
            ).json()

        if self.name == "DTDC":
            headers = {
                "x-access-token": access_obj["auth_token"],
                "Content-Type": "application/json",
            }
            payload = json.dumps({"trkType": "cnno", "strcnno": str(order[1]), "addtnlDtl": "N"})
            requested_order = self.try_check_status_url(config[self.name]["status_url"], "GET", headers, payload).json()

        return requested_order, exotel_idx, exotel_sms_data

    def check_if_data_exists(self, requested_order):
        """
        Once the data is recieved from the courier API, sanity checks are
        performed wherever necessary.
        """
        return_object = {"type": None}
        if self.name == "Xpressbees":
            if not requested_order["ShipmentSummary"]:
                return_object["type"] = "continue"
                return return_object

        if self.name == "Pidge":
            payload = requested_order["data"]["current_status"]
            reason_code_number = payload.get("trip_status")

            if not reason_code_number:
                return_object["type"] = "continue"
                return return_object

            if payload.get("attempt_type") not in (10, 30, 40, 70):
                return_object["type"] = "continue"
                return return_object

            if reason_code_number not in (130, 150, 170, 190, 5):
                return_object["type"] = "continue"
                return return_object

            return_object["reason_code_number"] = reason_code_number

        if self.name == "DTDC":
            if requested_order["status"] == "FAILED":
                logger.error(
                    "Error: "
                    + requested_order["errorDetails"][1]["value"]
                    + " "
                    + requested_order["errorDetails"][0]["value"]
                )
                return_object["type"] = "continue"

        return return_object

    def get_courier_specific_order_data(self, order):
        """
        This function has logic to pickup right keys to extract information
        from the courier API response. Different couriers provide different
        kinds of information. Accordingly logic is segregated.
        """
        order_new_status = {"type": "continue"}
        if self.name == "Delhivery":
            order_new_status["new_status"] = order["Shipment"]["Status"]["Status"]
            order_new_status["current_awb"] = order["Shipment"]["AWB"]

        if self.name == "Xpressbees":
            order_new_status["new_status"] = order["ShipmentSummary"][0]["StatusCode"]
            order_new_status["current_awb"] = order["AWBNo"]

        if self.name == "Bluedart":
            if order["StatusType"] == "NF":
                return order_new_status
            order_new_status["current_awb"] = order["@WaybillNo"] if "@WaybillNo" in order else ""
            try:
                order_new_status["scan_group"] = order["Scans"]["ScanDetail"][0]["ScanGroupType"]
                order_new_status["scan_code"] = order["Scans"]["ScanDetail"][0]["ScanCode"]
            except Exception as e:
                order_new_status["scan_group"] = order["Scans"]["ScanDetail"]["ScanGroupType"]
                order_new_status["scan_code"] = order["Scans"]["ScanDetail"]["ScanCode"]

            if (
                order_new_status["scan_group"] not in config[self.name]["status_mapping"]
                or order_new_status["scan_code"]
                not in config[self.name]["status_mapping"][order_new_status["scan_group"]]
            ):
                return order_new_status

            order_new_status["new_status"] = config[self.name]["status_mapping"][order_new_status["scan_group"]][
                order_new_status["scan_code"]
            ][0]
            order_new_status["current_awb"] = order["@WaybillNo"]

        if self.name == "Ecom Express":
            order_new_status["scan_code"] = order["reason_code_number"]
            if order_new_status["scan_code"] not in config[self.name]["status_mapping"]:
                return order_new_status

            order_new_status["new_status"] = config[self.name]["status_mapping"][order_new_status["scan_code"]][0]
            order_new_status["current_awb"] = order["awb_number"]

        if self.name == "Pidge":
            order_new_status["current_awb"] = str(order["data"]["current_status"]["PBID"])

        if self.name == "DTDC":
            order_new_status["current_awb"] = order["trackHeader"]["strShipmentNo"]
            order_new_status["scan_code"] = order["trackHeader"]["strStatus"]
            order_new_status["new_status"] = config[self.name]["status_mapping"][order_new_status["scan_code"]][0]

        order_new_status["type"] = None
        return order_new_status

    def set_courier_specific_flags(self, requested_order, order_new_status, orders_dict, check_obj):
        """
        Courier specific sanity checks and custom flags to be used later in their logic are
        performed here.
        """
        flags = {"type": "continue"}
        if self.name == "Xpressbees":
            flags["order_picked_check"] = False

        if self.name == "Bluedart":
            flags["is_return"] = False
            if "@RefNo" in requested_order and str(requested_order["@RefNo"]).startswith("074"):
                order_new_status["current_awb"] = str(str(requested_order["@RefNo"]).split("-")[1]).strip()
                flags["is_return"] = True

            if flags["is_return"] and order_new_status["new_status"] != "DELIVERED":
                return flags, order_new_status

        if self.name == "Ecom Express":
            if (
                orders_dict[order_new_status["current_awb"]][2] == "CANCELED"
                and order_new_status["new_status"] != "IN TRANSIT"
            ) or order_new_status["new_status"] in ("READY TO SHIP", "PICKUP REQUESTED"):
                return flags, order_new_status

        if self.name == "Pidge":
            payload = requested_order["data"]["current_status"]
            flags["is_return"] = False
            if payload.get("attempt_type") == 30:
                flags["is_return"] = True
            order_new_status["new_status"] = ""

            if check_obj["reason_code_number"] in config[self.name]["status_mapping"]:
                order_new_status["new_status"] = config[self.name]["status_mapping"][check_obj["reason_code_number"]][0]

            if not order_new_status["new_status"] or order_new_status["new_status"] in (
                "READY TO SHIP",
                "PICKUP REQUESTED",
            ):
                return flags, order_new_status

        if self.name == "DTDC":
            if order_new_status["new_status"] == "READY TO SHIP":
                return flags, order_new_status

        flags["type"] = None
        return flags, order_new_status

    def convert_courier_status_to_wareiq_status(self, requested_order, order_new_status, existing_order, flags):
        """
        Convert terinology of order details in courier API to a uniform WareIQ format.
        """
        new_status = dict()
        if self.name == "Delhivery":
            for each_scan in requested_order["Shipment"]["Scans"]:
                status_time = each_scan["ScanDetail"]["StatusDateTime"]
                if status_time:
                    if len(status_time) == 19:
                        status_time = datetime.strptime(status_time, config[self.name]["status_time_format"])
                    else:
                        status_time = datetime.strptime(status_time, config[self.name]["status_time_format"] + ".%f")

                to_record_status = config[self.name]["status_mapper_fn"](each_scan)
                if not to_record_status:
                    continue

                if to_record_status not in new_status:
                    new_status[to_record_status] = (
                        existing_order[0],
                        self.id,
                        existing_order[10],
                        each_scan["ScanDetail"]["ScanType"],
                        to_record_status,
                        each_scan["ScanDetail"]["Instructions"],
                        each_scan["ScanDetail"]["ScannedLocation"],
                        each_scan["ScanDetail"]["CityLocation"],
                        status_time,
                    )
                elif to_record_status == "In Transit" and new_status[to_record_status][8] < status_time:
                    new_status[to_record_status] = (
                        existing_order[0],
                        self.id,
                        existing_order[10],
                        each_scan["ScanDetail"]["ScanType"],
                        to_record_status,
                        each_scan["ScanDetail"]["Instructions"],
                        each_scan["ScanDetail"]["ScannedLocation"],
                        each_scan["ScanDetail"]["CityLocation"],
                        status_time,
                    )

        if self.name == "Xpressbees":
            for each_scan in requested_order["ShipmentSummary"]:
                if not each_scan.get("Location"):
                    continue
                status_time = each_scan["StatusDate"] + "T" + each_scan["StatusTime"]
                if status_time:
                    status_time = datetime.strptime(status_time, config[self.name]["status_time_format"])

                to_record_status, flags = config[self.name]["status_mapper_fn"](each_scan, flags)
                if not to_record_status:
                    continue

                if to_record_status not in new_status:
                    new_status[to_record_status] = (
                        existing_order[0],
                        self.id,
                        existing_order[10],
                        config[self.name]["status_mapping"][each_scan["StatusCode"]][1],
                        to_record_status,
                        each_scan["Status"],
                        each_scan["Location"],
                        each_scan["Location"].split(", ")[1],
                        status_time,
                    )
                elif to_record_status == "In Transit" and new_status[to_record_status][8] < status_time:
                    new_status[to_record_status] = (
                        existing_order[0],
                        self.id,
                        existing_order[10],
                        config[self.name]["status_mapping"][each_scan["StatusCode"]][1],
                        to_record_status,
                        each_scan["Status"],
                        each_scan["Location"],
                        each_scan["Location"].split(", ")[1],
                        status_time,
                    )

        if self.name == "Bluedart":
            if isinstance(requested_order["Scans"]["ScanDetail"], list):
                scan_list = requested_order["Scans"]["ScanDetail"]
            else:
                scan_list = [requested_order["Scans"]["ScanDetail"]]
            for each_scan in scan_list:
                status_time = each_scan["ScanDate"] + "T" + each_scan["ScanTime"]
                if status_time:
                    status_time = datetime.strptime(status_time, config[self.name]["status_time_format"])

                to_record_status, flags = config[self.name]["status_mapper_fn"](
                    each_scan, order_new_status["new_status"], flags
                )
                if not to_record_status:
                    continue

                if to_record_status not in new_status:
                    new_status[to_record_status] = (
                        existing_order[0],
                        self.id,
                        existing_order[10],
                        each_scan["ScanType"],
                        to_record_status,
                        each_scan["Scan"],
                        each_scan["ScannedLocation"],
                        each_scan["ScannedLocation"],
                        status_time,
                    )
                elif (
                    to_record_status == "In Transit"
                    and new_status[to_record_status][8] < status_time
                    and not flags["is_return"]
                ):
                    new_status[to_record_status] = (
                        existing_order[0],
                        self.id,
                        existing_order[10],
                        each_scan["ScanType"],
                        to_record_status,
                        each_scan["Scan"],
                        each_scan["ScannedLocation"],
                        each_scan["ScannedLocation"],
                        status_time,
                    )

        if self.name == "Ecom Express":
            for each_scan in requested_order["scans"]:
                status_time = each_scan["updated_on"]
                if status_time:
                    status_time = datetime.strptime(status_time, config[self.name]["status_time_format"])

                to_record_status, status_time = config[self.name]["status_mapper_fn"](
                    each_scan, order_new_status, requested_order, status_time
                )
                if not to_record_status:
                    continue

                if to_record_status not in new_status:
                    new_status[to_record_status] = (
                        existing_order[0],
                        self.id,
                        existing_order[10],
                        each_scan["reason_code_number"],
                        to_record_status,
                        each_scan["status"],
                        each_scan["location_city"],
                        each_scan["city_name"],
                        status_time,
                    )
                elif to_record_status == "In Transit" and new_status[to_record_status][8] < status_time:
                    new_status[to_record_status] = (
                        existing_order[0],
                        self.id,
                        existing_order[10],
                        each_scan["reason_code_number"],
                        to_record_status,
                        each_scan["status"],
                        each_scan["location_city"],
                        each_scan["city_name"],
                        status_time,
                    )

        if self.name == "Pidge":
            for each_scan in requested_order["data"]["past_status"]:
                if each_scan.get("attempt_type") == 20:
                    continue
                if (
                    each_scan.get("trip_status") in (20, 100, 120, 5)
                    or each_scan.get("trip_status") not in config[self.name]["status_mapping"]
                ):
                    continue

                status_time = each_scan["status_datetime"]
                if status_time:
                    status_time = datetime.strptime(status_time, config[self.name]["status_time_format"])

                to_record_status = config[self.name]["status_mapping"][each_scan.get("trip_status")][2]

                if to_record_status not in new_status:
                    new_status[to_record_status] = (
                        existing_order[0],
                        self.id,
                        existing_order[10],
                        each_scan["trip_status"],
                        to_record_status,
                        each_scan["trip_status"],
                        "",
                        "",
                        status_time,
                    )
                elif to_record_status == "In Transit" and new_status[to_record_status][8] < status_time:
                    new_status[to_record_status] = (
                        existing_order[0],
                        self.id,
                        existing_order[10],
                        each_scan["trip_status"],
                        to_record_status,
                        each_scan["trip_status"],
                        "",
                        "",
                        status_time,
                    )

        if self.name == "DTDC":
            for each_scan in requested_order["trackDetails"]:
                status_time = each_scan["strActionDate"] + "-" + each_scan["strActionTime"]
                if status_time:
                    status_time = datetime.strptime(status_time, config[self.name]["status_time_format"])

                to_record_status = config[self.name]["status_mapper_fn"](each_scan)
                if to_record_status not in new_status:
                    new_status[to_record_status] = (
                        existing_order[0],
                        self.id,
                        existing_order[10],
                        each_scan["strRemarks"],
                        to_record_status,
                        each_scan["strAction"],
                        "",
                        "",
                        status_time,
                    )
                elif to_record_status == "In Transit" and new_status[to_record_status][8] < status_time:
                    new_status[to_record_status] = (
                        existing_order[0],
                        self.id,
                        existing_order[10],
                        each_scan["strRemarks"],
                        to_record_status,
                        each_scan["strAction"],
                        "",
                        "",
                        status_time,
                    )
        return new_status, flags

    def update_shipment_data(self, requested_order, order_new_status, existing_order, current_awb, flags, check_obj):
        """
        Once order status data is converted to uniform WareIQ format,
        update them in DB accordingly.
        """
        return_object = {
            "type": "continue",
            "data": {"status_type": None, "status_detail": None, "status_code": None, "edd": None},
        }
        if self.name == "Delhivery":
            if order_new_status["new_status"] == "Manifested":
                return return_object, order_new_status

            order_new_status["new_status"] = order_new_status["new_status"].upper()
            if (existing_order[2] == "CANCELED" and order_new_status["new_status"] != "IN TRANSIT") or order_new_status[
                "new_status"
            ] in ("READY TO SHIP", "NOT PICKED", "PICKUP REQUESTED"):
                return return_object, order_new_status

            return_object["data"]["status_type"] = requested_order["Shipment"]["Status"]["StatusType"]
            if order_new_status["new_status"] == "PENDING":
                return_object["data"]["status_code"] = requested_order["Shipment"]["Scans"][-1]["ScanDetail"][
                    "StatusCode"
                ]

            return_object["data"]["edd"] = requested_order["Shipment"]["expectedDate"]

        if self.name == "Xpressbees":
            new_status_temp = config[self.name]["status_mapping"][order_new_status["new_status"]][0].upper()
            try:
                return_object["data"]["status_type"] = config[self.name]["status_mapping"][
                    order_new_status["new_status"]
                ][1]
            except KeyError:
                return_object["data"]["status_type"] = None

            if new_status_temp in ("READY TO SHIP", "PICKUP REQUESTED"):
                return return_object, order_new_status

            order_new_status["new_status"] = new_status_temp

            if existing_order[2] == "CANCELED" and order_new_status["new_status"] != "IN TRANSIT":
                return return_object, order_new_status

            return_object["data"]["edd"] = requested_order["ShipmentSummary"][0].get("ExpectedDeliveryDate")

        if self.name == "Bluedart":
            if flags["is_return"] and order_new_status["new_status"] == "DELIVERED":
                order_new_status["new_status"] = "RTO"

            return_object["data"]["status_type"] = requested_order["StatusType"]
            if order_new_status["new_status"] in ("NOT PICKED", "READY TO SHIP", "PICKUP REQUESTED"):
                return return_object, order_new_status
            return_object["data"]["status_detail"] = None
            return_object["data"]["status_code"] = order_new_status["scan_code"]

            if existing_order[2] == "CANCELED" and order_new_status["new_status"] != "IN TRANSIT":
                return return_object, order_new_status

            return_object["data"]["edd"] = (
                requested_order["ExpectedDeliveryDate"] if "ExpectedDeliveryDate" in requested_order else None
            )

        if self.name == "Ecom Express":
            return_object["data"]["edd"] = (
                requested_order["expected_date"] if "expected_date" in requested_order else None
            )
            return_object["data"]["status_type"] = config[self.name]["status_mapping"][order_new_status["scan_code"]][1]
            return_object["data"]["status_detail"] = None
            return_object["data"]["status_code"] = order_new_status["scan_code"]

        if self.name == "Pidge":
            if check_obj["reason_code_number"] in config[self.name]["status_mapping"]:
                return_object["data"]["status_type"] = "UD" if not flags["is_return"] else "RT"

        if self.name == "DTDC":
            new_status_temp = order_new_status["new_status"].upper()
            try:
                return_object["data"]["status_type"] = config[self.name]["status_mapping"][
                    order_new_status["new_status"]
                ][1]
            except KeyError:
                return_object["data"]["status_type"] = None

            return_object["data"]["status_code"] = order_new_status["scan_code"]
            if new_status_temp == "READY TO SHIP":
                return return_object, order_new_status

            order_new_status["new_status"] = new_status_temp

        if return_object["data"]["edd"]:
            try:
                return_object["data"]["edd"] = datetime.strptime(
                    return_object["data"]["edd"], config[self.name]["edd_time_format"]
                )
                if datetime.utcnow().hour < 4:
                    self.cursor.execute(
                        "UPDATE shipments SET edd=%s WHERE awb=%s", (return_object["data"]["edd"], current_awb)
                    )
                    self.cursor.execute(
                        "UPDATE shipments SET pdd=%s WHERE awb=%s and pdd is null",
                        (return_object["data"]["edd"], current_awb),
                    )
            except Exception as e:
                logger.error(str(e.args))

            return_object["type"] = None

        return return_object, order_new_status

    def send_delivered_update(self, order_new_status, existing_order, customer_phone):
        """
        Send updates for the orders with new status as delivered.
        """
        update_delivered_on_channels(existing_order)
        webhook_updates(
            existing_order,
            self.cursor,
            order_new_status["new_status"],
            "Shipment Delivered",
            "",
            (datetime.utcnow() + timedelta(hours=5.5)).strftime("%Y-%m-%d %H:%M:%S"),
        )
        tracking_link = "https://webapp.wareiq.com/tracking/" + order_new_status["current_awb"]
        tracking_link = UrlShortner.get_short_url(tracking_link, self.cursor)
        send_delivered_event(customer_phone, existing_order, self.name, tracking_link)

    def send_rto_update(self, new_status, existing_order):
        """
        Send updates for the orders with new status as RTO.
        """
        update_rto_on_channels(existing_order)
        webhook_updates(
            existing_order,
            self.cursor,
            new_status,
            "Shipment RTO",
            "",
            (datetime.utcnow() + timedelta(hours=5.5)).strftime("%Y-%m-%d %H:%M:%S"),
        )

    def send_new_to_transit_update(
        self, pickup_count, pickup_dict, order_new_status, existing_order, status_obj, customer_phone, flags
    ):
        """
        Send updates for the orders with new status as picked up for delivery.
        """
        if self.name == "Xpressbees":
            if not flags["order_picked_check"]:
                status_obj["type"] = "continue"
                return pickup_count, pickup_dict, status_obj

        pickup_count += 1
        if existing_order[11] not in pickup_dict:
            pickup_dict[existing_order[11]] = 1
        else:
            pickup_dict[existing_order[11]] += 1

        time_now = datetime.utcnow() + timedelta(hours=5.5)
        self.cursor.execute(
            "UPDATE order_pickups SET picked=%s, pickup_time=%s WHERE order_id=%s", (True, time_now, existing_order[0])
        )

        # update_picked_on_channels(existing_order, self.cursor, courier=self.get_dict())
        # webhook_updates(
        #     existing_order,
        #     self.cursor,
        #     order_new_status["new_status"],
        #     "Shipment Picked Up",
        #     "",
        #     (datetime.utcnow() + timedelta(hours=5.5)).strftime("%Y-%m-%d %H:%M:%S"),
        # )

        if status_obj["data"]["edd"]:
            self.cursor.execute(
                "UPDATE shipments SET pdd=%s WHERE awb=%s", (status_obj["data"]["edd"], order_new_status["current_awb"])
            )

        tracking_link = "https://webapp.wareiq.com/tracking/" + order_new_status["current_awb"]
        tracking_link = UrlShortner.get_short_url(tracking_link, self.cursor)
        # send_shipped_event(
        #     customer_phone,
        #     existing_order[19],
        #     existing_order,
        #     status_obj["data"]["edd"].strftime("%-d %b") if status_obj["data"]["edd"] else "",
        #     self.name,
        #     tracking_link,
        # )

        return pickup_count, pickup_dict, status_obj

    def send_pending_update(self, order_new_status, status_code, existing_order, requested_order):
        """
        Send updates for the orders with new status as pending.
        Also perform verifications for NDR.
        """
        try:
            ndr_reason = None
            if self.name == "Delhivery":
                if order_new_status["new_status"] == "PENDING" and status_code in config[self.name]["status_mapping"]:
                    ndr_reason = config[self.name]["status_mapping"][status_code]

            if self.name == "Xpressbees":
                if requested_order["ShipmentSummary"][0]["StatusCode"] == "UD":
                    ndr_reason = config[self.name]["ndr_mapper_fn"](requested_order)

            if self.name == "Bluedart":
                if (
                    order_new_status["new_status"] == "PENDING"
                    and status_code in config[self.name]["status_mapping"][order_new_status["scan_group"]]
                ):
                    ndr_reason = config[self.name]["status_mapping"][order_new_status["scan_group"]][status_code][3]

            if self.name == "Ecom Express":
                if order_new_status["new_status"] == "PENDING" and status_code in config[self.name]["ndr_reasons"]:
                    ndr_reason = config[self.name]["ndr_reasons"][status_code]

            if self.name == "DTDC":
                if order_new_status["new_status"] == "PENDING" and status_code in config[self.name]["ndr_reasons"]:
                    ndr_reason = config[self.name]["ndr_reasons"][status_code]

            if ndr_reason:
                verification_text(existing_order, self.cursor, ndr_reason=ndr_reason)
                webhook_updates(
                    existing_order,
                    self.cursor,
                    order_new_status["new_status"],
                    "",
                    "",
                    (datetime.utcnow() + timedelta(hours=5.5)).strftime("%Y-%m-%d %H:%M:%S"),
                    ndr_id=ndr_reason,
                )
                pass
        except Exception as e:
            logger.error("NDR confirmation not sent. Order id: " + str(existing_order[0]))

    def courier_specific_status_updates(
        self, new_status, existing_order, exotel_idx, exotel_sms_data, customer_phone, current_awb
    ):
        """
        Send updates for the orders with new status specific to couriers.
        """
        if self.name == "Delhivery":
            if new_status == "DTO":
                webhook_updates(
                    existing_order,
                    self.cursor,
                    new_status,
                    "Shipment delivered to origin",
                    "",
                    (datetime.utcnow() + timedelta(hours=5.5)).strftime("%Y-%m-%d %H:%M:%S"),
                )
                sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                sms_body_key = "Messages[%s][Body]" % str(exotel_idx)
                exotel_sms_data[sms_to_key] = customer_phone
                exotel_sms_data[sms_body_key] = (
                    "Delivered: Your %s order via Delhivery to seller - https://webapp.wareiq.com/tracking/%s . Powered by WareIQ"
                    % (existing_order[[20]], current_awb)
                )
                exotel_idx += 1

            if (
                existing_order[2] in ("SCHEDULED", "DISPATCHED")
                and new_status == "IN TRANSIT"
                and existing_order[13].lower() == "pickup"
            ):
                sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                sms_body_key = "Messages[%s][Body]" % str(exotel_idx)
                exotel_sms_data[sms_to_key] = customer_phone
                exotel_sms_data[sms_body_key] = (
                    "Picked: Your %s order via Delhivery - https://webapp.wareiq.com/tracking/%s . Powered by WareIQ"
                    % (existing_order[[20]], current_awb)
                )
                exotel_idx += 1
                webhook_updates(
                    existing_order,
                    self.cursor,
                    "DTO " + new_status,
                    "Shipment picked from customer",
                    "",
                    (datetime.utcnow() + timedelta(hours=5.5)).strftime("%Y-%m-%d %H:%M:%S"),
                )

        return exotel_idx, exotel_sms_data

    def send_exotel_messages(self, exotel_idx, exotel_sms_data):
        """
        Once order status has been updated, send updates via Exotel to end customers.
        """
        if exotel_idx:
            logger.info("Sending messages...count:" + str(exotel_idx))
            try:
                lad = requests.post(config[self.name]["exotel_url"], data=exotel_sms_data)
            except Exception as e:
                logger.error("messages not sent." + "   Error: " + str(e.args[0]))
        return
