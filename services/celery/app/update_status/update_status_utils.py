from time import sleep
from woocommerce import API
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests, json, hmac, hashlib, base64, logging, boto3

from .queries import *
from .order_shipped import order_shipped
from ..db_utils import UrlShortner


logger = logging.getLogger()
logger.setLevel(logging.INFO)

RAVEN_URL = "https://api.ravenapp.dev/v1/apps/ccaaf889-232e-49df-aeb8-869e3153509d/events/send"
RAVEN_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "AuthKey K4noY3GgzaW8OEedfZWAOyg+AmKZTsqO/h/8Y4LVtFA=",
}

email_client = boto3.client(
    "ses",
    region_name="us-east-1",
    aws_access_key_id="AKIAWRT2R3KC3YZUBFXY",
    aws_secret_access_key="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",
)


def verification_text(current_order, cur, ndr_reason=None):
    ndr_confirmation_link = "http://track.wareiq.com/core/v1/passthru/ndr?CustomField=%s" % str(current_order[0])
    ndr_confirmation_link = UrlShortner.get_short_url(ndr_confirmation_link, cur)

    insert_cod_ver_tuple = (current_order[0], ndr_confirmation_link, datetime.now())
    date_today = (datetime.utcnow() + timedelta(hours=5.5)).strftime("%Y-%m-%d")
    cur.execute(
        "SELECT * from ndr_shipments WHERE shipment_id=%s and date_created::date='%s';"
        % (str(current_order[10]), date_today)
    )
    if not cur.fetchone():
        ndr_ship_tuple = (
            current_order[0],
            current_order[10],
            ndr_reason,
            "required",
            datetime.utcnow() + timedelta(hours=5.5),
        )
        cur.execute(
            "INSERT INTO ndr_shipments (order_id, shipment_id, reason_id, current_status, date_created) VALUES (%s,%s,%s,%s,%s);",
            ndr_ship_tuple,
        )
        if current_order[37] != False and ndr_reason in (1, 3, 9, 11):
            cur.execute("SELECT * FROM ndr_verification where order_id=%s;" % str(current_order[0]))
            if not cur.fetchone():
                cur.execute(
                    "INSERT INTO ndr_verification (order_id, verification_link, date_created) VALUES (%s,%s,%s);",
                    insert_cod_ver_tuple,
                )
                customer_phone = current_order[4].replace(" ", "")
                customer_phone = "0" + customer_phone[-10:]
                send_ndr_event(customer_phone, current_order, ndr_confirmation_link)


def woocommerce_fulfillment(order):
    wcapi = API(url=order[9], consumer_key=order[7], consumer_secret=order[8], version="wc/v3")
    status_mark = order[27]
    if not status_mark:
        status_mark = "completed"
    r = wcapi.post(
        "orders/%s?consumer_key=%s&consumer_secret=%s" % (str(order[5]), order[7], order[8]),
        data={"status": status_mark},
    )
    try:
        r = wcapi.post(
            "orders/%s/shipment-trackings" % str(order[5]),
            data={"tracking_provider": "WareIQ", "tracking_number": order[1]},
        )
    except Exception:
        pass


def lotus_organics_update(order, status):
    url = "https://lotusapi.farziengineer.co/plugins/plugin.wareiq/order/update"
    headers = {"x-api-key": "c2d8f4d497ee44649653074f139eddf2"}
    data = {"id": int(order[5]), "ware_iq_id": order[0], "awb_number": str(order[1]), "status_information": status}

    req = requests.post(url, headers=headers, data=data)


def lotus_botanicals_shipped(order):
    try:
        url = "http://webapps.lotusbotanicals.com/orders/update/shipping/" + str(order[0])
        headers = {"Content-Type": "application/json", "Authorization": "Ae76eH239jla*fgna#q6fG&5Khswq_kpaj$#1a"}
        tracking_link = "http://webapp.wareiq.com/tracking/%s" % str(order[1])
        data = {"tracking_service": "WareIQ", "tracking_number": str(order[1]), "url": tracking_link}
        req = requests.post(url, headers=headers, data=json.dumps(data))

    except Exception as e:
        logger.error("Couldn't update lotus for: " + str(order[0]) + "\nError: " + str(e.args))


def lotus_botanicals_delivered(order):
    try:
        url = "http://webapps.lotusbotanicals.com/orders/update/delivered/" + str(order[0])
        headers = {"Content-Type": "application/json", "Authorization": "Ae76eH239jla*fgna#q6fG&5Khswq_kpaj$#1a"}
        data = {}
        req = requests.post(url, headers=headers, data=json.dumps(data))
    except Exception as e:
        logger.error("Couldn't update lotus for: " + str(order[0]) + "\nError: " + str(e.args))


def woocommerce_returned(order):
    wcapi = API(url=order[9], consumer_key=order[7], consumer_secret=order[8], version="wc/v3")
    status_mark = order[33]
    if not status_mark:
        status_mark = "cancelled"
    r = wcapi.post("orders/%s" % str(order[5]), data={"status": status_mark})


def shopify_fulfillment(order, cur):
    if not order[25]:
        get_locations_url = "https://%s:%s@%s/admin/api/2019-10/locations.json" % (order[7], order[8], order[9])
        req = requests.get(get_locations_url).json()
        location_id = str(req["locations"][0]["id"])
        cur.execute("UPDATE client_channel set unique_parameter=%s where id=%s" % (location_id, order[34]))
    else:
        location_id = str(order[25])

    create_fulfillment_url = "https://%s:%s@%s/admin/api/2019-10/orders/%s/fulfillments.json" % (
        order[7],
        order[8],
        order[9],
        order[5],
    )
    tracking_link = "http://webapp.wareiq.com/tracking/%s" % str(order[1])
    ful_header = {"Content-Type": "application/json"}
    fulfil_data = {
        "fulfillment": {
            "tracking_number": str(order[1]),
            "tracking_urls": [tracking_link],
            "tracking_company": "WareIQ",
            "location_id": int(location_id),
            "notify_customer": True,
        }
    }
    req_ful = requests.post(create_fulfillment_url, data=json.dumps(fulfil_data), headers=ful_header)
    fulfillment_id = None
    try:
        fulfillment_id = str(req_ful.json()["fulfillment"]["id"])
    except KeyError:
        if req_ful.json().get("errors") and req_ful.json().get("errors") == "Not Found":
            get_locations_url = "https://%s:%s@%s/admin/api/2019-10/locations.json" % (order[7], order[8], order[9])
            req = requests.get(get_locations_url).json()
            location_id = str(req["locations"][0]["id"])
            cur.execute("UPDATE client_channel set unique_parameter=%s where id=%s" % (location_id, order[34]))
            fulfil_data["fulfillment"]["location_id"] = int(location_id)
            req_ful = requests.post(create_fulfillment_url, data=json.dumps(fulfil_data), headers=ful_header)
            fulfillment_id = str(req_ful.json()["fulfillment"]["id"])
    if fulfillment_id and tracking_link:
        cur.execute(
            "UPDATE shipments SET channel_fulfillment_id=%s, tracking_link=%s WHERE id=%s",
            (fulfillment_id, tracking_link, order[10]),
        )
    return fulfillment_id, tracking_link


def hepta_fulfilment(order):
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": "Basic c2VydmljZS5hcGl1c2VyOllQSGpBQXlXY3RWYzV5MWg=",
    }
    hepta_url = "https://www.nashermiles.com/alexandria/api/v1/shipment/create"
    hepta_body = {
        "order_id": str(order[5]),
        "awb_number": str(order[1]),
        "tracking_link": "http://webapp.wareiq.com/tracking/%s" % str(order[1]),
    }
    req_ful = requests.post(hepta_url, headers=headers, data=json.dumps(hepta_body))


def shopify_markpaid(order):
    get_transactions_url = "https://%s:%s@%s/admin/api/2019-10/orders/%s/transactions.json" % (
        order[7],
        order[8],
        order[9],
        order[5],
    )

    tra_header = {"Content-Type": "application/json"}
    transaction_data = {
        "transaction": {"kind": "sale", "source": "external", "amount": str(order[35]), "currency": "INR"}
    }
    req_ful = requests.post(get_transactions_url, data=json.dumps(transaction_data), headers=tra_header)


def instamojo_push_awb(order):
    push_awb_url = "https://api.instamojo.com/v2/store/orders/%s/" % str(order[5])
    tra_header = {"Authorization": "Bearer " + order[7]}
    tracking_link = "https://webapp.wareiq.com/tracking/%s" % str(order[1])
    push_awb_data = {"shipping": {"tracking_url": tracking_link, "waybill": str(order[1]), "courier_partner": "WareIQ"}}
    req_ful = requests.patch(push_awb_url, data=push_awb_data, headers=tra_header)


def instamojo_update_status(order, status, status_text):
    push_awb_url = "https://api.instamojo.com/v2/store/orders/%s/update-order/" % str(order[5])
    tra_header = {"Authorization": "Bearer " + order[7]}
    push_awb_data = {"order_status": status, "comments": status_text}

    req_ful = requests.patch(push_awb_url, data=push_awb_data, headers=tra_header)


def shopify_cancel(order):
    get_cancel_url = "https://%s:%s@%s/admin/api/2019-10/orders/%s/cancel.json" % (
        order[7],
        order[8],
        order[9],
        order[5],
    )

    tra_header = {"Content-Type": "application/json"}
    cancel_data = {"restock": False}
    if order[3] in ("BEHIR", "SHAHIKITCHEN", "SUKHILIFE", "SUCCESSCRAFT", "NEWYOURCHOICE"):
        cancel_data = {"restock": True}
    req_ful = requests.post(get_cancel_url, data=json.dumps(cancel_data), headers=tra_header)


def magento_fulfillment(order, cur, courier=None):
    create_fulfillment_url = "%s/V1/order/%s/ship" % (order[9], order[5])
    tracking_link = "http://webapp.wareiq.com/tracking/%s" % str(order[1])
    ful_header = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + order[7],
        "User-Agent": "WareIQ server",
    }

    items_list = list()
    for idx, sku in enumerate(order[16]):
        if sku:
            items_list.append({"extension_attributes": {}, "order_item_id": int(sku), "qty": int(order[17][idx])})
    fulfil_data = {
        "items": items_list,
        "notify": False,
        "tracks": [
            {
                "extension_attributes": {"warehouse_name": str(order[36])} if order[3] == "KAMAAYURVEDA" else {},
                "track_number": str(order[1]),
                "title": courier[1],
                "carrier_code": courier[1],
            }
        ],
    }
    req_ful = requests.post(create_fulfillment_url, data=json.dumps(fulfil_data), headers=ful_header)

    if type(req_ful.json()) == str:
        cur.execute(
            "UPDATE shipments SET channel_fulfillment_id=%s, tracking_link=%s WHERE id=%s",
            (req_ful.json(), tracking_link, order[10]),
        )

    shipped_comment_url = "%s/V1/orders/%s/comments" % (order[9], order[5])

    status_mark = order[27]
    if not status_mark:
        status_mark = "shipped"
    time_now = datetime.utcnow() + timedelta(hours=5.5)
    time_now = time_now.strftime("%Y-%m-%d %H:%M:%S")
    complete_data = {
        "statusHistory": {
            "comment": "Shipment Created",
            "created_at": time_now,
            "parent_id": int(order[5]),
            "is_customer_notified": 0,
            "is_visible_on_front": 0,
            "status": status_mark,
        }
    }
    req_ful = requests.post(shipped_comment_url, data=json.dumps(complete_data), headers=ful_header)
    return req_ful.json(), tracking_link


def magento_invoice(order):
    create_invoice_url = "%s/V1/order/%s/invoice" % (order[9], order[5])
    ful_header = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + order[7],
        "User-Agent": "WareIQ server",
    }

    items_list = list()
    for idx, sku in enumerate(order[16]):
        if sku:
            items_list.append({"extension_attributes": {}, "order_item_id": int(sku), "qty": int(order[17][idx])})

    invoice_data = {"capture": False, "notify": False}
    req_ful = requests.post(create_invoice_url, data=json.dumps(invoice_data), headers=ful_header)

    invoice_comment_url = "%s/V1/orders/%s/comments" % (order[9], order[5])

    status_mark = order[29]
    if not status_mark:
        status_mark = "invoiced"
    time_now = datetime.utcnow() + timedelta(hours=5.5)
    time_now = time_now.strftime("%Y-%m-%d %H:%M:%S")
    complete_data = {
        "statusHistory": {
            "comment": "Invoice Created",
            "created_at": time_now,
            "parent_id": int(order[5]),
            "is_customer_notified": 0,
            "is_visible_on_front": 0,
            "status": status_mark,
        }
    }
    req_ful = requests.post(invoice_comment_url, data=json.dumps(complete_data), headers=ful_header)


def magento_complete_order(order):
    complete_order_url = "%s/V1/orders/%s/comments" % (order[9], order[5])
    ful_header = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + order[7],
        "User-Agent": "WareIQ server",
    }

    status_mark = order[31]
    if not status_mark:
        status_mark = "delivered"
    time_now = datetime.utcnow() + timedelta(hours=5.5)
    time_now = time_now.strftime("%Y-%m-%d %H:%M:%S")
    complete_data = {
        "statusHistory": {
            "comment": "Order Delivered",
            "created_at": time_now,
            "parent_id": int(order[5]),
            "is_customer_notified": 0,
            "is_visible_on_front": 0,
            "status": status_mark,
        }
    }
    req_ful = requests.post(complete_order_url, data=json.dumps(complete_data), headers=ful_header)


def magento_return_order(order):
    complete_order_url = "%s/V1/orders/%s/comments" % (order[9], order[5])
    ful_header = {"Content-Type": "application/json", "Authorization": "Bearer " + order[7]}

    status_mark = order[33]
    if not status_mark:
        status_mark = "returned"
    time_now = datetime.utcnow() + timedelta(hours=5.5)
    time_now = time_now.strftime("%Y-%m-%d %H:%M:%S")
    complete_data = {
        "statusHistory": {
            "comment": "Order Returned",
            "created_at": time_now,
            "parent_id": int(order[5]),
            "is_customer_notified": 0,
            "is_visible_on_front": 0,
            "status": status_mark,
        }
    }
    req_ful = requests.post(complete_order_url, data=json.dumps(complete_data), headers=ful_header)


def update_picked_on_channels(order, cur, courier=None):
    if order[3] == "NASHER" and order[5]:
        hepta_fulfilment(order)
    if order[26] != False:
        if order[14] == 5:
            try:
                woocommerce_fulfillment(order)
            except Exception as e:
                logger.error("Couldn't update woocommerce for: " + str(order[0]) + "\nError: " + str(e.args))
        elif order[14] == 1:
            try:
                shopify_fulfillment(order, cur)
            except Exception as e:
                logger.error("Couldn't update shopify for: " + str(order[0]) + "\nError: " + str(e.args))
        elif order[14] == 6:  # Magento fulfilment
            try:
                if order[28] != False:
                    magento_invoice(order)
                magento_fulfillment(order, cur, courier=courier)
            except Exception as e:
                logger.error("Couldn't update Magento for: " + str(order[0]) + "\nError: " + str(e.args))
        elif order[14] == 8:  # Bikayi fulfilment
            try:
                update_bikayi_status(order, "IN_PROGRESS")
            except Exception as e:
                logger.error("Couldn't update Bikayi for: " + str(order[0]) + "\nError: " + str(e.args))
        elif order[3] == "LOTUSBOTANICALS":
            lotus_botanicals_shipped(order)
        elif order[3] == "LOTUSORGANICS":
            try:
                lotus_organics_update(order, "Order Shipped")
            except Exception as e:
                pass
        elif order[14] == 7:  # Easyecom fulfilment
            try:
                update_easyecom_status(order, 2)
            except Exception as e:
                logger.error("Couldn't update Easyecom for: " + str(order[0]) + "\nError: " + str(e.args))
        elif order[14] == 13:  # Instamojo fulfilment
            try:
                instamojo_push_awb(order)
                instamojo_update_status(order, "dispatched", "Order picked up by courier")
            except Exception as e:
                logger.error("Couldn't update Instamojo for: " + str(order[0]) + "\nError: " + str(e.args))


def update_delivered_on_channels(order):
    if order[30] != False:
        if order[14] == 6:  # Magento complete
            try:
                magento_complete_order(order)
            except Exception as e:
                logger.error("Couldn't complete Magento for: " + str(order[0]) + "\nError: " + str(e.args))

    if order[28] != False and str(order[13]).lower() == "cod" and order[14] == 1:  # mark paid on shopify
        try:
            shopify_markpaid(order)
        except Exception as e:
            logger.error("Couldn't mark paid Shopify for: " + str(order[0]) + "\nError: " + str(e.args))

    elif order[3] == "LOTUSBOTANICALS":
        lotus_botanicals_delivered(order)

    elif order[3] == "LOTUSORGANICS":
        try:
            lotus_organics_update(order, "Order Delivered")
        except Exception as e:
            pass

    elif order[14] == 7:  # Easyecom Delivered
        try:
            update_easyecom_status(order, 3)
        except Exception as e:
            logger.error("Couldn't update Easyecom for: " + str(order[0]) + "\nError: " + str(e.args))
    elif order[14] == 8:  # Bikayi delivered
        try:
            update_bikayi_status(order, "DELIVERED")
        except Exception as e:
            logger.error("Couldn't update Bikayi for: " + str(order[0]) + "\nError: " + str(e.args))
    elif order[14] == 13:  # Instamojo delivered
        try:
            instamojo_update_status(order, "completed", "Order delivered to customer")
        except Exception as e:
            logger.error("Couldn't update Instamojo for: " + str(order[0]) + "\nError: " + str(e.args))


def update_rto_on_channels(order):
    if order[32] != False:
        if order[14] == 6:  # Magento return
            try:
                magento_return_order(order)
            except Exception as e:
                logger.error("Couldn't return Magento for: " + str(order[0]) + "\nError: " + str(e.args))
        elif order[14] == 5:  # Woocommerce Cancelled
            try:
                woocommerce_returned(order)
            except Exception as e:
                logger.error("Couldn't cancel on woocommerce for: " + str(order[0]) + "\nError: " + str(e.args))

        elif order[14] == 1:  # Shopify Cancelled
            try:
                shopify_cancel(order)
            except Exception as e:
                logger.error("Couldn't cancel on Shopify for: " + str(order[0]) + "\nError: " + str(e.args))

        elif order[3] == "LOTUSORGANICS":
            try:
                lotus_organics_update(order, "RTO")
            except Exception as e:
                pass
        elif order[14] == 7:  # Easyecom RTO
            try:
                update_easyecom_status(order, 9)
            except Exception as e:
                logger.error("Couldn't update Easyecom for: " + str(order[0]) + "\nError: " + str(e.args))
        elif order[14] == 8:  # Bikayi RTO
            try:
                update_bikayi_status(order, "RETURNED")
            except Exception as e:
                logger.error("Couldn't update Bikayi for: " + str(order[0]) + "\nError: " + str(e.args))
        elif order[14] == 13:  # Instamojo RTO
            try:
                instamojo_update_status(order, "completed", "Order returned to seller")
            except Exception as e:
                logger.error("Couldn't update instamojo for: " + str(order[0]) + "\nError: " + str(e.args))


def update_easyecom_status(order, status_id):
    create_fulfillment_url = "%s/Carrier/updateTrackingStatus?api_token=%s" % (order[9], order[7])
    ful_header = {"Content-Type": "application/json"}
    fulfil_data = {
        "api_token": order[7],
        "current_shipment_status_id": status_id,
        "awb": order[1],
    }
    req_ful = requests.post(create_fulfillment_url, data=json.dumps(fulfil_data), headers=ful_header)


def update_bikayi_status(order, status):
    bikayi_update_url = """https://asia-south1-bikai-d5ee5.cloudfunctions.net/platformPartnerFunctions-updateOrder"""
    key = "3f638d4ff80defb82109951b9638fae3fe0ff8a2d6dc20ed8c493783"
    secret = "6e130520777eb175c300aefdfc1270a4f9a57f2309451311ad3fdcfb"
    timestamp = (datetime.utcnow() + timedelta(hours=5.5)).strftime("%s")
    req_body = {
        "appId": "WAREIQ",
        "merchantId": order[3].split("_")[1],
        "timestamp": timestamp,
        "orderId": str(order[12]),
        "status": status,
        "trackingLink": "https://webapp.wareiq.com/tracking/" + order[1],
        "notes": status,
        "wayBill": order[1],
    }
    signature = hmac.new(
        bytes(secret.encode()),
        (key.encode() + "|".encode() + base64.b64encode(json.dumps(req_body).replace(" ", "").encode())),
        hashlib.sha256,
    ).hexdigest()
    headers = {"Content-Type": "application/json", "authorization": signature}
    data = requests.post(bikayi_update_url, headers=headers, data=json.dumps(req_body)).json()


def ecom_express_convert_xml_dict(elem):
    req_obj = dict()
    for elem2 in elem["field"]:
        req_obj[elem2["@name"]] = None
        if "#text" in elem2:
            req_obj[elem2["@name"]] = elem2["#text"]
        elif "object" in elem2:
            if type(elem2["object"]) == list:
                scan_list = list()
                for obj in elem2["object"]:
                    scan_obj = dict()
                    for newobj in obj["field"]:
                        scan_obj[newobj["@name"]] = None
                        if "#text" in newobj:
                            scan_obj[newobj["@name"]] = newobj["#text"]
                    scan_list.append(scan_obj)
                req_obj[elem2["@name"]] = scan_list
            else:
                req_obj[elem2["@name"]] = elem2["object"]

    return req_obj


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
        "user": {"mobile": mobile, "email": email if email else ""},
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
            "client_logo": client_logo,
        },
        "override": {"email": {"from": {"name": client_name}}},
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
        "data": {"client_name": client_name, "courier_name": courier_name, "tracking_link": tracking_link},
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
        "data": {"client_name": client_name, "courier_name": courier_name, "tracking_link": tracking_link},
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
        "data": {"client_name": client_name, "courier_name": courier_name, "tracking_link": tracking_link},
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
        },
    }

    req = requests.post(RAVEN_URL, headers=RAVEN_HEADERS, data=json.dumps(payload))


def send_bulk_emails(emails):
    logger.info("Sending Emails....count: " + str(len(emails)) + "  Time: " + str(datetime.utcnow()))
    for email in emails:
        try:
            response = email_client.send_raw_email(
                Source=email[0]["From"],
                Destinations=email[1],
                RawMessage={
                    "Data": email[0].as_string(),
                },
            )
            sleep(0.08)
        except Exception as e:
            logger.error("Couldn't send email: " + str(email["TO"]) + "\nError: " + str(e.args[0]))


def create_email(order, edd, email):
    try:
        background_color = str(order[24]) if order[24] else "#B5D0EC"
        client_logo = str(order[21]) if order[21] else "https://logourls.s3.amazonaws.com/client_logos/logo_ane.png"
        client_name = str(order[20]) if order[20] else "WareIQ"
        email_title = str(order[22]) if order[22] else "Your order has been shipped!"
        order_id = str(order[12]) if order[12] else ""
        customer_name = str(order[18]) if order[18] else "Customer"
        courier_name = "WareIQ"
        if order[23] in (1, 2, 8, 11, 12):
            courier_name = "Delhivery"
        elif order[23] in (5, 13):
            courier_name = "Xpressbees"
        elif order[23] in (4,):
            courier_name = "Shadowfax"
        elif order[23] in (9,):
            courier_name = "Bluedart"

        edd = edd if edd else ""
        awb_number = str(order[1]) if order[1] else ""
        tracking_link = "http://webapp.wareiq.com/tracking/" + str(order[1])

        html = (
            order_shipped.replace("__CLIENT_LOGO__", client_logo)
            .replace("__CLIENT_NAME__", client_name)
            .replace("__BACKGROUND_COLOR__", background_color)
            .replace("__EMAIL_TITLE__", email_title)
            .replace("__CUSTOMER_NAME__", customer_name)
            .replace("__ORDER_ID__", order_id)
            .replace("__COURIER_NAME__", courier_name)
            .replace("__EDD__", edd)
            .replace("__AWB_NUMBER__", awb_number)
            .replace("__TRACKING_LINK__", tracking_link)
        )

        # create message object instance
        msg = MIMEMultipart("alternative")

        recipients = [email]
        msg["From"] = "%s <noreply@wareiq.com>" % client_name
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = email_title

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
                cur.execute("SELECT reason FROM ndr_reasons WHERE id=%s" % str(ndr_id))
                status_text = cur.fetchone()[0]
            cur.execute(
                "SELECT webhook_url, header_key, header_value, webhook_secret, id FROM webhooks WHERE status='active' and client_prefix='%s'"
                % order[3]
            )
            all_webhooks = cur.fetchall()
            for webhook in all_webhooks:
                try:
                    req_body = {
                        "awb": order[1],
                        "status": status,
                        "event_time": status_time,
                        "location": location,
                        "order_id": order[12],
                        "status_text": status_text,
                    }

                    headers = {"Content-Type": "application/json"}
                    if webhook[1] and webhook[2]:
                        headers[webhook[1]] = webhook[2]

                    req = requests.post(webhook[0], headers=headers, json=req_body, timeout=5)
                    if not str(req.status_code).startswith("2"):
                        cur.execute("UPDATE webhooks SET fail_count=fail_count+1 WHERE id=%s" % str(webhook[4]))
                except Exception:
                    cur.execute("UPDATE webhooks SET fail_count=fail_count+1 WHERE id=%s" % str(webhook[4]))
                    pass
        except Exception:
            pass
