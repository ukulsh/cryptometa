import psycopg2, requests, os, json, logging, boto3
from datetime import datetime, timedelta
from time import sleep
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from woocommerce import API
from flask import request, jsonify, current_app
from functools import wraps
from .order_shipped import order_shipped

logger = logging.getLogger()
logger.setLevel(logging.INFO)

email_client = boto3.client('ses', region_name="us-east-1", aws_access_key_id='AKIAWRT2R3KC3YZUBFXY',
    aws_secret_access_key='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')


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


def shopify_cancel(order):
    get_cancel_url = "https://%s:%s@%s/admin/api/2019-10/orders/%s/cancel.json" % (
        order[7], order[8],
        order[9], order[5])

    tra_header = {'Content-Type': 'application/json'}
    cancel_data = {}
    req_ful = requests.post(get_cancel_url, data=json.dumps(cancel_data),
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


def update_ndr_shipment(order, cur, ndr_reason):
    insert_ndr_ver_tuple = (order[0], "", datetime.utcnow() + timedelta(hours=5.5))
    ndr_ship_tuple = (
        order[0], order[10], ndr_reason, "required", datetime.utcnow() + timedelta(hours=5.5))
    cur.execute(
        "INSERT INTO ndr_shipments (order_id, shipment_id, reason_id, current_status, date_created) VALUES (%s,%s,%s,%s,%s);",
        ndr_ship_tuple)
    if ndr_reason in (1, 3, 9, 11):
        cur.execute(
            "INSERT INTO ndr_verification (order_id, verification_link, date_created) VALUES (%s,%s,%s);",
            insert_ndr_ver_tuple)


def mark_picked_channel(order, cur):
    if order[26] != False:
        if order[14] == 5:
            try:
                woocommerce_fulfillment(order)
            except Exception as e:
                logger.error("Couldn't update woocommerce for: " + str(order[0])
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


def mark_delivered_channel(order):
    if order[30] != False:
        if order[14] == 6:  # Magento complete
            try:
                magento_complete_order(order)
            except Exception as e:
                logger.error(
                    "Couldn't complete Magento for: " + str(order[0])
                    + "\nError: " + str(e.args))

    if order[28] != False and str(order[13]).lower() == 'cod' and order[14] == 1:  # mark paid on shopify
        try:
            shopify_markpaid(order)
        except Exception as e:
            logger.error(
                "Couldn't mark paid Shopify for: " + str(order[0])
                + "\nError: " + str(e.args))


def mark_rto_channel(order):
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


def exotel_send_shipped_sms(order, courier):
    try:
        exotel_sms_data = {
            'From': 'LM-WAREIQ'
        }
        client_name = str(order[20])
        customer_phone = order[4].replace(" ", "")
        customer_phone = "0" + customer_phone[-10:]

        sms_to_key = "Messages[0][To]"
        sms_body_key = "Messages[0][Body]"

        exotel_sms_data[sms_to_key] = customer_phone

        tracking_link_wareiq = "http://webapp.wareiq.com/tracking/" + str(order[1])

        exotel_sms_data[sms_body_key] = "Shipped: Your %s order via %s. Track here: %s . Thanks!" % (
        client_name, courier, tracking_link_wareiq)
        logger.info("Sending shipped message to:" + str(customer_phone))
        lad = requests.post(
            'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend',
            data=exotel_sms_data)
    except Exception as e:
        logger.error("messages not sent." + "   Error: " + str(e.args[0]))


def send_shipped_email(order):
    if order[19]:
        emails_list = list()
        email = create_email(order, "",
                             order[19])
        if email:
            emails_list.append((email, [order[19]]))
            send_bulk_emails(emails_list)


def mark_order_picked_pickups(order, cur):
    time_now = datetime.utcnow() + timedelta(hours=5.5)
    cur.execute("UPDATE order_pickups SET picked=%s, pickup_time=%s WHERE order_id=%s",
                (True, time_now, order[0]))


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
        elif order[23] in (5,13,17):
            courier_name = "Xpressbees"
        elif order[23] in (4,):
            courier_name = "Shadowfax"
        elif order[23] in (15,):
            courier_name = "Ecom Express"

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


def authenticate_username_password(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        response_object = {
            'status': 'error',
            'message': 'Something went wrong. Please contact us.'
        }
        code = 401
        username = request.headers.get('username')
        password = request.headers.get('password')
        if not username or not password:
            response_object['message'] = 'Provide valid login details.'
            code = 403
            return jsonify(response_object), code

        response = ensure_authenticated(username, password)
        if not response:
            response_object['message'] = 'Invalid details.'
            return jsonify(response_object), code
        return f(response, *args, **kwargs)
    return decorated_function


def ensure_authenticated(username, password):
    if current_app.config['TESTING']:
        return True
    url = '{0}/auth/loginAPI'.format(current_app.config['USERS_SERVICE_URL'])
    headers = {'username': username, "password": password}
    response = requests.post(url, json=headers)
    data = json.loads(response.text)
    if response.status_code == 200 and \
       data['status'] == 'success' and \
       data['data']['active']:
        return data
    else:
        return False