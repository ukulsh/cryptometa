import psycopg2, requests, os, json, logging
from datetime import datetime, timedelta
from woocommerce import API
from flask import request, jsonify, current_app
from functools import wraps

logger = logging.getLogger()
logger.setLevel(logging.INFO)


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