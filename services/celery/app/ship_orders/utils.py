import logging, requests, random, string, json, os
from app.db_utils import DbConnection, UrlShortner
from datetime import datetime, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

conn_2 = DbConnection.get_pincode_db_connection_instance()
cur_2 = conn_2.cursor()

RAVEN_URL = "https://api.ravenapp.dev/v1/apps/ccaaf889-232e-49df-aeb8-869e3153509d/events/send"
RAVEN_HEADERS = {"Content-Type": "application/json", "Authorization": "AuthKey K4noY3GgzaW8OEedfZWAOyg+AmKZTsqO/h/8Y4LVtFA="}


def calculate_order_weight_dimensions(order):
    if order[52]:
        return float(order[52]), float(order[52]), {"length": 1, "breadth": 1, "height": 1}
    dimensions = order[33][0]
    weight = order[34][0] * order[35][0]
    volumetric_weight = (dimensions['length'] * dimensions['breadth'] * dimensions['height'] * order[35][0]) / 5000
    for idx, dim in enumerate(order[33]):
        if idx == 0:
            continue
        volumetric_weight += (dim['length'] * dim['breadth'] * dim['height'] * (order[35][idx])) / 5000
        weight += order[34][idx] * (order[35][idx])
    if dimensions['length'] and dimensions['breadth']:
        dimensions['height'] = round(
            (volumetric_weight * 5000) / (dimensions['length'] * dimensions['breadth']))

    return weight, volumetric_weight, dimensions


def get_delivery_zone(pick_pincode, del_pincode):
    cur_2.execute("SELECT city from city_pin_mapping where pincode='%s';" % str(pick_pincode).rstrip())
    pickup_city = cur_2.fetchone()
    if not pickup_city:
        return None
    pickup_city = pickup_city[0]
    cur_2.execute("SELECT city from city_pin_mapping where pincode='%s';" % str(del_pincode).rstrip())
    deliver_city = cur_2.fetchone()
    if not deliver_city:
        return None
    deliver_city = deliver_city[0]
    zone_select_tuple = (pickup_city, deliver_city)
    cur_2.execute("SELECT zone_value from city_zone_mapping where zone=%s and city=%s;",
                  zone_select_tuple)
    delivery_zone = cur_2.fetchone()
    if not delivery_zone:
        return None
    delivery_zone = delivery_zone[0]
    if not delivery_zone:
        return None

    if delivery_zone in ('D1', 'D2'):
        delivery_zone = 'D'
    if delivery_zone in ('C1', 'C2'):
        delivery_zone = 'C'

    return delivery_zone


def get_courier_id_in_serviceability(courier_name):
    if courier_name.startswith('Delhivery'):
        return 2
    if courier_name.startswith('Xpressbees'):
        return 5
    if courier_name.startswith('Bluedart'):
        return 9
    if courier_name.startswith('Ecom'):
        return 15
    if courier_name.startswith('Pidge'):
        return 27
    if courier_name.startswith('Self Ship'):
        return 19
    if courier_name.startswith('ATS'):
        return 33
    if courier_name.startswith('SDD'):
        return 3
    if courier_name.startswith('Shadowfax'):
        return 4
    if courier_name.startswith('DTDC'):
        return 42
    if courier_name.startswith('Blowhorn'):
        return 45


def get_courier_id_to_ship_with(rule, del_pincode, cur):
    next_priority = []
    for idx in (6,7,8,9):
        if rule[idx]:
            serv_courier_id=get_courier_id_in_serviceability(rule[idx+4])
            cur.execute("""select serviceable from pincode_serviceability 
                            where courier_id=%s and pincode=%s;""", (serv_courier_id, del_pincode))
            serviceable = cur.fetchone()
            if serviceable and serviceable[0]:
                idx_new = idx+1
                while idx_new<=9 and rule[idx_new]:
                    next_priority.append(rule[idx_new])
                    idx_new+=1
                return rule[idx], next_priority
    return None, next_priority


def check_condition_match_for_order(each_condition, order):
    if each_condition['param'] == 'payment_mode':
        if each_condition['condition'] == 'is' and order[26].lower() == each_condition['value'].lower():
            return True
        elif each_condition['condition'] == 'is_not' and order[26].lower() != each_condition['value'].lower():
            return True
    if each_condition['param'] == 'order_amount':
        if each_condition['condition'] == 'greater_than' and order[27] >= float(each_condition['value']):
            return True
        elif each_condition['condition'] == 'less_than' and order[27] <= float(each_condition['value']):
            return True
    if each_condition['param'] == 'pickup':
        if each_condition['condition'] == 'is' and order[57] == each_condition['value']:
            return True
        elif each_condition['condition'] == 'is_not' and order[57] != each_condition['value']:
            return True
        elif each_condition['condition'] == 'any_of' and order[57] in each_condition['value'].replace(' ','').split(','):
            return True
    if each_condition['param'] == 'delivery_pincode':
        if each_condition['condition'] == 'is' and str(order[18]) == each_condition['value']:
            return True
        elif each_condition['condition'] == 'is_not' and str(order[18]) != each_condition['value']:
            return True
        elif each_condition['condition'] == 'any_of' and str(order[18]) in each_condition['value'].replace(' ','').split(','):
            return True
        elif each_condition['condition'] == 'starts_with' and str(order[18]).startswith(each_condition['value']):
            return True
    if each_condition['param'] == 'zone':
        zone = get_delivery_zone(str(order[58]), str(order[18]))
        if each_condition['condition'] == 'is' and zone == each_condition['value']:
            return True
        elif each_condition['condition'] == 'is_not' and zone != each_condition['value']:
            return True
        elif each_condition['condition'] == 'any_of' and zone in each_condition['value'].replace(' ','').split(','):
            return True
    if each_condition['param'] == 'weight':
        weight, volumetric_weight, dimensions = calculate_order_weight_dimensions(order)
        if each_condition['condition'] == 'greater_than' and (weight>=float(each_condition['value']) or volumetric_weight>=float(each_condition['value'])):
            return True
        elif each_condition['condition'] == 'less_than' and (weight<=float(each_condition['value']) or volumetric_weight<=float(each_condition['value'])):
            return True
    if each_condition['param'] == 'sku':
        for sku in order[59]:
            if each_condition['condition'] == 'is' and sku == each_condition['value']:
                return True
            elif each_condition['condition'] == 'is_not' and sku != each_condition['value']:
                return True
            elif each_condition['condition'] == 'any_of' and sku in each_condition['value'].replace(' ','').split(','):
                return True
            elif each_condition['condition'] == 'starts_with' and sku.startswith(each_condition['value']):
                return True
    return False


def get_lat_lon_pickup(pickup_point, cur):
    try:
        lat, lon = None, None
        address = pickup_point[4]
        if pickup_point[5]:
            address += " " + pickup_point[5]
        if pickup_point[6]:
            address += ", " + pickup_point[6]
        if pickup_point[10]:
            address += ", " + pickup_point[10]
        if pickup_point[8]:
            address += ", " + str(pickup_point[8])
        res = requests.get("https://maps.googleapis.com/maps/api/geocode/json?address=%s&key=%s" % (
            address, "AIzaSyBg7syNb_e1gZgyL1lHXBHRmg3jeaXrkco"))
        if not res.json()['results']:
            address = str(pickup_point[8]) + ", " + pickup_point[6]
            res = requests.get("https://maps.googleapis.com/maps/api/geocode/json?address=%s&key=%s" % (
                address, "AIzaSyBg7syNb_e1gZgyL1lHXBHRmg3jeaXrkco"))
        loc_rank = 0
        location_rank_dict = {"ROOFTOP": 1,
                              "RANGE_INTERPOLATED": 2,
                              "GEOMETRIC_CENTER": 3,
                              "APPROXIMATE": 4}
        for result in res.json()['results']:
            if location_rank_dict[result['geometry']['location_type']] > loc_rank:
                loc_rank = location_rank_dict[result['geometry']['location_type']]
                lat, lon = result['geometry']['location']['lat'], result['geometry']['location']['lng']

        if lat and lon:
            cur.execute("UPDATE pickup_points SET latitude=%s, longitude=%s WHERE id=%s", (lat, lon, pickup_point[1]))
        return lat, lon
    except Exception as e:
        logger.error("lat lon on found for order: ." + str(pickup_point[1]) + "   Error: " + str(e.args[0]))
        return None, None


def get_lat_lon(order, cur):
    try:
        lat, lon = None, None
        address = order[15]
        if order[16]:
            address += " " + order[16]
        if order[17]:
            address += ", " + order[17]
        if order[19]:
            address += ", " + order[19]
        if order[18]:
            address += ", " + order[18]
        res = requests.get("https://maps.googleapis.com/maps/api/geocode/json?address=%s&key=%s" % (
            address, "AIzaSyBg7syNb_e1gZgyL1lHXBHRmg3jeaXrkco"))
        if not res.json()['results']:
            address = order[18] + ", " + order[17]
            res = requests.get("https://maps.googleapis.com/maps/api/geocode/json?address=%s&key=%s" % (
                address, "AIzaSyBg7syNb_e1gZgyL1lHXBHRmg3jeaXrkco"))
        loc_rank = 0
        location_rank_dict = {"ROOFTOP": 1,
                              "RANGE_INTERPOLATED": 2,
                              "GEOMETRIC_CENTER": 3,
                              "APPROXIMATE": 4}
        for result in res.json()['results']:
            if location_rank_dict[result['geometry']['location_type']] > loc_rank:
                loc_rank = location_rank_dict[result['geometry']['location_type']]
                lat, lon = result['geometry']['location']['lat'], result['geometry']['location']['lng']

        if lat and lon:
            cur.execute("UPDATE shipping_address SET latitude=%s, longitude=%s WHERE id=%s", (lat, lon, order[12]))
        return lat, lon
    except Exception as e:
        logger.error("lat lon on found for order: ." + str(order[0]) + "   Error: " + str(e.args[0]))
        return None, None


def invoice_order(cur, last_inv_no, inv_prefix, order_id, pickup_data_id):
    try:
        if not last_inv_no:
            last_inv_no = 0
        inv_no = last_inv_no+1
        inv_text = str(inv_no)
        inv_text = inv_text.zfill(5)
        if inv_prefix:
            inv_text = inv_prefix + "-" + inv_text

        qr_url="https://track.wareiq.com/orders/v1/invoice/%s?uid=%s"%(str(order_id), ''.join(random.choices(string.ascii_lowercase+string.ascii_uppercase + string.digits, k=6)))

        cur.execute("""INSERT INTO orders_invoice (order_id, pickup_data_id, invoice_no_text, invoice_no, date_created, qr_url) 
                        VALUES (%s, %s, %s, %s, %s, %s);""", (order_id, pickup_data_id, inv_text, inv_no, datetime.utcnow()+timedelta(hours=5.5), qr_url))
        return inv_no
    except Exception as e:
        return last_inv_no


def push_awb_easyecom(invoice_id, api_token, awb, courier, cur, companyCarrierId, client_channel_id, pushLabel=None, order_id=None):
    try:
        if not companyCarrierId or not companyCarrierId.isdigit():
            cur.execute("""SELECT id, unique_parameter FROM client_channel
                        WHERE id=%s;"""%str(client_channel_id))

            cour = cur.fetchone()
            if not cour[1] or not cour[1].isdigit():
                add_url = "https://api.easyecom.io/Credentials/addCarrierCredentials?api_token=%s"%api_token
                post_body = {
                    "carrier_id": 14039,
                    "username":"wareiq",
                    "password":"wareiq",
                    "token": "wareiq"
                }

                req = requests.post(add_url, data=post_body).json()
                cur.execute("UPDATE client_channel SET unique_parameter='%s' WHERE id=%s"%(req['data']['companyCarrierId'], str(client_channel_id)))
                companyCarrierId = req['data']['companyCarrierId']
            else:
                companyCarrierId = cour[1]

        post_url = "https://api.easyecom.io/Carrier/assignAWB?api_token=%s"%api_token
        post_body = {
            "invoiceId": invoice_id,
            "api_token": api_token,
            "courier": courier[10],
            "awbNum": awb,
            "companyCarrierId": int(companyCarrierId)
        }
        if pushLabel:
            headers_new = {"Content-Type": "application/json",
                           "Authorization": "Token B52Si3qU6uOUbxCbidWTLaJlQEk9UfWkPK7BvTIt"}
            req_new = requests.post(os.environ.get('CORE_SERVICE_URL')+"/orders/v1/download/shiplabels", headers=headers_new, json={"order_ids": [order_id]})
            if req_new.status_code==200:
                post_body['shippingLabelUrl']=req_new.json()['url']
        req = requests.post(post_url, data=post_body)
        if req.status_code!=200:
            requests.post(post_url, data=post_body)
            try:
                error = str(req.json())
            except Exception:
                error = None
            if error:
                cur.execute("UPDATE orders SET status_detail=%s WHERE order_id_channel_unique=%s and client_channel_id=%s",
                            (str(req.json()), invoice_id, client_channel_id))
    except Exception as e:
        logger.error("Easyecom not updated.")


def cod_verification_text(order, cur):
    cod_confirmation_link = "https://track.wareiq.com/core/v1/passthru/cod?CustomField=%s" % str(order[0])
    cod_confirmation_link = UrlShortner.get_short_url(cod_confirmation_link, cur)

    insert_cod_ver_tuple = (order[0], cod_confirmation_link, datetime.now())
    cur.execute("INSERT INTO cod_verification (order_id, verification_link, date_created) VALUES (%s,%s,%s);",
                insert_cod_ver_tuple)
    client_name = order[51]
    customer_phone = order[5].replace(" ", "")
    customer_phone = "0" + customer_phone[-10:]
    payload = {
        "event": "cod_verification",
        "user": {
            "mobile": customer_phone,
        },
        "data": {
            "client_name": client_name,
            "order_amount": str(order[27]),
            "verification_link": cod_confirmation_link
        }
    }

    req = requests.post(RAVEN_URL, headers=RAVEN_HEADERS, data=json.dumps(payload))


def send_received_event(client_name, customer_phone, tracking_link):
    payload = {
        "event": "received",
        "user": {
            "mobile": customer_phone,
        },
        "data": {
            "client_name": client_name,
            "tracking_link": tracking_link,
        }
    }

    req = requests.post(RAVEN_URL, headers=RAVEN_HEADERS, data=json.dumps(payload))
