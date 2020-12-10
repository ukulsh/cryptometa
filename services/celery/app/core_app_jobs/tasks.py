from .contants import *
from .queries import *
from .utils import *
from datetime import datetime
from woocommerce import API
from math import ceil
from app.db_utils import DbConnection
from app.ship_orders.function import ship_orders

conn = DbConnection.get_db_connection_instance()
conn_2 = DbConnection.get_pincode_db_connection_instance()
conn_3 = DbConnection.get_users_db_connection_instance()


def consume_ecom_scan_util(payload):
    try:
        cur = conn.cursor()
        awb = payload.get('awb')
        if not awb:
            return "Skipped: no awb"

        reason_code_number = payload.get('reason_code_number')
        if not reason_code_number:
            return "Skipped: no reason code"

        cur.execute(get_order_details_query%str(awb))
        try:
            status_time = payload.get("datetime")
            status_time = datetime.strptime(status_time, "%Y-%m-%d %H:%M:%S")
        except Exception:
            status_time = datetime.utcnow() + timedelta(hours=5.5)

        order = None
        try:
            order = cur.fetchone()
        except Exception:
            pass

        if not order:
            return "Failed: order not found"

        status_code = str(payload.get("reason_code_number"))
        status = str(payload.get("reason_code"))
        status_text = str(payload.get("status"))
        location = str(payload.get("location"))
        location_city = str(payload.get("city"))

        if reason_code_number in ecom_express_status_mapping:
            status = ecom_express_status_mapping[reason_code_number][0]
            status_type = ecom_express_status_mapping[reason_code_number][1]
            status_text = ecom_express_status_mapping[reason_code_number][3]
        else:
            cur.execute(insert_scan_query, (
                order[0], order[38], order[10], status_code, status, status_text, location, location_city, status_time))
            conn.commit()
            return "Successful: scan saved only"

        if str(payload.get("status")) == "R999":
            status = "RTO"

        cur.execute(insert_scan_query, (
            order[0], order[38], order[10], status_code, status, status_text, location, location_city, status_time))

        if not status:
            return "Successful: scan saved only"

        tracking_status = ecom_express_status_mapping[reason_code_number][2]
        if tracking_status:
            cur.execute(insert_status_query, (
                order[0], order[38], order[10], status_type, tracking_status, status_text, location, location_city, status_time))

        if tracking_status == "Picked":
            mark_picked_channel(order, cur)
            exotel_send_shipped_sms(order, "Ecom Express")
            send_shipped_email(order)
            mark_order_picked_pickups(order, cur)

        elif tracking_status == "Delivered":
            mark_delivered_channel(order)
            exotel_send_delivered_sms(order)

        elif tracking_status == "RTO":
            mark_rto_channel(order)

        if reason_code_number in ecom_express_ndr_reasons:
            ndr_reason = ecom_express_ndr_reasons[reason_code_number]
            update_ndr_shipment(order, cur, ndr_reason)

        cur.execute("UPDATE orders SET status=%s, status_type=%s WHERE id=%s;", (status, status_type, order[0]))

        conn.commit()
    except Exception as e:
        conn.rollback()
        return "Failed: " + str(e.args[0])
    return "Successful: all tasks done"


def sync_all_products_with_channel(client_prefix):
    cur = conn.cursor()
    cur.execute("""select shop_url, api_key, api_password, channel_name, bb.id from client_channel aa
                    left join master_channels bb on aa.channel_id=bb.id
                    where connection_status=true and status=true and client_prefix='%s'"""%client_prefix)
    all_channels = cur.fetchall()
    for channel in all_channels:
        try:
            if channel[3] == "Shopify":
                since_id = "1"
                count = 250
                while count == 250:
                    create_fulfillment_url = "https://%s:%s@%s/admin/api/2020-07/products.json?limit=250&since_id=%s" % (channel[1], channel[2], channel[0], since_id)
                    qs = requests.get(create_fulfillment_url)
                    for prod in qs.json()['products']:
                        for prod_obj in prod['variants']:
                            cur.execute("""select id from products where sku='%s' and client_prefix='%s';"""%(str(prod_obj['id']), client_prefix))
                            prod_obj_x = cur.fetchone()
                            prod_name = prod['title']
                            if prod_obj['title'] != 'Default Title':
                                prod_name += " - " + prod_obj['title']
                            if prod_obj_x:
                                cur.execute("""UPDATE products SET master_sku=%s, price=%s, name=%s WHERE id=%s""", (prod_obj['sku'], float(prod_obj['price']), prod_name, prod_obj_x[0]))
                            else:
                                cur.execute("""INSERT INTO products (name, sku, active, channel_id, date_created, price, master_sku, client_prefix) VALUES 
                                                (%s,%s,%s,%s,%s,%s,%s,%s);""", (prod_name, str(prod_obj['id']), True, channel[4], datetime.now(),
                                                                             float(prod_obj['price']), prod_obj['sku'], client_prefix))

                        conn.commit()

                    count = len(qs.json()['products'])
                    since_id = str(qs.json()['products'][-1]['id'])
                    conn.commit()

            elif channel[3] == "WooCommerce":
                try:
                    auth_session = API(
                        url=channel[0],
                        consumer_key=channel[1],
                        consumer_secret=channel[2],
                        version="wc/v3"
                    )
                    r = auth_session.get("products")
                except Exception:
                    auth_session = API(
                        url=channel[5],
                        consumer_key=channel[3],
                        consumer_secret=channel[4],
                        version="wc/v3",
                        verify_ssl=False
                    )
                    r = auth_session.get("products")
                page = 1
                count = 100
                while count == 100:
                    qs = auth_session.get("products?per_page=100&page=%s"%str(page))
                    all_prods = qs.json()
                    for prod in all_prods:
                        prod_name = prod['name']
                        if prod['variations']:
                            qs = auth_session.get("products/%s/variations" % str(prod['id']))
                            all_variants = qs.json()
                            for prod_obj in all_variants:
                                cur.execute("""select id from products where sku='%s' and client_prefix='%s';"""%(str(prod_obj['id']), client_prefix))
                                prod_obj_x = cur.fetchone()
                                if prod_obj_x:
                                    cur.execute("""UPDATE products SET master_sku=%s, price=%s, name=%s WHERE id=%s""", (prod_obj['sku'], float(prod_obj['price']), prod_name, prod_obj_x[0]))
                                else:
                                    cur.execute("""INSERT INTO products (name, sku, active, channel_id, date_created, price, master_sku, client_prefix) VALUES 
                                                    (%s,%s,%s,%s,%s,%s,%s,%s);""", (prod_name, str(prod_obj['id']), True, channel[4], datetime.now(),
                                                                                 float(prod_obj['price']), prod_obj['sku'], client_prefix))

                            conn.commit()
                        else:
                            cur.execute("""select id from products where sku='%s' and client_prefix='%s';""" % (
                            str(prod['id']), client_prefix))
                            prod_obj_x = cur.fetchone()
                            if prod_obj_x:
                                cur.execute("""UPDATE products SET master_sku=%s, price=%s, name=%s WHERE id=%s""",
                                            (prod['sku'], float(prod['price']), prod_name, prod_obj_x[0]))
                            else:
                                cur.execute("""INSERT INTO products (name, sku, active, channel_id, date_created, price, master_sku, client_prefix) VALUES 
                                                                                (%s,%s,%s,%s,%s,%s,%s,%s);""",
                                            (prod_name, str(prod['id']), True, channel[4], datetime.now(),
                                             float(prod['price']) if prod['price'] else None, prod['sku'], client_prefix))
                            conn.commit()

                    count = len(all_prods)
                    page += 1
                    conn.commit()

        except Exception as e:
            logger.error("Product sync failed for: "+client_prefix+" "+channel[3])

    conn.commit()
    return "Synced channel products for " + client_prefix


def create_cod_remittance_entry():
    cur = conn.cursor()

    cur.execute("select distinct(client_prefix) FROM orders aa WHERE client_prefix is not null order by client_prefix")
    all_clients = cur.fetchall()
    insert_tuple = list()
    insert_value_str = ""
    remittance_date = datetime.utcnow() + timedelta(hours=5.5) + timedelta(days=8)
    for client in all_clients:
        remittance_id = client[0] + "_" + str(remittance_date.date())
        last_remittance_id = client[0] + "_" + str((remittance_date - timedelta(days=7)).date())
        cur.execute("SELECT * from cod_remittance WHERE remittance_id=%s", (last_remittance_id,))
        try:
            cur.fetchone()[0]
        except Exception as e:
            insert_tuple.append(
                (client[0], last_remittance_id, remittance_date - timedelta(days=7), 'processing',
                 datetime.utcnow() + timedelta(hours=5.5)))
            insert_value_str += "%s,"
        insert_tuple.append(
            (client[0], remittance_id, remittance_date, 'processing', datetime.utcnow() + timedelta(hours=5.5)))
        insert_value_str += "%s,"

    insert_value_str = insert_value_str.rstrip(",")

    cur.execute(
        "INSERT INTO cod_remittance (client_prefix, remittance_id, remittance_date, status, date_created) VALUES __IVS__;".replace(
            '__IVS__', insert_value_str), tuple(insert_tuple))

    conn.commit()


def queue_cod_remittance_razorpay():
    cur = conn.cursor()
    cur_3 = conn_3.cursor()

    remittance_date = datetime.utcnow() + timedelta(hours=5.5)

    query_to_run = select_remittance_amount_query.replace('__REMITTANCE_DATE__', str(remittance_date.date()))

    cur.execute(query_to_run)
    all_remittance = cur.fetchall()
    for remit in all_remittance:
        try:
            cur.execute("SELECT account_type, current_balance, lock_cod from client_mapping where client_prefix=%s", (remit[1],))
            balance_data = cur.fetchone()
            if str(balance_data[0]).lower() == 'prepaid' and balance_data[1] < 500:
                continue
            elif balance_data[2]:
                continue

            cur_3.execute(
                "SELECT account_name, ifsc_code, account_no, primary_email FROM clients WHERE client_prefix=%s",
                (remit[1],))
            account_data = cur_3.fetchone()

            if not account_data or not account_data[0] or not account_data[1] or not account_data[2]:
                print("Account data not avaiable for: "+str(remit[1]))
                continue

            amount = int(remit[6] * 100)
            razorpay_body = {
                "account_number": "7878780047779262",
                "amount": amount,
                "currency": "INR",
                "mode": "NEFT",
                "purpose": "COD Remittance",
                "fund_account": {
                    "account_type": "bank_account",
                    "bank_account": {
                        "name": account_data[0],
                        "ifsc": account_data[1],
                        "account_number": account_data[2]
                    },
                    "contact": {
                        "name": remit[1],
                        "email": account_data[3],
                        "type": "customer",
                        "reference_id": remit[1],
                        "notes": {
                            "notes_key_1": "COD remittance " + remit[1]
                        }
                    }
                },
                "queue_if_low_balance": True,
                "reference_id": remit[1] + str(remit[0]),
                "narration": "COD remittance " + remit[1],
                "notes": {
                    "notes_key_1": "COD remittance " + remit[1] + "\nDate: " + str(remittance_date.date()),
                }
            }

            headers = {
                'Content-Type': 'application/json',
            }

            response = requests.post('https://api.razorpay.com/v1/payouts', headers=headers,
                                     data=json.dumps(razorpay_body),
                                     auth=("rzp_live_FGAwxhtumHezAw", "IZ7C97EEef0rvyqZJLy0CYNb"))

            cur.execute("UPDATE cod_remittance SET payout_id=%s WHERE id=%s", (response.json()['id'], remit[0]))
            conn.commit()

        except Exception as e:
            logger.error(
                "Couldn't create remittance on razorpay X for: " + str(remit[1]) + "\nError: " + str(e.args[0]))


def calculate_costs_util():
    cur = conn.cursor()
    cur_2 = conn_2.cursor()
    current_time = datetime.utcnow() + timedelta(hours=5) - timedelta(days=40)
    current_time = current_time.strftime('%Y-%m-%d')
    cur.execute(select_orders_to_calculate_query.replace('__STATUS_TIME__', current_time))
    all_orders = cur.fetchall()

    for order in all_orders:
        try:
            delivery_zone = order[15]
            if not delivery_zone:
                cur_2.execute("SELECT city from city_pin_mapping where pincode='%s';" % order[7])
                pickup_city = cur_2.fetchone()
                if not pickup_city:
                    logger.info("pickup city not found: " + str(order[0]))
                    continue
                pickup_city = pickup_city[0]
                cur_2.execute("SELECT city from city_pin_mapping where pincode='%s';" % order[8])
                deliver_city = cur_2.fetchone()
                if not deliver_city:
                    logger.info("deliver city not found: " + str(order[0]))
                    continue
                deliver_city = deliver_city[0]

                zone_select_tuple = (pickup_city, deliver_city)
                cur_2.execute("SELECT zone_value from city_zone_mapping where zone=%s and city=%s;",
                              zone_select_tuple)
                delivery_zone = cur_2.fetchone()
                if not delivery_zone:
                    logger.info("deliver zone not found: " + str(order[0]))
                    continue
                delivery_zone = delivery_zone[0]
                if not delivery_zone:
                    logger.info("deliver zone not found: " + str(order[0]))
                    continue

                if delivery_zone in ('D1', 'D2'):
                    delivery_zone = 'D'
                if delivery_zone in ('C1', 'C2'):
                    delivery_zone = 'C'

            calculate_courier_cost(cur, delivery_zone, order)
            charged_weight = order[4] if order[4] else 0

            # if order[6] != 'NASHER':
            if order[3] and order[3] > charged_weight:
                charged_weight = order[3]
            '''
            else:
            if courier_id==1:
                volumetric_weight = (order[14]['length']*order[14]['breadth']*order[14]['height'])/4500
            else:
                volumetric_weight = (order[14]['length']*order[14]['breadth']*order[14]['height'])/5000
            if volumetric_weight > charged_weight:
                charged_weight = volumetric_weight
            '''
            if not charged_weight:
                logger.info("charged weight not found: " + str(order[0]))
                cur.execute(
                    """INSERT INTO client_deductions (weight_charged, zone, shipment_id) VALUES (%s,%s,%s) RETURNING id;""",
                    (charged_weight, delivery_zone, order[0]))
                logger.info("charged weight not found: " + str(order[0]))
                continue

            try:
                # if order[6] != 'NASHER' or (order[6] == 'NASHER' and charged_weight < 10.0):
                cost_select_tuple = (order[6], order[2])
                cur.execute(
                    "SELECT __ZONE__, cod_min, cod_ratio, rto_ratio, __ZONE_STEP__, rvp_ratio from cost_to_clients WHERE client_prefix=%s and courier_id=%s;".replace(
                        '__ZONE__', zone_column_mapping[delivery_zone]).replace('__ZONE_STEP__',
                                                                                zone_step_charge_column_mapping[
                                                                                    delivery_zone]), cost_select_tuple)
                charge_rate_values = cur.fetchone()
                if not charge_rate_values:
                    cur.execute(
                        "SELECT __ZONE__, cod_min, cod_ratio, rto_ratio, __ZONE_STEP__, rvp_ratio from client_default_cost WHERE courier_id=%s;".replace(
                            '__ZONE__', zone_column_mapping[delivery_zone]).replace('__ZONE_STEP__',
                                                                                    zone_step_charge_column_mapping[
                                                                                        delivery_zone]), (order[2],))
                    charge_rate_values = cur.fetchone()
                if not charge_rate_values:
                    cur.execute(
                        """INSERT INTO client_deductions (weight_charged, zone, shipment_id) VALUES (%s,%s,%s) RETURNING id;""",
                        (charged_weight, delivery_zone, order[0]))

                    logger.info("charge_rate_values not found: " + str(order[0]))
                    continue

                cur.execute("select weight_offset, additional_weight_offset from master_couriers where id=%s;",
                            (order[2],))
                courier_data = cur.fetchone()
                charge_rate = charge_rate_values[0]
                forward_charge = charge_rate
                per_step_charge = charge_rate_values[4] if charge_rate_values and len(charge_rate_values) >= 5 else 0.0
                per_step_charge = 0.0 if per_step_charge is None else per_step_charge
                if courier_data[0] != 0 and courier_data[1] != 0:
                    if not per_step_charge:
                        per_step_charge = charge_rate
                    if charged_weight > courier_data[0]:
                        forward_charge = charge_rate + ceil(
                            (charged_weight - courier_data[0] * 1.0) / courier_data[1]) * per_step_charge
                else:
                    multiple = ceil(charged_weight / 0.5)
                    forward_charge = charge_rate * multiple
                forward_charge_gst = forward_charge * 1.18
                rto_charge = 0
                rto_charge_gst = 0
                cod_charge = 0
                cod_charged_gst = 0
                if order[13] == 'RTO':
                    rto_charge = forward_charge * charge_rate_values[3]
                    rto_charge_gst = forward_charge_gst * charge_rate_values[3]
                elif order[13] == 'DTO':
                    rto_charge = forward_charge * charge_rate_values[5]
                    rto_charge_gst = forward_charge_gst * charge_rate_values[5]
                else:
                    if order[11] and order[11].lower() == 'cod':
                        if order[12]:
                            cod_charge = order[12] * (charge_rate_values[2] / 100)
                            if charge_rate_values[1] > cod_charge:
                                cod_charge = charge_rate_values[1]
                        else:
                            cod_charge = charge_rate_values[1]

                        cod_charged_gst = cod_charge * 1.18
                '''
                else:
                    charge_rate_values = (None, 32, 1.5, 1)
                    intial_charge = nasher_zonal_mapping[delivery_zone][0]
                    next_weight = charged_weight-10.0
                    charge_rate = nasher_zonal_mapping[delivery_zone][1]
                    multiple = ceil(next_weight / 1.0)

                    forward_charge = charge_rate * multiple + intial_charge
                    forward_charge_gst = forward_charge * 1.18

                    rto_charge = 0
                    rto_charge_gst = 0
                    cod_charge = 0
                    cod_charged_gst = 0
                    if order[13] in ('RTO','DTO'):
                        rto_charge = forward_charge * charge_rate_values[3]
                        rto_charge_gst = forward_charge_gst * charge_rate_values[3]
                    else:
                        if order[11] and order[11].lower() == 'cod':
                            if order[12]:
                                cod_charge = order[12] * (charge_rate_values[2] / 100)
                                if charge_rate_values[1] > cod_charge:
                                    cod_charge = charge_rate_values[1]
                            else:
                                cod_charge = charge_rate_values[1]

                            cod_charged_gst = cod_charge * 1.18
                '''

                if order[9]:
                    deduction_time = order[9]
                elif order[10]:
                    deduction_time = order[10]
                else:
                    deduction_time = datetime.now()

                if order[13] == "DTO":
                    forward_charge = 0
                    forward_charge_gst = 0

                total_charge = forward_charge + cod_charge + rto_charge
                total_charge_gst = forward_charge_gst + rto_charge_gst + cod_charged_gst
                insert_rates_tuple = (charged_weight, delivery_zone, deduction_time, cod_charge, cod_charged_gst,
                                      forward_charge, forward_charge_gst, rto_charge, rto_charge_gst, order[0],
                                      total_charge, total_charge_gst, datetime.now(), datetime.now())

                cur.execute(update_client_balance, (total_charge_gst+5.9, order[6]))
                cur.execute(insert_into_deduction_query, insert_rates_tuple)
            except Exception as e:
                logger.error("couldn't calculate order: " + str(order[0]) + "\nError: " + str(e))
                cur.execute(
                    """INSERT INTO client_deductions (weight_charged, zone, shipment_id) VALUES (%s,%s,%s) RETURNING id;""",
                    (charged_weight, delivery_zone, order[0]))
                continue
            conn.commit()
        except Exception as e:
            logger.error("couldn't calculate order: " + str(order[0]) + "\nError: " + str(e))

    conn.commit()


def calculate_courier_cost(cur, delivery_zone, order):
    try:
        charged_weight = order[4] if order[4] else 0
        volumetric_weight = (order[14]['length'] * order[14]['breadth'] * order[14]['height']) / 5000
        if volumetric_weight > charged_weight:
            charged_weight = volumetric_weight

        cost_select_tuple = (order[2],)
        cur.execute(
            "SELECT __ZONE__, __ZONE___add, cod_min, cod_ratio, rto_ratio, first_step, next_step from courier_costs WHERE courier_id=%s;".replace(
                '__ZONE__', zone_column_mapping_courier[delivery_zone]), cost_select_tuple)
        charge_rate_values = cur.fetchone()
        if not charge_rate_values:
            logger.info("courier cost not found: " + str(order[0]))
            return None

        if order[2] != 8:
            first_step_cost = charge_rate_values[0]
            next_step_cost = 0
            if (charged_weight - charge_rate_values[5]) > 0:
                next_step_cost = ceil((charged_weight - charge_rate_values[5]) / charge_rate_values[6]) * \
                                 charge_rate_values[1]

            forward_charge = first_step_cost + next_step_cost

            rto_charge = 0
            cod_charge = 0
            if order[13] == 'RTO':
                if order[2] not in (11, 12):
                    rto_charge = forward_charge * charge_rate_values[4]
                else:
                    rto_multiple = ceil(charged_weight)
                    if order[2] == 11:
                        rto_charge = rto_multiple * rto_heavy_2[delivery_zone]
                    else:
                        rto_charge = rto_multiple * rto_heavy_1[delivery_zone]

            else:
                if order[11] and order[11].lower() == 'cod':
                    if order[12]:
                        cod_charge = order[12] * (charge_rate_values[3] / 100)
                        if charge_rate_values[2] > cod_charge:
                            cod_charge = charge_rate_values[2]
                    else:
                        cod_charge = charge_rate_values[2]
        else:
            first_step_cost = charge_rate_values[0]
            next_step_cost = 0
            if (charged_weight - charge_rate_values[5]) > 3:
                second_step_cost = 3 * bulk_second_step[delivery_zone]
                next_step_cost = ceil((charged_weight - 5) / charge_rate_values[6]) * \
                                 charge_rate_values[1]
            else:
                second_step_cost = ceil((charged_weight - charge_rate_values[5])) * bulk_second_step[delivery_zone]

            forward_charge = first_step_cost + second_step_cost + next_step_cost

            rto_charge = 0
            cod_charge = 0
            if order[13] == 'RTO':
                rto_multiple = ceil(charged_weight)
                rto_charge = rto_multiple * rto_bulk[delivery_zone]
            else:
                if order[11] and order[11].lower() == 'cod':
                    if order[12]:
                        cod_charge = order[12] * (charge_rate_values[3] / 100)
                        if charge_rate_values[2] > cod_charge:
                            cod_charge = charge_rate_values[2]
                    else:
                        cod_charge = charge_rate_values[2]

        total_charge = forward_charge + rto_charge + cod_charge

        if order[9]:
            deduction_time = order[9]
        elif order[10]:
            deduction_time = order[10]
        else:
            deduction_time = datetime.now()

        insert_rates_tuple = (
        charged_weight, delivery_zone, deduction_time, cod_charge, forward_charge, rto_charge, order[0],
        total_charge, datetime.now(), datetime.now())

        cur.execute(insert_into_courier_cost_query, insert_rates_tuple)
        conn.commit()

    except Exception as e:
        logger.error("couldn't calculate courier cost order: " + str(order[0]) + "\nError: " + str(e))


def upload_products_util(prod_list):
    cur = conn.cursor()
    for prod_item in prod_list:
        try:
            cur.execute("SELECT * from products WHERE master_sku=%s and client_prefix=%s", (prod_item[0], "NASHER"))
            try:
                cur.fetchone()[0]
            except Exception as e:
                cur.execute("""INSERT INTO products (name, sku, active, channel_id, client_prefix, dimensions, 
                            price, weight, master_sku) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);""", (prod_item[0], prod_item[0],
                                                                                                 True, 4, "NASHER", json.dumps(prod_item[3]),
                                                                                                 prod_item[1], prod_item[2], prod_item[0]))
        except Exception as e:
            logger.error("Couldn't upload prod: "+str(prod_item[0])+"\nError: "+str(e.args[0]))

        conn.commit()


def ship_bulk_orders(order_list, auth_data, courier):
    cur = conn.cursor()
    if auth_data['user_group'] not in ('client', 'super-admin', 'multi-vendor'):
        return {"success":False, "msg": "invalid user"}, 400

    if len(order_list)==1:
        order_tuple_str = "("+str(order_list[0])+")"
    else:
        order_tuple_str = str(tuple(order_list))

    query_to_run = """SELECT array_agg(id) FROM orders WHERE id in __ORDER_IDS__ __CLIENT_FILTER__;""".replace("__ORDER_IDS__", order_tuple_str)

    if auth_data['user_group'] == 'client':
        query_to_run = query_to_run.replace('__CLIENT_FILTER__', "AND client_prefix='%s'"%auth_data['client_prefix'])
    elif auth_data['user_group'] == 'multi-vendor':
        cur.execute("SELECT vendor_list FROM multi_vendor WHERE client_prefix='%s';" % auth_data['client_prefix'])
        vendor_list = cur.fetchone()[0]
        query_to_run = query_to_run.replace("__CLIENT_FILTER__",
                                            "AND client_prefix in %s" % str(tuple(vendor_list)))
    else:
        query_to_run = query_to_run.replace("__CLIENT_FILTER__","")

    cur.execute(query_to_run)
    order_ids = cur.fetchone()[0]
    if not order_ids:
        return {"success": False, "msg": "invalid order ids"}, 400
    ship_orders(courier_name=courier, order_ids=order_ids, force_ship=True)

    return {"success": True, "msg": "shipped successfully"}, 200


def update_available_quantity():
    cur = conn.cursor()
    cur.execute(fetch_inventory_quantity_query)
    all_prods_status = cur.fetchall()
    quantity_dict = dict()
    combo_dict = dict()

    for prod_status in all_prods_status:
        if not prod_status[2]:
            continue
        if prod_status[0] not in quantity_dict:
            quantity_dict[prod_status[0]] = {prod_status[2]: {"available_quantity": 0,
                                                              "current_quantity": 0,
                                                              "inline_quantity": 0,
                                                              "rto_quantity": 0}}
        elif prod_status[2] not in quantity_dict[prod_status[0]]:
            quantity_dict[prod_status[0]][prod_status[2]] = {"available_quantity": 0,
                                                              "current_quantity": 0,
                                                              "inline_quantity": 0,
                                                              "rto_quantity": 0}

        if prod_status[1] in ('DELIVERED','DISPATCHED','IN TRANSIT','ON HOLD','PENDING'):
            quantity_dict[prod_status[0]][prod_status[2]]['current_quantity'] -= prod_status[3]
            quantity_dict[prod_status[0]][prod_status[2]]['available_quantity'] -= prod_status[3]
        elif prod_status[1] in ('NEW','PICKUP REQUESTED','READY TO SHIP', 'PENDING PAYMENT'):
            quantity_dict[prod_status[0]][prod_status[2]]['inline_quantity'] += prod_status[3]
            quantity_dict[prod_status[0]][prod_status[2]]['available_quantity'] -= prod_status[3]
        elif prod_status[1] in ('RTO', 'DTO'):
            quantity_dict[prod_status[0]][prod_status[2]]['rto_quantity'] += prod_status[3]
            if prod_status[1]=="DTO":
                quantity_dict[prod_status[0]][prod_status[2]]['current_quantity'] += prod_status[3]
                quantity_dict[prod_status[0]][prod_status[2]]['available_quantity'] += prod_status[3]

        if prod_status[4] and prod_status[0] not in combo_dict:
            combo_dict[prod_status[0]] = {'prod_ids': prod_status[4], 'prod_quan': prod_status[5]}

    for prod_id, item_list in combo_dict.items():
        for warehouse, quan_values in quantity_dict[prod_id].items():
            quantity_dict[prod_id][warehouse] = {'available_quantity': 0,
                                                 'current_quantity': 0,
                                                 'inline_quantity': 0,
                                                 'rto_quantity': 0}

            for idx, new_prod_id in enumerate(item_list['prod_ids']):
                mul_fac = item_list['prod_quan'][idx]
                if new_prod_id not in quantity_dict:
                    quantity_dict[new_prod_id] = {warehouse: {'available_quantity': quan_values['available_quantity']*mul_fac,
                                                              'current_quantity': quan_values['current_quantity']*mul_fac,
                                                              'inline_quantity': quan_values['inline_quantity']*mul_fac,
                                                              'rto_quantity': quan_values['rto_quantity']*mul_fac}}
                elif warehouse not in quantity_dict[new_prod_id]:
                    quantity_dict[new_prod_id][warehouse] = {'available_quantity': quan_values['available_quantity']*mul_fac,
                                                              'current_quantity': quan_values['current_quantity']*mul_fac,
                                                              'inline_quantity': quan_values['inline_quantity']*mul_fac,
                                                              'rto_quantity': quan_values['rto_quantity']*mul_fac}

                else:
                    quantity_dict[new_prod_id][warehouse]['available_quantity'] += quan_values['available_quantity']*mul_fac
                    quantity_dict[new_prod_id][warehouse]['current_quantity'] += quan_values['current_quantity']*mul_fac
                    quantity_dict[new_prod_id][warehouse]['inline_quantity'] += quan_values['inline_quantity']*mul_fac
                    quantity_dict[new_prod_id][warehouse]['rto_quantity'] += quan_values['rto_quantity']*mul_fac

    for prod_id, wh_dict in quantity_dict.items():
        for warehouse, quan_values in wh_dict.items():
            update_tuple = (quan_values['available_quantity'], quan_values['current_quantity'], quan_values['inline_quantity'],
                            quan_values['rto_quantity'], prod_id, warehouse)
            cur.execute(update_inventory_quantity_query, update_tuple)

    conn.commit()


def update_available_quantity_on_channel():
    cur = conn.cursor()
    cur.execute("""SELECT client_prefix, channel_id, api_key, api_password, shop_url FROM client_channel WHERE sync_inventory=true;""")
    all_channels = cur.fetchall()

    for channel in all_channels:
        if channel[1]==6: #mangento sync
            cur.execute("""select master_sku, GREATEST(available_quantity, 0) as available_quantity from
                                (select master_sku, sum(available_quantity) as available_quantity from products_quantity aa
                                left join products bb on aa.product_id=bb.id
                                where bb.client_prefix='__CLIENT_PREFIX__'
                                group by master_sku
                                order by available_quantity) xx""".replace('__CLIENT_PREFIX__', channel[0]))

            all_quan = cur.fetchall()
            source_items = list()
            headers = {'Authorization': "Bearer " + channel[2],
                       'Content-Type': 'application/json',
                       'User-Agent': 'WareIQ server'}
            for quan in all_quan:
                salable_quan = quan[1]
                try:
                    salable_quan = requests.get("%s/V1/inventory/get-product-salable-quantity/%s/1"%(channel[4], quan[0]), headers=headers).json()
                except Exception as e:
                    pass
                if type(salable_quan) != int:
                    continue
                if salable_quan<0:
                    source_items.append({
                        "sku": quan[0],
                        "source_code": "default",
                        "quantity": quan[1] + (quan[1]-salable_quan) if type(salable_quan)==int else quan[1],
                        "status": 1
                    })

            magento_url = channel[4]+ "/V1/inventory/source-items"
            body = {
                "sourceItems": source_items}
            r = requests.post(magento_url, headers=headers, data=json.dumps(body))

