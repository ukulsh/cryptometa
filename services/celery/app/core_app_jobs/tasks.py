from .contants import *
from .queries import *
from .utils import *
from datetime import datetime
from woocommerce import API
from app.db_utils import DbConnection

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

    cur.execute("select distinct(client_prefix) FROM orders aa order by client_prefix")
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
                (client[0], remittance_id, remittance_date - timedelta(days=7), 'processing',
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
    cur_2 = conn_3.cursor()

    remittance_date = datetime.utcnow() + timedelta(hours=5.5)

    query_to_run = select_remittance_amount_query.replace('__REMITTANCE_DATE__', str(remittance_date.date()))

    cur.execute(query_to_run)
    all_remittance = cur.fetchall()
    for remit in all_remittance:
        try:
            cur.execute("SELECT account_type, current_balance from client_mapping where client_prefix=%s", (remit[1],))
            balance_data = cur.fetchone()
            if str(balance_data[0]).lower() == 'prepaid' and balance_data[1] < 500:
                continue

            cur_2.execute(
                "SELECT account_name, ifsc_code, account_no, primary_email FROM clients WHERE client_prefix=%s",
                (remit[1],))
            account_data = cur_2.fetchone()

            amount = int(remit[6] * 100)
            razorpay_body = {
                "account_number": "2323230053880955",
                "amount": amount,
                "currency": "INR",
                "mode": "NEFT",
                "purpose": "COD Remittance",
                "fund_account": {
                    "account_type": "bank_account",
                    "bank_account": {
                        "name": account_data[0] if account_data[0] else "Ravi Chaudhary",
                        "ifsc": account_data[1] if account_data[1] else "ICIC0002333",
                        "account_number": account_data[2] if account_data[2] else "233301514571"
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
                                     auth=('rzp_test_6k89T5DcoLmvCO', 'wEM0vuFABblEjNMotlar9bxz'))

            cur.execute("UPDATE cod_remittance SET payout_id=%s WHERE id=%s", (response.json()['id'], remit[0]))
            conn.commit()

        except Exception as e:
            logger.error(
                "Couldn't create remittance on razorpay X for: " + str(remit[1]) + "\nError: " + str(e.args[0]))


select_remittance_amount_query = """select * from
                                        (select xx.unique_id, xx.client_prefix, xx.remittance_id, xx.date as remittance_date, 
                                         xx.status, xx.transaction_id, sum(yy.amount) as remittance_total from
                                        (select id as unique_id, client_prefix, remittance_id, transaction_id, DATE(remittance_date), 
                                        ((DATE(remittance_date)) - INTERVAL '8 DAY') AS order_start,
                                        ((DATE(remittance_date)) - INTERVAL '1 DAY') AS order_end,
                                        status from cod_remittance) xx 
                                        left join 
                                        (select client_prefix, channel_order_id, order_date, payment_mode, amount, cc.status_time as delivered_date from orders aa
                                        left join orders_payments bb on aa.id=bb.order_id
                                        left join (select * from order_status where status='Delivered') cc
                                        on aa.id=cc.order_id
                                        where aa.status = 'DELIVERED'
                                        and bb.payment_mode ilike 'cod') yy
                                        on xx.client_prefix=yy.client_prefix 
                                        and yy.delivered_date BETWEEN xx.order_start AND xx.order_end
                                        group by xx.unique_id, xx.client_prefix, xx.remittance_id, xx.date, xx.status, xx.transaction_id) zz
                                        WHERE remittance_total is not null
                                        and remittance_date='__REMITTANCE_DATE__'
                                        order by remittance_date DESC, remittance_total DESC"""


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

