from .contants import *
from .queries import *
from .utils import *
from datetime import datetime
from woocommerce import API
from app.db_utils import DbConnection

conn = DbConnection.get_db_connection_instance()
conn_2 = DbConnection.get_pincode_db_connection_instance()


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