import psycopg2, requests, os, json
from datetime import datetime, timedelta
from requests_oauthlib.oauth1_session import OAuth1Session
import logging

from .queries import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)
"""
host = os.environ('DTATBASE_HOST')
database = os.environ('DTATBASE_NAME')
user = os.environ('DTATBASE_USER')
password = os.environ('DTATBASE_PASSWORD')
conn = psycopg2.connect(host=host, database=database, user=user, password=password)
"""
conn = psycopg2.connect(host="wareiq-core-prod2.cvqssxsqruyc.us-east-1.rds.amazonaws.com", database="core_prod",
                        user="postgres", password="aSderRFgd23")
conn_2 = psycopg2.connect(host="wareiq-core-prod.cvqssxsqruyc.us-east-1.rds.amazonaws.com", database="core_prod",
                          user="postgres", password="aSderRFgd23")


def lambda_handler():
    cur = conn.cursor()
    cur_2 = conn_2.cursor()
    cur.execute(fetch_client_channels_query)
    for channel in cur.fetchall():
        if channel[11] == "Shopify":
            shopify_orders_url = "https://%s:%s@%s/admin/api/2019-10/orders.json?since_id=%s&limit=250" % (
            channel[3], channel[4], channel[5], channel[6])
            data = requests.get(shopify_orders_url).json()
            products_quantity_dict = dict()
            for order in data['orders']:
                try:
                    product_exists = True
                    if channel[1] == "DAPR":  # serving only DAPR available skus, Have to create generic logic for this
                        for prod in order['line_items']:
                            product_sku = str(prod['variant_id'])
                            prod_tuple = (product_sku, channel[1])
                            cur.execute(select_products_query, prod_tuple)
                            try:
                                product_id = cur.fetchone()[0]
                            except Exception:
                                if product_sku in ("19675086585915", "30690984558651"):
                                    continue
                                product_exists = False
                                break

                    if not product_exists:
                        continue

                    cur.execute("SELECT count(*) FROM client_pickups WHERE client_prefix='%s';" % str(channel[1]))
                    pickup_count = cur.fetchone()[0]
                    if pickup_count == 1:
                        cur.execute(
                            "SELECT id, client_prefix FROM client_pickups WHERE client_prefix='%s';" % str(channel[1]))
                        pickup_data_id = cur.fetchone()[0]
                    else:
                        pickup_data_id = None  # change this as we move to dynamic pickups

                    customer_name = order['customer']['first_name'] + " " + order['customer']['last_name']
                    shipping_tuple = (order['shipping_address']['first_name'],
                                      order['shipping_address']['last_name'],
                                      order['shipping_address']['address1'],
                                      order['shipping_address']['address2'],
                                      order['shipping_address']['city'],
                                      order['shipping_address']['zip'],
                                      order['shipping_address']['province'],
                                      order['shipping_address']['country'],
                                      order['shipping_address']['phone'],
                                      order['shipping_address']['latitude'],
                                      order['shipping_address']['longitude'],
                                      order['shipping_address']['country_code']
                                      )

                    cur.execute(insert_shipping_address_query, shipping_tuple)
                    shipping_address_id = cur.fetchone()[0]

                    customer_phone = order['customer']['phone'] if order['customer']['phone'] else \
                    order['shipping_address']['phone']
                    customer_phone = ''.join(e for e in str(customer_phone) if e.isalnum())
                    customer_phone = "0" + customer_phone[-10:]

                    orders_tuple = (
                    str(order['order_number']), order['created_at'], customer_name, order['customer']['email'],
                    customer_phone if customer_phone else "", shipping_address_id,
                    datetime.now(), "NEW", channel[1], channel[0], str(order['id']), pickup_data_id)

                    cur.execute(insert_orders_data_query, orders_tuple)
                    order_id = cur.fetchone()[0]

                    total_amount = float(order['subtotal_price_set']['shop_money']['amount']) + float(
                        order['total_shipping_price_set']['shop_money']['amount'])

                    if order['financial_status'] == 'paid':
                        financial_status = 'prepaid'
                    elif order['financial_status'] == 'pending':
                        financial_status = 'COD'
                    else:
                        financial_status = order['financial_status']

                    payments_tuple = (
                    financial_status, total_amount, float(order['subtotal_price_set']['shop_money']['amount']),
                    float(order['total_shipping_price_set']['shop_money']['amount']), order["currency"], order_id)

                    cur.execute(insert_payments_data_query, payments_tuple)

                    for prod in order['line_items']:
                        product_sku = str(prod['variant_id'])
                        prod_tuple = (product_sku, channel[1])
                        cur.execute(select_products_query, prod_tuple)
                        try:
                            product_id = cur.fetchone()[0]
                        except Exception:
                            if product_sku == "19675086585915" and channel[
                                1] == 'DAPR':  # DAPR combination sku not present in products
                                for i in (3204, 3206):
                                    product_id = i
                                    op_tuple = (product_id, order_id, prod['quantity'])
                                    cur.execute(insert_op_association_query, op_tuple)
                                    if product_id not in products_quantity_dict:
                                        products_quantity_dict[product_id] = prod['quantity']
                                    else:
                                        products_quantity_dict[product_id] += prod['quantity']
                                continue
                            if product_sku == "30690984558651" and channel[
                                1] == 'DAPR':  # DAPR combination sku not present in products
                                for i in (3249, 3250):
                                    product_id = i
                                    op_tuple = (product_id, order_id, prod['quantity'])
                                    cur.execute(insert_op_association_query, op_tuple)
                                    if product_id not in products_quantity_dict:
                                        products_quantity_dict[product_id] = prod['quantity']
                                    else:
                                        products_quantity_dict[product_id] += prod['quantity']
                                continue
                            if channel[1] == "KYORIGIN":
                                if 'Hoodie' in prod['name']:
                                    dimensions = {"length": 6.87, "breadth": 22.5, "height": 27.5}
                                    weight = 0.51
                                else:
                                    dimensions = {"length": 2.5, "breadth": 22.5, "height": 27.5}
                                    weight = 0.18
                            else:
                                dimensions = {"length": 2.5, "breadth": 22.5, "height": 27.5}
                                weight = 0.20
                            product_insert_tuple = (prod['name'], str(prod['variant_id']), True, channel[2],
                                                    channel[1], datetime.now(), json.dumps(dimensions),
                                                    float(prod['price']), weight)
                            cur.execute(insert_product_query, product_insert_tuple)
                            product_id = cur.fetchone()[0]

                            product_quantity_insert_tuple = (
                            product_id, 100, 100, 100, channel[1], "APPROVED", datetime.now())
                            cur.execute(insert_product_quantity_query, product_quantity_insert_tuple)

                        op_tuple = (product_id, order_id, prod['quantity'])

                        cur.execute(insert_op_association_query, op_tuple)

                        if product_id not in products_quantity_dict:
                            products_quantity_dict[product_id] = prod['quantity']
                        else:
                            products_quantity_dict[product_id] += prod['quantity']
                except Exception as e:
                    logger.error("order fetch failed for" + str(order['order_number']) + "\nError:" + str(e))

            if data['orders']:
                last_sync_tuple = (str(data['orders'][-1]['id']), datetime.utcnow()+timedelta(hours=5.5), channel[0])
                cur.execute(update_last_fetched_data_query, last_sync_tuple)

            for prod_id, quan in products_quantity_dict.items():
                prod_quan_tuple = (quan, quan, prod_id)
                cur.execute(update_product_quantity_query, prod_quan_tuple)

            conn.commit()

        if channel[11] == "WooCommerce":
            auth_session = OAuth1Session(channel[3],
                                 client_secret=channel[4])
            url = '%s/wp-json/wc/v3/orders?per_page=100&after=%s&order=asc'%(channel[5], channel[7].isoformat())
            r = auth_session.get(url)
            data = list()
            try:
                data = r.json()
            except Exception as e:
                logger.error("Client order fetch failed for: " + str(channel[0]) +"\nError: " + str(e.args[0]))
            products_quantity_dict = dict()
            for order in data:
                try:
                    if order['status'] in ('failed',):
                        continue
                    cur.execute("SELECT count(*) FROM client_pickups WHERE client_prefix='%s';" % str(channel[1]))
                    pickup_count = cur.fetchone()[0]
                    if pickup_count == 1:
                        cur.execute(
                            "SELECT id, client_prefix FROM client_pickups WHERE client_prefix='%s';" % str(channel[1]))
                        pickup_data_id = cur.fetchone()[0]
                    else:
                        pickup_data_id = None  # change this as we move to dynamic pickups

                    customer_name = order['shipping']['first_name'] + " " + order['shipping']['last_name']
                    shipping_tuple = (order['shipping']['first_name'],
                                      order['shipping']['last_name'],
                                      order['shipping']['address_1'],
                                      order['shipping']['address_2'],
                                      order['shipping']['city'],
                                      order['shipping']['postcode'],
                                      order['shipping']['state'],
                                      order['shipping']['country'],
                                      order['billing']['phone'],
                                      None,
                                      None,
                                      order['shipping']['country']
                                      )

                    cur.execute(insert_shipping_address_query, shipping_tuple)
                    shipping_address_id = cur.fetchone()[0]

                    customer_phone = order['billing']['phone']
                    customer_phone = ''.join(e for e in str(customer_phone) if e.isalnum())
                    customer_phone = "0" + customer_phone[-10:]

                    orders_tuple = (
                    str(order['number']), order['date_created'], customer_name, order['billing']['email'],
                    customer_phone if customer_phone else "", shipping_address_id,
                    datetime.now(), "NEW", channel[1], channel[0], str(order['id']), pickup_data_id)

                    cur.execute(insert_orders_data_query, orders_tuple)
                    order_id = cur.fetchone()[0]

                    total_amount = float(order['total']) + float(order['shipping_total'])

                    if order['payment_method'].lower() != 'cod' and order['date_paid']:
                        financial_status = 'prepaid'
                    else:
                        financial_status = 'cod'

                    payments_tuple = (financial_status, total_amount, float(order['total']),
                                      float(order['shipping_total']), order["currency"], order_id)

                    cur.execute(insert_payments_data_query, payments_tuple)

                    for prod in order['line_items']:
                        product_sku = str(prod['sku'])
                        prod_tuple = (product_sku, channel[1])
                        cur.execute(select_products_query, prod_tuple)
                        try:
                            product_id = cur.fetchone()[0]
                        except Exception:
                            if channel[1] == "NYOR":
                                dimensions = {"length": 5, "breadth": 11.43, "height": 11.43}
                                weight = 0.20
                            else:
                                dimensions = {"length": 2.5, "breadth": 22.5, "height": 27.5}
                                weight = 0.20
                            product_insert_tuple = (prod['name'], str(prod['sku']), True, channel[2],
                                                    channel[1], datetime.now(), json.dumps(dimensions),
                                                    float(prod['price']), weight)
                            cur.execute(insert_product_query, product_insert_tuple)
                            product_id = cur.fetchone()[0]

                            product_quantity_insert_tuple = (
                            product_id, 100, 100, 100, channel[1], "APPROVED", datetime.now())
                            cur.execute(insert_product_quantity_query, product_quantity_insert_tuple)

                        op_tuple = (product_id, order_id, prod['quantity'])

                        cur.execute(insert_op_association_query, op_tuple)

                        if product_id not in products_quantity_dict:
                            products_quantity_dict[product_id] = prod['quantity']
                        else:
                            products_quantity_dict[product_id] += prod['quantity']
                except Exception as e:
                    logger.error("order fetch failed for" + str(order['order_number']) + "\nError:" + str(e))

            if data:
                last_sync_tuple = (str(data[-1]['id']), datetime.utcnow()+timedelta(hours=5.5), channel[0])
                cur.execute(update_last_fetched_data_query, last_sync_tuple)

            for prod_id, quan in products_quantity_dict.items():
                prod_quan_tuple = (quan, quan, prod_id)
                cur.execute(update_product_quantity_query, prod_quan_tuple)

            conn.commit()

    assign_pickup_points_for_unassigned(cur, cur_2)

    cur.close()


def assign_pickup_points_for_unassigned(cur, cur_2):
    time_after = datetime.utcnow() + timedelta(hours=4.5)
    cur.execute(get_orders_to_assign_pickups, (time_after,))
    all_orders = cur.fetchall()
    for order in all_orders:
        try:
            sku_dict = dict()
            for idx, sku in enumerate(order[3]):
                sku_dict[sku] = order[4][idx]

            sku_string = "('"

            for key, value in sku_dict.items():
                sku_string += key + "','"
            sku_string = sku_string.rstrip("'")
            sku_string = sku_string.rstrip(",")
            sku_string += ")"

            no_sku = len(order[3])
            try:
                cur.execute(
                    available_warehouse_product_quantity.replace('__SKU_STR__', sku_string).replace('__CLIENT_PREFIX__',
                                                                                                    order[1]))
            except Exception:
                conn.rollback()

            prod_wh_tuple = cur.fetchall()
            wh_dict = dict()
            courier_id = 2
            courier_id_weight = 0.0
            for prod_wh in prod_wh_tuple:
                if prod_wh[5] > courier_id_weight:
                    courier_id = prod_wh[4]
                    courier_id_weight = prod_wh[5]
                if sku_dict[prod_wh[2]] <= prod_wh[3]:
                    if prod_wh[0] not in wh_dict:
                        wh_dict[prod_wh[0]] = {"pincode": prod_wh[6], "count": 1}
                    else:
                        wh_dict[prod_wh[0]]['count'] += 1

            warehouse_pincode_str = ""
            for key, value in wh_dict.items():
                if value['count'] == no_sku:
                    warehouse_pincode_str += "('" + key + "','" + str(value['pincode']) + "'),"

            warehouse_pincode_str = warehouse_pincode_str.rstrip(',')
            if not warehouse_pincode_str:
                logger.info(str(order[0]) + ": One or more SKUs not serviceable")
                continue

            if courier_id in (8, 11, 12):
                courier_id = 1

            try:
                cur_2.execute(
                    fetch_warehouse_to_pick_from.replace('__WAREHOUSE_PINCODES__', warehouse_pincode_str).replace(
                        '__COURIER_ID__', str(courier_id)).replace('__DELIVERY_PINCODE__', str(order[2])))
            except Exception as e:
                conn_2.rollback()
                logger.info(str(order[0]) + ": " + str(e.args[0]))
                continue

            final_wh = cur_2.fetchone()

            if not final_wh or final_wh[1] is None:
                logger.info(str(order[0]) + "Not serviceable")
                continue

            cur.execute("""select aa.id from client_pickups aa
                            left join pickup_points bb on aa.pickup_id=bb.id
                            where bb.warehouse_prefix=%s""", (final_wh[0],))

            pickup_id = cur.fetchone()
            if not pickup_id:
                logger.info(str(order[0]) + "Pickup id not found")
                continue

            cur.execute("""UPDATE orders SET pickup_data_id = %s WHERE id=%s""", (pickup_id[0], order[0]))

        except Exception as e:
            logger.error("couldn't assign pickup for order " + str(order[0]) + "\nError: " + str(e))

    conn.commit()
