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
            if 'orders' not in data:
                continue
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
                                    op_tuple = (product_id, order_id, prod['quantity'], float(prod['quantity']*float(prod['price'])))
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
                                    op_tuple = (product_id, order_id, prod['quantity'], float(prod['quantity']*float(prod['price'])))
                                    cur.execute(insert_op_association_query, op_tuple)
                                    if product_id not in products_quantity_dict:
                                        products_quantity_dict[product_id] = prod['quantity']
                                    else:
                                        products_quantity_dict[product_id] += prod['quantity']
                                continue
                            dimensions = None
                            weight = None
                            product_insert_tuple = (prod['name'], str(prod['variant_id']), True, channel[2],
                                                    channel[1], datetime.now(), dimensions,
                                                    float(prod['price']), weight, str(prod['variant_id']))
                            cur.execute(insert_product_query, product_insert_tuple)
                            product_id = cur.fetchone()[0]

                            product_quantity_insert_tuple = (
                            product_id, 100, 100, 100, channel[1], "APPROVED", datetime.now())
                            cur.execute(insert_product_quantity_query, product_quantity_insert_tuple)

                        op_tuple = (product_id, order_id, prod['quantity'], float(prod['quantity']*float(prod['price'])))
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
            url = '%s/wp-json/wc/v3/orders?per_page=100&after=%s&order=asc&consumer_key=%s&consumer_secret=%s'%(channel[5],
                                                                                                                channel[7].isoformat(), channel[3], channel[4])
            r = auth_session.get(url)
            data = list()
            try:
                data = r.json()
            except Exception as e:
                logger.error("Client order fetch failed for: " + str(channel[0]) +"\nError: " + str(e.args[0]))
            products_quantity_dict = dict()
            if type(data) != list:
                logger.error("Client order fetch failed for: " + str(channel[0]))
                continue
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

                    if order['payment_method'].lower() == 'cod':
                        financial_status = 'cod'
                    else:
                        financial_status = 'prepaid'

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

                    if order['status'] in ('completed', 'processing'):
                        insert_status = "NEW"
                    elif order['status'] == 'cancelled':
                        insert_status = "CANCELED"
                    elif order['status'] in ('pending') or (not order['date_paid'] and order['payment_method'].lower() != 'cod'):
                        insert_status = "PENDING PAYMENT"
                    else:
                        insert_status = "NEW - " + order['status'].upper()

                    orders_tuple = (
                    str(order['number']), order['date_created'], customer_name, order['billing']['email'],
                    customer_phone if customer_phone else "", shipping_address_id,
                    datetime.now(), insert_status, channel[1], channel[0], str(order['id']), pickup_data_id)

                    cur.execute(insert_orders_data_query, orders_tuple)
                    order_id = cur.fetchone()[0]

                    total_amount = float(order['total']) + float(order['shipping_total'])

                    payments_tuple = (financial_status, total_amount, float(order['total']),
                                      float(order['shipping_total']), order["currency"], order_id)

                    cur.execute(insert_payments_data_query, payments_tuple)

                    for prod in order['line_items']:
                        sku_id = prod['variation_id']
                        if not sku_id:
                            sku_id = prod['product_id']
                        product_sku = str(sku_id)
                        prod_tuple = (product_sku, channel[1])
                        cur.execute(select_products_query, prod_tuple)
                        try:
                            product_id = cur.fetchone()[0]
                        except Exception:
                            dimensions = None
                            weight = None
                            product_insert_tuple = (prod['name'], str(sku_id), True, channel[2],
                                                    channel[1], datetime.now(), dimensions,
                                                    float(prod['price']), weight, str(sku_id))
                            cur.execute(insert_product_query, product_insert_tuple)
                            product_id = cur.fetchone()[0]

                            product_quantity_insert_tuple = (
                            product_id, 100, 100, 100, channel[1], "APPROVED", datetime.now())
                            cur.execute(insert_product_quantity_query, product_quantity_insert_tuple)

                        op_tuple = (product_id, order_id, prod['quantity'], float(prod['quantity']*float(prod['price'])))

                        cur.execute(insert_op_association_query, op_tuple)

                        if product_id not in products_quantity_dict:
                            products_quantity_dict[product_id] = prod['quantity']
                        else:
                            products_quantity_dict[product_id] += prod['quantity']
                except Exception as e:
                    logger.error("order fetch failed for" + str(order['number']) + "\nError:" + str(e))

            if data:
                last_sync_tuple = (str(data[-1]['id']), datetime.utcnow()+timedelta(hours=5.5), channel[0])
                cur.execute(update_last_fetched_data_query, last_sync_tuple)

            for prod_id, quan in products_quantity_dict.items():
                prod_quan_tuple = (quan, quan, prod_id)
                cur.execute(update_product_quantity_query, prod_quan_tuple)

            conn.commit()

            cur.execute("SELECT order_id_channel_unique, id from orders where client_channel_id=%s and status='PENDING PAYMENT';", (channel[0],))
            update_orders = cur.fetchall()
            auth_session = OAuth1Session(channel[3],
                                         client_secret=channel[4])

            for order in update_orders:
                url = '%s/wp-json/wc/v3/orders/' % (channel[5])
                url += str(order[0])
                url += "?consumer_key=%s&consumer_secret=%s"%(channel[3], channel[4])
                r = auth_session.get(url)
                data = None
                try:
                    data = r.json()
                except Exception as e:
                    logger.error("Client order update status failed for: " + str(channel[0]) + "\nError: " + str(e.args[0]))

                if data and 'status' in data:
                    if data['status'] in ('pending'):
                        continue
                    elif data['status'] in ('completed', 'processing'):
                        new_status = 'NEW'
                    elif data['status'] == 'cancelled':
                        new_status = 'CANCELED'
                    else:
                        new_status = 'NEW - '+data['status'].upper()

                    cur.execute("UPDATE orders SET status=%s WHERE id=%s", (new_status, order[1]))

            conn.commit()

    assign_pickup_points_for_unassigned(cur, cur_2)
    update_available_quantity(cur)

    cur.close()


def assign_pickup_points_for_unassigned(cur, cur_2):
    time_after = datetime.utcnow() - timedelta(hours=6.5)
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
                        wh_dict[prod_wh[0]] = {"pincode": prod_wh[6], "count": 1, "prod_list": [prod_wh[1]]}
                    else:
                        wh_dict[prod_wh[0]]['count'] += 1
                        wh_dict[prod_wh[0]]['prod_list'].append(prod_wh[1])

            warehouse_pincode_str = ""
            for key, value in wh_dict.items():
                if value['count'] == no_sku:
                    warehouse_pincode_str += "('" + key + "','" + str(value['pincode']) + "'),"

            warehouse_pincode_str = warehouse_pincode_str.rstrip(',')
            if not warehouse_pincode_str:
                total_count = 0
                prod_list = list()
                for key, value in wh_dict.items():
                    total_count+=value['count']
                    prod_list.append(value['prod_list'])
                if total_count == no_sku:
                    prod_list.sort(key=len, reverse=True)
                    split_order(cur, order[0], prod_list)
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


def split_order(cur, order_id, prod_list):
    try:
        sub_id = 'A'
        cur.execute("SELECT shipping_charges, payment_mode, currency from orders_payments WHERE order_id=%s" % str(order_id))
        fetched_tuple = cur.fetchone()
        shipping_cost_each = fetched_tuple[0]/len(prod_list)
        payment_mode = fetched_tuple[1]
        currency = fetched_tuple[2]
        for idx, prods in enumerate(prod_list):
            if len(prods)==1:
                prods_tuple = "("+str(prods[0])+")"
            else:
                prods_tuple = str(tuple(prods))
            cur.execute("SELECT sum(amount) FROM op_association WHERE order_id=%s and product_id in %s"%(str(order_id), prods_tuple))
            prod_amount = cur.fetchone()[0]
            if idx==0: #first order remains same
                cur.execute("UPDATE orders_payments SET subtotal=%s, shipping_charges=%s, amount=%s WHERE order_id=%s",
                            (prod_amount, shipping_cost_each, prod_amount+shipping_cost_each, order_id))

                continue

            sub_id_str = '-' + sub_id
            duplicate_order_query = """INSERT INTO orders (channel_order_id, order_date, customer_name, customer_email, 
                                customer_phone, delivery_address_id, date_created, status, client_prefix, client_channel_id, 
                                order_id_channel_unique, pickup_data_id)
                                SELECT CONCAT(channel_order_id, '%s'), order_date, customer_name, customer_email, 
                                customer_phone, delivery_address_id, date_created, status, client_prefix, client_channel_id, 
                                order_id_channel_unique, pickup_data_id FROM orders WHERE id=%s 
                                RETURNING id;"""%(sub_id_str, str(order_id))
            cur.execute(duplicate_order_query)
            new_order_id = cur.fetchone()[0]

            cur.execute("UPDATE op_association SET order_id=%s WHERE order_id=%s and product_id in %s"%
                        (str(new_order_id), str(order_id), prods_tuple))

            cur.execute("""INSERT INTO orders_payments (payment_mode, amount, currency, order_id, shipping_charges, subtotal)
                            VALUES (%s,%s,%s,%s,%s,%s)""", (payment_mode, prod_amount+shipping_cost_each, currency,
                                                            new_order_id, shipping_cost_each, prod_amount))

            sub_id = chr(ord(sub_id) + 1)
        conn.commit()
    except Exception as e:
        logger.error("couldn't split order " + str(order_id) + "\nError: " + str(e))


def update_available_quantity(cur):
    cur.execute(fetch_inventory_quantity_query)
    all_prods_status = cur.fetchall()
    quantity_dict = dict()

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
        elif prod_status[1] in ('RTO'):
            quantity_dict[prod_status[0]][prod_status[2]]['rto_quantity'] += prod_status[3]

    for prod_id, wh_dict in quantity_dict.items():
        for warehouse, quan_values in wh_dict.items():
            update_tuple = (quan_values['available_quantity'], quan_values['current_quantity'], quan_values['inline_quantity'],
                            quan_values['rto_quantity'], prod_id, warehouse)
            cur.execute(update_inventory_quantity_query, update_tuple)

    conn.commit()