import psycopg2, requests, os, json, hmac, hashlib, base64
from datetime import datetime, timedelta
from requests_oauthlib.oauth1_session import OAuth1Session
from woocommerce import API
import logging
from app.db_utils import DbConnection

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

conn = DbConnection.get_db_connection_instance()
conn_2 = DbConnection.get_pincode_db_connection_instance()


def fetch_orders():
    cur = conn.cursor()
    cur_2 = conn_2.cursor()
    cur.execute(fetch_client_channels_query)
    for channel in cur.fetchall():
        if channel[11] == "Shopify":
            try:
                fetch_shopify_orders(cur, channel)
            except Exception as e:
                logger.error("Couldn't fetch orders: " + str(channel[1]) + "\nError: " + str(e.args))

        elif channel[11] == "WooCommerce":
            try:
                fetch_woocommerce_orders(cur, channel)
            except Exception as e:
                logger.error("Couldn't fetch orders: " + str(channel[1]) + "\nError: " + str(e.args))

        elif channel[11] == "Magento 2":
            try:
                fetch_magento_orders(cur, channel)
            except Exception as e:
                logger.error("Couldn't fetch orders: " + str(channel[1]) + "\nError: " + str(e.args))

        elif channel[11] == "EasyEcom":
            try:
                fetch_easyecom_orders(cur, channel)
            except Exception as e:
                logger.error("Couldn't fetch orders: " + str(channel[1]) + "\nError: " + str(e.args))

        elif channel[11] == "Bikayi":
            try:
                fetch_bikayi_orders(cur, channel)
            except Exception as e:
                logger.error("Couldn't fetch orders: " + str(channel[1]) + "\nError: " + str(e.args))

    assign_pickup_points_for_unassigned(cur, cur_2)
    update_available_quantity(cur)
    update_thirdwatch_data(cur)

    cur.close()


def fetch_shopify_orders(cur, channel):

    time_now = datetime.utcnow()
    if channel[7] and not (time_now.hour == 21 and 0<time_now.minute<30):
        updated_after = (channel[7] - timedelta(hours=5.5)).strftime("%Y-%m-%dT%X")
    else:
        updated_after = datetime.utcnow() - timedelta(days=30)
        updated_after = updated_after.strftime("%Y-%m-%dT%X")

    last_synced_time = datetime.utcnow() + timedelta(hours=5.5)
    data = list()
    count = 250
    next_url = None
    while count == 250 or next_url:
        shopify_orders_url = "https://%s:%s@%s/admin/api/2020-07/orders.json?updated_at_min=%s&limit=250&fulfillment_status=unfulfilled" % (
            channel[3], channel[4], channel[5], updated_after)
        if next_url:
            shopify_orders_url = "https://"+channel[3]+":"+channel[4]+"@"+next_url.split("https://")[1]
        req = requests.get(shopify_orders_url)
        if 'orders' not in req.json():
            return None
        next_url = None
        try:
            next_url = req.links['next']['url']
        except Exception:
            pass
        data += req.json()['orders']
        count = len(req.json()['orders'])
    for order in data:
        try:
            cur.execute("SELECT id from orders where order_id_channel_unique='%s' and client_prefix='%s'" % (
            str(order['id']), channel[1]))
            try:
                existing_order = cur.fetchone()[0]
            except Exception as e:
                existing_order = False
                pass
            if existing_order:
                continue

            cur.execute("SELECT count(*) FROM client_pickups WHERE client_prefix='%s' and active=true;" % str(channel[1]))
            pickup_count = cur.fetchone()[0]
            if pickup_count == 1 and not channel[17]:
                cur.execute(
                    "SELECT id, client_prefix FROM client_pickups WHERE client_prefix='%s' and active=true;" % str(channel[1]))
                pickup_data_id = cur.fetchone()[0]
            else:
                pickup_data_id = None  # change this as we move to dynamic pickups

            customer_name = order['customer']['first_name']
            if customer_name and order['customer']['last_name']:
                customer_name += " " + order['customer']['last_name']
            if not customer_name:
                customer_name = order['shipping_address']['first_name']

            if not customer_name and order['customer']['last_name']:
                customer_name = order['customer']['last_name']

            customer_phone = order['customer']['phone'] if order['customer']['phone'] else \
                order['shipping_address']['phone']
            customer_phone = ''.join(e for e in str(customer_phone) if e.isalnum())
            customer_phone = "0" + customer_phone[-10:]

            shopping_address_1 = order['shipping_address']['company'] + " " + order['shipping_address']['address1'] if order['shipping_address']['company'] else order['shipping_address']['address1']
            shipping_tuple = (order['shipping_address']['first_name'],
                              order['shipping_address']['last_name'],
                              shopping_address_1,
                              order['shipping_address']['address2'],
                              order['shipping_address']['city'],
                              order['shipping_address']['zip'],
                              order['shipping_address']['province'],
                              order['shipping_address']['country'],
                              order['shipping_address']['phone'] if order['shipping_address']['phone'] else customer_phone,
                              order['shipping_address']['latitude'],
                              order['shipping_address']['longitude'],
                              order['shipping_address']['country_code']
                              )

            billing_address_key = "billing_address" if "billing_address" in order else "shipping_address"
            billing_address_1 = order[billing_address_key]['company'] + " " + order[billing_address_key]['address1'] if order[billing_address_key]['company'] else order[billing_address_key]['address1']
            billing_tuple = (order[billing_address_key]['first_name'],
                              order[billing_address_key]['last_name'],
                              billing_address_1,
                              order[billing_address_key]['address2'],
                              order[billing_address_key]['city'],
                              order[billing_address_key]['zip'],
                              order[billing_address_key]['province'],
                              order[billing_address_key]['country'],
                              order[billing_address_key]['phone'],
                              order[billing_address_key]['latitude'],
                              order[billing_address_key]['longitude'],
                              order[billing_address_key]['country_code']
                              )

            cur.execute(insert_shipping_address_query, shipping_tuple)
            shipping_address_id = cur.fetchone()[0]

            cur.execute(insert_billing_address_query, billing_tuple)
            billing_address_id = cur.fetchone()[0]

            channel_order_id = str(order['order_number'])
            if channel[16]:
                channel_order_id = str(channel[16]) + channel_order_id

            orders_tuple = (
                channel_order_id, order['created_at'], customer_name, order['customer']['email'],
                customer_phone if customer_phone else "", shipping_address_id, billing_address_id,
                datetime.now(), "NEW", channel[1], channel[0], str(order['id']), pickup_data_id, 1)

            cur.execute(insert_orders_data_query, orders_tuple)
            order_id = cur.fetchone()[0]

            total_amount = float(order['total_price'])
            shipping_amount = float(order['total_shipping_price_set']['shop_money']['amount'])
            subtotal_amount = total_amount- shipping_amount

            if order['financial_status'] == 'paid':
                financial_status = 'prepaid'
            elif order['financial_status'] == 'pending':
                financial_status = 'COD'
            else:
                financial_status = order['financial_status']

            payments_tuple = (
                financial_status, total_amount, subtotal_amount,
                shipping_amount, order["currency"], order_id)

            cur.execute(insert_payments_data_query, payments_tuple)

            try:
                extra_details_tuple = (order_id, order['client_details']['browser_ip'], order['client_details']['user_agent'],
                                       order['checkout_token'], str(order['customer']['id']), order['customer']['created_at'], order['customer']['orders_count'],
                                       order['customer']['verified_email'], order['token'], order['gateway'], order['processing_method'])

                cur.execute(insert_order_extra_details_query, extra_details_tuple)
            except Exception as e:
                pass

            for prod in order['line_items']:
                product_sku = str(prod['variant_id']) if prod['variant_id'] else str(prod['id'])
                prod_tuple = (product_sku, channel[1])
                cur.execute(select_products_query, prod_tuple)
                try:
                    product_id = cur.fetchone()[0]
                except Exception:
                    if product_sku == "19675086585915" and channel[
                        1] == 'DAPR':  # DAPR combination sku not present in products
                        for i in (3204, 3206):
                            product_id = i
                            op_tuple = (
                            product_id, order_id, prod['quantity'], float(prod['quantity'] * float(prod['price'])), None, json.dumps([]))
                            cur.execute(insert_op_association_query, op_tuple)
                        continue
                    if product_sku == "30690984558651" and channel[
                        1] == 'DAPR':  # DAPR combination sku not present in products
                        for i in (3249, 3250):
                            product_id = i
                            op_tuple = (
                            product_id, order_id, prod['quantity'], float(prod['quantity'] * float(prod['price'])), None, json.dumps([]))
                            cur.execute(insert_op_association_query, op_tuple)
                        continue

                    dimensions = None
                    weight = None
                    warehouse_prefix = channel[1]
                    subcategory_id = None
                    master_sku = prod['sku']
                    if not master_sku:
                        master_sku = product_sku
                    try:
                        cur.execute("SELECT keywords, warehouse_prefix, dimensions, weight, subcategory_id FROM keyword_weights WHERE client_prefix='%s'"%channel[1])
                        all_weights = cur.fetchall()
                        for obj in all_weights:
                            if all(x.lower() in master_sku.lower() for x in obj[0]) or all(x.lower() in prod['name'].lower() for x in obj[0]):
                                warehouse_prefix = obj[1]
                                dimensions = json.dumps(obj[2])
                                weight = obj[3]
                                subcategory_id = obj[4]
                                break
                    except Exception as e:
                        logger.error("product weight assignment failed for: " + str(order['order_number']) + "\nError:" + str(e))
                    product_insert_tuple = (prod['name'], product_sku, True, channel[2],
                                            channel[1], datetime.now(), dimensions,
                                            float(prod['price']), weight, master_sku, subcategory_id)
                    cur.execute(insert_product_query, product_insert_tuple)
                    product_id = cur.fetchone()[0]

                tax_lines = list()
                try:
                    for tax_line in prod['tax_lines']:
                        tax_lines.append({'title': tax_line['title'], 'rate': tax_line['rate']})
                except Exception as e:
                    logger.error("Couldn't fetch tex for: " + str(order_id))

                op_tuple = (product_id, order_id, prod['quantity'], float(prod['quantity'] * float(prod['price'])), None, json.dumps(tax_lines))
                cur.execute(insert_op_association_query, op_tuple)

        except Exception as e:
            logger.error("order fetch failed for" + str(order['order_number']) + "\nError:" + str(e))
            conn.rollback()

        conn.commit()

    if data:
        last_sync_tuple = (str(data[0]['id']), last_synced_time, channel[0])
        cur.execute(update_last_fetched_data_query, last_sync_tuple)

    conn.commit()


def fetch_woocommerce_orders(cur, channel):
    if channel[7]:
        time_after = channel[7] - timedelta(days=10)
        time_after_ids = channel[7] - timedelta(days=2)
    else:
        time_after = datetime.utcnow() - timedelta(days=10)
        time_after_ids = datetime.utcnow() - timedelta(days=2)
    cur.execute("""SELECT order_id_channel_unique from orders aa
                    left join client_channel bb on aa.client_channel_id=bb.id
                    WHERE order_date>%s and aa.client_prefix=%s and bb.channel_id=5;""", (time_after_ids, channel[1]))

    fetch_status = ",".join(channel[15])
    exclude_ids = ""
    all_fetched_ids = cur.fetchall()
    for fetch_id in all_fetched_ids:
        exclude_ids += str(fetch_id[0]) + ","

    url = 'orders?per_page=100&after=%s&order=asc&exclude=%s&status=%s&consumer_key=%s&consumer_secret=%s' % (
    time_after.isoformat(), exclude_ids, fetch_status, channel[3], channel[4])
    last_order_time = datetime.utcnow() + timedelta(hours=5.5)
    try:
        auth_session = API(
                url=channel[5],
                consumer_key=channel[3],
                consumer_secret=channel[4],
                version="wc/v3"
            )
        r = auth_session.get(url)
    except Exception:
        auth_session = API(
            url=channel[5],
            consumer_key=channel[3],
            consumer_secret=channel[4],
            version="wc/v3",
            verify_ssl=False
        )
        r = auth_session.get(url)
    data = list()
    try:
        data = r.json()
    except Exception as e:
        logger.error("Client order fetch failed for: " + str(channel[0]) + "\nError: " + str(e.args[0]))
    if type(data) != list:
        logger.error("Client order fetch failed for: " + str(channel[0]))
        return None
    for order in data:
        try:
            cur.execute("SELECT id from orders where order_id_channel_unique='%s' and client_prefix='%s'" % (
                str(order['id']), channel[1]))
            try:
                existing_order = cur.fetchone()[0]
            except Exception as e:
                existing_order = False
                pass
            if existing_order:
                continue
            cur.execute("SELECT count(*) FROM client_pickups WHERE client_prefix='%s' and active=true;" % str(channel[1]))
            pickup_count = cur.fetchone()[0]
            if pickup_count == 1 and not channel[17]:
                cur.execute(
                    "SELECT id, client_prefix FROM client_pickups WHERE client_prefix='%s' and active=true;" % str(channel[1]))
                pickup_data_id = cur.fetchone()[0]
            else:
                pickup_data_id = None  # change this as we move to dynamic pickups

            if order['payment_method'].lower() in ('cod', 'cash on delivery', 'cashondelivery'):
                financial_status = 'cod'
            else:
                financial_status = 'prepaid'

            customer_name = order['shipping']['first_name']
            customer_name += " " + order['shipping']['last_name'] if order['shipping']['last_name'] else ""
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

            billing_tuple = (order['billing']['first_name'],
                              order['billing']['last_name'],
                              order['billing']['address_1'],
                              order['billing']['address_2'],
                              order['billing']['city'],
                              order['billing']['postcode'],
                              order['billing']['state'],
                              order['billing']['country'],
                              order['billing']['phone'],
                              None,
                              None,
                              order['billing']['country']
                              )

            cur.execute(insert_shipping_address_query, shipping_tuple)
            shipping_address_id = cur.fetchone()[0]

            cur.execute(insert_billing_address_query, billing_tuple)
            billing_address_id = cur.fetchone()[0]

            customer_phone = order['billing']['phone']
            customer_phone = ''.join(e for e in str(customer_phone) if e.isalnum())
            customer_phone = "0" + customer_phone[-10:]

            insert_status = "NEW"

            channel_order_id = str(order['number'])
            if channel[16]:
                channel_order_id = str(channel[16]) + channel_order_id

            orders_tuple = (
                channel_order_id, order['date_created'], customer_name, order['billing']['email'],
                customer_phone if customer_phone else "", shipping_address_id, billing_address_id,
                datetime.now(), insert_status, channel[1], channel[0], str(order['id']), pickup_data_id, 5)

            cur.execute(insert_orders_data_query, orders_tuple)
            order_id = cur.fetchone()[0]

            total_amount = float(order['total'])
            total_shipping = float(order['shipping_total']) + float(order['shipping_tax'])
            subtotal_amount = total_amount - total_shipping

            payments_tuple = (financial_status, total_amount, subtotal_amount,
                              total_shipping, order["currency"], order_id)

            cur.execute(insert_payments_data_query, payments_tuple)

            try:
                extra_details_tuple = (order_id, order['customer_ip_address'], order['customer_user_agent'],
                                       order['cart_hash'], str(order['customer_id']), order['date_created'], 1,
                                       None, order['transaction_id'], order['payment_method'], order['payment_method_title'])

                cur.execute(insert_order_extra_details_query, extra_details_tuple)
            except Exception as e:
                pass

            for prod in order['line_items']:
                sku_id = prod['variation_id']
                if not sku_id:
                    sku_id = prod['product_id']
                product_sku = str(sku_id)
                master_sku = prod['sku']
                if not master_sku:
                    master_sku = sku_id
                warehouse_prefix = channel[1]
                prod_tuple = (product_sku, channel[1])
                cur.execute(select_products_query, prod_tuple)
                try:
                    product_id = cur.fetchone()[0]
                except Exception:
                    dimensions = None
                    weight = None
                    subcategory_id = None
                    try:
                        cur.execute("SELECT keywords, warehouse_prefix, dimensions, weight, subcategory_id FROM keyword_weights WHERE client_prefix='%s'"%channel[1])
                        all_weights = cur.fetchall()
                        for obj in all_weights:
                            if all(x.lower() in master_sku.lower() for x in obj[0]) or all(x.lower() in prod['name'].lower() for x in obj[0]):
                                warehouse_prefix = obj[1]
                                dimensions = json.dumps(obj[2])
                                weight = obj[3]
                                subcategory_id = obj[4]
                                break
                    except Exception as e:
                        logger.error("product weight assignment failed for: " + str(order['number']) + "\nError:" + str(e))
                    product_insert_tuple = (prod['name'], product_sku, True, channel[2],
                                            channel[1], datetime.now(), dimensions,
                                            float(prod['price']), weight, master_sku, subcategory_id)
                    cur.execute(insert_product_query, product_insert_tuple)
                    product_id = cur.fetchone()[0]

                tax_lines = list()
                try:
                    for tax_line in order['tax_lines']:
                        tax_lines.append({'title': tax_line['label'], 'rate': tax_line['rate_percent']/100})
                except Exception as e:
                    logger.error("Couldn't fetch tex for: " + str(order_id))

                op_tuple = (product_id, order_id, prod['quantity'], float(prod['quantity'] * (float(prod['total'])+float(prod['total_tax']))), None, json.dumps(tax_lines))

                cur.execute(insert_op_association_query, op_tuple)

        except Exception as e:
            logger.error("order fetch failed for" + str(order['number']) + "\nError:" + str(e))

    if data:
        last_sync_tuple = (str(data[-1]['id']), last_order_time, channel[0])
        cur.execute(update_last_fetched_data_query, last_sync_tuple)

    conn.commit()


def fetch_magento_orders(cur, channel):

    time_now = datetime.utcnow()
    if channel[7] and not (time_now.hour == 21 and 0<time_now.minute<30):
        updated_after = channel[7].strftime("%Y-%m-%d %X")
    else:
        updated_after = datetime.utcnow() - timedelta(days=30)
        updated_after = updated_after.strftime("%Y-%m-%d %X")

    magento_orders_url = """%s/V1/orders?searchCriteria[filter_groups][0][filters][0][field]=updated_at&searchCriteria[filter_groups][0][filters][0][value]=%s&searchCriteria[filter_groups][0][filters][0][condition_type]=gt&searchCriteria[filter_groups][0][filters][1][field]=created_at&searchCriteria[filter_groups][0][filters][1][value]=%s&searchCriteria[filter_groups][0][filters][1][condition_type]=gt""" % (
        channel[5], updated_after, updated_after)
    fetch_status = ",".join(channel[15])
    magento_orders_url += """&searchCriteria[filter_groups][1][filters][0][field]=status&searchCriteria[filter_groups][1][filters][0][value]=__STATUS__&searchCriteria[filter_groups][1][filters][0][condition_type]=in""".replace('__STATUS__', fetch_status)
    headers = {'Authorization': "Bearer " + channel[3],
               'Content-Type': 'application/json',
               'User-Agent': 'WareIQ server'}
    data = requests.get(magento_orders_url, headers=headers)
    if 'items' in data.json() and not data.json()['items']:
        updated_after = datetime.utcnow() - timedelta(days=5)
        updated_after = updated_after.strftime("%Y-%m-%d %X")
        magento_orders_url = """%s/V1/orders?searchCriteria[filter_groups][0][filters][0][field]=updated_at&searchCriteria[filter_groups][0][filters][0][value]=%s&searchCriteria[filter_groups][0][filters][0][condition_type]=gt&searchCriteria[filter_groups][0][filters][1][field]=created_at&searchCriteria[filter_groups][0][filters][1][value]=%s&searchCriteria[filter_groups][0][filters][1][condition_type]=gt""" % (
            channel[5], updated_after, updated_after)
        fetch_status = ",".join(channel[15])
        magento_orders_url += """&searchCriteria[filter_groups][1][filters][0][field]=status&searchCriteria[filter_groups][1][filters][0][value]=__STATUS__&searchCriteria[filter_groups][1][filters][0][condition_type]=in""".replace(
            '__STATUS__', fetch_status)
        data = requests.get(magento_orders_url, headers=headers)
    logger.info(str(len(data.json())))
    if data.status_code == 200:
        data = data.json()
    else:
        return None
    last_synced_time = datetime.utcnow()
    if 'items' not in data:
        return None
    for order in data['items']:
        try:
            cur.execute("SELECT id from orders where channel_order_id='%s' and client_prefix='%s'"%(str(order['increment_id']), channel[1]))
            try:
                existing_order = cur.fetchone()[0]
            except Exception as e:
                existing_order = False
                pass
            if existing_order:
                continue
            cur.execute("SELECT count(*) FROM client_pickups WHERE client_prefix='%s' and active=true;" % str(channel[1]))
            pickup_count = cur.fetchone()[0]
            if pickup_count == 1 and not channel[17]:
                cur.execute(
                    "SELECT id, client_prefix FROM client_pickups WHERE client_prefix='%s' and active=true;" % str(channel[1]))
                pickup_data_id = cur.fetchone()[0]
            else:
                pickup_data_id = None  # change this as we move to dynamic pickups

            customer_name = order['billing_address']['firstname'] if order['billing_address']['firstname'] else order['billing_address']['lastname']
            customer_name += " " + order['billing_address']['lastname'] if order['billing_address']['lastname'] else ""

            address_1 = ""
            for addr in order['billing_address']['street']:
                address_1 += str(addr)

            billing_tuple = (order['billing_address'].get('firstname'),
                             order['billing_address'].get('lastname'),
                             address_1,
                             "",
                             order['billing_address'].get('city'),
                             order['billing_address'].get('postcode'),
                             order['billing_address'].get('region'),
                             order['billing_address'].get('country_id'),
                             order['billing_address'].get('telephone'),
                             None,
                             None,
                             order['billing_address'].get('country_id')
                             )
            try:
                address_1 = ""
                for addr in order['extension_attributes']['shipping_assignments'][0]['shipping']['address']['street']:
                    address_1 += str(addr)
                shipping_tuple = (order['extension_attributes']['shipping_assignments'][0]['shipping']['address']['firstname'],
                                  order['extension_attributes']['shipping_assignments'][0]['shipping']['address']['lastname'],
                                  address_1,
                                  "",
                                  order['extension_attributes']['shipping_assignments'][0]['shipping']['address']['city'],
                                  order['extension_attributes']['shipping_assignments'][0]['shipping']['address']['postcode'],
                                  order['extension_attributes']['shipping_assignments'][0]['shipping']['address']['region'],
                                  order['extension_attributes']['shipping_assignments'][0]['shipping']['address']['country_id'],
                                  order['extension_attributes']['shipping_assignments'][0]['shipping']['address']['telephone'],
                                  None, None,
                                  order['extension_attributes']['shipping_assignments'][0]['shipping']['address']['country_id'],
                                  )
            except Exception:
                shipping_tuple = billing_tuple

            cur.execute(insert_shipping_address_query, shipping_tuple)
            shipping_address_id = cur.fetchone()[0]
            cur.execute(insert_billing_address_query, billing_tuple)
            billing_address_id = cur.fetchone()[0]

            try:
                customer_phone = order['extension_attributes']['shipping_assignments'][0]['shipping']['address']['telephone']
            except Exception:
                customer_phone = order['billing_address']['telephone']
            customer_phone = ''.join(e for e in str(customer_phone) if e.isalnum())
            customer_phone = "0" + customer_phone[-10:]

            if order['payment']['method'].lower() == 'cashondelivery':
                financial_status = 'cod'
            else:
                financial_status = 'prepaid'

            insert_status = "NEW"

            order_time = datetime.strptime(order['created_at'], "%Y-%m-%d %X") + timedelta(hours=5.5)
            channel_order_id = str(order['increment_id'])
            if channel[16]:
                channel_order_id = str(channel[16]) + channel_order_id
            orders_tuple = (
                channel_order_id, order_time, customer_name, order['customer_email'],
                customer_phone if customer_phone else "", shipping_address_id, billing_address_id,
                datetime.now(), insert_status, channel[1], channel[0], str(order['entity_id']), pickup_data_id, 6)

            cur.execute(insert_orders_data_query, orders_tuple)
            order_id = cur.fetchone()[0]

            total_amount = float(order['grand_total'])

            payments_tuple = (
                financial_status, total_amount, float(order['subtotal_incl_tax']),
                float(order['shipping_incl_tax']), order["base_currency_code"], order_id)

            cur.execute(insert_payments_data_query, payments_tuple)

            already_used_prods = list()
            mark_delivered = True
            for prod in order['items']:
                if "parent_item" in prod:
                    prod = prod['parent_item']
                if prod['item_id'] not in already_used_prods:
                    already_used_prods.append(prod['item_id'])
                else:
                    continue
                product_sku = str(prod['product_id'])
                master_sku = str(prod['sku'])
                if "GC" not in master_sku:
                    mark_delivered = False
                prod_tuple = (master_sku, channel[1])
                select_products_query_temp = """SELECT id from products where master_sku=%s and client_prefix=%s;"""
                cur.execute(select_products_query_temp, prod_tuple)
                try:
                    product_id = cur.fetchone()[0]
                except Exception:
                    dimensions = None
                    weight = None
                    subcategory_id = None
                    warehouse_prefix = channel[1]
                    master_sku = str(prod['sku'])
                    if not master_sku:
                        master_sku = product_sku
                    try:
                        cur.execute("SELECT keywords, warehouse_prefix, dimensions, weight, subcategory_id FROM keyword_weights WHERE client_prefix='%s'"%channel[1])
                        all_weights = cur.fetchall()
                        for obj in all_weights:
                            if all(x.lower() in master_sku.lower() for x in obj[0]) or all(x.lower() in prod['name'].lower() for x in obj[0]):
                                warehouse_prefix = obj[1]
                                dimensions = json.dumps(obj[2])
                                weight = obj[3]
                                subcategory_id = obj[4]
                                break
                    except Exception as e:
                        logger.error("product weight assignment failed for: " + str(order['increment_id']) + "\nError:" + str(e))
                    product_insert_tuple = (prod['name'], product_sku, True, channel[2],
                                            channel[1], datetime.now(), dimensions, float(prod['original_price']),
                                            weight, master_sku, subcategory_id)
                    cur.execute(insert_product_query, product_insert_tuple)
                    product_id = cur.fetchone()[0]

                tax_lines = list()
                try:
                    tax_lines.append({'title': "GST", 'rate': prod['tax_percent']/100})
                except Exception as e:
                    logger.error("Couldn't fetch tex for: " + str(order_id))

                op_tuple = (product_id, order_id, prod['qty_ordered'], float(prod['qty_ordered'] * float(prod['price_incl_tax'])), str(prod['item_id']), json.dumps(tax_lines))
                cur.execute(insert_op_association_query, op_tuple)

            if mark_delivered and channel[1]=='KAMAAYURVEDA':
                cur.execute("UPDATE orders SET status='DELIVERED' WHERE id=%s", (order_id, ))

        except Exception as e:
            logger.error("order fetch failed for" + str(order['increment_id']) + "\nError:" + str(e))

    if data['items']:
        last_sync_tuple = (str(data['items'][-1]['entity_id']), last_synced_time, channel[0])
        cur.execute(update_last_fetched_data_query, last_sync_tuple)

    conn.commit()


def fetch_easyecom_orders(cur, channel):

    time_now = datetime.utcnow()
    if channel[7] and not (time_now.hour == 21 and 0<time_now.minute<30):
        created_after = channel[7].strftime("%Y-%m-%d %X")
    else:
        created_after = datetime.utcnow() - timedelta(days=30)
        created_after = created_after.strftime("%Y-%m-%d %X")

    fetch_status="1,2,3"
    if channel[15]:
        fetch_status = ','.join(str(x) for x in channel[15])
    easyecom_orders_url = "%s/orders/getAllOrders?api_token=%s&created_after=%s&status_id=%s" % (channel[5], channel[3], created_after, fetch_status)
    data = requests.get(easyecom_orders_url).json()
    last_synced_time = datetime.utcnow() + timedelta(hours=5.5)
    if 'data' not in data:
        return None
    for order in data['data']:
        try:
            cur.execute("SELECT id from orders where order_id_channel_unique='%s' and client_prefix='%s'" % (
            str(order['invoice_id']), channel[1]))
            try:
                existing_order = cur.fetchone()[0]
            except Exception as e:
                existing_order = False
                pass
            if existing_order:
                continue

            pickup_data_id = None
            try:
                cur.execute(
                    "SELECT aa.id FROM client_pickups aa "
                    "WHERE aa.client_prefix='%s' and aa.active=true "
                    "and aa.easyecom_loc_code='%s';" % (str(channel[1]), order['company_name']))
                pickup_data_id = cur.fetchone()[0]
            except Exception:
                pass

            customer_name = order['customer_name']

            customer_phone = order['contactNum']
            customer_phone = ''.join(e for e in str(customer_phone) if e.isalnum())
            customer_phone = "0" + customer_phone[-10:]

            shipping_tuple = (customer_name,
                              "",
                              order['address_line_1'],
                              order['address_line_2'],
                              order['city'],
                              order['pin_code'],
                              order['state'],
                              "India" if len(order['pin_code'])==6 else "",
                              customer_phone,
                              None,
                              None,
                              "IN" if len(order['pin_code'])==6 else ""
                              )

            billing_tuple = (customer_name,
                              "",
                              order['address_line_1'],
                              order['address_line_2'],
                              order['city'],
                              order['pin_code'],
                              order['state'],
                              "India" if len(order['pin_code'])==6 else "",
                              customer_phone,
                              None,
                              None,
                              "IN" if len(order['pin_code'])==6 else ""
                              )

            cur.execute(insert_shipping_address_query, shipping_tuple)
            shipping_address_id = cur.fetchone()[0]

            cur.execute(insert_billing_address_query, billing_tuple)
            billing_address_id = cur.fetchone()[0]

            channel_order_id = str(order['reference_code'])
            if channel[16]:
                channel_order_id = str(channel[16]) + channel_order_id

            order_status="NEW"
            if order['courier'] and order['courier']!='Self Ship':
                order_status = "NOT SHIPPED"

            if order['marketplace'] in easyecom_wareiq_channel_map:
                master_channel_id = easyecom_wareiq_channel_map[order['marketplace']]
            else:
                continue

            orders_tuple = (
                channel_order_id, order['order_date'], customer_name, order['email'],
                customer_phone if customer_phone else "", shipping_address_id, billing_address_id,
                datetime.now(), order_status, channel[1], channel[0], str(order['invoice_id']), pickup_data_id, master_channel_id)

            cur.execute(insert_orders_data_query, orders_tuple)
            order_id = cur.fetchone()[0]

            total_amount = float(order['total_amount'])
            shipping_amount = 0
            subtotal_amount = total_amount- shipping_amount

            if str(order['payment_mode']).lower() in ('cod', 'cashondelivery', 'cash on delivery'):
                financial_status = 'COD'
            elif order['payment_mode'] is None:
                financial_status = "Unknown"
            else:
                financial_status = 'Prepaid'

            payments_tuple = (
                financial_status, total_amount, subtotal_amount,
                shipping_amount, "INR", order_id)

            cur.execute(insert_payments_data_query, payments_tuple)

            if order['courier'] and order['courier'] in easyecom_wareiq_courier_map:
                cur.execute("INSERT INTO shipments (awb, status, order_id, courier_id) VALUES ('%s', 'Success', %s, %s)"%(str(order['awb_number']),
                                                                                                                          order_id, easyecom_wareiq_courier_map[order['courier']]))

            for prod in order['suborders']:
                product_sku = str(prod['company_product_id'])
                prod_tuple = (product_sku, channel[1])
                cur.execute(select_products_query, prod_tuple)
                try:
                    product_id = cur.fetchone()[0]
                except Exception:
                    try:
                        dimensions = json.dumps({"length":float(prod['length']),
                                                 "breadth":float(prod['width']),
                                                 "height":float(prod['height'])})
                    except Exception:
                        dimensions=None
                    try:
                        weight = float(prod['weight'])/1000
                    except Exception:
                        weight=None
                    subcategory_id = None
                    master_sku = prod['sku']
                    if not master_sku:
                        master_sku = product_sku
                    try:
                        cur.execute("SELECT keywords, warehouse_prefix, dimensions, weight, subcategory_id FROM keyword_weights WHERE client_prefix='%s'"%channel[1])
                        all_weights = cur.fetchall()
                        for obj in all_weights:
                            if all(x.lower() in master_sku.lower() for x in obj[0]) or all(x.lower() in prod['name'].lower() for x in obj[0]):
                                dimensions = json.dumps(obj[2])
                                weight = obj[3]
                                subcategory_id = obj[4]
                                break
                    except Exception as e:
                        logger.error("product weight assignment failed for: " + str(order['order_id']) + "\nError:" + str(e))
                    product_insert_tuple = (prod['productName'], product_sku, True, channel[2],
                                            channel[1], datetime.now(), dimensions,
                                            float(prod['mrp']), weight, master_sku, subcategory_id)
                    cur.execute(insert_product_query, product_insert_tuple)
                    product_id = cur.fetchone()[0]

                tax_lines = list()
                try:
                    tax_lines.append({'title': "GST",
                                      'rate': prod['Item_Amount_IGST']/(prod['Item_Amount_IGST']+prod['Item_Amount_Excluding_Tax'])})
                except Exception as e:
                    logger.error("Couldn't fetch tax for: " + str(order_id))

                op_tuple = (product_id, order_id, prod['quantity'], float(prod['quantity'] * float(prod['mrp'])), None, json.dumps(tax_lines))
                cur.execute(insert_op_association_query, op_tuple)

        except Exception as e:
            logger.error("order fetch failed for" + str(order['order_id']) + "\nError:" + str(e))
            conn.rollback()

        conn.commit()

    if data['data']:
        last_sync_tuple = (str(data['data'][-1]['order_id']), last_synced_time, channel[0])
        cur.execute(update_last_fetched_data_query, last_sync_tuple)

    conn.commit()


def fetch_bikayi_orders(cur, channel):

    time_now = datetime.utcnow()
    if channel[7] and not (time_now.hour == 21 and 0<time_now.minute<30):
        updated_after = channel[7].strftime("%s")
    else:
        updated_after = datetime.utcnow() - timedelta(days=30)
        updated_after = updated_after.strftime("%s")

    bikayi_orders_url = """%s/platformPartnerFunctions-fetchOrders""" % (channel[5],)
    key = "3f638d4ff80defb82109951b9638fae3fe0ff8a2d6dc20ed8c493783"
    secret = "6e130520777eb175c300aefdfc1270a4f9a57f2309451311ad3fdcfb"
    req_body = {"appId": "WAREIQ",
                "merchantId": channel[3],
                "timestamp": updated_after}
    signature = hmac.new(bytes(secret.encode()),
                         (key.encode() + "|".encode() + base64.b64encode(
                             json.dumps(req_body).replace(" ", "").encode())),
                         hashlib.sha256).hexdigest()
    headers = {"Content-Type": "application/json",
               "authorization": signature}
    data = requests.post(bikayi_orders_url, headers=headers, data=json.dumps(req_body)).json()
    last_synced_time = datetime.utcnow()+timedelta(hours=5.5)
    if 'orders' not in data or not data['orders']:
        return None
    for order in data['orders']:
        try:
            cur.execute("SELECT id from orders where order_id_channel_unique='%s' and client_prefix='%s'"%(str(order['orderId']), channel[1]))
            try:
                existing_order = cur.fetchone()[0]
            except Exception as e:
                existing_order = False
                pass
            if existing_order:
                continue
            cur.execute("SELECT count(*) FROM client_pickups WHERE client_prefix='%s' and active=true;" % str(channel[1]))
            pickup_count = cur.fetchone()[0]
            if pickup_count == 1 and not channel[17]:
                cur.execute(
                    "SELECT id, client_prefix FROM client_pickups WHERE client_prefix='%s' and active=true;" % str(channel[1]))
                pickup_data_id = cur.fetchone()[0]
            else:
                pickup_data_id = None  # change this as we move to dynamic pickups

            customer_name = order['customerName']

            customer_phone = order['customerPhone']
            customer_phone = ''.join(e for e in str(customer_phone) if e.isalnum())
            customer_phone = "0" + customer_phone[-10:]

            address_1 = order["customerAddress"]["address"]

            billing_tuple = (customer_name,
                             "",
                             address_1,
                             "",
                             order["customerAddress"]["city"],
                             order['customerAddress']['pinCode'],
                             order['customerAddress']['city'],
                             "India",
                             customer_phone,
                             None,
                             None,
                             "IN"
                             )

            shipping_tuple = billing_tuple

            cur.execute(insert_shipping_address_query, shipping_tuple)
            shipping_address_id = cur.fetchone()[0]
            cur.execute(insert_billing_address_query, billing_tuple)
            billing_address_id = cur.fetchone()[0]

            if order['paymentMethod'].lower() in ('cashondelivery','cod','cash','cash on delivery'):
                financial_status = 'cod'
            else:
                financial_status = 'prepaid'

            insert_status = "NEW"

            order_time = datetime.fromtimestamp(order['date']/1000)
            channel_order_id = str(order['orderId'])
            if channel[16]:
                channel_order_id = str(channel[16]) + channel_order_id
            orders_tuple = (
                channel_order_id, order_time, customer_name, "",
                customer_phone if customer_phone else "", shipping_address_id, billing_address_id,
                datetime.now(), insert_status, channel[1], channel[0], str(order['orderId']), pickup_data_id, 8)

            cur.execute(insert_orders_data_query, orders_tuple)
            order_id = cur.fetchone()[0]

            total_amount = float(order['total'])

            payments_tuple = (
                financial_status, total_amount, total_amount, 0, "INR", order_id)

            cur.execute(insert_payments_data_query, payments_tuple)

            for prod in order['items']:
                product_sku = str(prod['id'])
                master_sku = product_sku
                prod_tuple = (master_sku, channel[1])
                select_products_query_temp = """SELECT id from products where master_sku=%s and client_prefix=%s;"""
                cur.execute(select_products_query_temp, prod_tuple)
                try:
                    product_id = cur.fetchone()[0]
                except Exception:
                    dimensions = None
                    weight = None
                    subcategory_id = None
                    master_sku = str(prod['id'])
                    if not master_sku:
                        master_sku = product_sku
                    product_insert_tuple = (prod['name'], product_sku, True, channel[2],
                                            channel[1], datetime.now(), dimensions, prod['unitPrice'],
                                            weight, master_sku, subcategory_id)
                    cur.execute(insert_product_query, product_insert_tuple)
                    product_id = cur.fetchone()[0]

                tax_lines = list()

                op_tuple = (product_id, order_id, prod['quantity'], None, None, json.dumps(tax_lines))
                cur.execute(insert_op_association_query, op_tuple)

        except Exception as e:
            logger.error("order fetch failed for" + str(order['orderId']) + "\nError:" + str(e))

    if data['orders']:
        last_sync_tuple = (str(data['orders'][-1]['orderId']), last_synced_time, channel[0])
        cur.execute(update_last_fetched_data_query, last_sync_tuple)

    conn.commit()


def assign_pickup_points_for_unassigned(cur, cur_2):
    time_after = datetime.utcnow() - timedelta(days=1)
    cur.execute(get_orders_to_assign_pickups, (time_after,))
    all_orders = cur.fetchall()
    for order in all_orders:
        try:
            sku_dict = dict()
            kitted_skus = dict()
            for idx, sku in enumerate(order[3]):
                try:
                    cur.execute("""select bb.sku, aa.quantity from products_combos aa
                                    left join products bb on aa.combo_prod_id=bb.id WHERE aa.combo_id in
                                    (SELECT id from products where sku = %s and client_prefix=%s)""", (sku, order[1]))
                    combo_skus = cur.fetchall()
                    if combo_skus:
                        kitted_skus[sku] = combo_skus
                        for new_sku in combo_skus:
                            sku_dict[new_sku[0]] = order[4][idx]*new_sku[1]
                    else:
                        sku_dict[sku] = order[4][idx]

                except Exception:
                    sku_dict[sku] = order[4][idx]

            sku_string = "('"

            for key, value in sku_dict.items():
                sku_string += key + "','"
            sku_string = sku_string.rstrip("'")
            sku_string = sku_string.rstrip(",")
            sku_string += ")"

            no_sku = len(sku_dict)
            try:
                cur.execute(
                    available_warehouse_product_quantity.replace('__SKU_STR__', sku_string).replace('__CLIENT_PREFIX__',
                                                                                                    order[1]))
            except Exception:
                conn.rollback()

            prod_wh_tuple = cur.fetchall()
            wh_dict = dict()
            courier_id = 1 #todo: do something generic for this
            courier_id_weight = 0.0
            for prod_wh in prod_wh_tuple:
                if prod_wh[4] and prod_wh[5] and prod_wh[5] > courier_id_weight:
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
            if not warehouse_pincode_str and not kitted_skus: #todo: define order split in case of kitting
                prod_list = list()
                set_list = list()
                for key, value in wh_dict.items():
                    append_list = list(set(value['prod_list']) - set(set_list))
                    set_list = list(set(set_list)|set(value['prod_list']))
                    if append_list:
                        prod_list.append(append_list)
                if len(set_list) == no_sku and order[5]!=False:
                    prod_list.sort(key=len, reverse=True)
                    split_order(cur, order[0], prod_list)
                elif order[6]:
                    assign_default_wh(cur, order)
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

            if not final_wh:
                if order[6]:
                    assign_default_wh(cur, order)
                else:
                    logger.info(str(order[0]) + "Not serviceable")
                continue

            cur.execute("""select aa.id from client_pickups aa
                            left join pickup_points bb on aa.pickup_id=bb.id
                            where bb.warehouse_prefix=%s and aa.client_prefix=%s and active=true;""", (final_wh[0],order[1]))

            pickup_id = cur.fetchone()
            if not pickup_id:
                logger.info(str(order[0]) + "Pickup id not found")
                continue

            cur.execute("""UPDATE orders SET pickup_data_id = %s WHERE id=%s""", (pickup_id[0], order[0]))

        except Exception as e:
            if order[6]:
                assign_default_wh(cur, order)
            logger.error("couldn't assign pickup for order " + str(order[0]) + "\nError: " + str(e))

    conn.commit()


def assign_default_wh(cur, order):
    cur.execute("""select aa.id from client_pickups aa
                                                    left join pickup_points bb on aa.pickup_id=bb.id
                                                    where bb.warehouse_prefix=%s and aa.client_prefix=%s
                                                    and aa.active=true;""",
                (order[6], order[1]))

    pickup_id = cur.fetchone()
    if pickup_id:
        cur.execute("""UPDATE orders SET pickup_data_id = %s WHERE id=%s""", (pickup_id[0], order[0]))


def split_order(cur, order_id, prod_list):
    try:
        sub_id = 'A'
        cur.execute("SELECT shipping_charges, payment_mode, currency, amount from orders_payments WHERE order_id=%s" % str(order_id))
        fetched_tuple = cur.fetchone()
        shipping_cost_each = fetched_tuple[0]/len(prod_list)
        payment_mode = fetched_tuple[1]
        currency = fetched_tuple[2]
        order_total = int(fetched_tuple[3])
        if fetched_tuple[0]:
            order_total -= int(fetched_tuple[0])

        all_products = list()
        for prod_new in prod_list:
            all_products+=prod_new
        cur.execute("SELECT sum(amount) FROM op_association WHERE order_id=%s and product_id in %s" % (str(order_id), str(tuple(all_products))))
        products_total = cur.fetchone()[0]
        for idx, prods in enumerate(prod_list):
            if len(prods)==1:
                prods_tuple = "("+str(prods[0])+")"
            else:
                prods_tuple = str(tuple(prods))
            cur.execute("SELECT sum(amount) FROM op_association WHERE order_id=%s and product_id in %s"%(str(order_id), prods_tuple))
            prod_amount = cur.fetchone()[0]
            prod_amount = round(prod_amount*(order_total/products_total))
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


def update_thirdwatch_data(cur):
    cur.execute(select_thirdwatch_check_orders_query.replace('__ORDER_TIME__', str(datetime.utcnow().date())))
    all_orders=cur.fetchall()
    for order in all_orders:
        try:
            if order[32] and order[15].lower()!='cod':
                continue
            items = list()
            shipping_name = order[6] + " " +order[7] if order[7] else order[6]
            for idx, dim in enumerate(order[16]):
                items.append({"id":order[16][idx],
                              "title":order[17][idx],
                              "amount":str(round(order[18][idx]*100)),
                              "currency":"INR",
                              "brand":order[28],
                              "category":order[28],
                              "quantity":order[19][idx],
                              "is_onsale":False,
                              "sku":order[20][idx],
                              })

            thirdwatch_data = {
                                  "device":{
                                    "ip":str(order[0]),
                                    "session_id":str(order[1]),
                                    "user-agent": str(order[2])
                                  },
                                  "user":{
                                    "id":str(order[3]),
                                    "created_at":str(round((datetime.strptime(order[4][:19], "%Y-%m-%dT%X")).timestamp() * 1000)),
                                    "email":order[5],
                                    "first_name":order[6],
                                    "last_name":order[7],
                                    "contact":order[8],
                                    "first_purchase":True if order[9]<2 else False,
                                    "email_verification":"verified" if order[10] else "unverified",
                                    "contact_verification":"verified" if order[11] else "unverified"
                                  },
                                  "order":{
                                    "id":str(order[12]),
                                    "created_at":str(round(order[13].timestamp() * 1000)),
                                    "amount":str(round(order[14]*100)),
                                    "currency":"INR",
                                    "prepaid":False if order[15].lower()=='cod' else True,
                                    "items":items,
                                    "shipping_address":{
                                      "name":shipping_name,
                                      "phone":order[21],
                                      "line1":order[22],
                                      "line2":order[23],
                                      "city":order[24],
                                      "state":order[25],
                                      "country":order[27],
                                      "postal_code":order[26],
                                      "type":"home"
                                    },
                                  "payment": {
                                      "id": order[29],
                                      "status": "success",
                                      "gateway": "cod" if order[15].lower()=='cod' else order[30],
                                      "amount": str(round(order[14]*100)),
                                      "currency": "INR",
                                      "method": "cod" if order[15].lower()=='cod' else order[31]
                                      }
                                  }
                                }
            headers = {"X-THIRDWATCH-API-KEY": "4b1824140e",
                       "Content-Type": "application/json"}

            req = requests.post("https://api.razorpay.com/v1/thirdwatch/orders", headers=headers, data=json.dumps(thirdwatch_data)).json()
        except Exception as e:
            logger.error("Couldn't check thirdwatch for: "+str(order[12]))


easyecom_wareiq_channel_map = {"Amazon.in": 2,
                           "Shopify": 1,
                           "FlipkartSmart": 3,
                               "Flipkart":3}

easyecom_wareiq_courier_map = {"eKart": 7}