import psycopg2, requests, os, json
from datetime import datetime

from .queries import *
"""
host = os.environ('DTATBASE_HOST')
database = os.environ('DTATBASE_NAME')
user = os.environ('DTATBASE_USER')
password = os.environ('DTATBASE_PASSWORD')
conn = psycopg2.connect(host=host, database=database, user=user, password=password)
"""
conn = psycopg2.connect(host="wareiq-core-prod2.cvqssxsqruyc.us-east-1.rds.amazonaws.com", database="core_prod", user="postgres", password="aSderRFgd23")

def lambda_handler():
    cur = conn.cursor()
    cur.execute(fetch_client_channels_query)
    for channel in cur.fetchall():
        if channel[11] == "Shopify":
            shopify_orders_url = "https://%s:%s@%s/admin/api/2019-10/orders.json?since_id=%s&limit=250"%(channel[3], channel[4], channel[5], channel[6])
            data = requests.get(shopify_orders_url).json()
            products_quantity_dict = dict()
            for order in data['orders']:
                try:
                    product_exists = True
                    if channel[1] == "DAPR": #serving only DAPR available skus, Have to create generic logic for this
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

                    cur.execute("SELECT id, client_prefix FROM client_pickups WHERE client_prefix='%s';" % str(channel[1]))
                    pickup_data_id = cur.fetchone()[0] #change this as we move to dynamic pickups

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

                    customer_phone = order['customer']['phone'] if order['customer']['phone'] else order['shipping_address']['phone']
                    customer_phone = str(customer_phone).replace(" ", "")
                    customer_phone = "0" + customer_phone[-10:]

                    orders_tuple = (str(order['order_number']), order['created_at'], customer_name, order['customer']['email'],
                                    customer_phone if customer_phone else "", shipping_address_id,
                                    datetime.now(), "NEW", channel[1], channel[0], str(order['id']), pickup_data_id)

                    cur.execute(insert_orders_data_query, orders_tuple)
                    order_id = cur.fetchone()[0]

                    total_amount = float(order['subtotal_price_set']['shop_money']['amount'])+float(order['total_shipping_price_set']['shop_money']['amount'])

                    if order['financial_status'] == 'paid':
                        financial_status = 'prepaid'
                    elif order['financial_status'] == 'pending':
                        financial_status = 'COD'
                    else:
                        financial_status = order['financial_status']

                    payments_tuple = (financial_status, total_amount, float(order['subtotal_price_set']['shop_money']['amount']),
                                      float(order['total_shipping_price_set']['shop_money']['amount']), order["currency"], order_id)

                    cur.execute(insert_payments_data_query, payments_tuple)

                    for prod in order['line_items']:
                        product_sku = str(prod['variant_id'])
                        prod_tuple = (product_sku, channel[1])
                        cur.execute(select_products_query, prod_tuple)
                        try:
                            product_id = cur.fetchone()[0]
                        except Exception:
                            if product_sku == "19675086585915" and channel[1] == 'DAPR': #DAPR combination sku not present in products
                                for i in (3204,3206):
                                    product_id = i
                                    op_tuple = (product_id, order_id, prod['quantity'])
                                    cur.execute(insert_op_association_query, op_tuple)
                                    if product_id not in products_quantity_dict:
                                        products_quantity_dict[product_id] = prod['quantity']
                                    else:
                                        products_quantity_dict[product_id] += prod['quantity']
                                continue
                            if product_sku == "30690984558651" and channel[1] == 'DAPR': #DAPR combination sku not present in products
                                for i in (3249,3250):
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
                                    dimensions = {"length":6.87, "breadth":22.5, "height":27.5}
                                    weight = 0.51
                                else:
                                    dimensions = {"length": 2.5, "breadth": 22.5, "height": 27.5}
                                    weight = 0.18
                            else:
                                dimensions = { "length": 2.5, "breadth": 22.5, "height": 27.5}
                                weight = 0.20
                            product_insert_tuple = (prod['name'], str(prod['variant_id']), True, channel[2],
                                                    channel[1], datetime.now(), json.dumps(dimensions), float(prod['price']), weight)
                            cur.execute(insert_product_query, product_insert_tuple)
                            product_id = cur.fetchone()[0]

                            product_quantity_insert_tuple = (product_id,100,100,100,channel[1],"APPROVED",datetime.now())
                            cur.execute(insert_product_quantity_query, product_quantity_insert_tuple)

                        op_tuple = (product_id, order_id, prod['quantity'])

                        cur.execute(insert_op_association_query, op_tuple)

                        if product_id not in products_quantity_dict:
                            products_quantity_dict[product_id] = prod['quantity']
                        else:
                            products_quantity_dict[product_id] += prod['quantity']
                except Exception as e:
                    print("order fetch failed for" + str(order['order_number']) + "\nError:" + str(e))

            if data['orders']:
                last_sync_tuple = (str(data['orders'][-1]['id']), datetime.now(), channel[0])
                cur.execute(update_last_fetched_data_query, last_sync_tuple)

            for prod_id, quan in products_quantity_dict.items():
                prod_quan_tuple = (quan, quan, prod_id)
                cur.execute(update_product_quantity_query, prod_quan_tuple)

        conn.commit()
    cur.close()
