import psycopg2, requests, os
from datetime import datetime

from .queries import *
"""
host = os.environ('DTATBASE_HOST')
database = os.environ('DTATBASE_NAME')
user = os.environ('DTATBASE_USER')
password = os.environ('DTATBASE_PASSWORD')
conn = psycopg2.connect(host=host, database=database, user=user, password=password)
"""
conn = psycopg2.connect(host="wareiq-core-prod.cvqssxsqruyc.us-east-1.rds.amazonaws.com", database="core_prod", user="postgres", password="postgres")

def lambda_handler():
    cur = conn.cursor()
    cur.execute(fetch_client_channels_query)
    for channel in cur.fetchall():
        if channel[11] == "Shopify":
            shopify_orders_url = "https://%s:%s@%s/admin/api/2019-10/orders.json?since_id=%s&limit=250"%(channel[3], channel[4], channel[5], channel[6])
            data = requests.get(shopify_orders_url).json()
            for order in data['orders']:
                try:
                    customer_name = order['customer']['first_name'] + order['customer']['last_name']
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

                    orders_tuple = (str(order['order_number']), order['created_at'], customer_name, order['customer']['email'],
                                    customer_phone if customer_phone else "", shipping_address_id,
                                    datetime.now(), "NEW", channel[1], channel[0])

                    cur.execute(insert_orders_data_query, orders_tuple)
                    order_id = cur.fetchone()[0]

                    total_amount = float(order['subtotal_price_set']['shop_money']['amount'])+float(order['total_shipping_price_set']['shop_money']['amount'])

                    if order['financial_status'] == 'paid':
                        financial_status = 'prepaid'
                    else:
                        financial_status = order['financial_status']

                    payments_tuple = (financial_status, total_amount, float(order['subtotal_price_set']['shop_money']['amount']),
                                      float(order['total_shipping_price_set']['shop_money']['amount']), order["currency"], order_id)

                    cur.execute(insert_payments_data_query, payments_tuple)

                    for prod in order['line_items']:
                        prod_tuple = (str(prod['variant_id']), channel[1])
                        cur.execute(select_products_query, prod_tuple)
                        product_id = cur.fetchone()[0]

                        op_tuple = (product_id, order_id, prod['quantity'])

                        cur.execute(insert_op_association_query, op_tuple)
                except Exception as e:
                    print("order fetch failed for" + str(order['order_number']) + "\nError:" + str(e))

            if data['orders']:
                last_sync_tuple = (str(data['orders'][-1]['id']), datetime.now(), channel[0])
                cur.execute(update_last_fetched_data_query, last_sync_tuple)

    conn.commit()
    cur.close()


from reportlab.pdfgen import canvas


def create_pdf():
    c = canvas.Canvas("testing.pdf")
    c.drawString(100, 100, "Hello World")
    c.showPage()
    c.save()
