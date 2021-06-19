import psycopg2, requests, os, json, hmac, hashlib, base64, random, string
from datetime import datetime, timedelta
from requests_oauthlib.oauth1_session import OAuth1Session
from woocommerce import API
import logging
from app.db_utils import DbConnection
from app.ship_orders.function import kama_chn_sdd_pincodes
from app.core_app_jobs.queries import insert_status_query

from .queries import *

logger = logging.getLogger()
logger.setLevel(logging.INFO)

conn = DbConnection.get_db_connection_instance()


def sync_channel_status(client_prefix=None):
    cur = conn.cursor()
    if not client_prefix:
        cur.execute(fetch_client_channels_query + " AND aa.channel_id=7")
    else:
        cur.execute(fetch_client_channels_query+" AND aa.client_prefix='%s'"%client_prefix)
    time_now = datetime.utcnow() + timedelta(hours=5.5)
    sync_1_days = True if time_now.hour in (6, 16) else None
    for channel in cur.fetchall():
        if channel[11] == "EasyEcom":
            try:
                resync_easyecom_orders(cur, channel, manual=True if sync_1_days else None)
            except Exception as e:
                logger.error("Couldn't sync status orders: " + str(channel[1]) + "\nError: " + str(e.args))

    cur.close()


def resync_easyecom_orders(cur, channel, manual=None):
    if not manual:
        time_1_hours_ago = datetime.utcnow() + timedelta(hours=4)
    else:
        time_1_hours_ago = datetime.utcnow() - timedelta(days=1)

    updated_after = time_1_hours_ago.strftime("%Y-%m-%d %X")
    updated_before = (datetime.utcnow()+timedelta(hours=5.5)).strftime("%Y-%m-%d %X")
    data = list()
    easyecom_orders_url = "%s/orders/V2/getAllOrders?api_token=%s&updated_after=%s&updated_before=%s" % (channel[5], channel[3], updated_after, updated_before)
    while easyecom_orders_url:
        req = requests.get(easyecom_orders_url).json()
        if req['data']:
            data += req['data']['orders']
            easyecom_orders_url = "https://api.easyecom.io"+req['data']['nextUrl'] if req['data']['nextUrl'] else None
        else:
            easyecom_orders_url = None
    if not data:
        return None
    for order in data:
        conn.commit()
        try:
            cur.execute("SELECT id, status, pickup_data_id from orders where order_id_channel_unique='%s' and client_prefix='%s'" % (
            str(order['invoice_id']), channel[1]))
            try:
                existing_order = cur.fetchone()
            except Exception as e:
                existing_order = []
                pass
            if not existing_order:
                continue

            failed_order = None
            for item in order['suborders']:
                if item['item_status'] == 'Pending':
                    failed_order = True
                    break

            if failed_order:
                raise Exception("Inventory not assigned")

            if order['marketplace'] in easyecom_wareiq_channel_map:
                master_channel_id = easyecom_wareiq_channel_map[order['marketplace']]
            else:
                continue

            if existing_order[1] == 'NOT SHIPPED' and order['order_status']=='Shipped':
                cur.execute("SELECT id, courier_id, awb from shipments where order_id=%s;", (existing_order[0],))
                existing_shipment = []
                try:
                    existing_shipment = cur.fetchone()
                except Exception:
                    pass
                if order['courier'] in easyecom_wareiq_courier_map:
                    if existing_shipment and len(existing_shipment[2])<5:
                        cur.execute("UPDATE shipments SET awb=%s where id=%s", (str(order['awb_number']), existing_shipment[0]))
                    elif order['courier'] and order['courier'] in easyecom_wareiq_courier_map and not existing_shipment:
                        cur.execute("INSERT INTO shipments (awb, status, order_id, courier_id) VALUES ('%s', 'Success', %s, %s) RETURNING id, courier_id, awb"
                                    %(str(order['awb_number']), existing_order[0], easyecom_wareiq_courier_map[order['courier']]))
                        existing_shipment = cur.fetchone()
                if existing_shipment:
                    cur.execute(insert_status_query, (
                        existing_order[0], existing_shipment[1], existing_shipment[0], "UD", "Shipped", "Order Shipped", "", "",
                        datetime.utcnow()+timedelta(hours=5.5)))
                cur.execute("UPDATE orders SET status='SHIPPED' where id=%s", (existing_order[0], ))
                continue

            if order['order_status']=='Cancelled':
                cur.execute("UPDATE orders SET status='CANCELED' where id=%s", (existing_order[0], ))
                continue

            if existing_order[1] == 'SHIPPED' and order['order_status']=='Returned':
                cur.execute(insert_status_query, (
                    existing_order[0], existing_shipment[1], existing_shipment[0], "DL", "RTO", "Order returned", "","",
                    datetime.utcnow() + timedelta(hours=5.5)))
                cur.execute("UPDATE orders SET status='RTO' where id=%s", (existing_order[0], ))
                continue

            unit_count = 0
            for suborder in order['suborders']:
                unit_count += suborder['suborder_quantity']

            cur.execute("""SELECT sum(quantity) from op_association where order_id=%s"""%(existing_order[0], ))
            existing_count = cur.fetchone()[0]
            if unit_count!=existing_count:
                cur.execute("""update orders set client_prefix='DELETED' where id=%s""" % (existing_order[0],))
                continue

            try:
                cur.execute(
                    "SELECT aa.id FROM client_pickups aa "
                    "WHERE aa.client_prefix='%s' and aa.active=true "
                    "and aa.easyecom_loc_code='%s';" % (str(channel[1]), order['company_name']))
                pickup_data_id = cur.fetchone()[0]
            except Exception:
                pickup_data_id = update_easyecom_wh_mapping(order, channel[1], cur)
                pass

            if pickup_data_id and pickup_data_id!=existing_order[2]:
                cur.execute("UPDATE orders SET pickup_data_id=%s WHERE id=%s"%(pickup_data_id, existing_order[0]))

        except Exception as e:
            conn.rollback()

    conn.commit()


easyecom_wareiq_channel_map = {"Amazon.in": 2,
                               "Shopify": 1,
                               "FlipkartSmart": 3,
                               "Flipkart":3,
                               "Offline":4,
                               "Shopify1": 1,
                               "MenXP":11,
                               "PayTM":12,
                               "Snapdeal":10,
                               "Woocommerce":5
                               }

easyecom_wareiq_courier_map = {"eKart": 7}


def update_easyecom_wh_mapping(order, client_prefix, cur):
    try:
        cur.execute("""select aa.id from client_pickups aa
                        left join pickup_points bb on aa.pickup_id=bb.id
                        where aa.client_prefix='%s'
                        and aa.active=true
                        and bb.pincode = '%s';"""%(client_prefix, order['pickup_pin_code']))
        pickup_data_id = cur.fetchone()[0]
        cur.execute("""UPDATE client_pickups SET easyecom_loc_code='%s' WHERE id=%s"""%(order['company_name'], str(pickup_data_id)))
        return pickup_data_id
    except Exception:
        return None