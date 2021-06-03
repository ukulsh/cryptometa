from .contants import *
from .queries import *
from .utils import *
from datetime import datetime
import json, random, string
from woocommerce import API
from math import ceil
from app.db_utils import DbConnection, UrlShortner
from app.ship_orders.function import ship_orders
from app.update_status.function import update_delivered_on_channels, verification_text, \
    delhivery_status_code_mapping_dict, xpressbees_status_mapping, Xpressbees_ndr_reasons
from app.update_status.update_status_utils import send_shipped_event, send_delivered_event, send_ndr_event, \
    webhook_updates, send_picked_rvp_event, send_delivered_rvp_event
from app.download_queues.tasks import session

conn = DbConnection.get_db_connection_instance()
conn_2 = DbConnection.get_pincode_db_connection_instance()
conn_3 = DbConnection.get_users_db_connection_instance()


def consume_ecom_scan_util(payload):
    with psycopg2.connect(host=os.environ.get('DATABASE_HOST'), database=os.environ.get('DATABASE_NAME'),
                                user=os.environ.get('DATABASE_USER'), password=os.environ.get('DATABASE_PASSWORD')) as conn:
        try:
            cur = conn.cursor()
            awb = payload.get('awb')
            if not awb:
                return "Skipped: no awb"

            reason_code_number = payload.get('reason_code_number')
            if not reason_code_number:
                return "Skipped: no reason code"

            cur.execute(get_order_details_query.replace('__FILTER_ORDER__', "bb.awb='%s'"%str(awb)))
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

            is_return = False
            if payload.get("ref_awb") and str(payload.get("reason_code_number"))!='777':
                is_return = True
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
                    order[0], order[40], order[10], status_code, status, status_text, location, location_city, status_time))
                conn.commit()
                return "Successful: scan saved only"

            if str(payload.get("status")) == "R999":
                status = "RTO"

            cur.execute(insert_scan_query, (
                order[0], order[40], order[10], status_code, status, status_text, location, location_city, status_time))

            if not status or status == 'READY TO SHIP':
                return "Successful: scan saved only"

            if status!='RTO' and is_return:
                return "Successful: scan saved only"

            tracking_status = ecom_express_status_mapping[reason_code_number][2] if status!='RTO' else 'RTO'
            if tracking_status:
                cur.execute(insert_status_query, (
                    order[0], order[40], order[10], status_type, tracking_status, status_text, location, location_city, status_time))

            customer_phone = order[4].replace(" ", "")
            customer_phone = "0" + customer_phone[-10:]
            tracking_link = "https://webapp.wareiq.com/tracking/" + order[1]

            if tracking_status == "Picked":
                mark_picked_channel(order, cur)
                tracking_link = UrlShortner.get_short_url(tracking_link, cur)
                send_shipped_event(customer_phone, order[19], order, "", "Ecom Express", tracking_link)
                mark_order_picked_pickups(order, cur)
                webhook_updates(order, cur, status, "Shipment Picked Up", location, status_time.strftime('%Y-%m-%d %H:%M:%S'), ndr_id=None)

            elif tracking_status == "Delivered":
                mark_delivered_channel(order)
                tracking_link = UrlShortner.get_short_url(tracking_link, cur)
                send_delivered_event(customer_phone, order, "Ecom Express", tracking_link)
                webhook_updates(order, cur, status, "Shipment Delivered", location, status_time.strftime('%Y-%m-%d %H:%M:%S'), ndr_id=None)

            elif tracking_status == "RTO":
                mark_rto_channel(order)
                webhook_updates(order, cur, status, "Shipment RTO", location, status_time.strftime('%Y-%m-%d %H:%M:%S'), ndr_id=None)

            if reason_code_number in ecom_express_ndr_reasons:
                ndr_reason = ecom_express_ndr_reasons[reason_code_number]
                verification_text(order, cur, ndr_reason=ndr_reason)
                webhook_updates(order, cur, status, "", location, status_time.strftime('%Y-%m-%d %H:%M:%S'), ndr_id=ndr_reason)

            cur.execute("UPDATE orders SET status=%s, status_type=%s WHERE id=%s;", (status, status_type, order[0]))

            conn.commit()
        except Exception as e:
            conn.rollback()
            return "Failed: " + str(e.args[0])
        return "Successful: all tasks done"


def consume_sfxsdd_scan_util(payload):
    with psycopg2.connect(host=os.environ.get('DATABASE_HOST'), database=os.environ.get('DATABASE_NAME'),
                                user=os.environ.get('DATABASE_USER'), password=os.environ.get('DATABASE_PASSWORD')) as conn:
        try:
            cur = conn.cursor()
            awb = payload.get('sfx_order_id')
            if not awb:
                return "Skipped: no awb"

            reason_code_number = payload.get('order_status')
            if not reason_code_number:
                return "Skipped: no reason code"

            cur.execute(get_order_details_query.replace('__FILTER_ORDER__', "bb.awb='%s'"%str(awb)))
            try:
                status_time = next(v for (k,v) in payload.items() if k.endswith('time'))
                status_time = datetime.strptime(status_time.split('.')[0], "%Y-%m-%dT%H:%M:%S")
            except Exception:
                status_time = datetime.utcnow() + timedelta(hours=5.5)

            order = None
            try:
                order = cur.fetchone()
            except Exception:
                pass

            if not order:
                return "Failed: order not found"

            status_code = reason_code_number
            status = reason_code_number
            status_text = ""
            location = order[41]
            location_city = order[41]

            if reason_code_number in sfxsdd_status_mapping:
                status = sfxsdd_status_mapping[reason_code_number][0]
                status_type = sfxsdd_status_mapping[reason_code_number][1]
                status_text = sfxsdd_status_mapping[reason_code_number][3]
            else:
                cur.execute(insert_scan_query, (
                    order[0], order[40], order[10], status_code, status, status_text, location, location_city, status_time))
                conn.commit()
                return "Successful: scan saved only"

            cur.execute(insert_scan_query, (
                order[0], order[40], order[10], status_code, status, status_text, location, location_city, status_time))

            if not status or status == 'READY TO SHIP':
                return "Successful: scan saved only"

            tracking_status = sfxsdd_status_mapping[reason_code_number][2]
            if tracking_status:
                cur.execute(insert_status_query, (
                    order[0], order[40], order[10], status_type, tracking_status, status_text, location, location_city, status_time))

            customer_phone = order[4].replace(" ", "")
            customer_phone = "0" + customer_phone[-10:]
            tracking_link = "https://webapp.wareiq.com/tracking/" + order[1]

            if tracking_status == "Picked":
                mark_picked_channel(order, cur)
                tracking_link = UrlShortner.get_short_url(tracking_link, cur)
                send_shipped_event(customer_phone, order[19], order, "", "Shadowfax", tracking_link)
                mark_order_picked_pickups(order, cur)
                webhook_updates(order, cur, status, "Shipment Picked Up", location, status_time.strftime('%Y-%m-%d %H:%M:%S'), ndr_id=None)

            elif tracking_status == "Delivered":
                mark_delivered_channel(order)
                tracking_link = UrlShortner.get_short_url(tracking_link, cur)
                send_delivered_event(customer_phone, order, "Shadowfax", tracking_link)
                webhook_updates(order, cur, status, "Shipment Delivered", location, status_time.strftime('%Y-%m-%d %H:%M:%S'), ndr_id=None)

            elif tracking_status == "RTO":
                mark_rto_channel(order)
                webhook_updates(order, cur, status, "Shipment RTO", location, status_time.strftime('%Y-%m-%d %H:%M:%S'), ndr_id=None)

            cur.execute("UPDATE orders SET status=%s, status_type=%s WHERE id=%s;", (status, status_type, order[0]))

            conn.commit()
        except Exception as e:
            conn.rollback()
            return "Failed: " + str(e.args[0])
        return "Successful: all tasks done"


def consume_pidge_scan_util(payload):
    with psycopg2.connect(host=os.environ.get('DATABASE_HOST'), database=os.environ.get('DATABASE_NAME'),
                                user=os.environ.get('DATABASE_USER'), password=os.environ.get('DATABASE_PASSWORD')) as conn:
        try:
            cur = conn.cursor()
            awb = payload.get('PBID')
            if not awb:
                return "Skipped: no awb"

            reason_code_number = payload.get('trip_status')
            if not reason_code_number:
                return "Skipped: no reason code"

            if payload.get("attempt_type") == 20:
                return "Skipped: no status to update"

            if reason_code_number in (20, 100, 120, 5):
                return "No status to update"

            vendor_order_id = payload.get('vendor_order_id')
            cur.execute(get_order_details_query.replace('__FILTER_ORDER__', "aa.id=%s"%str(vendor_order_id)))
            try:
                status_time = payload.get("timestamp")
                status_time = datetime.strptime(status_time, "%Y-%m-%dT%H:%M:%S.%fZ")
                status_time = status_time + timedelta(hours=5.5)
            except Exception:
                status_time = datetime.utcnow() + timedelta(hours=5.5)

            order = None
            try:
                order = cur.fetchone()
            except Exception:
                pass

            if not order or not order[40]:
                return "Failed: order not found"

            try:
                cur.execute("select * from master_couriers where id=__COURIER_ID__ and courier_name ilike 'Pidge%'".replace('__COURIER_ID__', str(order[40])))
                temp = cur.fetchone()[0]
            except Exception:
                return "Failed: order not found"

            is_return = False
            if payload.get("attempt_type")==30:
                is_return = True
            status_code = str(reason_code_number)
            status = ""
            status_text = str(reason_code_number)
            location = order[41]
            location_city = order[41]

            if reason_code_number in pidge_status_mapping:
                status = pidge_status_mapping[reason_code_number][0]
                status_type = "UD" if not is_return else "RT"
                status_text = pidge_status_mapping[reason_code_number][3]
            else:
                cur.execute(insert_scan_query, (
                    order[0], order[40], order[10], status_code, status, status_text, location, location_city, status_time))
                conn.commit()
                return "Successful: scan saved only"

            if reason_code_number == 190 and is_return:
                status = "RTO"

            cur.execute(insert_scan_query, (
                order[0], order[40], order[10], status_code, status, status_text, location, location_city, status_time))

            if not status or status == 'READY TO SHIP':
                return "Successful: scan saved only"

            tracking_status = ""
            if not is_return:
                tracking_status = pidge_status_mapping[reason_code_number][2]
            elif reason_code_number in (150, 170):
                tracking_status = "Returned"
            elif reason_code_number in (190, ):
                tracking_status = "RTO"

            if tracking_status:
                cur.execute(insert_status_query, (
                    order[0], order[40], order[10], status_type, tracking_status, status_text, location, location_city, status_time))

            customer_phone = order[4].replace(" ", "")
            customer_phone = "0" + customer_phone[-10:]
            tracking_link = "https://webapp.wareiq.com/tracking/" + order[1]

            if tracking_status == "Picked":
                mark_picked_channel(order, cur)
                tracking_link = UrlShortner.get_short_url(tracking_link, cur)
                send_shipped_event(customer_phone, order[19], order, "", "Pidge", tracking_link)
                mark_order_picked_pickups(order, cur)
                webhook_updates(order, cur, status, "Shipment Picked Up", location, status_time.strftime('%Y-%m-%d %H:%M:%S'), ndr_id=None)

            elif tracking_status == "Delivered":
                mark_delivered_channel(order)
                tracking_link = UrlShortner.get_short_url(tracking_link, cur)
                send_delivered_event(customer_phone, order, "Pidge", tracking_link)
                webhook_updates(order, cur, status, "Shipment Delivered", location, status_time.strftime('%Y-%m-%d %H:%M:%S'), ndr_id=None)

            elif tracking_status == "RTO":
                mark_rto_channel(order)
                webhook_updates(order, cur, status, "Shipment RTO", location, status_time.strftime('%Y-%m-%d %H:%M:%S'), ndr_id=None)

            cur.execute("UPDATE orders SET status=%s, status_type=%s WHERE id=%s;", (status, status_type, order[0]))

            conn.commit()
        except Exception as e:
            conn.rollback()
            return "Failed: " + str(e.args[0])
        return "Successful: all tasks done"


def consume_delhivery_scan_util(payload):
    with psycopg2.connect(host=os.environ.get('DATABASE_HOST'), database=os.environ.get('DATABASE_NAME'),
                                user=os.environ.get('DATABASE_USER'), password=os.environ.get('DATABASE_PASSWORD')) as conn:
        try:
            cur = conn.cursor()
            payload = payload.get('Shipment')
            awb = payload.get('AWB')
            if not awb:
                return "Skipped: no awb"

            reason_code_number = payload['Status'].get('Status')
            if not reason_code_number:
                return "Skipped: no reason code"

            cur.execute(get_order_details_query.replace('__FILTER_ORDER__', "bb.awb='%s'"%str(awb)))
            try:
                status_time = payload['Status'].get('StatusDateTime')
                if len(status_time) == 19:
                    status_time = datetime.strptime(status_time, '%Y-%m-%dT%H:%M:%S')
                else:
                    status_time = datetime.strptime(status_time, '%Y-%m-%dT%H:%M:%S.%f')
            except Exception:
                status_time = datetime.utcnow() + timedelta(hours=5.5)

            order = None
            try:
                order = cur.fetchone()
            except Exception:
                pass

            if not order or not order[40]:
                return "Failed: order not found"

            try:
                cur.execute("select * from master_couriers where id=__COURIER_ID__ and courier_name ilike 'Delhivery%'".replace('__COURIER_ID__', str(order[40])))
                temp = cur.fetchone()[0]
            except Exception:
                return "Failed: order not found"

            status_code = str(payload.get('NSLCode'))
            status = payload['Status'].get('Status').upper()
            status_type = payload['Status'].get('StatusType')
            status_text = payload['Status'].get('Instructions')
            location = payload['Status'].get('StatusLocation')
            location_city = payload['Status'].get('StatusLocation')

            cur.execute(insert_scan_query, (
                order[0], order[40], order[10], status_code, status, status_text, location, location_city, status_time))

            if not status or status in ('READY TO SHIP', 'NOT PICKED', 'PICKUP REQUESTED', 'MANIFESTED'):
                return "Successful: scan saved only"

            tracking_status = None
            if status == "IN TRANSIT" and "picked" in status_text.lower() and status_type=='UD':
                tracking_status = "Picked"
            elif status == "IN TRANSIT" and status_code=='EOD-77':
                tracking_status = "Picked RVP"
            elif status == "IN TRANSIT" and status_type in ('UD', 'PU'):
                tracking_status = "In Transit"
            elif status == "DISPATCHED" and status_type=='PU':
                tracking_status = "Dispatched for DTO"
            elif status == "DISPATCHED" and status_type=='RT':
                tracking_status = "Dispatched for DTO"
            elif status == "DISPATCHED" and "out for delivery" in status_text.lower():
                tracking_status = "Out for delivery"
            elif status == "DELIVERED":
                tracking_status = "Delivered"
            elif status_type == "RT" and 'RT' in status_code:
                tracking_status = "Returned"
            elif status == "RTO":
                tracking_status = "RTO"
            elif status == "DTO":
                tracking_status = "DTO"

            if tracking_status:
                cur.execute(insert_status_query, (
                    order[0], order[40], order[10], status_type, tracking_status, status_text, location, location_city, status_time))

            customer_phone = order[4].replace(" ", "")
            customer_phone = "0" + customer_phone[-10:]
            tracking_link = "https://webapp.wareiq.com/tracking/" + order[1]

            if tracking_status == "Picked":
                mark_picked_channel(order, cur)
                tracking_link = UrlShortner.get_short_url(tracking_link, cur)
                send_shipped_event(customer_phone, order[19], order, "", "Delhivery", tracking_link)
                mark_order_picked_pickups(order, cur)
                webhook_updates(order, cur, status, "Shipment Picked Up", location, status_time.strftime('%Y-%m-%d %H:%M:%S'), ndr_id=None)

            elif tracking_status == "Delivered":
                mark_delivered_channel(order)
                tracking_link = UrlShortner.get_short_url(tracking_link, cur)
                send_delivered_event(customer_phone, order, "Delhivery", tracking_link)
                webhook_updates(order, cur, status, "Shipment Delivered", location, status_time.strftime('%Y-%m-%d %H:%M:%S'), ndr_id=None)

            elif tracking_status == "RTO":
                mark_rto_channel(order)
                webhook_updates(order, cur, status, "Shipment RTO", location, status_time.strftime('%Y-%m-%d %H:%M:%S'), ndr_id=None)

            elif tracking_status == "DTO":
                tracking_link = UrlShortner.get_short_url(tracking_link, cur)
                send_delivered_rvp_event(customer_phone, order, "Delhivery", tracking_link)

            elif tracking_status == "Picked RVP":
                tracking_link = UrlShortner.get_short_url(tracking_link, cur)
                send_picked_rvp_event(customer_phone, order, "Delhivery", tracking_link)

            if status_code in delhivery_status_code_mapping_dict:
                ndr_reason = delhivery_status_code_mapping_dict[status_code]
                verification_text(order, cur, ndr_reason=ndr_reason)
                webhook_updates(order, cur, status, "", location, status_time.strftime('%Y-%m-%d %H:%M:%S'), ndr_id=ndr_reason)

            cur.execute("UPDATE orders SET status=%s, status_type=%s WHERE id=%s;", (status, status_type, order[0]))

            conn.commit()
        except Exception as e:
            conn.rollback()
            return "Failed: " + str(e.args[0])
        return "Successful: all tasks done"


def consume_xpressbees_scan_util(payload):
    with psycopg2.connect(host=os.environ.get('DATABASE_HOST'), database=os.environ.get('DATABASE_NAME'),
                                user=os.environ.get('DATABASE_USER'), password=os.environ.get('DATABASE_PASSWORD')) as conn:
        try:
            cur = conn.cursor()
            awb = payload.get('AWBNO')
            if not awb:
                return "Skipped: no awb"

            cur.execute(get_order_details_query.replace('__FILTER_ORDER__', "bb.awb='%s'"%str(awb)))
            try:
                status_time = payload['StatusDate'] + " " + payload['StatusTime']
                status_time = datetime.strptime(status_time, '%d-%m-%Y %H%M')
            except Exception:
                status_time = datetime.utcnow() + timedelta(hours=5.5)

            order = None
            try:
                order = cur.fetchone()
            except Exception:
                pass

            if not order or not order[40]:
                return "Failed: order not found"

            try:
                cur.execute("select * from master_couriers where id=__COURIER_ID__ and courier_name ilike 'Xpressbees%'".replace('__COURIER_ID__', str(order[40])))
                temp = cur.fetchone()[0]
            except Exception:
                return "Failed: order not found"

            status_code = str(payload.get('StatusCode'))
            status = xpressbees_status_mapping[status_code][0]
            status_type = xpressbees_status_mapping[status_code][1]
            status_text = payload['Remarks']
            location = payload['CurrentLocation']
            location_city = payload['CurrentLocation']

            cur.execute(insert_scan_query, (
                order[0], order[40], order[10], status_code, status, status_text, location, location_city, status_time))

            if not status or status in ('READY TO SHIP', 'NOT PICKED', 'PICKUP REQUESTED', 'MANIFESTED'):
                return "Successful: scan saved only"

            tracking_status = None
            if status_code == "PUD":
                tracking_status = "Picked"
            elif status_code in ("IT", "RAD"):
                tracking_status = "In Transit"
            elif status_code == 'OFD':
                tracking_status = "Out for delivery"
            elif status_code == 'DLVD':
                tracking_status = "Delivered"
            elif status_code == 'RTO':
                tracking_status = "Returned"
            elif status_code == 'RTD':
                tracking_status = "RTO"

            if tracking_status:
                cur.execute(insert_status_query, (
                    order[0], order[40], order[10], status_type, tracking_status, status_text, location, location_city, status_time))

            customer_phone = order[4].replace(" ", "")
            customer_phone = "0" + customer_phone[-10:]
            tracking_link = "https://webapp.wareiq.com/tracking/" + order[1]

            if tracking_status == "Picked":
                mark_picked_channel(order, cur)
                tracking_link = UrlShortner.get_short_url(tracking_link, cur)
                send_shipped_event(customer_phone, order[19], order, "", "Xpressbees", tracking_link)
                mark_order_picked_pickups(order, cur)
                webhook_updates(order, cur, status, "Shipment Picked Up", location, status_time.strftime('%Y-%m-%d %H:%M:%S'), ndr_id=None)

            elif tracking_status == "Delivered":
                mark_delivered_channel(order)
                tracking_link = UrlShortner.get_short_url(tracking_link, cur)
                send_delivered_event(customer_phone, order, "Xpressbees", tracking_link)
                webhook_updates(order, cur, status, "Shipment Delivered", location, status_time.strftime('%Y-%m-%d %H:%M:%S'), ndr_id=None)

            elif tracking_status == "RTO":
                mark_rto_channel(order)
                webhook_updates(order, cur, status, "Shipment RTO", location, status_time.strftime('%Y-%m-%d %H:%M:%S'), ndr_id=None)

            elif tracking_status == "DTO":
                tracking_link = UrlShortner.get_short_url(tracking_link, cur)
                send_delivered_rvp_event(customer_phone, order, "Xpressbees", tracking_link)

            elif tracking_status == "Picked RVP":
                tracking_link = UrlShortner.get_short_url(tracking_link, cur)
                send_picked_rvp_event(customer_phone, order, "Xpressbees", tracking_link)

            if status_code == 'UD':
                if status_text in Xpressbees_ndr_reasons:
                    ndr_reason = Xpressbees_ndr_reasons[status_text]
                elif "future delivery" in status_text.lower():
                    ndr_reason = 4
                elif "open delivery" in status_text.lower():
                    ndr_reason = 10
                elif "address incomplete" in status_text.lower():
                    ndr_reason = 2
                elif "amount not ready" in status_text.lower():
                    ndr_reason = 15
                elif "customer not available" in status_text.lower():
                    ndr_reason = 1
                elif "entry not permitted" in status_text.lower():
                    ndr_reason = 7
                elif "customer refused to accept" in status_text.lower():
                    ndr_reason = 3
                else:
                    ndr_reason = 14
                verification_text(order, cur, ndr_reason=ndr_reason)
                webhook_updates(order, cur, status, "", location, status_time.strftime('%Y-%m-%d %H:%M:%S'), ndr_id=ndr_reason)

            cur.execute("UPDATE orders SET status=%s, status_type=%s WHERE id=%s;", (status, status_type, order[0]))

            conn.commit()
        except Exception as e:
            conn.rollback()
            return "Failed: " + str(e.args[0])
        return "Successful: all tasks done"


def mark_order_delivered_channels(data):
    cur = conn.cursor()
    order_ids = data.get("order_ids")
    if len(order_ids) == 1:
        order_tuple = "(" + str(order_ids[0]) + ")"
    else:
        order_tuple = str(tuple(order_ids))
    cur.execute(get_order_details_query.replace('__FILTER_ORDER__', "aa.id in %s" % order_tuple))
    all_orders = cur.fetchall()
    for order in all_orders:
        mark_delivered_channel(order)

    return ""


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

                            cur.execute("""select id from master_products where sku='%s' and client_prefix='%s';""" % (str(prod_obj['sku']), client_prefix))
                            try:
                                master_obj_x = cur.fetchone()[0]
                            except Exception:
                                cur.execute("""INSERT INTO master_products (name, sku, active, client_prefix, date_created, 
                                                dimensions, price, weight, subcategory_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;""",
                                            (prod_name, prod_obj['sku'] if prod_obj['sku'] else str(prod_obj['id']), True, client_prefix, datetime.now(), None,
                                                float(prod_obj['price']), None, None))
                                master_obj_x = cur.fetchone()[0]

                            if prod_obj_x:
                                cur.execute("""UPDATE products SET master_sku=%s, price=%s, name=%s WHERE id=%s""", (prod_obj['sku'], float(prod_obj['price']), prod_name, prod_obj_x[0]))
                            else:
                                cur.execute("""INSERT INTO products (name, sku, channel_id, date_created, price, master_sku, client_prefix, master_product_id) VALUES 
                                                (%s,%s,%s,%s,%s,%s,%s,%s);""", (prod_name, str(prod_obj['id']), channel[4], datetime.now(),
                                                                             float(prod_obj['price']), prod_obj['sku'] if prod_obj['sku'] else str(prod_obj['id']), client_prefix, master_obj_x))

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
                                cur.execute("""select id from master_products where sku='%s' and client_prefix='%s';""" % (str(prod_obj['sku']), client_prefix))
                                try:
                                    master_obj_x = cur.fetchone()[0]
                                except Exception:
                                    cur.execute("""INSERT INTO master_products (name, sku, active, client_prefix, date_created, 
                                                    dimensions, price, weight, subcategory_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;""",
                                                (prod_name, prod_obj['sku'] if prod_obj['sku'] else str(prod_obj['id']), True, client_prefix, datetime.now(), None,
                                                 float(prod_obj['price']), None, None))
                                    master_obj_x = cur.fetchone()[0]
                                if prod_obj_x:
                                    cur.execute("""UPDATE products SET master_sku=%s, price=%s, name=%s WHERE id=%s""", (prod_obj['sku'], float(prod_obj['price']), prod_name, prod_obj_x[0]))
                                else:
                                    cur.execute("""INSERT INTO products (name, sku, channel_id, date_created, price, master_sku, client_prefix, master_product_id) VALUES 
                                                    (%s,%s,%s,%s,%s,%s,%s,%s);""", (prod_name, str(prod_obj['id']), channel[4], datetime.now(),
                                                                                 float(prod_obj['price']), prod_obj['sku'] if prod_obj['sku'] else str(prod_obj['id']), client_prefix, master_obj_x))

                            conn.commit()
                        else:
                            cur.execute("""select id from products where sku='%s' and client_prefix='%s';""" % (
                            str(prod['id']), client_prefix))
                            prod_obj_x = cur.fetchone()
                            master_obj_x = None
                            if prod['sku']:
                                cur.execute("""select id from master_products where sku='%s' and client_prefix='%s';""" % (str(prod['sku']), client_prefix))
                                try:
                                    master_obj_x = cur.fetchone()[0]
                                except Exception:
                                    cur.execute("""INSERT INTO master_products (name, sku, active, client_prefix, date_created, 
                                                    dimensions, price, weight, subcategory_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;""",
                                                (prod_name, prod['sku'], True, client_prefix, datetime.now(), None,
                                                 float(prod['price']), None, None))
                                    master_obj_x = cur.fetchone()[0]
                            if prod_obj_x:
                                cur.execute("""UPDATE products SET master_sku=%s, price=%s, name=%s WHERE id=%s""",
                                            (prod['sku'], float(prod['price']), prod_name, prod_obj_x[0]))
                            else:
                                cur.execute("""INSERT INTO products (name, sku, channel_id, date_created, price, master_sku, client_prefix, master_product_id) VALUES 
                                                                                (%s,%s,%s,%s,%s,%s,%s,%s);""",
                                            (prod_name, str(prod['id']), channel[4], datetime.now(),
                                             float(prod['price']) if prod['price'] else None, prod['sku'], client_prefix, master_obj_x))
                            conn.commit()

                    count = len(all_prods)
                    page += 1
                    conn.commit()

            elif channel[3] == "EasyEcom":
                create_fulfillment_url = "%s/Products/getProductData?api_token=%s" % (channel[0], channel[1])
                qs = requests.get(create_fulfillment_url)
                for key, prod in qs.json()['data'].items():
                    cur.execute("""select id from products where sku='%s' and client_prefix='%s';"""%(str(prod['productId']), client_prefix))
                    prod_obj_x = cur.fetchone()
                    prod_name = prod['name']
                    dimensions = None
                    weight = None
                    if prod['length'] and prod['width'] and prod['height']:
                        dimensions = {"length": float(prod['length']), "breadth": float(prod['width']),
                                      "height": float(prod['height'])}
                    if prod['weight']:
                        weight = float(prod['weight']) / 1000
                    cur.execute("""select id from master_products where sku='%s' and client_prefix='%s';""" % (str(prod['sku']), client_prefix))
                    try:
                        master_obj_x = cur.fetchone()[0]
                    except Exception:
                        cur.execute("""INSERT INTO master_products (name, sku, active, client_prefix, date_created, 
                                        dimensions, price, weight, subcategory_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;""",
                                    (prod_name, prod['sku'] if prod['sku'] else str(prod['productId']), True, client_prefix, datetime.now(), json.dumps(dimensions),
                                     float(prod['mrp']), weight, None))
                        master_obj_x = cur.fetchone()[0]

                    if prod_obj_x:
                        cur.execute("""UPDATE products SET master_sku=%s, price=%s, name=%s, weight=%s, dimensions=%s WHERE id=%s""",
                                    (prod['sku'], float(prod['mrp']), prod_name, weight, json.dumps(dimensions), prod_obj_x[0]))
                    else:
                        cur.execute("""INSERT INTO products (name, sku, channel_id, date_created, price, master_sku, weight, dimensions, client_prefix, master_product_id) VALUES 
                                        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);""", (prod_name, str(prod['productId']), channel[4], datetime.now(),
                                                                     float(prod['mrp']), prod['sku'] if prod['sku'] else str(prod['productId']), weight, json.dumps(dimensions), client_prefix, master_obj_x))

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
            del_from = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=7)
            del_to = datetime.utcnow() + timedelta(hours=5.5)
            insert_tuple.append(
                (client[0], last_remittance_id, remittance_date - timedelta(days=7), 'processing',
                 datetime.utcnow() + timedelta(hours=5.5), del_from, del_to))
            insert_value_str += "%s,"
        del_from = datetime.utcnow()+timedelta(hours=5.5)
        del_to = datetime.utcnow()+timedelta(hours=5.5)+timedelta(days=7)
        insert_tuple.append(
            (client[0], remittance_id, remittance_date, 'processing', datetime.utcnow() + timedelta(hours=5.5), del_from, del_to))
        insert_value_str += "%s,"

    insert_value_str = insert_value_str.rstrip(",")

    cur.execute(
        "INSERT INTO cod_remittance (client_prefix, remittance_id, remittance_date, status, date_created, del_from, del_to) VALUES __IVS__;".replace(
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

            payment_mode = "NEFT"
            if remit[6]<200000:
                payment_mode = "IMPS"
            amount = int(remit[6] * 100)
            razorpay_body = {
                "account_number": "409001472401",
                "amount": amount,
                "currency": "INR",
                "mode": payment_mode,
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
                "narration": "COD remittance",
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
                cur.execute("SELECT __ZONE__, cod_min, cod_ratio, rto_ratio, __ZONE_STEP__, rvp_ratio from cost_to_clients WHERE client_prefix=%s and courier_id=%s;".replace(
                        '__ZONE__', zone_column_mapping[delivery_zone]).replace('__ZONE_STEP__',
                                                                                zone_step_charge_column_mapping[
                                                                                    delivery_zone]), cost_select_tuple)
                charge_rate_values = cur.fetchone()
                if not charge_rate_values:
                    cur.execute("SELECT __ZONE__, cod_min, cod_ratio, rto_ratio, __ZONE_STEP__, rvp_ratio from client_default_cost WHERE courier_id=%s;".replace(
                            '__ZONE__', zone_column_mapping[delivery_zone]).replace('__ZONE_STEP__',
                                                                                    zone_step_charge_column_mapping[
                                                                                        delivery_zone]), (order[2],))
                    charge_rate_values = cur.fetchone()
                if not charge_rate_values:
                    cur.execute("""INSERT INTO client_deductions (weight_charged, zone, shipment_id) VALUES (%s,%s,%s) RETURNING id;""",
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
                try:
                    closing_balance = cur.fetchone()[0]
                    time_now = datetime.utcnow()+timedelta(hours=5.5)
                    cur.execute("""INSERT INTO wallet_passbook (client_prefix, credit, debit, closing_balance, ref_no, 
                                descr, category, txn_time, date_created) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                                (order[6], 0, total_charge_gst+5.9, closing_balance, "shpId:"+str(order[0]),
                                "Deduction for awb: "+str(order[1]), "Shipment charge", time_now, time_now))
                except Exception as e:
                    logger.error("couldn't insert into passbook, order: " + str(order[0]) + "\nError: " + str(e))
                    pass
                cur.execute(insert_into_deduction_query, insert_rates_tuple)
            except Exception as e:
                logger.error("couldn't calculate order: " + str(order[0]) + "\nError: " + str(e))
                cur.execute("""INSERT INTO client_deductions (weight_charged, zone, shipment_id) VALUES (%s,%s,%s) RETURNING id;""",
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
        cur.execute("SELECT __ZONE__, __ZONE___add, cod_min, cod_ratio, rto_ratio, first_step, next_step from courier_costs WHERE courier_id=%s;".replace(
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

        insert_rates_tuple = (charged_weight, delivery_zone, deduction_time, cod_charge, forward_charge, rto_charge, order[0],
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
    with psycopg2.connect(host=os.environ.get('DATABASE_HOST'), database=os.environ.get('DATABASE_NAME'),
                          user=os.environ.get('DATABASE_USER'), password=os.environ.get('DATABASE_PASSWORD')) as conn:
        try:
            cur = conn.cursor()
            if auth_data['user_group'] not in ('client', 'super-admin', 'multi-vendor'):
                return {"success":False, "msg": "invalid user"}, 400

            if auth_data['user_group'] != 'super-admin':
                cur.execute("SELECT account_type, current_balance FROM client_mapping WHERE client_prefix='%s'"%auth_data['client_prefix'])
                try:
                    bal_data = cur.fetchone()
                    if bal_data[0].lower()=='prepaid' and bal_data[1]<500:
                        return {"success": False, "msg": "balance low, please recharge"}, 400

                except Exception:
                    return {"success":False, "msg": "Something went wrong"}, 400

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
            ship_orders(courier_name=courier, order_ids=order_ids, force_ship=True, cur=cur)
            conn.commit()

            return {"success": True, "msg": "shipped successfully"}, 200

        except Exception as e:
            conn.rollback()
            return {"success": False, "msg": str(e.args[0])}, 400


def update_available_quantity():
    try:
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

            if prod_status[1] in ('DELIVERED', 'DISPATCHED', 'IN TRANSIT', 'PENDING', 'DAMAGED', 'LOST', 'SHORTAGE', 'SHIPPED'):
                quantity_dict[prod_status[0]][prod_status[2]]['current_quantity'] -= prod_status[3]
                quantity_dict[prod_status[0]][prod_status[2]]['available_quantity'] -= prod_status[3]
            elif prod_status[1] in ('NEW', 'PICKUP REQUESTED', 'READY TO SHIP'):
                quantity_dict[prod_status[0]][prod_status[2]]['inline_quantity'] += prod_status[3]
                quantity_dict[prod_status[0]][prod_status[2]]['available_quantity'] -= prod_status[3]
            elif prod_status[1] in ('RTO', 'DTO'):
                quantity_dict[prod_status[0]][prod_status[2]]['rto_quantity'] += prod_status[3]
                if prod_status[1] == "DTO":
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
                        quantity_dict[new_prod_id] = {
                            warehouse: {'available_quantity': quan_values['available_quantity'] * mul_fac,
                                        'current_quantity': quan_values['current_quantity'] * mul_fac,
                                        'inline_quantity': quan_values['inline_quantity'] * mul_fac,
                                        'rto_quantity': quan_values['rto_quantity'] * mul_fac}}
                    elif warehouse not in quantity_dict[new_prod_id]:
                        quantity_dict[new_prod_id][warehouse] = {
                            'available_quantity': quan_values['available_quantity'] * mul_fac,
                            'current_quantity': quan_values['current_quantity'] * mul_fac,
                            'inline_quantity': quan_values['inline_quantity'] * mul_fac,
                            'rto_quantity': quan_values['rto_quantity'] * mul_fac}

                    else:
                        quantity_dict[new_prod_id][warehouse]['available_quantity'] += quan_values[
                                                                                           'available_quantity'] * mul_fac
                        quantity_dict[new_prod_id][warehouse]['current_quantity'] += quan_values[
                                                                                         'current_quantity'] * mul_fac
                        quantity_dict[new_prod_id][warehouse]['inline_quantity'] += quan_values['inline_quantity'] * mul_fac
                        quantity_dict[new_prod_id][warehouse]['rto_quantity'] += quan_values['rto_quantity'] * mul_fac

        cur.execute("""update products_quantity set available_quantity=approved_quantity, current_quantity=approved_quantity, 
                        inline_quantity=0, rto_quantity=0;""")
        conn.commit()

        for prod_id, wh_dict in quantity_dict.items():
            for warehouse, quan_values in wh_dict.items():
                update_tuple = (
                quan_values['available_quantity'], quan_values['current_quantity'], quan_values['inline_quantity'],
                quan_values['rto_quantity'], prod_id, warehouse)
                cur.execute(update_inventory_quantity_query, update_tuple)

        conn.commit()
    except Exception as e:
        logger.error("Couldn't calculate inventory, Error: " + str(e.args))
        conn.rollback()


def update_available_quantity_from_easyecom():
    cur = conn.cursor()
    try:
        cur.execute("select client_prefix, api_key from client_channel where channel_id=7;")
        all_clients = cur.fetchall()

        for client in all_clients:
            try:
                cur.execute("select array_agg(sku) from master_products where client_prefix='%s';"%client[0])
                all_skus = cur.fetchone()[0]
                chunks = [all_skus[x:x + 20] for x in range(0, len(all_skus), 20)]
                for chunk in chunks:
                    try:
                        req_url = "https://api.easyecom.io/wms/V2/getInventoryDetails?api_token=%s&includeLocations=1&sku=%s"%(client[1], ",".join(chunk))
                        while req_url:
                            req = requests.get(req_url)
                            req_data = req.json()

                            inventory_dict = dict()

                            for req in req_data['data']['inventoryData']:
                                if req['companyName'] not in inventory_dict:
                                    inventory_dict[req['companyName']] = [(req['sku'], int(req['availableInventory']) if req['availableInventory'] else 0,
                                                                           int(req['reservedInventory']) if req['reservedInventory'] else 0)]
                                else:
                                    inventory_dict[req['companyName']].append((req['sku'], int(req['availableInventory']) if req['availableInventory'] else 0,
                                                                           int(req['reservedInventory']) if req['reservedInventory'] else 0))

                            for ee_loc, val_list in inventory_dict.items():
                                cur.execute("""select bb.warehouse_prefix from client_pickups aa
                                                                        left join pickup_points bb on aa.pickup_id=bb.id
                                                                        where aa.easyecom_loc_code='%s'""" % ee_loc)
                                try:
                                    warehouse_prefix = cur.fetchone()[0]
                                except Exception:
                                    continue

                                for val_tuple in val_list:
                                    cur.execute("""select * from products_quantity aa
                                    left join master_products bb on aa.product_id=bb.id
                                    where aa.warehouse_prefix='%s' and bb.sku='%s'"""%(warehouse_prefix, val_tuple[0]))

                                    if cur.fetchall():
                                        cur.execute(update_easyecom_inventory_query, (val_tuple[1], val_tuple[2], val_tuple[1]+val_tuple[2], warehouse_prefix, val_tuple[0], client[0]))
                                    else:
                                        cur.execute(insert_easyecom_inventory_query, (val_tuple[1], warehouse_prefix, val_tuple[1]+val_tuple[2], val_tuple[2], val_tuple[1], client[0], val_tuple[0]))

                                conn.commit()

                            req_url = "https://api.easyecom.io"+req_data['data']['nextUrl'] if req_data['data']['nextUrl'] else None
                    except Exception as e:
                        conn.rollback()
                        logger.error("Couldn't update inventory for: " + str(client[0]) + "\nError: " + str(e.args)+"\nSKUs: "+str(chunk))
            except Exception as e:
                conn.rollback()
                logger.error("Couldn't update inventory for: "+str(client[0])+"\nError: "+str(e.args))

    except Exception as e:
        conn.rollback()
        logger.error("Couldn't fetch inventory from Easyecom, Error: " + str(e.args))

    try:  # kama store inventory update
        cur.execute("""select bb.warehouse_prefix, aa.client_prefix from client_pickups aa
                            left join pickup_points bb on aa.pickup_id=bb.id
                            where aa.client_prefix='KAMAAYURVEDA'
                            and aa.enable_sdd=true
                            and bb.warehouse_prefix='TNPMRO'""")

        pickup_points = cur.fetchall()
        token_headers = {"Username": "WareIQ",
                         "Password": "Wondersoft#12",
                         "SERVICE_METHODNAME": "GetToken"}
        token_url = "http://103.25.172.69:7006/eShopaidAPI/eShopaidService.svc/Token"
        token_req = requests.post(token_url, headers=token_headers, json={})
        auth_token = token_req.json()['Response']['Access_Token'].strip()
        for pickup_point in pickup_points:
            gi_headers = {"SERVICE_METHODNAME": "GetInventory",
                          "AUTHORIZATION": auth_token}
            gi_url = "http://103.25.172.69:7006/eShopaidAPI/eShopaidService.svc/ProcessData"
            gi_body = {"Params": {"Location": pickup_point[0], "DateFilter": "", "ProductCode": ""}}
            inventory_req = requests.post(gi_url, headers=gi_headers, json=gi_body)
            prod_quan_list = inventory_req.json()['Response']['Data']['Inventory']['Items']['Item']
            item_code_str = ""
            for itm in prod_quan_list:
                item_code_str += "('%s', %s)," % (itm['ItemCode'], str(int(float(itm['Stock']))))

            item_code_str = item_code_str.rstrip(",")

            update_quan_query = """update products_quantity aa set 
                                     current_quantity=cc.cnt, available_quantity=cc.cnt-inline_quantity, 
                                     total_quantity=total_quantity-current_quantity+cc.cnt
                                      from (select xx.id, yy.cnt from (values
                                                 %s
                                                ) yy(sku, cnt)
                                      left join master_products xx
                                      on xx.sku=yy.sku
                                      where client_prefix='%s') cc(product_id, cnt)
                                      where aa.product_id=cc.product_id
                                      and aa.warehouse_prefix='%s'""" % (
            item_code_str, pickup_point[1], pickup_point[0])
            cur.execute(update_quan_query)
            conn.commit()
    except Exception:
        conn.rollback()
        pass


def update_available_quantity_on_channel():
    cur = conn.cursor()
    cur.execute("""SELECT client_prefix, channel_id, api_key, api_password, shop_url, unique_parameter FROM client_channel WHERE sync_inventory=true and connection_status=true and status=true;""")
    all_channels = cur.fetchall()

    for channel in all_channels:
        try:
            logger.info("Syncing %s inventory" % str(channel[0]))
            if channel[1]==6: #mangento sync
                cur.execute("""select sku, sum(available_quantity) as available_quantity from products_quantity aa
                                    left join master_products bb on aa.product_id=bb.id
                                    where bb.client_prefix='__CLIENT_PREFIX__'
                                    group by sku
                                    order by available_quantity""".replace('__CLIENT_PREFIX__', channel[0]))

                all_quan = cur.fetchall()
                source_items = list()
                headers = {'Authorization': "Bearer " + channel[2],
                           'Content-Type': 'application/json',
                           'User-Agent': 'WareIQ server'}
                for quan in all_quan:
                    update_quan = quan[1]
                    try:
                        if update_quan>0:
                            reserved_quan = requests.get("%s/V1/reserved-products/get/sku/%s"%(channel[4], quan[0]), headers=headers).json()
                            reserved_quan = reserved_quan['quantity']
                            update_quan -= reserved_quan if reserved_quan<0 else 0
                        else:
                            update_quan=0
                        source_items.append({
                            "sku": quan[0],
                            "source_code": "default",
                            "quantity": max(update_quan, 0),
                            "status": 1
                        })
                    except Exception as e:
                        pass

                magento_url = channel[4]+ "/V1/inventory/source-items"
                body = {
                    "sourceItems": source_items}
                r = requests.post(magento_url, headers=headers, data=json.dumps(body))

            elif channel[1]==1: #shopify sync
                cur.execute("""select cc.sku, sum(available_quantity) as available_quantity from products_quantity aa
                                    left join master_products bb on aa.product_id=bb.id
                                    left join products cc on cc.master_product_id=bb.id
                                    where bb.client_prefix='__CLIENT_PREFIX__'
                                    and cc.sku is not null
                                    group by cc.sku
                                    order by available_quantity""".replace('__CLIENT_PREFIX__', channel[0]))

                all_quan = cur.fetchall()
                if not channel[5]:
                    location_url = "https://%s:%s@%s/admin/api/2021-04/locations.json"% (channel[2], channel[3], channel[4])
                    loc_req = requests.get(location_url).json()
                    location_id = loc_req['locations'][0]['id']
                else:
                    location_id = int(channel[5])

                headers = {'Content-Type': 'application/json'}
                url = "https://%s:%s@%s/admin/api/2021-04/inventory_levels/set.json" % (channel[2], channel[3], channel[4])
                for quan in all_quan:
                    try:
                        prod_url = "https://%s:%s@%s/admin/api/2021-04/variants/%s.json"% (channel[2], channel[3], channel[4], quan[0])
                        req = requests.get(prod_url).json()
                        fulfil_data = {
                                          "location_id": location_id,
                                          "inventory_item_id": req['variant']['inventory_item_id'],
                                          "available": quan[1] if quan[1] and quan[1]>0 else 0,
                                          "disconnect_if_necessary": True
                                        }
                        req_ful = requests.post(url, data=json.dumps(fulfil_data), headers=headers)
                        sleep(1)
                        if req_ful.status_code == 403:
                            break
                    except Exception as e:
                        logger.info("Failed syncing %s inventory, SKU: %s \nError: %s" % (str(channel[0]), str(quan[0]), str(e.args[0])))
                        continue

        except Exception as e:
            logger.error("Couldn't sync inventory to channel: "+str(channel[0]) + "\nError: "+str(e.args))


def ndr_push_reattempts_util():
    cur = conn.cursor()
    time_after = (datetime.utcnow() - timedelta(days=2, hours=5.5)).strftime('%Y-%m-%d')
    cur.execute("""select bb.awb, cc.courier_name, cc.api_url, cc.api_key, cc.api_password, aa.defer_dd, aa.updated_add, aa.updated_phone, ee.pincode from ndr_shipments aa
                    left join shipments bb on aa.shipment_id=bb.id
                    left join master_couriers cc on bb.courier_id=cc.id
                    left join orders dd on dd.id=aa.order_id
                    left join shipping_address ee on ee.id=dd.delivery_address_id
                    where aa.date_created>%s
                    and dd.status='PENDING'
                    and aa.current_status='reattempt'""", (time_after,))

    all_orders = cur.fetchall()
    for order in all_orders:
        try:
            if order[1].startswith('Delhivery'):  # Delhivery
                delhivery_data = list()
                if order[5]:
                    delhivery_data.append({
                                            "waybill": order[0],
                                            "act": "DEFER_DLV",
                                            "action_data": {
                                                "deferred_date": order[5].strftime('%Y-%m-%d')
                                            }
                                        })
                if order[6] or order[7]:
                    app_obj = { "waybill": order[0],
                                "act": "EDIT_DETAILS",
                                "action_data": {}}
                    if order[6]:
                        app_obj['action_data']['add']=order[6]
                    if order[7]:
                        app_obj['action_data']['phone']=order[7]

                    delhivery_data.append(app_obj)

                delhivery_data.append({ "waybill": order[0],
                                        "act": "RE-ATTEMPT"})

                delhivery_url = order[2] + "api/p/update"
                headers = {"Authorization": "Token " + order[3],
                           "Content-Type": "application/json"}
                delivery_shipments_body = json.dumps({"data": delhivery_data})

                req = requests.post(delhivery_url, headers=headers, data=delivery_shipments_body)

            if order[1].startswith('Xpressbees'):  # Xpressbees
                headers = {"Content-Type": "application/json",
                           "XBKey": order[3]}
                body = {"ShippingID": order[0]}
                if order[5]:
                    body['DeferredDeliveryDate'] = order[5].strftime('%Y-%m-%d %X')
                else:
                    body['DeferredDeliveryDate'] = (datetime.utcnow()+timedelta(days=2)).strftime('%Y-%m-%d %X')

                if order[6]:
                    body['AlternateCustomerAddress'] = order[6]
                if order[7]:
                    body['AlternateCustomerMobileNumber'] = order[7]
                if order[6] or order[7]:
                    body['CustomerPincode'] = order[8]

                xpress_url = order[2]+"POSTShipmentService.svc/UpdateNDRDeferredDeliveryDate"
                req = requests.post(xpress_url, headers=headers, data=json.dumps(body))

            if order[1].startswith('Ecom'):  # Ecom
                body = {"awb": order[0],
                        "comments": "re-attempt requested",
                        "scheduled_delivery_slot": "2",
                        "instruction": "RAD"}

                if order[6]:
                    body["comments"] += ", Alternate address: "+ order[6]
                if order[7]:
                    body['comments'] += ", Alternate phone: "+ order[7]
                if order[5]:
                    body['scheduled_delivery_date'] = order[5].strftime('%Y-%m-%d')
                else:
                    body['scheduled_delivery_date'] = (datetime.utcnow()+timedelta(days=1)).strftime('%Y-%m-%d')

                req = requests.post("https://api.ecomexpress.in/apiv2/ndr_resolutions/", data={"username": order[3], "password": order[4],
                                                   "json_input": json.dumps([body])})

            # if order[1].startswith('Bluedart'):  # Bluedart
            #     from zeep import Client
            #     login_id = order[4].split('|')[0]
            #     bluedart_url = "https://netconnect.bluedart.com/Ver1.9/ShippingAPI/ALTInstruction/ALTInstructionUpdate.svc?wsdl"
            #     waybill_client = Client(bluedart_url)
            #     client_profile = {
            #         "LoginID": login_id,
            #         "LicenceKey": order[3],
            #         "Api_type": "S",
            #         "Version": "1.3"
            #     }
            #     request_data = {
            #         "altreq": {
            #             "AWBNo": order[0],
            #             "AltInstRequestType": "DT",
            #             "MobileNo": order[7] if order[7] else "",
            #         },
            #         "profile": client_profile
            #     }
            #     req = waybill_client.service.CustALTInstructionUpdate(**request_data)

        except Exception as e:
            logger.error("NDR push failed for: " + order[0])


def create_pickups_entry_util():
    try:
        cur = conn.cursor()
        cur.execute(mark_30_days_old_orders_not_shipped)
        conn.commit()
        cur.execute(get_pickup_requests_query)
        orders_qs = cur.fetchall()

        pur_dict = dict()

        for order in orders_qs:
            if order[0] not in pur_dict:
                pur_dict[order[0]] = {order[1]: [order]}
            elif order[1] not in pur_dict[order[0]]:
                pur_dict[order[0]][order[1]] = [order]
            else:
                pur_dict[order[0]][order[1]].append(order)

        pickup_time_ist = datetime.utcnow() + timedelta(hours=5.5)
        if pickup_time_ist.hour > 15:
            pickup_time_ist = pickup_time_ist + timedelta(days=1)
        pickup_time_str = pickup_time_ist.strftime("%Y-%m-%d")
        for pickup_data_id, courier_dict in pur_dict.items():
            for courier_id, order_list in courier_dict.items():
                cur.execute("SELECT id FROM manifests WHERE client_pickup_id=%s and courier_id=%s and pickup_date>='%s'"%(str(pickup_data_id), str(courier_id), pickup_time_str))
                try:
                    manifest_id = cur.fetchone()[0]
                except Exception:
                    manifest_id_str = pickup_time_ist.strftime('%Y_%m_%d_') + ''.join(random.choices(string.ascii_uppercase, k=8))

                    cur.execute(insert_manifest_query, (manifest_id_str, order_list[0][2], courier_id, pickup_data_id,
                                                        order_list[0][4], pickup_time_ist.replace(hour=13, minute=0, second=0), "", len(order_list)))

                    manifest_id = cur.fetchone()[0]

                pickups_insert_list = list()
                pickups_insert_str = ""
                for order in order_list:
                    pickups_insert_str += "%s,"
                    pickups_insert_list.append((manifest_id, order[3], False))

                cur.execute(insert_order_pickups_query.replace('__INSERT_STR__', pickups_insert_str.strip(",")), tuple(pickups_insert_list))

            conn.commit()
        conn.commit()
    except Exception:
        conn.rollback()


def update_pincode_serviceability_table():
    courier_list = (15, 2, 5, 9, 27, 12)
    with conn.cursor() as cur:
        for courier in courier_list:
            try:
                cur.execute("SELECT id, courier_name, api_key, api_password, api_url FROM master_couriers WHERE id=%s", (courier, ))
                courer_data = cur.fetchone()
                if courier==15:
                    url = "https://api.ecomexpress.in/apiv2/pincodes/"
                    req = requests.post(url, data={"username": courer_data[2], "password": courer_data[3]})
                    pincode_list = req.json()
                    for pincode in pincode_list:
                        serviceable = pincode.get('active')
                        pincode_str = str(pincode.get('pincode'))
                        sortcode = pincode.get('route')
                        cur.execute(update_pincode_serviceability_query, (pincode_str, courier, serviceable,
                                                                          serviceable, serviceable, serviceable, sortcode,
                                                                          datetime.utcnow()+timedelta(hours=5.5)))
                        conn.commit()
                elif courier in (2, 12):
                    url = "https://track.delhivery.com/c/api/pin-codes/json/"
                    headers = {"Content-Type": "application/json",
                               "Authorization": "Token %s"%(courer_data[2])}
                    req = requests.get(url, headers=headers)
                    pincode_list = req.json()
                    for pincode in pincode_list['delivery_codes']:
                        serviceable = True if pincode['postal_code'].get('pre_paid').upper()=='Y' else False
                        cod_available = True if pincode['postal_code'].get('cod').upper()=='Y' else False
                        pickup = True if pincode['postal_code'].get('pickup').upper()=='Y' else False
                        pincode_str = str(pincode['postal_code'].get('pin'))
                        sortcode = str(pincode['postal_code'].get('sort_code'))
                        cur.execute(update_pincode_serviceability_query, (pincode_str, courier, serviceable,
                                                                          cod_available, pickup, pickup, sortcode,
                                                                          datetime.utcnow()+timedelta(hours=5.5)))

                        conn.commit()

                elif courier==9:
                    from zeep import Client
                    check_url = "https://netconnect.bluedart.com/Ver1.9/ShippingAPI/Finder/ServiceFinderQuery.svc?wsdl"
                    pincode_client = Client(check_url)
                    login_id = courer_data[3].split('|')[0]
                    client_profile = {
                        "LoginID": login_id,
                        "LicenceKey": courer_data[2],
                        "Api_type": "S",
                        "Version": "1.3"
                    }
                    cur.execute("SELECT pincode FROM pincode_serviceability WHERE courier_id=15;")
                    all_pincodes = cur.fetchall()
                    for pincode in all_pincodes:
                        request_data = {
                            'pinCode': str(pincode[0]),
                            "profile": client_profile
                        }
                        req = pincode_client.service.GetServicesforPincode(**request_data)
                        serviceable = True if req['eTailPrePaidAirInbound']=='Yes' else False
                        cod_available = True if req['eTailCODAirInbound']=='Yes' else False
                        pickup = True if req['eTailPrePaidAirOutound']=='Yes' else False
                        pincode_str = str(req['PinCode'])
                        sortcode = None
                        cur.execute(update_pincode_serviceability_query, (pincode_str, courier, serviceable,
                                                                          cod_available, pickup, pickup, sortcode,
                                                                          datetime.utcnow()+timedelta(hours=5.5)))

                        conn.commit()
            except Exception as e:
                logger.error("Couldn't update serviceability for "+str(courier)+"\nError: "+str(e.args[0]))

        try:
            import csv
            cur.execute(create_pincode_serv_file_query)
            all_pincodes = cur.fetchall()
            filename = "wareiq_pincode_serviceability.csv"
            with open(filename, 'w') as mycsvfile:
                cw = csv.writer(mycsvfile)
                cw.writerow(["Pincode", "City", "State", "Prepaid Delivery", "COD Delivery", "Pickup"])
                for pincode in all_pincodes:
                    new_row = list()
                    new_row.append(str(pincode[0]))
                    new_row.append(str(pincode[1]))
                    new_row.append(str(pincode[2]))
                    new_row.append('Y' if pincode[3] else 'N')
                    new_row.append('Y' if pincode[4] else 'N')
                    new_row.append('Y' if pincode[5] else 'N')
                    cw.writerow(new_row)

            s3 = session.resource('s3')
            bucket = s3.Bucket("wareiqfiles")
            bucket.upload_file(filename, "downloads/" + filename, ExtraArgs={'ACL': 'public-read'})
            os.remove(filename)

        except Exception as e:
            logger.error("Couldn't create csv for serviceability"+ "\nError: " + str(e.args[0]))
