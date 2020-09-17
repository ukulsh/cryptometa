from .contants import *
from .queries import *
from .utils import *
from datetime import datetime
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
        else:
            cur.execute(insert_scan_query, (
                order[0], order[38], order[10], status_code, status, status_text, location, location_city, status_time))
            conn.commit()
            return "Successful: scan saved only"

        cur.execute(insert_scan_query, (
            order[0], order[38], order[10], status_code, status, status_text, location, location_city, status_time))

        tracking_status = ecom_express_status_mapping[reason_code_number][2]
        if tracking_status:
            cur.execute(insert_status_query, (
                order[0], order[38], order[10], status_type, tracking_status, status_text, location, location_city, status_time))

        if tracking_status == "Picked":
            if order[26] != False:
                if order[14] == 5:
                    try:
                        woocommerce_fulfillment(order)
                    except Exception as e:
                        logger.error("Couldn't update woocommerce for: " + str(order[0])
                                     + "\nError: " + str(e.args))
                elif order[14] == 1:
                    try:
                        shopify_fulfillment(order, cur)
                    except Exception as e:
                        logger.error("Couldn't update shopify for: " + str(order[0])
                                     + "\nError: " + str(e.args))
                elif order[14] == 6:  # Magento fulfilment
                    try:
                        if order[28] != False:
                            magento_invoice(order)
                        magento_fulfillment(order, cur)
                    except Exception as e:
                        logger.error("Couldn't update Magento for: " + str(order[0])
                                     + "\nError: " + str(e.args))

        if reason_code_number in ecom_express_ndr_reasons:
            ndr_reason = ecom_express_ndr_reasons[reason_code_number]
            insert_ndr_ver_tuple = (order[0], "", datetime.utcnow()+timedelta(hours=5.5))
            ndr_ship_tuple = (
                order[0], order[10], ndr_reason, "required", datetime.utcnow() + timedelta(hours=5.5))
            cur.execute(
                "INSERT INTO ndr_shipments (order_id, shipment_id, reason_id, current_status, date_created) VALUES (%s,%s,%s,%s,%s);",
                ndr_ship_tuple)
            if ndr_reason in (1, 3, 9, 11):
                cur.execute(
                    "INSERT INTO ndr_verification (order_id, verification_link, date_created) VALUES (%s,%s,%s);",
                    insert_ndr_ver_tuple)

        cur.execute("UPDATE orders SET status=%s, status_type=%s WHERE id=%s;", (status, status_type, order[0]))

        conn.commit()
    except Exception as e:
        conn.rollback()
        return "Failed: " + str(e.args[0])
    return "Successful: all tasks done"