import re
import csv
import io
from datetime import datetime, timedelta
from flask import make_response

from project.api.utilities.db_utils import DbConnection
conn = DbConnection.get_db_connection_instance()
ORDERS_DOWNLOAD_HEADERS = ["Order ID", "Customer Name", "Customer Email", "Customer Phone", "Order Date",
                           "Courier", "Weight", "awb", "Expected Delivery Date", "Status", "Address_one", "Address_two",
                           "City", "State", "Country", "Pincode", "Pickup Point", "Product", "SKU", "Quantity", "Order Type",
                           "Amount", "Manifest Time", "Pickup Date", "Delivered Date", "COD Verfication", "COD Verified Via", "NDR Verfication", "NDR Verified Via","PDD"]
cur = conn.cursor()


def filter_query(filters, query_to_run, auth_data):

    if 'status' in filters:
        if len(filters['status']) == 1:
            status_tuple = "('" + filters['status'][0] + "')"
        else:
            status_tuple = str(tuple(filters['status']))
        query_to_run = query_to_run.replace("__STATUS_FILTER__", "AND aa.status in %s" % status_tuple)

    if 'courier' in filters:
        if len(filters['courier']) == 1:
            courier_tuple = "('" + filters['courier'][0] + "')"
        else:
            courier_tuple = str(tuple(filters['courier']))
        query_to_run = query_to_run.replace("__COURIER_FILTER__", "AND courier_name in %s" % courier_tuple)

    if 'client' in filters and auth_data['user_group'] != 'client':
        if len(filters['client']) == 1:
            client_tuple = "('" + filters['client'][0] + "')"
        else:
            client_tuple = str(tuple(filters['client']))
        query_to_run = query_to_run.replace("__CLIENT_FILTER__", "AND aa.client_prefix in %s" % client_tuple)

    if 'pickup_point' in filters:
        if len(filters['pickup_point']) == 1:
            pickup_tuple = "('" + filters['pickup_point'][0] + "')"
        else:
            pickup_tuple = str(tuple(filters['pickup_point']))
        query_to_run = query_to_run.replace("__PICKUP_FILTER__", "AND ii.warehouse_prefix in %s" % pickup_tuple)

    if 'ndr_reason' in filters:
        if len(filters['ndr_reason']) == 1:
            reason_tuple = "('" + filters['ndr_reason'][0] + "')"
        else:
            reason_tuple = str(tuple(filters['ndr_reason']))
        query_to_run = query_to_run.replace("__NDR_REASON_FILTER__", "AND rr.reason in %s" % reason_tuple)

    if 'ndr_type' in filters:
        if 'Action Requested' in filters['ndr_type'] and 'Action Required' in filters['ndr_type']:
            ndr_type_filter = ""
        elif 'Action Requested' in filters['ndr_type']:
            ndr_type_filter = "AND nn.ndr_verified in ('true', 'false')"
        else:
            ndr_type_filter = "AND nn.ndr_verified is null"

        query_to_run = query_to_run.replace("__NDR_TYPE_FILTER__", ndr_type_filter)

    if 'order_type' in filters:
        if len(filters['order_type']) == 1:
            type_tuple = "('" + filters['order_type'][0] + "')"
        else:
            type_tuple = str(tuple(filters['order_type']))
        print("here")
        query_to_run = query_to_run.replace("__TYPE_FILTER__", "AND upper(payment_mode) in %s" % type_tuple)

    if 'order_date' in filters:
        filter_date_start = filters['order_date'][0][0:19].replace('T', ' ')
        filter_date_end = filters['order_date'][1][0:19].replace('T', ' ')
        query_to_run = query_to_run.replace("__ORDER_DATE_FILTER__", "AND order_date between '%s' and '%s'" % (
        filter_date_start, filter_date_end))

    if 'thirdwatch_score' in filters:
        score_from = float(filters['thirdwatch_score'][0])
        score_to = float(filters['thirdwatch_score'][1])
        query_to_run = query_to_run.replace("__THIRDWATCH_SCORE_FILTER__",
                                            "AND uu.score between %s and %s" % (score_from, score_to))

    if 'thirdwatch_flag' in filters:
        if len(filters['thirdwatch_flag']) == 1:
            flag_tuple = "('" + filters['thirdwatch_flag'][0] + "')"
        else:
            flag_tuple = str(tuple(filters['thirdwatch_flag']))
        query_to_run = query_to_run.replace("__TYPE_FILTER__", "AND lower(uu.flag) in %s" % flag_tuple)

    if 'pickup_time' in filters:
        filter_date_start = filters['pickup_time'][0][0:19].replace('T', ' ')
        filter_date_end = filters['pickup_time'][1][0:19].replace('T', ' ')
        query_to_run = query_to_run.replace("__PICKUP_TIME_FILTER__", "AND pickup_time between '%s' and '%s'" % (
        filter_date_start, filter_date_end))

    if 'manifest_time' in filters:
        filter_date_start = filters['manifest_time'][0][0:19].replace('T', ' ')
        filter_date_end = filters['manifest_time'][1][0:19].replace('T', ' ')
        query_to_run = query_to_run.replace("__MANIFEST_DATE_FILTER__", "AND manifest_time between '%s' and '%s'" % (
        filter_date_start, filter_date_end))

    if 'delivered_time' in filters:
        filter_date_start = filters['delivered_time'][0][0:19].replace('T', ' ')
        filter_date_end = filters['delivered_time'][1][0:19].replace('T', ' ')
        query_to_run = query_to_run.replace("__PICKUP_TIME_FILTER__", "AND delivered_time between '%s' and '%s'" % (
        filter_date_start, filter_date_end))

    if 'channel' in filters:
        if len(filters['channel']) == 1:
            channel_tuple = "('" + filters['channel'][0] + "')"
        else:
            channel_tuple = str(tuple(filters['channel']))
        query_to_run = query_to_run.replace("__MASTER_CHANNEL__", "AND vv.channel_name in %s" % channel_tuple)

    if 'edd' in filters:
        filter_date_start = filters['edd'][0][0:19].replace('T', ' ')
        filter_date_end = filters['edd'][1][0:19].replace('T', ' ')
        query_to_run = query_to_run.replace("__EDD_FILTER__",
                                            "AND bb.edd between '%s' and '%s'" % (filter_date_start, filter_date_end))

    return query_to_run


def download_flag_func(query_to_run, get_selected_product_details, auth_data, filters, hide_weights):

    client_prefix = auth_data.get('client_prefix')
    if not [i for i in ['order_date', 'pickup_time', 'manifest_time', 'delivered_time'] if i in filters]:
        date_month_ago = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=31)
        date_month_ago = date_month_ago.strftime("%Y-%m-%d %H:%M:%S")
        query_to_run = query_to_run.replace('__ORDER_DATE_FILTER__', "AND order_date > '%s' " % date_month_ago)
        query_to_run = query_to_run.replace('__PAGINATION__', "")
    query_to_run = re.sub(r"""__.+?__""", "", query_to_run)
    cur.execute(query_to_run)
    orders_qs_data = cur.fetchall()
    order_id_data = ','.join([str(it[1]) for it in orders_qs_data])
    product_detail_by_order_id = {}
    if order_id_data:
        update_product_details_query = get_selected_product_details.replace('__FILTERED_ORDER_ID__',
                                                                            order_id_data)
        cur.execute(update_product_details_query)
        product_detail_data = cur.fetchall()
        for it in product_detail_data:
            product_detail_by_order_id[it[0]] = [it[1], it[2], it[3], it[4], it[5]]
    si = io.StringIO()
    cw = csv.writer(si)
    cw.writerow(ORDERS_DOWNLOAD_HEADERS)
    for order in orders_qs_data:
        try:
            product_data = product_detail_by_order_id[order[1]] if order[1] in product_detail_by_order_id else []
            if product_data and product_data[0]:
                for idx, val in enumerate(product_data[0]):
                    new_row = list()
                    new_row.append(str(order[0]))
                    new_row.append(str(order[13]))
                    new_row.append(str(order[15]))
                    new_row.append(str(order[14]))
                    new_row.append(order[2].strftime("%Y-%m-%d %H:%M:%S") if order[2] else "N/A")
                    new_row.append(str(order[7]))
                    new_row.append(str(order[9]) if not hide_weights else "")
                    new_row.append(str(order[5]))
                    new_row.append(order[8].strftime("%Y-%m-%d") if order[8] else "N/A")
                    new_row.append(str(order[3]))
                    new_row.append(str(order[16]))
                    new_row.append(str(order[17]))
                    new_row.append(str(order[18]))
                    new_row.append(str(order[19]))
                    new_row.append(str(order[20]))
                    new_row.append(str(order[21]))
                    new_row.append(order[26])
                    new_row.append(str(val))
                    new_row.append(str(product_data[1][idx]))
                    new_row.append(str(product_data[2][idx]))
                    new_row.append(str(order[24]))
                    new_row.append(order[25])
                    new_row.append(order[34].strftime("%Y-%m-%d %H:%M:%S") if order[34] else "N/A")
                    new_row.append(order[23].strftime("%Y-%m-%d %H:%M:%S") if order[23] else "N/A")
                    new_row.append(order[22].strftime("%Y-%m-%d %H:%M:%S") if order[22] else "N/A")
                    if order[27] and order[28] is not None:
                        new_row.append("Confirmed" if order[28] else "Cancelled")
                        new_row.append(str(order[29]))
                    else:
                        new_row.append("N/A")
                        new_row.append("N/A")
                    if order[30] and order[31] is not None:
                        new_row.append("Cancelled" if order[31] else "Re-attempt")
                        new_row.append(str(order[32]))
                    else:
                        new_row.append("N/A")
                        new_row.append("N/A")
                    new_row.append(order[39].strftime("%Y-%m-%d %H:%M:%S") if order[39] else "N/A")
                    not_shipped = None
                    if not product_data[4][idx]:
                        not_shipped = "Weight/dimensions not entered for product(s)"
                    elif order[12] == "Pincode not serviceable":
                        not_shipped = "Pincode not serviceable"
                    elif not order[26]:
                        not_shipped = "Pickup point not assigned"
                    if not_shipped:
                        new_row.append(not_shipped)
                    if auth_data.get('user_group') == 'super-admin':
                        new_row.append(order[38])
                    cw.writerow(new_row)
        except Exception as e:
            pass

    output = make_response(si.getvalue())
    filename = str(client_prefix) + "_EXPORT.csv"
    output.headers["Content-Disposition"] = "attachment; filename=" + filename
    output.headers["Content-type"] = "text/csv"
    return output


def user_group_filter(query_to_run, auth_data):

    if auth_data['user_group'] == 'client':
        query_to_run = query_to_run.replace("__CLIENT_FILTER__", "AND aa.client_prefix = '%s'" % auth_data.get('client_prefix'))
    if auth_data['user_group'] == 'warehouse':
        query_to_run = query_to_run.replace("__PICKUP_FILTER__",
                                            "AND ii.warehouse_prefix = '%s'" % auth_data.get('warehouse_prefix'))
    if auth_data['user_group'] == 'multi-vendor':
        cur.execute("SELECT vendor_list FROM multi_vendor WHERE client_prefix='%s';" % auth_data.get('client_prefix'))
        vendor_list = cur.fetchone()[0]
        query_to_run = query_to_run.replace("__MV_CLIENT_FILTER__",
                                            "AND aa.client_prefix in %s" % str(tuple(vendor_list)))
    else:
        query_to_run = query_to_run.replace("__MV_CLIENT_FILTER__", "")

    return query_to_run