import re
import csv, boto3, os
import io, random, string
from datetime import datetime, timedelta
from flask import make_response, jsonify

from project.api.queries import (
    select_orders_list_query,
)

from project.api.utilities.db_utils import DbConnection

conn = DbConnection.get_db_connection_instance()
cur = conn.cursor()

session = boto3.Session(
    aws_access_key_id="AKIAWRT2R3KC3YZUBFXY",
    aws_secret_access_key="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",
)


def get_filled_query(
    search_key, search_key_on_customer_detail, since_id, type, filters, auth_data
):
    query_to_run = select_orders_list_query

    if search_key:
        regex_check = "where (aa.channel_order_id ilike '%__SEARCH_KEY__%' or awb ilike '%__SEARCH_KEY__%')"
        query_to_run = query_to_run.replace("__SEARCH_KEY_FILTER__", regex_check)
        query_to_run = query_to_run.replace("__SEARCH_KEY__", search_key)
    else:
        query_to_run = query_to_run.replace("__SEARCH_KEY_FILTER__", "where (1=1)")

    if search_key_on_customer_detail:
        regex_check_customer_details = " AND (customer_name ilike '%__SEARCH_KEY_ON_CUSTOMER_DETAILS__%' or customer_phone ilike '%__SEARCH_KEY_ON_CUSTOMER_DETAILS__%' or customer_email ilike '%__SEARCH_KEY_ON_CUSTOMER_DETAILS__%')"
        query_to_run = query_to_run.replace(
            "__SEARCH_KEY_FILTER_ON_CUSTOMER__", regex_check_customer_details
        )
        query_to_run = query_to_run.replace(
            "__SEARCH_KEY_ON_CUSTOMER_DETAILS__", search_key_on_customer_detail
        )

    query_to_run = user_group_filter(query_to_run, auth_data)

    if since_id:
        query_to_run = query_to_run.replace(
            "__SINCE_ID_FILTER__", "AND id>%s" % str(since_id)
        )

    if type == "new":
        query_to_run = query_to_run.replace(
            "__TAB_STATUS_FILTER__",
            "AND aa.status = 'NEW' AND gg.payment_mode!='Pickup'",
        )
    elif type == "ready_to_ship":
        query_to_run = query_to_run.replace(
            "__TAB_STATUS_FILTER__",
            "AND aa.status in ('READY TO SHIP', 'PICKUP REQUESTED') AND gg.payment_mode!='Pickup'",
        )
    elif type == "shipped":
        query_to_run = query_to_run.replace(
            "__TAB_STATUS_FILTER__",
            "AND aa.status not in ('NEW', 'READY TO SHIP', 'PICKUP REQUESTED','NOT PICKED','CANCELED', 'CLOSED', 'NOT SHIPPED') AND gg.payment_mode!='Pickup'",
        )
    elif type == "return":
        query_to_run = query_to_run.replace(
            "__TAB_STATUS_FILTER__",
            "AND (aa.status_type='RT' or (aa.status_type='DL' and aa.status='RTO')) AND gg.payment_mode!='Pickup'",
        )
    elif type == "ndr":
        query_to_run = query_to_run.replace(
            "__NDR_AGGREGATION__",
            """left join (select ss.order_id, max(ss.id) as ndr_id, array_agg(tt.id order by ss.id desc) as reason_id, 
            array_agg(tt.reason order by ss.id desc) as reason, array_agg(ss.date_created order by ss.id desc) as ndr_date,
            array_agg(ss.current_status order by ss.id desc) as current_status
            from ndr_shipments ss left join ndr_reasons tt on ss.reason_id=tt.id 
            group by order_id) rr
            on aa.id=rr.order_id""",
        )
        query_to_run = query_to_run.replace(
            "__NDR_AGG_SEL_1__", "rr.reason_id, rr.reason, rr.ndr_date,"
        )
        query_to_run = query_to_run.replace("__NDR_AGG_SEL_2__", "rr.ndr_id, rr.current_status, ")
        query_to_run = query_to_run.replace(
            "__TAB_STATUS_FILTER__",
            "AND (rr.ndr_id is not null AND aa.status='PENDING' AND aa.status_type!='RT') AND gg.payment_mode!='Pickup'",
        )
    elif type == "rvp":
        query_to_run = query_to_run.replace(
            "__TAB_STATUS_FILTER__", "AND gg.payment_mode ilike 'pickup'"
        )
    elif type == "all":
        pass
    else:
        return {"success": False, "msg": "Invalid URL"}, 404

    query_to_run = query_to_run.replace("__NDR_AGG_SEL_1__", "null, null, null,")
    query_to_run = query_to_run.replace("__NDR_AGG_SEL_2__", "null, null, ")

    if filters:
        query_to_run = filter_query(filters, query_to_run, auth_data)

    return query_to_run


def filter_query(filters, query_to_run, auth_data):

    if "status" in filters:
        if len(filters["status"]) == 1:
            status_tuple = "('" + filters["status"][0] + "')"
        else:
            status_tuple = str(tuple(filters["status"]))
        query_to_run = query_to_run.replace(
            "__STATUS_FILTER__", "AND aa.status in %s" % status_tuple
        )

    if "courier" in filters:
        if len(filters["courier"]) == 1:
            courier_tuple = "('" + filters["courier"][0] + "')"
        else:
            courier_tuple = str(tuple(filters["courier"]))
        query_to_run = query_to_run.replace(
            "__COURIER_FILTER__", "AND courier_name in %s" % courier_tuple
        )

    if "client" in filters and auth_data["user_group"] != "client":
        if len(filters["client"]) == 1:
            client_tuple = "('" + filters["client"][0] + "')"
        else:
            client_tuple = str(tuple(filters["client"]))
        query_to_run = query_to_run.replace(
            "__CLIENT_FILTER__", "AND aa.client_prefix in %s" % client_tuple
        )

    if "pickup_point" in filters:
        if len(filters["pickup_point"]) == 1:
            pickup_tuple = "('" + filters["pickup_point"][0] + "')"
        else:
            pickup_tuple = str(tuple(filters["pickup_point"]))
        query_to_run = query_to_run.replace(
            "__PICKUP_FILTER__", "AND ii.warehouse_prefix in %s" % pickup_tuple
        )

    if "ndr_reason" in filters:
        if len(filters["ndr_reason"]) == 1:
            reason_tuple = "('" + filters["ndr_reason"][0] + "')"
        else:
            reason_tuple = str(tuple(filters["ndr_reason"]))
        query_to_run = query_to_run.replace(
            "__NDR_REASON_FILTER__", "AND rr.reason[1] in %s" % reason_tuple
        )

    if "ndr_type" in filters:
        if (
            "Action Requested" in filters["ndr_type"]
            and "Action Required" in filters["ndr_type"]
        ):
            ndr_type_filter = ""
        elif "Action Requested" in filters["ndr_type"]:
            ndr_type_filter = "AND rr.current_status[1] in ('reattempt', 'cancelled')"
        else:
            ndr_type_filter = "AND rr.current_status[1] = 'required'"

        query_to_run = query_to_run.replace("__NDR_TYPE_FILTER__", ndr_type_filter)

    if "order_type" in filters:
        if len(filters["order_type"]) == 1:
            type_tuple = "('" + filters["order_type"][0] + "')"
        else:
            type_tuple = str(tuple(filters["order_type"]))
        query_to_run = query_to_run.replace(
            "__TYPE_FILTER__", "AND upper(payment_mode) in %s" % type_tuple
        )

    if "order_date" in filters:
        filter_date_start = filters["order_date"][0][0:19].replace("T", " ")
        filter_date_end = filters["order_date"][1][0:19].replace("T", " ")
        query_to_run = query_to_run.replace(
            "__ORDER_DATE_FILTER__",
            "AND order_date between '%s' and '%s'"
            % (filter_date_start, filter_date_end),
        )

    if "thirdwatch_score" in filters:
        score_from = float(filters["thirdwatch_score"][0])
        score_to = float(filters["thirdwatch_score"][1])
        query_to_run = query_to_run.replace(
            "__THIRDWATCH_SCORE_FILTER__",
            "AND uu.score between %s and %s" % (score_from, score_to),
        )

    if "thirdwatch_flag" in filters:
        if len(filters["thirdwatch_flag"]) == 1:
            flag_tuple = "('" + filters["thirdwatch_flag"][0] + "')"
        else:
            flag_tuple = str(tuple(filters["thirdwatch_flag"]))
        query_to_run = query_to_run.replace(
            "__THIRDWATCH_FLAG_FILTER__", "AND uu.flag in %s" % flag_tuple
        )

    if "thirdwatch_tags" in filters:
        flag_tuple = str(filters["thirdwatch_tags"])
        query_to_run = query_to_run.replace(
            "__THIRDWATCH_TAGS_FILTER__",
            "AND uu.tags @> ARRAY%s::varchar[]" % flag_tuple,
        )

    if "updated_after" in filters:
        updated_after = filters["updated_after"]
        query_to_run = query_to_run.replace(
            "__UPDATED_AFTER__", "AND aa.date_updated > '%s'" % updated_after
        )

    if "pickup_time" in filters:
        filter_date_start = filters["pickup_time"][0][0:19].replace("T", " ")
        filter_date_end = filters["pickup_time"][1][0:19].replace("T", " ")
        query_to_run = query_to_run.replace(
            "__PICKUP_TIME_FILTER__",
            "AND pickup_time between '%s' and '%s'"
            % (filter_date_start, filter_date_end),
        )

    if "manifest_time" in filters:
        filter_date_start = filters["manifest_time"][0][0:19].replace("T", " ")
        filter_date_end = filters["manifest_time"][1][0:19].replace("T", " ")
        query_to_run = query_to_run.replace(
            "__MANIFEST_DATE_FILTER__",
            "AND manifest_time between '%s' and '%s'"
            % (filter_date_start, filter_date_end),
        )

    if "delivered_time" in filters:
        filter_date_start = filters["delivered_time"][0][0:19].replace("T", " ")
        filter_date_end = filters["delivered_time"][1][0:19].replace("T", " ")
        query_to_run = query_to_run.replace(
            "__PICKUP_TIME_FILTER__",
            "AND delivered_time between '%s' and '%s'"
            % (filter_date_start, filter_date_end),
        )

    if "channels" in filters:
        if len(filters["channels"]) == 1:
            channel_tuple = "('" + filters["channels"][0] + "')"
        else:
            channel_tuple = str(tuple(filters["channels"]))
        query_to_run = query_to_run.replace(
            "__MASTER_CHANNEL__", "AND vv.channel_name in %s" % channel_tuple
        )

    if "edd" in filters:
        filter_date_start = filters["edd"][0][0:19].replace("T", " ")
        filter_date_end = filters["edd"][1][0:19].replace("T", " ")
        query_to_run = query_to_run.replace(
            "__EDD_FILTER__",
            "AND bb.edd between '%s' and '%s'" % (filter_date_start, filter_date_end),
        )

    return query_to_run


def download_flag_func(
    query_to_run,
    get_selected_product_details,
    auth_data,
    ORDERS_DOWNLOAD_HEADERS,
    hide_weights,
    report_id,
):

    client_prefix = (
        auth_data.get("client_prefix")
        if auth_data.get("client_prefix")
        else auth_data.get("warehouse_prefix")
    )
    query_to_run = re.sub(r"""__.+?__""", "", query_to_run)
    cur.execute(query_to_run)
    orders_qs_data = cur.fetchall()
    order_id_data = ",".join([str(it[1]) for it in orders_qs_data])
    product_detail_by_order_id = {}
    if order_id_data:
        update_product_details_query = get_selected_product_details.replace(
            "__FILTERED_ORDER_ID__", order_id_data
        )
        cur.execute(update_product_details_query)
        product_detail_data = cur.fetchall()
        for it in product_detail_data:
            product_detail_by_order_id[it[0]] = [
                it[1],
                it[2],
                it[3],
                it[4],
                it[5],
                it[6],
                it[7],
                it[8],
            ]

    filename = (
        str(client_prefix)
        + "_EXPORT_orders_"
        + "".join(random.choices(string.ascii_letters + string.digits, k=8))
        + ".csv"
    )
    with open(filename, "w") as mycsvfile:
        cw = csv.writer(mycsvfile)
        cw.writerow(ORDERS_DOWNLOAD_HEADERS)
        for order in orders_qs_data:
            try:
                product_data = (
                    product_detail_by_order_id[order[1]]
                    if order[1] in product_detail_by_order_id
                    else []
                )
                if product_data and product_data[0]:
                    for idx, val in enumerate(product_data[0]):
                        order_disc = "N/A"
                        try:
                            order_disc = sum(product_data[5]) - order[25]
                            if order[43]:
                                order_disc += order[43]
                        except Exception:
                            pass
                        new_row = list()
                        new_row.append(str(order[0]))
                        new_row.append(str(order[13]))
                        new_row.append(str(order[15]))
                        new_row.append(str(order[14]))
                        new_row.append(
                            order[2].strftime("%Y-%m-%d %H:%M:%S")
                            if order[2]
                            else "N/A"
                        )
                        new_row.append(str(order[7]))
                        new_row.append(str(order[9]) if not hide_weights else "")
                        new_row.append(str(order[5]))
                        new_row.append(
                            order[8].strftime("%Y-%m-%d") if order[8] else "N/A"
                        )
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
                        new_row.append(
                            order[34].strftime("%Y-%m-%d %H:%M:%S")
                            if order[34]
                            else "N/A"
                        )
                        new_row.append(
                            order[23].strftime("%Y-%m-%d %H:%M:%S")
                            if order[23]
                            else "N/A"
                        )
                        new_row.append(
                            order[22].strftime("%Y-%m-%d %H:%M:%S")
                            if order[22]
                            else "N/A"
                        )
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
                        new_row.append(
                            order[39].strftime("%Y-%m-%d %H:%M:%S")
                            if order[39]
                            else "N/A"
                        )
                        new_row.append(str(order[43]) if order[43] else "0")
                        new_row.append(str(order[44]) if order[44] else "N/A")
                        new_row.append(
                            order[45].strftime("%Y-%m-%d %H:%M:%S")
                            if order[45]
                            else "N/A"
                        )
                        new_row.append(str(order_disc))
                        prod_amount = (
                            product_data[5][idx]
                            if product_data[5][idx] is not None
                            else product_data[7][idx]
                        )
                        cgst, sgst, igst = "", "", ""
                        try:
                            taxable_amount = prod_amount / (1 + product_data[6][idx])
                            if prod_amount and product_data[6][idx] and order[48]:
                                cgst = taxable_amount * product_data[6][idx] / 2
                                sgst = cgst
                            elif prod_amount and product_data[6][idx] and not order[48]:
                                igst = taxable_amount * product_data[6][idx]
                        except Exception:
                            pass
                        new_row.append(str(prod_amount))
                        new_row.append(str(cgst))
                        new_row.append(str(sgst))
                        new_row.append(str(igst))
                        new_row.append(str(order[1]))
                        not_shipped = None
                        if not product_data[4][idx]:
                            not_shipped = "Weight/dimensions not entered for product(s)"
                        elif order[12] == "Pincode not serviceable":
                            not_shipped = "Pincode not serviceable"
                        elif not order[26]:
                            not_shipped = "Pickup point not assigned"
                        elif order[12] and "incorrect phone" in order[12].lower():
                            not_shipped = "Invalid contact number"
                        if not_shipped:
                            new_row.append(not_shipped)
                        if auth_data.get("user_group") in ("super-admin", "warehouse"):
                            new_row.append(order[38])
                        cw.writerow(new_row)
            except Exception as e:
                pass

    s3 = session.resource("s3")
    bucket = s3.Bucket("wareiqfiles")
    bucket.upload_file(
        filename, "downloads/" + filename, ExtraArgs={"ACL": "public-read"}
    )
    invoice_url = "https://wareiqfiles.s3.amazonaws.com/downloads/" + filename
    file_size = os.path.getsize(filename)
    file_size = int(file_size / 1000)
    os.remove(filename)
    cur.execute(
        "UPDATE downloads SET download_link='%s', status='processed', file_size=%s where id=%s"
        % (invoice_url, file_size, report_id)
    )
    conn.commit()
    return jsonify({"url": invoice_url, "success": True}), 200


def user_group_filter(query_to_run, auth_data):

    if auth_data["user_group"] == "client":
        query_to_run = query_to_run.replace(
            "__CLIENT_FILTER__",
            "AND aa.client_prefix = '%s'" % auth_data.get("client_prefix"),
        )
    if auth_data["user_group"] == "warehouse":
        query_to_run = query_to_run.replace(
            "__PICKUP_FILTER__",
            "AND ii.warehouse_prefix = '%s'" % auth_data.get("warehouse_prefix"),
        )
    if auth_data["user_group"] == "multi-vendor":
        cur.execute(
            "SELECT vendor_list FROM multi_vendor WHERE client_prefix='%s';"
            % auth_data.get("client_prefix")
        )
        vendor_list = cur.fetchone()[0]
        query_to_run = query_to_run.replace(
            "__MV_CLIENT_FILTER__",
            "AND aa.client_prefix in %s" % str(tuple(vendor_list)),
        )
    else:
        query_to_run = query_to_run.replace("__MV_CLIENT_FILTER__", "")

    return query_to_run
