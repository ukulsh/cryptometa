import boto3, os, random, string, csv, json, math
from datetime import datetime, timedelta
from sqlalchemy import or_, func
from flask import Blueprint, request, jsonify
from flask_restful import Api
from project import db
from project.api.models import (
    MultiVendor,
    Orders,
    OrdersPayments,
    CodVerification,
    NDRVerification,
)
from project.api.utils import authenticate_restful
from project.api.utilities.db_utils import DbConnection
from project.api.queries import (
    select_state_performance_query,
    select_top_selling_state_query,
    select_courier_performance_query,
    select_zone_performance_query,
    select_transit_delays_query,
    select_rto_delays_query,
    select_ndr_reason_query,
    select_ndr_reason_orders_query,
    inventory_analytics_query,
    inventory_analytics_filters_query,
    inventory_analytics_in_transit_query,
)

analytics_blueprint = Blueprint("analytics", __name__)
api = Api(analytics_blueprint)

conn = DbConnection.get_db_connection_instance()
conn_2 = DbConnection.get_pincode_db_connection_instance()

session = boto3.Session(
    aws_access_key_id="AKIAWRT2R3KC3YZUBFXY",
    aws_secret_access_key="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",
)

STATE_DOWNLOAD_HEADERS = [
    "State",
    "TotalOrders",
    "OrderPerc",
    "AvgShipCost",
    "AvgTransitDays",
    "RTOPerc",
    "CODPerc",
    "AvgRevenue",
    "MostFrequentZone",
    "MostFrequentWH",
]
COURIER_DOWNLOAD_HEADERS = [
    "Courier",
    "TotalOrders",
    "OrderPerc",
    "AvgShipCost",
    "AvgTransitDays",
    "RTOPerc",
    "DeliveredPerc",
    "DeliveredWithinSLA",
    "NDRPerc",
]
ZONE_DOWNLOAD_HEADERS = [
    "Zone",
    "TotalOrders",
    "OrderPerc",
    "AvgShipCost",
    "AvgTransitDays",
    "RTOPerc",
    "DeliveredPerc",
    "DeliveredWithinSLA",
    "NDRPerc",
]
TRANSIT_DELAY_DOWNLOAD_HEADERS = [
    "OrderID",
    "Status",
    "AWB",
    "Courier",
    "ShippedDate",
    "PromisedDeliveryDate",
    "DelayedByDays",
    "Zone",
    "LastScan",
    "CustomerName",
    "CustomerPhone",
    "CustomerEmail",
]
RTO_DELAY_DOWNLOAD_HEADERS = [
    "OrderID",
    "Status",
    "AWB",
    "Courier",
    "ReturnMarkDate",
    "DelayedByDays",
    "Zone",
    "LastScan",
    "CustomerName",
    "CustomerPhone",
    "CustomerEmail",
]

NDR_ORDERS_DOWNLOAD_HEADERS = [
    "OrderID",
    "Status",
    "AWB",
    "Courier",
    "Action",
    "ActionBy",
    "AttemptCount",
    "LatestReason",
    "DeferredDeliveryDate",
    "UpdatedAddress",
    "UpdatedPhone",
    "OriginalPhone",
]


@analytics_blueprint.route("/analytics/v1/shipping/statePerformance", methods=["GET"])
@authenticate_restful
def get_state_performance(resp):
    response = {"success": False}
    cur = conn.cursor()
    try:
        auth_data = resp.get("data")
        if not auth_data:
            return jsonify({"msg": "Authentication Failed"}), 400

        if auth_data["user_group"] == "warehouse":
            response["data"] = {}
            return jsonify(response), 200

        from_date = request.args.get("from")
        to_date = request.args.get("to")
        mode = request.args.get("mode")

        if not from_date:
            from_date = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=30)
            from_date = from_date.strftime("%Y-%m-%d")

        if to_date:
            to_date = datetime.strptime(to_date, "%Y-%m-%d")
            to_date = to_date + timedelta(days=1)
            to_date = to_date.strftime("%Y-%m-%d")
        else:
            to_date = datetime.utcnow() + timedelta(hours=5.5) + timedelta(days=1)
            to_date = to_date.strftime("%Y-%m-%d")

        client_prefix = auth_data.get("client_prefix")

        query_to_run = select_state_performance_query % (from_date, to_date)

        all_vendors = None
        if auth_data["user_group"] == "multi-vendor":
            all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
            all_vendors = all_vendors.vendor_list

        if auth_data["user_group"] == "client":
            query_to_run = query_to_run.replace(
                "__CLIENT_FILTER__",
                "AND aa.client_prefix='%s'" % auth_data["client_prefix"],
            )
        elif all_vendors:
            query_to_run = query_to_run.replace(
                "__CLIENT_FILTER__",
                "AND aa.client_prefix in %s" % str(tuple(all_vendors)),
            )
        else:
            query_to_run = query_to_run.replace("__CLIENT_FILTER__", "")

        if mode:
            query_to_run = query_to_run.replace("__MODE_FILTER__", "and ii.payment_mode ilike '%s'" % str(mode).lower())
        else:
            query_to_run = query_to_run.replace("__MODE_FILTER__", "")

        cur.execute(query_to_run)
        state_qs = cur.fetchall()
        if request.args.get("download"):
            filename = (
                str(client_prefix)
                + "_EXPORT_states_"
                + "".join(random.choices(string.ascii_letters + string.digits, k=8))
                + ".csv"
            )
            with open(filename, "w") as mycsvfile:
                cw = csv.writer(mycsvfile)
                cw.writerow(STATE_DOWNLOAD_HEADERS)
                for state in state_qs:
                    try:
                        new_row = list()
                        new_row.append(str(state[0]))
                        new_row.append(str(state[1]))
                        new_row.append(str(state[2]))
                        new_row.append(str(state[3]))
                        new_row.append(str(state[4]))
                        new_row.append(str(state[5]))
                        new_row.append(str(state[8]))
                        new_row.append(str(state[6]))
                        new_row.append(str(state[7]))
                        new_row.append(str(state[9]))
                        cw.writerow(new_row)
                    except Exception as e:
                        pass

            s3 = session.resource("s3")
            bucket = s3.Bucket("wareiqfiles")
            bucket.upload_file(filename, "downloads/" + filename, ExtraArgs={"ACL": "public-read"})
            state_url = "https://wareiqfiles.s3.amazonaws.com/downloads/" + filename
            os.remove(filename)
            return jsonify({"url": state_url, "success": True}), 200

        data = list()
        for state in state_qs:
            st_obj = dict()
            st_obj["state"] = state[0]
            st_obj["total_orders"] = state[1]
            st_obj["order_perc"] = float(state[2]) if state[2] else None
            st_obj["avg_ship_cost"] = float(state[3]) if state[3] else None
            st_obj["avg_tras_days"] = float(state[4]) if state[4] else None
            st_obj["rto_perc"] = float(state[5]) if state[5] else None
            st_obj["rev_per_order"] = float(state[6]) if state[6] else None
            st_obj["freq_zone"] = state[7]
            st_obj["cod_perc"] = float(state[8]) if state[8] else None
            st_obj["freq_wh"] = state[9]
            data.append(st_obj)

        response["data"] = data
        response["success"] = True

        return jsonify(response), 200
    except Exception as e:
        conn.rollback()
        return jsonify(response), 400


@analytics_blueprint.route("/analytics/v1/shipping/courierPerformance", methods=["GET"])
@authenticate_restful
def get_courier_performance(resp):
    response = {"success": False}
    cur = conn.cursor()
    try:
        auth_data = resp.get("data")
        if not auth_data:
            return jsonify({"msg": "Authentication Failed"}), 400

        if auth_data["user_group"] == "warehouse":
            response["data"] = {}
            return jsonify(response), 200

        from_date = request.args.get("from")
        to_date = request.args.get("to")
        mode = request.args.get("mode")
        zone = request.args.get("zone")

        if not from_date:
            from_date = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=30)
            from_date = from_date.strftime("%Y-%m-%d")

        if to_date:
            to_date = datetime.strptime(to_date, "%Y-%m-%d")
            to_date = to_date + timedelta(days=1)
            to_date = to_date.strftime("%Y-%m-%d")
        else:
            to_date = datetime.utcnow() + timedelta(hours=5.5) + timedelta(days=1)
            to_date = to_date.strftime("%Y-%m-%d")

        client_prefix = auth_data.get("client_prefix")

        query_to_run = select_courier_performance_query % (from_date, to_date)

        all_vendors = None
        if auth_data["user_group"] == "multi-vendor":
            all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
            all_vendors = all_vendors.vendor_list

        if auth_data["user_group"] == "client":
            query_to_run = query_to_run.replace(
                "__CLIENT_FILTER__",
                "AND aa.client_prefix='%s'" % auth_data["client_prefix"],
            )
        elif all_vendors:
            query_to_run = query_to_run.replace(
                "__CLIENT_FILTER__",
                "AND aa.client_prefix in %s" % str(tuple(all_vendors)),
            )
        else:
            query_to_run = query_to_run.replace("__CLIENT_FILTER__", "")

        if mode:
            query_to_run = query_to_run.replace("__MODE_FILTER__", "and ii.payment_mode ilike '%s'" % str(mode).lower())
        else:
            query_to_run = query_to_run.replace("__MODE_FILTER__", "")

        if zone:
            zone = str(zone).split(",")
            if len(zone) == 1:
                zone = "('" + zone[0] + "')"
            else:
                zone = "('" + "','".join(zone) + "')"
            query_to_run = query_to_run.replace("__ZONE_FILTER__", "and hh.zone in %s" % zone)
        else:
            query_to_run = query_to_run.replace("__ZONE_FILTER__", "")

        cur.execute(query_to_run)
        state_qs = cur.fetchall()
        if request.args.get("download"):
            filename = (
                str(client_prefix)
                + "_EXPORT_couriers_"
                + "".join(random.choices(string.ascii_letters + string.digits, k=8))
                + ".csv"
            )
            with open(filename, "w") as mycsvfile:
                cw = csv.writer(mycsvfile)
                cw.writerow(COURIER_DOWNLOAD_HEADERS)
                for state in state_qs:
                    try:
                        new_row = list()
                        new_row.append(str(state[0]))
                        new_row.append(str(state[1]))
                        new_row.append(str(state[2]))
                        new_row.append(str(state[3]))
                        new_row.append(str(state[4]))
                        new_row.append(str(state[5]))
                        new_row.append(str(state[6]))
                        new_row.append(str(state[7]))
                        new_row.append(str(state[8]))
                        cw.writerow(new_row)
                    except Exception as e:
                        pass

            s3 = session.resource("s3")
            bucket = s3.Bucket("wareiqfiles")
            bucket.upload_file(filename, "downloads/" + filename, ExtraArgs={"ACL": "public-read"})
            state_url = "https://wareiqfiles.s3.amazonaws.com/downloads/" + filename
            os.remove(filename)
            return jsonify({"url": state_url, "success": True}), 200

        data = list()
        for state in state_qs:
            st_obj = dict()
            st_obj["courier"] = state[0]
            st_obj["total_orders"] = state[1]
            st_obj["order_perc"] = float(state[2]) if state[2] else None
            st_obj["avg_ship_cost"] = float(state[3]) if state[3] else None
            st_obj["avg_tras_days"] = float(state[4]) if state[4] else None
            st_obj["rto_perc"] = float(state[5]) if state[5] else None
            st_obj["delivered_perc"] = float(state[6]) if state[6] else None
            st_obj["del_within_sla"] = float(state[7]) if state[7] else None
            st_obj["ndr_perc"] = float(state[8]) if state[8] else None
            data.append(st_obj)

        response["data"] = data
        response["success"] = True

        return jsonify(response), 200
    except Exception as e:
        conn.rollback()
        return jsonify(response), 400


@analytics_blueprint.route("/analytics/v1/shipping/zonePerformance", methods=["GET"])
@authenticate_restful
def get_zone_performance(resp):
    response = {"success": False}
    cur = conn.cursor()
    try:
        auth_data = resp.get("data")
        if not auth_data:
            return jsonify({"msg": "Authentication Failed"}), 400

        if auth_data["user_group"] == "warehouse":
            response["data"] = {}
            return jsonify(response), 200

        from_date = request.args.get("from")
        to_date = request.args.get("to")

        if not from_date:
            from_date = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=30)
            from_date = from_date.strftime("%Y-%m-%d")

        if to_date:
            to_date = datetime.strptime(to_date, "%Y-%m-%d")
            to_date = to_date + timedelta(days=1)
            to_date = to_date.strftime("%Y-%m-%d")
        else:
            to_date = datetime.utcnow() + timedelta(hours=5.5) + timedelta(days=1)
            to_date = to_date.strftime("%Y-%m-%d")

        client_prefix = auth_data.get("client_prefix")

        query_to_run = select_zone_performance_query % (from_date, to_date)

        all_vendors = None
        if auth_data["user_group"] == "multi-vendor":
            all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
            all_vendors = all_vendors.vendor_list

        if auth_data["user_group"] == "client":
            query_to_run = query_to_run.replace(
                "__CLIENT_FILTER__",
                "AND aa.client_prefix='%s'" % auth_data["client_prefix"],
            )
        elif all_vendors:
            query_to_run = query_to_run.replace(
                "__CLIENT_FILTER__",
                "AND aa.client_prefix in %s" % str(tuple(all_vendors)),
            )
        else:
            query_to_run = query_to_run.replace("__CLIENT_FILTER__", "")

        cur.execute(query_to_run)
        state_qs = cur.fetchall()
        if request.args.get("download"):
            filename = (
                str(client_prefix)
                + "_EXPORT_zone_"
                + "".join(random.choices(string.ascii_letters + string.digits, k=8))
                + ".csv"
            )
            with open(filename, "w") as mycsvfile:
                cw = csv.writer(mycsvfile)
                cw.writerow(ZONE_DOWNLOAD_HEADERS)
                for state in state_qs:
                    try:
                        new_row = list()
                        new_row.append(str(state[0]))
                        new_row.append(str(state[1]))
                        new_row.append(str(state[2]))
                        new_row.append(str(state[3]))
                        new_row.append(str(state[4]))
                        new_row.append(str(state[5]))
                        new_row.append(str(state[6]))
                        new_row.append(str(state[7]))
                        new_row.append(str(state[8]))
                        cw.writerow(new_row)
                    except Exception as e:
                        pass

            s3 = session.resource("s3")
            bucket = s3.Bucket("wareiqfiles")
            bucket.upload_file(filename, "downloads/" + filename, ExtraArgs={"ACL": "public-read"})
            state_url = "https://wareiqfiles.s3.amazonaws.com/downloads/" + filename
            os.remove(filename)
            return jsonify({"url": state_url, "success": True}), 200

        data = list()
        for state in state_qs:
            st_obj = dict()
            st_obj["zone"] = state[0]
            st_obj["total_orders"] = state[1]
            st_obj["order_perc"] = float(state[2]) if state[2] else None
            st_obj["avg_ship_cost"] = float(state[3]) if state[3] else None
            st_obj["avg_tras_days"] = float(state[4]) if state[4] else None
            st_obj["rto_perc"] = float(state[5]) if state[5] else None
            st_obj["delivered_perc"] = float(state[6]) if state[6] else None
            st_obj["del_within_sla"] = float(state[7]) if state[7] else None
            st_obj["ndr_perc"] = float(state[8]) if state[8] else None
            data.append(st_obj)

        response["data"] = data
        response["success"] = True

        return jsonify(response), 200
    except Exception as e:
        conn.rollback()
        return jsonify(response), 400


@analytics_blueprint.route("/analytics/v1/shipping/topStates", methods=["GET"])
@authenticate_restful
def get_top_states(resp):
    response = {"success": False}
    cur = conn.cursor()
    try:
        auth_data = resp.get("data")
        if not auth_data:
            return jsonify({"msg": "Authentication Failed"}), 400

        if auth_data["user_group"] == "warehouse":
            response["data"] = {}
            return jsonify(response), 200

        from_date = request.args.get("from")
        to_date = request.args.get("to")

        if not from_date:
            from_date = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=30)
            from_date = from_date.strftime("%Y-%m-%d")

        if to_date:
            to_date = datetime.strptime(to_date, "%Y-%m-%d")
            to_date = to_date + timedelta(days=1)
            to_date = to_date.strftime("%Y-%m-%d")
        else:
            to_date = datetime.utcnow() + timedelta(hours=5.5) + timedelta(days=1)
            to_date = to_date.strftime("%Y-%m-%d")

        client_prefix = auth_data.get("client_prefix")

        query_to_run = select_top_selling_state_query % (from_date, to_date)

        all_vendors = None
        if auth_data["user_group"] == "multi-vendor":
            all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
            all_vendors = all_vendors.vendor_list

        if auth_data["user_group"] == "client":
            query_to_run = query_to_run.replace(
                "__CLIENT_FILTER__",
                "AND aa.client_prefix='%s'" % auth_data["client_prefix"],
            )
        elif all_vendors:
            query_to_run = query_to_run.replace(
                "__CLIENT_FILTER__",
                "AND aa.client_prefix in %s" % str(tuple(all_vendors)),
            )
        else:
            query_to_run = query_to_run.replace("__CLIENT_FILTER__", "")

        cur.execute(query_to_run)
        state_qs = cur.fetchall()
        data = list()
        for state in state_qs:
            st_obj = dict()
            st_obj["state"] = state[0]
            st_obj["total_orders"] = state[1]
            st_obj["order_perc"] = float(state[2]) if state[2] else None
            data.append(st_obj)

        response["data"] = data
        response["success"] = True

        return jsonify(response), 200
    except Exception as e:
        conn.rollback()
        return jsonify(response), 400


@analytics_blueprint.route("/analytics/v1/undelivered/transitDelays", methods=["POST"])
@authenticate_restful
def get_transit_delays(resp):
    response = {"success": False, "data": [], "meta": {}}
    cur = conn.cursor()
    try:
        data = json.loads(request.data)
        auth_data = resp.get("data")
        if not auth_data:
            return jsonify({"msg": "Authentication Failed"}), 400

        if auth_data["user_group"] == "warehouse":
            response["data"] = {}
            return jsonify(response), 200

        page = data.get("page", 1)
        per_page = data.get("per_page", 10)
        order_by = data.get("sort_by", "delayed_by_days")
        order_type = data.get("sort", "asc")
        filters = data.get("filters", {})
        download_flag = request.args.get("download", None)

        client_prefix = auth_data.get("client_prefix")

        query_to_run = select_transit_delays_query

        all_vendors = None
        if auth_data["user_group"] == "multi-vendor":
            all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
            all_vendors = all_vendors.vendor_list

        if auth_data["user_group"] == "client":
            query_to_run = query_to_run.replace(
                "__CLIENT_FILTER__",
                "AND aa.client_prefix='%s'" % auth_data["client_prefix"],
            )
        elif all_vendors:
            query_to_run = query_to_run.replace(
                "__CLIENT_FILTER__",
                "AND aa.client_prefix in %s" % str(tuple(all_vendors)),
            )
        else:
            query_to_run = query_to_run.replace("__CLIENT_FILTER__", "")

        if "mode" in filters:
            query_to_run = query_to_run.replace(
                "__MODE_FILTER__",
                "and gg.payment_mode ilike '%s'" % str(filters["mode"]).lower(),
            )
        else:
            query_to_run = query_to_run.replace("__MODE_FILTER__", "")

        query_to_run = query_to_run.replace("__ORDER_BY__", order_by)
        query_to_run = query_to_run.replace("__ORDER_TYPE__", order_type)

        if download_flag:
            cur.execute(query_to_run.replace("__PAGINATION__", ""))
            order_qs = cur.fetchall()
            filename = (
                str(client_prefix)
                + "_EXPORT_transit_delays_"
                + "".join(random.choices(string.ascii_letters + string.digits, k=8))
                + ".csv"
            )
            with open(filename, "w") as mycsvfile:
                cw = csv.writer(mycsvfile)
                cw.writerow(TRANSIT_DELAY_DOWNLOAD_HEADERS)
                for order in order_qs:
                    try:
                        new_row = list()
                        new_row.append(str(order[1]))
                        new_row.append(str(order[2]))
                        new_row.append(str(order[3]))
                        new_row.append(str(order[4]))
                        new_row.append(order[5].strftime("%Y-%m-%d %H:%M:%S") if order[5] else "N/A")
                        new_row.append(order[6].strftime("%Y-%m-%d") if order[6] else "N/A")
                        new_row.append(str(order[7]))
                        new_row.append(str(order[8]))
                        new_row.append(order[9].strftime("%Y-%m-%d %H:%M:%S") if order[9] else "N/A")
                        new_row.append(str(order[10]))
                        new_row.append(str(order[11]))
                        new_row.append(str(order[12]))
                        cw.writerow(new_row)
                    except Exception as e:
                        pass

            s3 = session.resource("s3")
            bucket = s3.Bucket("wareiqfiles")
            bucket.upload_file(filename, "downloads/" + filename, ExtraArgs={"ACL": "public-read"})
            state_url = "https://wareiqfiles.s3.amazonaws.com/downloads/" + filename
            os.remove(filename)
            return jsonify({"url": state_url, "success": True}), 200

        count_query = "select count(*) from (" + query_to_run.replace("__PAGINATION__", "") + ") xx"
        cur.execute(count_query)
        total_count = cur.fetchone()[0]
        query_to_run = query_to_run.replace(
            "__PAGINATION__",
            "OFFSET %s LIMIT %s" % (str((page - 1) * per_page), str(per_page)),
        )
        cur.execute(query_to_run)
        order_qs = cur.fetchall()
        data = list()
        for order in order_qs:
            st_obj = dict()
            st_obj["unique_id"] = order[0]
            st_obj["order_id"] = order[1]
            st_obj["status"] = order[2]
            st_obj["shipping"] = {
                "courier": order[4],
                "awb": order[3],
                "zone": order[8],
            }
            st_obj["shipped_date"] = order[5].strftime("%Y-%m-%d %H:%M:%S") if order[5] else "N/A"
            st_obj["pdd"] = order[6].strftime("%Y-%m-%d") if order[6] else "N/A"
            st_obj["delayed_by_days"] = order[7]
            st_obj["last_scan_time"] = order[9].strftime("%Y-%m-%d %H:%M:%S") if order[9] else "N/A"
            st_obj["customer"] = {
                "name": order[10],
                "phone": order[11],
                "email": order[12],
            }
            data.append(st_obj)

        response["data"] = data
        response["success"] = True
        total_pages = math.ceil(total_count / per_page)
        response["meta"]["pagination"] = {
            "total": total_count,
            "per_page": per_page,
            "current_page": page,
            "total_pages": total_pages,
        }

        return jsonify(response), 200
    except Exception as e:
        conn.rollback()
        return jsonify(response), 400


@analytics_blueprint.route("/analytics/v1/undelivered/rtoDelays", methods=["POST"])
@authenticate_restful
def get_rto_delays(resp):
    response = {"success": False, "data": [], "meta": {}}
    cur = conn.cursor()
    try:
        data = json.loads(request.data)
        auth_data = resp.get("data")
        if not auth_data:
            return jsonify({"msg": "Authentication Failed"}), 400

        if auth_data["user_group"] == "warehouse":
            response["data"] = {}
            return jsonify(response), 200

        page = data.get("page", 1)
        per_page = data.get("per_page", 10)
        order_by = data.get("sort_by", "delayed_by_days")
        order_type = data.get("sort", "asc")
        filters = data.get("filters", {})
        download_flag = request.args.get("download", None)

        client_prefix = auth_data.get("client_prefix")

        query_to_run = select_rto_delays_query

        all_vendors = None
        if auth_data["user_group"] == "multi-vendor":
            all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
            all_vendors = all_vendors.vendor_list

        if auth_data["user_group"] == "client":
            query_to_run = query_to_run.replace(
                "__CLIENT_FILTER__",
                "AND aa.client_prefix='%s'" % auth_data["client_prefix"],
            )
        elif all_vendors:
            query_to_run = query_to_run.replace(
                "__CLIENT_FILTER__",
                "AND aa.client_prefix in %s" % str(tuple(all_vendors)),
            )
        else:
            query_to_run = query_to_run.replace("__CLIENT_FILTER__", "")

        if "mode" in filters:
            query_to_run = query_to_run.replace(
                "__MODE_FILTER__",
                "and gg.payment_mode ilike '%s'" % str(filters["mode"]).lower(),
            )
        else:
            query_to_run = query_to_run.replace("__MODE_FILTER__", "")

        query_to_run = query_to_run.replace("__ORDER_BY__", order_by)
        query_to_run = query_to_run.replace("__ORDER_TYPE__", order_type)

        if download_flag:
            cur.execute(query_to_run.replace("__PAGINATION__", ""))
            order_qs = cur.fetchall()
            filename = (
                str(client_prefix)
                + "_EXPORT_rto_delays_"
                + "".join(random.choices(string.ascii_letters + string.digits, k=8))
                + ".csv"
            )
            with open(filename, "w") as mycsvfile:
                cw = csv.writer(mycsvfile)
                cw.writerow(RTO_DELAY_DOWNLOAD_HEADERS)
                for order in order_qs:
                    try:
                        new_row = list()
                        new_row.append(str(order[1]))
                        new_row.append(str(order[2]))
                        new_row.append(str(order[3]))
                        new_row.append(str(order[4]))
                        new_row.append(order[5].strftime("%Y-%m-%d %H:%M:%S") if order[5] else "N/A")
                        new_row.append(str(order[6]))
                        new_row.append(str(order[7]))
                        new_row.append(order[8].strftime("%Y-%m-%d %H:%M:%S") if order[8] else "N/A")
                        new_row.append(str(order[9]))
                        new_row.append(str(order[10]))
                        new_row.append(str(order[11]))
                        cw.writerow(new_row)
                    except Exception as e:
                        pass

            s3 = session.resource("s3")
            bucket = s3.Bucket("wareiqfiles")
            bucket.upload_file(filename, "downloads/" + filename, ExtraArgs={"ACL": "public-read"})
            state_url = "https://wareiqfiles.s3.amazonaws.com/downloads/" + filename
            os.remove(filename)
            return jsonify({"url": state_url, "success": True}), 200

        count_query = "select count(*) from (" + query_to_run.replace("__PAGINATION__", "") + ") xx"
        cur.execute(count_query)
        total_count = cur.fetchone()[0]

        query_to_run = query_to_run.replace(
            "__PAGINATION__",
            "OFFSET %s LIMIT %s" % (str((page - 1) * per_page), str(per_page)),
        )
        cur.execute(query_to_run)
        order_qs = cur.fetchall()
        data = list()
        for order in order_qs:
            st_obj = dict()
            st_obj["unique_id"] = order[0]
            st_obj["order_id"] = order[1]
            st_obj["status"] = order[2]
            st_obj["shipping"] = {
                "courier": order[4],
                "awb": order[3],
                "zone": order[7],
            }
            st_obj["return_mark_date"] = order[5].strftime("%Y-%m-%d %H:%M:%S") if order[5] else "N/A"
            st_obj["delayed_by_days"] = order[6]
            st_obj["last_scan_time"] = order[8].strftime("%Y-%m-%d %H:%M:%S") if order[8] else "N/A"
            st_obj["customer"] = {
                "name": order[9],
                "phone": order[10],
                "email": order[11],
            }
            data.append(st_obj)

        response["data"] = data
        response["success"] = True
        total_pages = math.ceil(total_count / per_page)
        response["meta"]["pagination"] = {
            "total": total_count,
            "per_page": per_page,
            "current_page": page,
            "total_pages": total_pages,
        }

        return jsonify(response), 200
    except Exception as e:
        conn.rollback()
        return jsonify(response), 400


@analytics_blueprint.route("/analytics/v1/undelivered/ndrReasons", methods=["GET"])
@authenticate_restful
def get_ndr_reasons(resp):
    response = {"success": False, "data": [], "meta": {}}
    cur = conn.cursor()
    try:
        auth_data = resp.get("data")
        if not auth_data:
            return jsonify({"msg": "Authentication Failed"}), 400

        if auth_data["user_group"] == "warehouse":
            response["data"] = {}
            return jsonify(response), 200

        download_flag = request.args.get("download", None)

        client_prefix = auth_data.get("client_prefix")
        if not download_flag:
            query_to_run = select_ndr_reason_query
        else:
            query_to_run = select_ndr_reason_orders_query

        all_vendors = None
        if auth_data["user_group"] == "multi-vendor":
            all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
            all_vendors = all_vendors.vendor_list

        if auth_data["user_group"] == "client":
            query_to_run = query_to_run.replace(
                "__CLIENT_FILTER__",
                "AND cc.client_prefix='%s'" % auth_data["client_prefix"],
            )
        elif all_vendors:
            query_to_run = query_to_run.replace(
                "__CLIENT_FILTER__",
                "AND cc.client_prefix in %s" % str(tuple(all_vendors)),
            )
        else:
            query_to_run = query_to_run.replace("__CLIENT_FILTER__", "")

        if download_flag:
            cur.execute(query_to_run)
            order_qs = cur.fetchall()
            filename = (
                str(client_prefix)
                + "_EXPORT_ndr_reasons_"
                + "".join(random.choices(string.ascii_letters + string.digits, k=8))
                + ".csv"
            )
            with open(filename, "w") as mycsvfile:
                cw = csv.writer(mycsvfile)
                cw.writerow(NDR_ORDERS_DOWNLOAD_HEADERS)
                for order in order_qs:
                    try:
                        new_row = list()
                        new_row.append(str(order[0]))
                        new_row.append(str(order[1]))
                        new_row.append(str(order[2]))
                        new_row.append(str(order[3]))
                        new_row.append(str(order[4]))
                        if order[4] in ("reattempt", "cancelled") and order[5] in (
                            "text",
                            "call",
                        ):
                            new_row.append("customer")
                        elif order[4] in ("reattempt", "cancelled") and order[5] == "manual":
                            new_row.append("seller")
                        else:
                            new_row.append("")
                        new_row.append(str(order[6]))
                        new_row.append(str(order[7]))
                        new_row.append(order[8].strftime("%-d %b") if order[8] else "")
                        new_row.append(order[9] if order[9] else "")
                        new_row.append(order[10] if order[10] else "")
                        new_row.append(order[11] if order[11] else "")
                        cw.writerow(new_row)
                    except Exception as e:
                        pass

            s3 = session.resource("s3")
            bucket = s3.Bucket("wareiqfiles")
            bucket.upload_file(filename, "downloads/" + filename, ExtraArgs={"ACL": "public-read"})
            state_url = "https://wareiqfiles.s3.amazonaws.com/downloads/" + filename
            os.remove(filename)
            return jsonify({"url": state_url, "success": True}), 200

        cur.execute(query_to_run)
        order_qs = cur.fetchall()
        data = list()
        for order in order_qs:
            st_obj = dict()
            st_obj["reason"] = order[0]
            st_obj["total_count"] = order[1]
            st_obj["reattempt_requested"] = order[2]
            st_obj["cancellation_confirmed"] = order[3]
            st_obj["current_out_for_delivery"] = order[4]
            data.append(st_obj)

        response["data"] = data
        response["success"] = True
        return jsonify(response), 200
    except Exception as e:
        conn.rollback()
        return jsonify(response), 400


@analytics_blueprint.route("/analytics/v1/inventory", methods=["POST"])
@authenticate_restful
def inventory_analytics(resp):
    """This function generates statistics for each product of a given client
    that is either available in a warehouse or that has active sales in the past
    requested time period.
    """
    response = {"success": False, "data": {}, "meta": {}}
    cur = conn.cursor()

    try:
        auth_data = resp.get("data")
        client_prefix = auth_data.get("client_prefix")

        # Threshold percentage in the range [0, 1] above which quantity is considered overstock
        overstock_threshold = 0

        if not auth_data:
            return jsonify({"msg": "Authentication Failed"}), 401

        # Extract data from payload
        try:
            data = json.loads(request.data)
            warehouses = data.get("warehouses")
            previous_sales_start_date = data.get("previous_sales_start_date")
            previous_sales_end_date = (
                datetime.strptime(data.get("previous_sales_end_date"), "%Y-%m-%d") + timedelta(days=1)
            ).strftime("%Y-%m-%d")
            future_time_period = int(data.get("future_time_period"))
            # Future number of days to be considered for calculating expected sales to determine overstock
            overstock_timeline = int(data.get("future_time_period"))
            expected_growth = float(data.get("expected_growth"))
            search_key = data.get("search_key")
            sort_by = data.get("sort_by")
            page = int(data.get("page"))
            per_page = int(data.get("per_page"))
        except Exception as e:
            response["data"] = {}
            return jsonify(response), 400

        past_time_period = (
            datetime.strptime(previous_sales_end_date, "%Y-%m-%d")
            - datetime.strptime(previous_sales_start_date, "%Y-%m-%d")
            - timedelta(days=1)
        ).days

        # Query to get stats on each product
        query_to_run = inventory_analytics_query.format(
            client_prefix, previous_sales_start_date, previous_sales_end_date
        )

        # Update warehouse filter
        if warehouses == "all":
            query_to_run = query_to_run.replace("__WAREHOUSE_FILTER__", "")
        else:
            if isinstance(warehouses, list):
                if len(warehouses) == 1:
                    query_to_run = query_to_run.replace(
                        "__WAREHOUSE_FILTER__",
                        "AND aa.warehouse_prefix IN {0}".format("('{0}')".format(str(warehouses[0]))),
                    )
                else:
                    query_to_run = query_to_run.replace(
                        "__WAREHOUSE_FILTER__",
                        "AND aa.warehouse_prefix IN {0}".format(str(tuple(warehouses))),
                    )
            else:
                query_to_run = query_to_run.replace("__WAREHOUSE_FILTER__", "")

        # Search key filter
        if search_key:
            query_to_run = query_to_run.replace(
                "__SEARCH_KEY_FILTER__",
                "AND (aa.product_name ILIKE '%{0}%' OR aa.sku ILIKE '%{0}%')".format(search_key),
            )
        else:
            query_to_run = query_to_run.replace("__SEARCH_KEY_FILTER__", "")

        # Sort wise filter logic
        if sort_by == "stock_out":
            query_to_run = query_to_run.replace(
                "__STOCK_OUT_FILTER__",
                "AND COALESCE(aa.sales*{0}*{1}/{2}, 0) > COALESCE(aa.available_quantity, 0)".format(
                    (1 + expected_growth), future_time_period, past_time_period
                ),
            )
            query_to_run = query_to_run.replace("__OVER_STOCK_FILTER__", "")
            query_to_run = query_to_run.replace("__BEST_SELLER_FILTER__", "")
            query_to_run = query_to_run.replace(
                "__SORT_BY__",
                "ORDER BY COALESCE(aa.available_quantity, 0) / NULLIF(aa.sales, 0) ASC, aa.sales DESC",
            )
        elif sort_by == "over_stock":
            query_to_run = query_to_run.replace("__STOCK_OUT_FILTER__", "")
            query_to_run = query_to_run.replace(
                "__OVER_STOCK_FILTER__",
                "AND COALESCE(aa.sales*{0}*{1}/{2}, 0) <= COALESCE(aa.available_quantity, 0)".format(
                    (1 + expected_growth), future_time_period, past_time_period
                ),
            )
            query_to_run = query_to_run.replace("__BEST_SELLER_FILTER__", "")
            query_to_run = query_to_run.replace(
                "__SORT_BY__",
                "ORDER BY COALESCE(aa.sales, 0) - COALESCE(aa.available_quantity, 0) ASC",
            )
        elif sort_by == "best_seller":
            query_to_run = query_to_run.replace("__STOCK_OUT_FILTER__", "")
            query_to_run = query_to_run.replace("__OVER_STOCK_FILTER__", "")
            query_to_run = query_to_run.replace(
                "__BEST_SELLER_FILTER__", "AND ((aa.sales IS NOT NULL) OR (NOT (aa.sales = 0)))"
            )
            query_to_run = query_to_run.replace(
                "__SORT_BY__",
                "ORDER BY aa.sales DESC NULLS LAST",
            )
        else:
            response["data"] = {}
            return jsonify(response), 400

        count_query = query_to_run.replace("__PAGINATION__", "")
        cur.execute(count_query)
        total_count = cur.rowcount

        query_to_run = query_to_run.replace(
            "__PAGINATION__",
            "OFFSET {0} LIMIT {1}".format((page - 1) * per_page, per_page),
        )
        cur.execute(query_to_run)
        stats = cur.fetchall()

        # Process the query result
        data = list()
        for stat in stats:
            data_obj = dict()
            data_obj["product"] = {
                "master_id": stat[1],
                "sku": stat[2],
                "name": stat[3],
            }
            data_obj["warehouse_prefix"] = stat[4]
            data_obj["available_qty"] = 0 if not stat[5] else int(stat[5])
            data_obj["sales"] = 0 if not stat[6] else int(stat[6])
            data_obj["in_transit_qty"] = 0 if not stat[7] else int(stat[7])
            data_obj["ead"] = None if not stat[8] else datetime.strftime(stat[8], "%d-%m-%Y")
            data_obj["sku_velocity"] = round(data_obj["sales"] / past_time_period, 2)
            if data_obj["sku_velocity"] != 0:
                data_obj["days_left"] = max(int(data_obj["available_qty"] / data_obj["sku_velocity"]), 0)
            else:
                data_obj["days_left"] = "Infinity"
            data_obj["qty_to_restock"] = (
                math.ceil(data_obj["sku_velocity"] * (1 + expected_growth) * future_time_period)
                - data_obj["available_qty"]
            )
            data_obj["overstock"] = max(
                data_obj["available_qty"]
                - math.ceil((1 + overstock_threshold) * data_obj["sku_velocity"] * overstock_timeline),
                0,
            )
            data.append(data_obj)

        response["data"] = data
        total_pages = math.ceil(total_count / per_page)
        response["meta"]["pagination"] = {
            "total": total_count,
            "per_page": per_page,
            "current_page": page,
            "total_pages": total_pages,
        }
        response["success"] = True
        return jsonify(response), 200
    except Exception as e:
        conn.rollback()
        return jsonify(response), 500


@analytics_blueprint.route("/analytics/v1/inventory/get_filters", methods=["GET"])
@authenticate_restful
def get_inventory_filters(resp):
    response = {"success": False, "data": {}}
    cur = conn.cursor()

    try:
        auth_data = resp.get("data")
        client_prefix = auth_data.get("client_prefix")

        if not auth_data:
            return jsonify({"msg": "Authentication Failed"}), 401

        # Run query to get filters on warehouses and number of products in each warehouse
        query_to_run = inventory_analytics_filters_query.format(client_prefix)
        cur.execute(query_to_run)
        filters = cur.fetchall()

        # Process the query result
        data = list()
        for filter in filters:
            data.append({"warehouse_prefix": filter[0], "product_count": filter[1]})

        response["data"] = data
        response["success"] = True
        return jsonify(response), 200
    except Exception as e:
        conn.rollback()
        return jsonify(response), 500


@analytics_blueprint.route("/analytics/v1/inventory/in_transit", methods=["POST"])
@authenticate_restful
def inventory_snapshot(resp):
    response = {"success": False, "data": {}, "meta": {}}
    cur = conn.cursor()

    try:
        auth_data = resp.get("data")
        client_prefix = auth_data.get("client_prefix")

        if not auth_data:
            return jsonify({"msg": "Authentication Failed"}), 401

        # Extract data from payload
        try:
            data = json.loads(request.data)
            warehouses = data.get("warehouses")
            search_key = data.get("search_key")
            page = int(data.get("page"))
            per_page = int(data.get("per_page"))
        except Exception as e:
            response["data"] = {}
            return jsonify(response), 400

        # Run query to get status on each in transit order to warehouse
        query_to_run = inventory_analytics_in_transit_query.format(client_prefix, (page - 1) * per_page, per_page)

        # Update warehouse filter
        if warehouses == "all":
            query_to_run = query_to_run.replace("__WAREHOUSE_FILTER__", "")
        else:
            if isinstance(warehouses, list):
                if len(warehouses) == 1:
                    query_to_run = query_to_run.replace(
                        "__WAREHOUSE_FILTER__",
                        "AND aa.warehouse_prefix IN {0}".format("('{0}')".format(str(warehouses[0]))),
                    )
                else:
                    query_to_run = query_to_run.replace(
                        "__WAREHOUSE_FILTER__",
                        "AND aa.warehouse_prefix IN {0}".format(str(tuple(warehouses))),
                    )
            else:
                query_to_run = query_to_run.replace("__WAREHOUSE_FILTER__", "")

        # Search key filter
        if search_key:
            query_to_run = query_to_run.replace(
                "__SEARCH_KEY_FILTER__",
                "AND (aa.name ILIKE '%{0}%' OR aa.sku ILIKE '%{0}%')".format(search_key),
            )
        else:
            query_to_run = query_to_run.replace("__SEARCH_KEY_FILTER__", "")

        count_query = query_to_run.replace("__PAGINATION__", "")
        cur.execute(count_query)
        total_count = cur.rowcount

        query_to_run = query_to_run.replace(
            "__PAGINATION__", "OFFSET {0} LIMIT {1}".format((page - 1) * per_page, per_page)
        )
        cur.execute(query_to_run)
        in_transit_orders = cur.fetchall()

        # Process the query result
        data = list()
        for order in in_transit_orders:
            data_obj = dict()
            data_obj["product"] = {
                "master_id": order[1],
                "sku": order[2],
                "name": order[3],
            }
            data_obj["warehouse_prefix"] = order[4]
            data_obj["in_transit_qty"] = 0 if not order[5] else int(order[5])
            data_obj["ead"] = None if not order[6] else datetime.strftime(order[6], "%d-%m-%Y")
            data.append(data_obj)

        response["data"] = data
        total_pages = math.ceil(total_count / per_page)
        response["meta"]["pagination"] = {
            "total": total_count,
            "per_page": per_page,
            "current_page": page,
            "total_pages": total_pages,
        }
        response["success"] = True
        return jsonify(response), 200
    except Exception as e:
        conn.rollback()
        return jsonify(response), 500
