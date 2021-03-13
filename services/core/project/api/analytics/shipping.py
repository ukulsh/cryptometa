import boto3, os, random, string, csv
from datetime import datetime, timedelta
from sqlalchemy import or_, func
from flask import Blueprint, request, jsonify
from flask_restful import Api
from project import db
from project.api.models import MultiVendor, Orders, OrdersPayments, CodVerification, NDRVerification
from project.api.utils import authenticate_restful
from project.api.utilities.db_utils import DbConnection
from project.api.queries import select_state_performance_query, select_top_selling_state_query, \
    select_courier_performance_query, select_zone_performance_query

shipping_blueprint = Blueprint('analytics', __name__)
api = Api(shipping_blueprint)

conn = DbConnection.get_db_connection_instance()
conn_2 = DbConnection.get_pincode_db_connection_instance()

session = boto3.Session(
    aws_access_key_id='AKIAWRT2R3KC3YZUBFXY',
    aws_secret_access_key='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',
)

STATE_DOWNLOAD_HEADERS = ["State", "TotalOrders", "OrderPerc", "AvgShipCost", "AvgTransitDays", "RTOPerc", "CODPerc", "AvgRevenue", "MostFrequentZone"]
COURIER_DOWNLOAD_HEADERS = ["Courier", "TotalOrders", "OrderPerc", "AvgShipCost", "AvgTransitDays", "RTOPerc", "DeliveredPerc", "DeliveredWithinSLA", "NDRPerc"]
ZONE_DOWNLOAD_HEADERS = ["Zone", "TotalOrders", "OrderPerc", "AvgShipCost", "AvgTransitDays", "RTOPerc", "DeliveredPerc", "DeliveredWithinSLA", "NDRPerc"]


@shipping_blueprint.route('/analytics/v1/shipping/statePerformance', methods=['GET'])
@authenticate_restful
def get_state_performance(resp):
    response = {"success": False}
    cur = conn.cursor()
    try:
        auth_data = resp.get('data')
        if not auth_data:
            return jsonify({"msg": "Authentication Failed"}), 400

        if auth_data['user_group'] == 'warehouse':
            response['data'] = {}
            return jsonify(response), 200

        from_date = request.args.get('from')
        to_date = request.args.get('to')

        if not from_date:
            from_date = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=30)
            from_date = from_date.strftime('%Y-%m-%d')

        if to_date:
            to_date = datetime.strptime(to_date, '%Y-%m-%d')
            to_date = to_date+timedelta(days=1)
            to_date = to_date.strftime('%Y-%m-%d')
        else:
            to_date = datetime.utcnow() + timedelta(hours=5.5) + timedelta(days=1)
            to_date = to_date.strftime('%Y-%m-%d')

        client_prefix = auth_data.get('client_prefix')

        query_to_run = select_state_performance_query%(from_date, to_date)

        all_vendors = None
        if auth_data['user_group'] == 'multi-vendor':
            all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
            all_vendors = all_vendors.vendor_list

        if auth_data['user_group'] == 'client':
            query_to_run = query_to_run.replace("__CLIENT_FILTER__",
                                                    "AND aa.client_prefix='%s'" % auth_data['client_prefix'])
        elif all_vendors:
            query_to_run = query_to_run.replace("__CLIENT_FILTER__",
                                                    "AND aa.client_prefix in %s" % str(tuple(all_vendors)))
        else:
            query_to_run = query_to_run.replace("__CLIENT_FILTER__", "")

        cur.execute(query_to_run)
        state_qs = cur.fetchall()
        if request.args.get('download'):
            filename = str(client_prefix) + "_EXPORT_states_" + ''.join(
                random.choices(string.ascii_letters + string.digits, k=8)) + ".csv"
            with open(filename, 'w') as mycsvfile:
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
                        cw.writerow(new_row)
                    except Exception as e:
                        pass

            s3 = session.resource('s3')
            bucket = s3.Bucket("wareiqfiles")
            bucket.upload_file(filename, "downloads/" + filename, ExtraArgs={'ACL': 'public-read'})
            state_url = "https://wareiqfiles.s3.amazonaws.com/downloads/" + filename
            os.remove(filename)
            return jsonify({"url": state_url, "success":True}), 200

        data = list()
        for state in state_qs:
            st_obj = dict()
            st_obj['state'] = state[0]
            st_obj['total_orders'] = state[1]
            st_obj['order_perc'] = float(state[2]) if state[2] else None
            st_obj['avg_ship_cost'] = float(state[3]) if state[3] else None
            st_obj['avg_tras_days'] = float(state[4]) if state[4] else None
            st_obj['rto_perc'] = float(state[5]) if state[5] else None
            st_obj['rev_per_order'] = float(state[6]) if state[6] else None
            st_obj['freq_zone'] = state[7]
            st_obj['cod_perc'] = float(state[8]) if state[8] else None
            data.append(st_obj)

        response['data'] = data
        response['success'] = True

        return jsonify(response), 200
    except Exception as e:
        conn.rollback()
        return jsonify(response), 400


@shipping_blueprint.route('/analytics/v1/shipping/courierPerformance', methods=['GET'])
@authenticate_restful
def get_courier_performance(resp):
    response = {"success": False}
    cur = conn.cursor()
    try:
        auth_data = resp.get('data')
        if not auth_data:
            return jsonify({"msg": "Authentication Failed"}), 400

        if auth_data['user_group'] == 'warehouse':
            response['data'] = {}
            return jsonify(response), 200

        from_date = request.args.get('from')
        to_date = request.args.get('to')

        if not from_date:
            from_date = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=30)
            from_date = from_date.strftime('%Y-%m-%d')

        if to_date:
            to_date = datetime.strptime(to_date, '%Y-%m-%d')
            to_date = to_date+timedelta(days=1)
            to_date = to_date.strftime('%Y-%m-%d')
        else:
            to_date = datetime.utcnow() + timedelta(hours=5.5) + timedelta(days=1)
            to_date = to_date.strftime('%Y-%m-%d')

        client_prefix = auth_data.get('client_prefix')

        query_to_run = select_courier_performance_query%(from_date, to_date)

        all_vendors = None
        if auth_data['user_group'] == 'multi-vendor':
            all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
            all_vendors = all_vendors.vendor_list

        if auth_data['user_group'] == 'client':
            query_to_run = query_to_run.replace("__CLIENT_FILTER__",
                                                    "AND aa.client_prefix='%s'" % auth_data['client_prefix'])
        elif all_vendors:
            query_to_run = query_to_run.replace("__CLIENT_FILTER__",
                                                    "AND aa.client_prefix in %s" % str(tuple(all_vendors)))
        else:
            query_to_run = query_to_run.replace("__CLIENT_FILTER__", "")

        cur.execute(query_to_run)
        state_qs = cur.fetchall()
        if request.args.get('download'):
            filename = str(client_prefix) + "_EXPORT_couriers_" + ''.join(
                random.choices(string.ascii_letters + string.digits, k=8)) + ".csv"
            with open(filename, 'w') as mycsvfile:
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

            s3 = session.resource('s3')
            bucket = s3.Bucket("wareiqfiles")
            bucket.upload_file(filename, "downloads/" + filename, ExtraArgs={'ACL': 'public-read'})
            state_url = "https://wareiqfiles.s3.amazonaws.com/downloads/" + filename
            os.remove(filename)
            return jsonify({"url": state_url, "success":True}), 200

        data = list()
        for state in state_qs:
            st_obj = dict()
            st_obj['courier'] = state[0]
            st_obj['total_orders'] = state[1]
            st_obj['order_perc'] = float(state[2]) if state[2] else None
            st_obj['avg_ship_cost'] = float(state[3]) if state[3] else None
            st_obj['avg_tras_days'] = float(state[4]) if state[4] else None
            st_obj['rto_perc'] = float(state[5]) if state[5] else None
            st_obj['delivered_perc'] = float(state[6]) if state[6] else None
            st_obj['del_within_sla'] = float(state[7]) if state[7] else None
            st_obj['ndr_perc'] = float(state[8]) if state[8] else None
            data.append(st_obj)

        response['data'] = data
        response['success'] = True

        return jsonify(response), 200
    except Exception as e:
        conn.rollback()
        return jsonify(response), 400


@shipping_blueprint.route('/analytics/v1/shipping/zonePerformance', methods=['GET'])
@authenticate_restful
def get_zone_performance(resp):
    response = {"success": False}
    cur = conn.cursor()
    try:
        auth_data = resp.get('data')
        if not auth_data:
            return jsonify({"msg": "Authentication Failed"}), 400

        if auth_data['user_group'] == 'warehouse':
            response['data'] = {}
            return jsonify(response), 200

        from_date = request.args.get('from')
        to_date = request.args.get('to')

        if not from_date:
            from_date = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=30)
            from_date = from_date.strftime('%Y-%m-%d')

        if to_date:
            to_date = datetime.strptime(to_date, '%Y-%m-%d')
            to_date = to_date+timedelta(days=1)
            to_date = to_date.strftime('%Y-%m-%d')
        else:
            to_date = datetime.utcnow() + timedelta(hours=5.5) + timedelta(days=1)
            to_date = to_date.strftime('%Y-%m-%d')

        client_prefix = auth_data.get('client_prefix')

        query_to_run = select_zone_performance_query%(from_date, to_date)

        all_vendors = None
        if auth_data['user_group'] == 'multi-vendor':
            all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
            all_vendors = all_vendors.vendor_list

        if auth_data['user_group'] == 'client':
            query_to_run = query_to_run.replace("__CLIENT_FILTER__",
                                                    "AND aa.client_prefix='%s'" % auth_data['client_prefix'])
        elif all_vendors:
            query_to_run = query_to_run.replace("__CLIENT_FILTER__",
                                                    "AND aa.client_prefix in %s" % str(tuple(all_vendors)))
        else:
            query_to_run = query_to_run.replace("__CLIENT_FILTER__", "")

        cur.execute(query_to_run)
        state_qs = cur.fetchall()
        if request.args.get('download'):
            filename = str(client_prefix) + "_EXPORT_zone_" + ''.join(
                random.choices(string.ascii_letters + string.digits, k=8)) + ".csv"
            with open(filename, 'w') as mycsvfile:
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

            s3 = session.resource('s3')
            bucket = s3.Bucket("wareiqfiles")
            bucket.upload_file(filename, "downloads/" + filename, ExtraArgs={'ACL': 'public-read'})
            state_url = "https://wareiqfiles.s3.amazonaws.com/downloads/" + filename
            os.remove(filename)
            return jsonify({"url": state_url, "success":True}), 200

        data = list()
        for state in state_qs:
            st_obj = dict()
            st_obj['zone'] = state[0]
            st_obj['total_orders'] = state[1]
            st_obj['order_perc'] = float(state[2]) if state[2] else None
            st_obj['avg_ship_cost'] = float(state[3]) if state[3] else None
            st_obj['avg_tras_days'] = float(state[4]) if state[4] else None
            st_obj['rto_perc'] = float(state[5]) if state[5] else None
            st_obj['delivered_perc'] = float(state[6]) if state[6] else None
            st_obj['del_within_sla'] = float(state[7]) if state[7] else None
            st_obj['ndr_perc'] = float(state[8]) if state[8] else None
            data.append(st_obj)

        response['data'] = data
        response['success'] = True

        return jsonify(response), 200
    except Exception as e:
        conn.rollback()
        return jsonify(response), 400

@shipping_blueprint.route('/analytics/v1/shipping/topStates', methods=['GET'])
@authenticate_restful
def get_top_states(resp):
    response = {"success": False}
    cur = conn.cursor()
    try:
        auth_data = resp.get('data')
        if not auth_data:
            return jsonify({"msg": "Authentication Failed"}), 400

        if auth_data['user_group'] == 'warehouse':
            response['data'] = {}
            return jsonify(response), 200

        from_date = request.args.get('from')
        to_date = request.args.get('to')

        if not from_date:
            from_date = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=30)
            from_date = from_date.strftime('%Y-%m-%d')

        if to_date:
            to_date = datetime.strptime(to_date, '%Y-%m-%d')
            to_date = to_date+timedelta(days=1)
            to_date = to_date.strftime('%Y-%m-%d')
        else:
            to_date = datetime.utcnow() + timedelta(hours=5.5) + timedelta(days=1)
            to_date = to_date.strftime('%Y-%m-%d')

        client_prefix = auth_data.get('client_prefix')

        query_to_run = select_top_selling_state_query%(from_date, to_date)

        all_vendors = None
        if auth_data['user_group'] == 'multi-vendor':
            all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
            all_vendors = all_vendors.vendor_list

        if auth_data['user_group'] == 'client':
            query_to_run = query_to_run.replace("__CLIENT_FILTER__",
                                                    "AND aa.client_prefix='%s'" % auth_data['client_prefix'])
        elif all_vendors:
            query_to_run = query_to_run.replace("__CLIENT_FILTER__",
                                                    "AND aa.client_prefix in %s" % str(tuple(all_vendors)))
        else:
            query_to_run = query_to_run.replace("__CLIENT_FILTER__", "")

        cur.execute(query_to_run)
        state_qs = cur.fetchall()
        data = list()
        for state in state_qs:
            st_obj = dict()
            st_obj['state'] = state[0]
            st_obj['total_orders'] = state[1]
            st_obj['order_perc'] = float(state[2]) if state[2] else None
            data.append(st_obj)

        response['data'] = data
        response['success'] = True

        return jsonify(response), 200
    except Exception as e:
        conn.rollback()
        return jsonify(response), 400