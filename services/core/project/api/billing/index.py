import json
import re
import io
import csv
import math
from flask import Blueprint, request, make_response, jsonify
from flask_restful import Api, Resource
from datetime import datetime, timedelta
from sqlalchemy import or_, and_
from project import db
from project.api.models import MultiVendor, WalletPassbook
from project.api.utils import authenticate_restful, check_client_order_ids
from project.api.utilities.db_utils import DbConnection
from project.api.queries import select_wallet_deductions_query, select_wallet_remittance_query, \
    select_wallet_remittance_orders_query, select_wallet_reconciliation_query
from project.api.utilities.s3_utils import process_upload_logo_file


billing_blueprint = Blueprint('billing', __name__)
api = Api(billing_blueprint)

conn = DbConnection.get_db_connection_instance()
conn_2 = DbConnection.get_pincode_db_connection_instance()

DEDUCTIONS_DOWNLOAD_HEADERS = ["Time", "Status", "Courier", "AWB", "order ID", "COD cost", "Forward cost", "Return cost",
                               "Management Fee", "Subtotal", "Total", "Zone", "Weight Charged"]

RECHARGES_DOWNLOAD_HEADERS = ["Payment Time", "Amount", "Transaction ID", "status"]

REMITTANCE_DOWNLOAD_HEADERS = ["Order ID", "Order Date", "Courier", "AWB", "Payment Mode", "Amount", "Delivered Date"]

RECONCILIATION_DOWNLOAD_HEADERS = ["Order ID", "Raised Date", "AWB", "Courier", "Entered Weight",
                                                       "Charged Weight", "Expected Amount", "Charged Amount", "Status",
                                                       "Dispute Raised Date", "Remark"]

class WalletDeductions(Resource):

    method_decorators = [authenticate_restful]

    def post(self, resp):
        try:
            cur = conn.cursor()
            response = {'status':'success', 'data': dict(), "meta": dict()}
            data = json.loads(request.data)
            page = data.get('page', 1)
            per_page = data.get('per_page', 10)
            if int(per_page) > 250:
                return {"success": False, "error": "upto 250 results allowed per page"}, 401
            search_key = data.get('search_key', '')
            filters = data.get('filters', {})
            download_flag = request.args.get("download", None)
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404
            if auth_data.get('user_group') in ('super-admin', 'client', 'multi-vendor'):
                client_prefix = auth_data.get('client_prefix')
                query_to_execute = select_wallet_deductions_query

                if filters:
                    if 'courier' in filters:
                        if len(filters['courier'])==1:
                            wh_filter = "AND bb.courier_name in ('%s')"%filters['courier'][0]
                        else:
                            wh_filter = "AND bb.courier_name in %s"%str(tuple(filters['courier']))

                        query_to_execute = query_to_execute.replace('__COURIER_FILTER__', wh_filter)
                    if 'client' in filters:
                        if len(filters['client'])==1:
                            cl_filter = "AND dd.client_prefix in ('%s')"%filters['client'][0]
                        else:
                            cl_filter = "AND dd.client_prefix in %s"%str(tuple(filters['client']))

                        query_to_execute = query_to_execute.replace('__CLIENT_FILTER__', cl_filter)
                    if 'time' in filters:
                        filter_date_start = filters['time'][0][0:19].replace('T',' ')
                        filter_date_end = filters['time'][1][0:19].replace('T',' ')
                        query_to_execute = query_to_execute.replace("__DATE_TIME_FILTER__", "AND aa.status_time between '%s' and '%s'" %(filter_date_start, filter_date_end))
                if auth_data['user_group'] == 'client':
                    query_to_execute = query_to_execute.replace('__CLIENT_FILTER__', "AND dd.client_prefix = '%s'"%client_prefix)
                if auth_data['user_group'] == 'multi-vendor':
                    cur.execute("SELECT vendor_list FROM multi_vendor WHERE client_prefix='%s';" % client_prefix)
                    vendor_list = cur.fetchone()[0]
                    query_to_execute = query_to_execute.replace("__MV_CLIENT_FILTER__",
                                                        "AND dd.client_prefix in %s" % str(tuple(vendor_list)))

                else:
                    query_to_execute = query_to_execute.replace("__MV_CLIENT_FILTER__", "")

                query_to_execute = query_to_execute.replace('__CLIENT_FILTER__',"").replace('__COURIER_FILTER__', "").replace('__DATE_TIME_FILTER__', '')
                query_to_execute = query_to_execute.replace('__SEARCH_KEY__',search_key)

                if download_flag:
                    query_to_run = query_to_execute.replace('__PAGINATION__', "")
                    query_to_run = re.sub(r"""__.+?__""", "", query_to_run)
                    cur.execute(query_to_run)
                    deductions_qs_data = cur.fetchall()
                    si = io.StringIO()
                    cw = csv.writer(si)
                    cw.writerow(DEDUCTIONS_DOWNLOAD_HEADERS)
                    for deduction in deductions_qs_data:
                        try:
                            new_row = list()
                            total_charge = deduction[9]
                            total_charge += deduction[12] if deduction[12] else 5
                            new_row.append(deduction[0].strftime("%Y-%m-%d %H:%M:%S") if deduction[0] else "N/A")
                            new_row.append(str(deduction[1]))
                            new_row.append(str(deduction[2]))
                            new_row.append(str(deduction[3]))
                            new_row.append(str(deduction[4]))
                            new_row.append(str(deduction[6]))
                            new_row.append(str(deduction[7]))
                            new_row.append(str(deduction[8]))
                            new_row.append(str(deduction[12]) if deduction[12] else '5')
                            new_row.append(str(total_charge))
                            new_row.append(str(total_charge*1.18))
                            new_row.append(str(deduction[10]))
                            new_row.append(str(deduction[11]))
                            cw.writerow(new_row)
                        except Exception as e:
                            pass

                    output = make_response(si.getvalue())
                    filename = str(client_prefix)+"_EXPORT.csv"
                    output.headers["Content-Disposition"] = "attachment; filename="+filename
                    output.headers["Content-type"] = "text/csv"
                    return output

                cur.execute("SELECT COALESCE(sum(tot_amount+total_charge), 0), count(*) FROM ("+query_to_execute.replace('__PAGINATION__', "")+") xx")
                ret_amount = cur.fetchone()
                total_count = ret_amount[1]
                total_deductions = ret_amount[0]

                query_to_execute = query_to_execute.replace('__PAGINATION__', "OFFSET %s LIMIT %s"%(str((page-1)*per_page), str(per_page)))

                balance = round(total_deductions*1.18, 1)
                cur.execute(query_to_execute)
                ret_data = list()
                fetch_data = cur.fetchall()
                for entry in fetch_data:
                    ret_obj = dict()
                    ret_obj['time'] = entry[0].strftime("%d %b %Y, %I:%M %p")
                    ret_obj['status'] = entry[1]
                    ret_obj['courier'] = entry[2]
                    ret_obj['awb'] = entry[3]
                    ret_obj['tracking_link'] = "http://webapp.wareiq.com/tracking/"+str(entry[3])
                    ret_obj['order_id'] = entry[4]
                    ret_obj['unique_id'] = entry[5]
                    total_charge = None
                    if entry[9]:
                        total_charge = float(entry[9])
                        total_charge += float(entry[12]) if entry[12] else 5
                        total_charge = total_charge*1.18
                    ret_obj['amount'] = round(total_charge, 1) if total_charge else None
                    ret_obj['zone'] = entry[10]
                    ret_obj['weight_charged'] = round(entry[11], 2) if entry[11] else None
                    ret_data.append(ret_obj)
                response['data'] = ret_data
                response['expense'] = balance

                total_pages = math.ceil(total_count/per_page)
                response['meta']['pagination'] = {'total': total_count,
                                                  'per_page':per_page,
                                                  'current_page': page,
                                                  'total_pages':total_pages}

                return response, 200
        except Exception as e:
            return {"success": False, "error":str(e.args[0])}, 404

    def get(self, resp):
        try:
            cur = conn.cursor()
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404
            all_vendors = None
            if auth_data['user_group'] == 'multi-vendor':
                all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == auth_data['client_prefix']).first()
                all_vendors = all_vendors.vendor_list
            filters = dict()
            query_to_run_courier = """SELECT cc.courier_name, count(*) FROM client_deductions aa
                                        LEFT JOIN shipments bb on aa.shipment_id=bb.id
                                        LEFT JOIN master_couriers cc on bb.courier_id=cc.id
                                        LEFT JOIN orders dd on bb.order_id=dd.id
                                        WHERE aa.deduction_time>'2020-04-01'
                                        __CLIENT_FILTER__
                                        GROUP BY courier_name
                                        ORDER BY courier_name"""

            if auth_data['user_group'] == 'client':
                query_to_run_courier = query_to_run_courier.replace("__CLIENT_FILTER__",
                                                    "AND dd.client_prefix='%s'" % auth_data['client_prefix'])
            elif auth_data['user_group'] in ('super-admin', 'multi-vendor'):
                query_to_run_client = """SELECT cc.client_prefix, count(*) FROM client_deductions aa
                                        LEFT JOIN shipments bb on aa.shipment_id=bb.id
                                        LEFT JOIN orders cc on bb.order_id=cc.id
                                        WHERE aa.deduction_time>'2020-04-01'
                                        __CLIENT_FILTER__
                                        GROUP BY client_prefix
                                        ORDER BY client_prefix"""
                if all_vendors:
                    query_to_run_client = query_to_run_client.replace("__CLIENT_FILTER__", "AND cc.client_prefix in %s"%str(tuple(all_vendors)))
                    query_to_run_courier = query_to_run_courier.replace("__CLIENT_FILTER__",
                                                                        "AND dd.client_prefix in %s" % str(
                                                                            tuple(all_vendors)))
                else:
                    query_to_run_client = query_to_run_client.replace("__CLIENT_FILTER__", "")
                    query_to_run_courier = query_to_run_courier.replace("__CLIENT_FILTER__", "")

                cur.execute(query_to_run_client)
                client_data = cur.fetchall()
                filters['client'] = list()
                for client in client_data:
                    if client[0]:
                        filters['client'].append({client[0]: client[1]})
            else:
                query_to_run_courier = query_to_run_courier.replace("__CLIENT_FILTER__","")

            cur.execute(query_to_run_courier)
            courier_data = cur.fetchall()
            filters['courier'] = list()
            for courier in courier_data:
                if courier[0]:
                    filters['courier'].append({courier[0]: courier[1]})

            return {"success": True, "filters": filters}, 200

        except Exception as e:
            return {"success": False, "msg": ""}, 404


class WalletRecharges(Resource):

    method_decorators = [authenticate_restful]

    def post(self, resp):
        try:
            cur = conn.cursor()
            response = {'status':'success', 'data': dict(), "meta": dict()}
            data = json.loads(request.data)
            page = data.get('page', 1)
            per_page = data.get('per_page', 10)
            download_flag = request.args.get("download", None)
            if int(per_page) > 250:
                return {"success": False, "error": "upto 250 results allowed per page"}, 401
            search_key = data.get('search_key', '')
            filters = data.get('filters', {})
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404
            if auth_data.get('user_group') in ('super-admin', 'client', 'multi-vendor'):
                client_prefix = auth_data.get('client_prefix')
                query_to_execute = """SELECT recharge_time, recharge_amount, bank_transaction_id, status FROM client_recharges aa
                                    WHERE (transaction_id ilike '%__SEARCH_KEY__%' or bank_transaction_id ilike '%__SEARCH_KEY__%')
                                    AND aa.status!='pending'
                                    __CLIENT_FILTER__
                                    __MV_CLIENT_FILTER__
                                    __DATE_TIME_FILTER__
                                    ORDER BY recharge_time DESC
                                    __PAGINATION__"""

                if filters:
                    if 'client' in filters:
                        if len(filters['client'])==1:
                            cl_filter = "AND client_prefix in ('%s')"%filters['client'][0]
                        else:
                            cl_filter = "AND client_prefix in %s"%str(tuple(filters['client']))

                        query_to_execute = query_to_execute.replace('__CLIENT_FILTER__', cl_filter)
                    if 'recharge_time' in filters:
                        filter_date_start = filters['recharge_time'][0][0:19].replace('T',' ')
                        filter_date_end = filters['recharge_time'][1][0:19].replace('T',' ')
                        query_to_execute = query_to_execute.replace("__DATE_TIME_FILTER__", "AND recharge_time between '%s' and '%s'" %(filter_date_start, filter_date_end))
                if auth_data['user_group'] == 'client':
                    query_to_execute = query_to_execute.replace('__CLIENT_FILTER__', "AND client_prefix = '%s'"%client_prefix)
                if auth_data['user_group'] == 'multi-vendor':
                    cur.execute("SELECT vendor_list FROM multi_vendor WHERE client_prefix='%s';" % client_prefix)
                    vendor_list = cur.fetchone()[0]
                    query_to_execute = query_to_execute.replace("__MV_CLIENT_FILTER__",
                                                        "AND client_prefix in %s" % str(tuple(vendor_list)))

                else:
                    query_to_execute = query_to_execute.replace("__MV_CLIENT_FILTER__", "")

                query_to_execute = query_to_execute.replace('__CLIENT_FILTER__',"").replace('__COURIER_FILTER__', "").replace('__DATE_TIME_FILTER__', '')
                query_to_execute = query_to_execute.replace('__SEARCH_KEY__',search_key)

                if download_flag:
                    query_to_run = query_to_execute.replace('__PAGINATION__', "")
                    query_to_run = re.sub(r"""__.+?__""", "", query_to_run)
                    cur.execute(query_to_run)
                    recharges_qs_data = cur.fetchall()
                    si = io.StringIO()
                    cw = csv.writer(si)
                    cw.writerow(RECHARGES_DOWNLOAD_HEADERS)
                    for recharge in recharges_qs_data:
                        try:
                            new_row = list()
                            new_row.append(recharge[0].strftime("%Y-%m-%d %H:%M:%S") if recharge[0] else "N/A")
                            new_row.append(str(recharge[1]))
                            new_row.append(str(recharge[2]))
                            new_row.append(str(recharge[3]))
                            cw.writerow(new_row)
                        except Exception as e:
                            pass

                    output = make_response(si.getvalue())
                    filename = str(client_prefix)+"_EXPORT.csv"
                    output.headers["Content-Disposition"] = "attachment; filename="+filename
                    output.headers["Content-type"] = "text/csv"
                    return output

                cur.execute(query_to_execute.replace('__PAGINATION__', ""))
                total_count = cur.rowcount
                query_to_execute = query_to_execute.replace('__PAGINATION__', "OFFSET %s LIMIT %s"%(str((page-1)*per_page), str(per_page)))

                cur.execute(query_to_execute)
                ret_data = list()
                fetch_data = cur.fetchall()
                for entry in fetch_data:
                    ret_obj = dict()
                    ret_obj['time'] = entry[0].strftime("%d %b %Y, %I:%M %p")
                    ret_obj['recharge_amount'] = entry[1]
                    ret_obj['transaction_id'] = entry[2]
                    ret_obj['status'] = entry[3]

                    ret_data.append(ret_obj)
                response['data'] = ret_data

                total_pages = math.ceil(total_count/per_page)
                response['meta']['pagination'] = {'total': total_count,
                                                  'per_page':per_page,
                                                  'current_page': page,
                                                  'total_pages':total_pages}

                return response, 200
        except Exception as e:
            return {"success": False, "error":str(e.args[0])}, 400

    def get(self, resp):
        try:
            cur = conn.cursor()
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404

            all_vendors = None
            if auth_data['user_group'] == 'multi-vendor':
                all_vendors = db.session.query(MultiVendor).filter(
                    MultiVendor.client_prefix == auth_data['client_prefix']).first()
                all_vendors = all_vendors.vendor_list

            filters = dict()
            if auth_data['user_group'] in ('super-admin', 'multi-vendor'):
                query_to_run= """SELECT client_prefix, count(*) FROM client_recharges
                                __CLIENT_FILTER__
                                GROUP BY client_prefix
                                ORDER BY client_prefix"""
                if all_vendors:
                    query_to_run = query_to_run.replace("__CLIENT_FILTER__", "WHERE client_prefix in %s"%str(tuple(all_vendors)))
                else:
                    query_to_run = query_to_run.replace("__CLIENT_FILTER__", "")

                cur.execute(query_to_run)
                client_data = cur.fetchall()
                filters['client'] = list()
                for client in client_data:
                    if client[0]:
                        filters['client'].append({client[0]:client[1]})

            return {"success": True, "filters": filters}, 200

        except Exception as e:
            return {"success": False, "msg": ""}, 400


class WalletRemittance(Resource):

    method_decorators = [authenticate_restful]

    def post(self, resp):
        try:
            cur = conn.cursor()
            response = {'status':'success', 'data': dict(), "meta": dict()}
            data = json.loads(request.data)
            page = data.get('page', 1)
            per_page = data.get('per_page', 10)
            if int(per_page) > 250:
                return {"success": False, "error": "upto 250 results allowed per page"}, 401
            search_key = data.get('search_key', '')
            filters = data.get('filters', {})
            unique_id = request.args.get("unique_id", None)
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404
            if auth_data.get('user_group') in ('super-admin', 'client', 'multi-vendor'):
                client_prefix = auth_data.get('client_prefix')
                if unique_id:
                    query_to_execute = select_wallet_remittance_orders_query.replace('__REMITTANCE_ID__', str(unique_id))
                    cur.execute(query_to_execute)
                    remittance_qs_data = cur.fetchall()
                    si = io.StringIO()
                    cw = csv.writer(si)
                    cw.writerow(REMITTANCE_DOWNLOAD_HEADERS)
                    for remittance in remittance_qs_data:
                        try:
                            new_row = list()
                            new_row.append(str(remittance[1]))
                            new_row.append(remittance[2].strftime("%Y-%m-%d %H:%M:%S") if remittance[2] else "N/A")
                            new_row.append(str(remittance[3]))
                            new_row.append(str(remittance[4]))
                            new_row.append(str(remittance[5]))
                            new_row.append(str(remittance[6]))
                            new_row.append(remittance[7].strftime("%Y-%m-%d %H:%M:%S") if remittance[7] else "N/A")
                            cw.writerow(new_row)
                        except Exception as e:
                            pass

                    output = make_response(si.getvalue())
                    filename = str(client_prefix) + "_EXPORT.csv"
                    output.headers["Content-Disposition"] = "attachment; filename=" + filename
                    output.headers["Content-type"] = "text/csv"
                    return output

                query_to_execute = select_wallet_remittance_query
                if filters:
                    if 'client' in filters:
                        if len(filters['client'])==1:
                            cl_filter = "AND client_prefix in ('%s')"%filters['client'][0]
                        else:
                            cl_filter = "AND client_prefix in %s"%str(tuple(filters['client']))

                        query_to_execute = query_to_execute.replace('__CLIENT_FILTER__', cl_filter)
                    if 'remittance_date' in filters:
                        filter_date_start = filters['remittance_date'][0][0:19].replace('T',' ')
                        filter_date_end = filters['remittance_date'][1][0:19].replace('T',' ')
                        query_to_execute = query_to_execute.replace("__REMITTANCE_DATE_FILTER__", "AND remittance_date between '%s' and '%s'" %(filter_date_start, filter_date_end))

                    if 'status' in filters:
                        if len(filters['status']) == 1:
                            st_filter = "AND status in ('%s')"%filters['status'][0]
                        else:
                            st_filter = "AND status in %s"%str(tuple(filters['status']))
                        query_to_execute = query_to_execute.replace('__STATUS_FILTER__', st_filter)

                query_to_execute = query_to_execute.replace('__STATUS_FILTER__', "")

                if auth_data['user_group'] == 'client':
                    query_to_execute = query_to_execute.replace('__CLIENT_FILTER__', "AND client_prefix = '%s'"%client_prefix)

                if auth_data['user_group'] == 'multi-vendor':
                    cur.execute("SELECT vendor_list FROM multi_vendor WHERE client_prefix='%s';" % client_prefix)
                    vendor_list = cur.fetchone()[0]
                    query_to_execute = query_to_execute.replace("__MV_CLIENT_FILTER__",
                                                        "AND client_prefix in %s" % str(tuple(vendor_list)))

                else:
                    query_to_execute = query_to_execute.replace("__MV_CLIENT_FILTER__", "")

                if search_key:
                    query_to_execute = query_to_execute.replace('__SEARCH_KEY_FILTER__', "AND transaction_id ilike '%__SEARCH_KEY__%'".replace('__SEARCH_KEY__', search_key))

                query_to_execute = query_to_execute.replace('__SEARCH_KEY_FILTER__',"").replace('__CLIENT_FILTER__', "").replace('__REMITTANCE_DATE_FILTER__', '')
                cur.execute(query_to_execute.replace('__PAGINATION__', ""))
                total_count = cur.rowcount
                query_to_execute = query_to_execute.replace('__PAGINATION__', "OFFSET %s LIMIT %s"%(str((page-1)*per_page), str(per_page)))

                cur.execute(query_to_execute)
                ret_data = list()
                fetch_data = cur.fetchall()
                for entry in fetch_data:
                    ret_obj = dict()
                    ret_obj['unique_id'] = entry[0]
                    ret_obj['remittance_id'] = entry[2]
                    ret_obj['remittance_date'] = entry[3].strftime("%d-%m-%Y")
                    ret_obj['status'] = entry[4]
                    ret_obj['transaction_id'] = entry[5]
                    ret_obj['amount'] = round(entry[6]) if entry[6] else None
                    ret_data.append(ret_obj)
                response['data'] = ret_data

                total_pages = math.ceil(total_count/per_page)
                response['meta']['pagination'] = {'total': total_count,
                                                  'per_page':per_page,
                                                  'current_page': page,
                                                  'total_pages':total_pages}

                return response, 200
        except Exception as e:
            return {"success": False, "error":str(e.args[0])}, 404

    def get(self, resp):
        try:
            cur = conn.cursor()
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404

            all_vendors = None
            if auth_data['user_group'] == 'multi-vendor':
                all_vendors = db.session.query(MultiVendor).filter(
                    MultiVendor.client_prefix == auth_data['client_prefix']).first()
                all_vendors = all_vendors.vendor_list
            filters = dict()
            query_to_run_status = """SELECT status, count(*) FROM cod_remittance
                                                    __CLIENT_FILTER__
                                                    GROUP BY status
                                                    ORDER BY status"""
            if auth_data['user_group']=='multi-vendor':
                query_to_run_status = query_to_run_status.replace("__CLIENT_FILTER__",
                                                                  "WHERE client_prefix in %s" % str(tuple(all_vendors)))
            elif auth_data['user_group']=='client':
                query_to_run_status = query_to_run_status.replace("__CLIENT_FILTER__",
                                                                  "WHERE client_prefix = '%s'" % auth_data['client_prefix'])
            else:
                query_to_run_status = query_to_run_status.replace("__CLIENT_FILTER__", "")

            filters['status'] = list()
            cur.execute(query_to_run_status)
            status_data = cur.fetchall()
            for status in status_data:
                filters['status'].append({status[0]: status[1]})

            if auth_data['user_group'] in ('super-admin', 'multi-vendor'):
                query_to_run_client = """SELECT client_prefix, count(*) FROM cod_remittance
                                        __CLIENT_FILTER__
                                        GROUP BY client_prefix
                                        ORDER BY client_prefix"""

                if all_vendors:
                    query_to_run_client = query_to_run_client.replace("__CLIENT_FILTER__", "WHERE client_prefix in %s"%str(tuple(all_vendors)))
                else:
                    query_to_run_client = query_to_run_client.replace("__CLIENT_FILTER__", "")

                cur.execute(query_to_run_client)
                client_data = cur.fetchall()
                filters['client'] = list()

                for client in client_data:
                    if client[0]:
                        filters['client'].append({client[0]:client[1]})

            return {"success": True, "filters": filters}, 200

        except Exception as e:
            return {"success": False, "msg": ""}, 404


class WalletReconciliation(Resource):

    method_decorators = [authenticate_restful]

    def post(self, resp):
        try:
            cur = conn.cursor()
            response = {'status':'success', 'data': dict(), "meta": dict()}
            data = json.loads(request.data)
            page = data.get('page', 1)
            per_page = data.get('per_page', 10)
            if int(per_page) > 250:
                return {"success": False, "error": "upto 250 results allowed per page"}, 401
            search_key = data.get('search_key', '')
            filters = data.get('filters', {})
            download_flag = request.args.get("download", None)
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404
            if auth_data.get('user_group') in ('super-admin', 'client', 'multi-vendor'):
                client_prefix = auth_data.get('client_prefix')
                query_to_execute = select_wallet_reconciliation_query
                if filters:
                    if 'courier' in filters:
                        if len(filters['courier'])==1:
                            wh_filter = "AND cc.courier_name in ('%s')"%filters['courier'][0]
                        else:
                            wh_filter = "AND cc.courier_name in %s"%str(tuple(filters['courier']))

                        query_to_execute = query_to_execute.replace('__COURIER_FILTER__', wh_filter)
                    if 'client' in filters:
                        if len(filters['client'])==1:
                            cl_filter = "AND ee.client_prefix in ('%s')"%filters['client'][0]
                        else:
                            cl_filter = "AND ee.client_prefix in %s"%str(tuple(filters['client']))

                        query_to_execute = query_to_execute.replace('__CLIENT_FILTER__', cl_filter)
                    if 'time' in filters:
                        filter_date_start = filters['time'][0][0:19].replace('T',' ')
                        filter_date_end = filters['time'][1][0:19].replace('T',' ')
                        query_to_execute = query_to_execute.replace("__DATE_TIME_FILTER__", "AND aa.raised_date between '%s' and '%s'" %(filter_date_start, filter_date_end))

                    if 'status' in filters:
                        if len(filters['status'])==1:
                            cl_filter = "AND ff.status in ('%s')"%filters['status'][0]
                        else:
                            cl_filter = "AND ff.status in %s"%str(tuple(filters['status']))
                        query_to_execute = query_to_execute.replace('__STATUS_FILTER__', cl_filter)

                if auth_data['user_group'] == 'client':
                    query_to_execute = query_to_execute.replace('__CLIENT_FILTER__', "AND ee.client_prefix = '%s'"%client_prefix)
                if auth_data['user_group'] == 'multi-vendor':
                    cur.execute("SELECT vendor_list FROM multi_vendor WHERE client_prefix='%s';" % client_prefix)
                    vendor_list = cur.fetchone()[0]
                    query_to_execute = query_to_execute.replace("__MV_CLIENT_FILTER__",
                                                        "AND ee.client_prefix in %s" % str(tuple(vendor_list)))

                else:
                    query_to_execute = query_to_execute.replace("__MV_CLIENT_FILTER__", "")

                query_to_execute = query_to_execute.replace('__CLIENT_FILTER__',"").replace('__COURIER_FILTER__', "").replace('__DATE_TIME_FILTER__', '').replace('__STATUS_FILTER__', '')
                query_to_execute = query_to_execute.replace('__SEARCH_KEY__',search_key)

                if download_flag:
                    query_to_run = query_to_execute.replace('__PAGINATION__', "")
                    query_to_run = re.sub(r"""__.+?__""", "", query_to_run)
                    cur.execute(query_to_run)
                    reconciliation_qs_data = cur.fetchall()
                    si = io.StringIO()
                    cw = csv.writer(si)
                    cw.writerow(RECONCILIATION_DOWNLOAD_HEADERS)
                    for deduction in reconciliation_qs_data:
                        try:
                            new_row = list()
                            new_row.append(str(deduction[0]))
                            new_row.append(deduction[2].strftime("%Y-%m-%d %H:%M:%S") if deduction[2] else "N/A")
                            new_row.append(str(deduction[4]))
                            new_row.append(str(deduction[3]))
                            new_row.append(str(deduction[5]))
                            new_row.append(str(deduction[6]))
                            new_row.append(str(deduction[7]))
                            new_row.append(str(deduction[8]))
                            new_row.append(str(deduction[9]))
                            new_row.append(deduction[11].strftime("%Y-%m-%d %H:%M:%S") if deduction[11] else "N/A")
                            new_row.append(str(deduction[10]))
                            if auth_data.get('user_group') == 'super-admin':
                                new_row.append(str(deduction[12]))
                            cw.writerow(new_row)
                        except Exception as e:
                            pass

                    output = make_response(si.getvalue())
                    filename = str(client_prefix)+"_EXPORT.csv"
                    output.headers["Content-Disposition"] = "attachment; filename="+filename
                    output.headers["Content-type"] = "text/csv"
                    return output

                cur.execute("SELECT count(*) FROM ("+query_to_execute.replace('__PAGINATION__', "")+") xx")
                ret_amount = cur.fetchone()
                total_count = ret_amount[0]

                query_to_execute = query_to_execute.replace('__PAGINATION__', "OFFSET %s LIMIT %s"%(str((page-1)*per_page), str(per_page)))

                cur.execute(query_to_execute)
                ret_data = list()
                fetch_data = cur.fetchall()
                for entry in fetch_data:
                    ret_obj = dict()
                    day_left = entry[2] + timedelta(hours=5.5) + timedelta(days=7)
                    day_left = (day_left - datetime.utcnow() +timedelta(hours=5.5)).days
                    ret_obj['discrepency_time'] = entry[2].strftime("%d %b %Y, %I:%M %p")
                    ret_obj['dispute_time'] = entry[11].strftime("%d %b %Y, %I:%M %p") if entry[11] else None
                    ret_obj['status'] = entry[9]
                    ret_obj['awb'] = entry[4]
                    ret_obj['courier'] = entry[3]
                    ret_obj['order_id'] = entry[0]
                    ret_obj['unique_id'] = entry[1]
                    ret_obj['entered_weight'] = entry[5]
                    ret_obj['charged_weight'] = entry[6]
                    ret_obj['expected_amount'] = entry[7]
                    ret_obj['charged_amount'] = entry[8]
                    ret_obj['days_left'] = day_left if day_left>=0 else None
                    ret_data.append(ret_obj)
                response['data'] = ret_data

                total_pages = math.ceil(total_count/per_page)
                response['meta']['pagination'] = {'total': total_count,
                                                  'per_page':per_page,
                                                  'current_page': page,
                                                  'total_pages':total_pages}

                return response, 200
        except Exception as e:
            return {"success": False, "error":str(e.args[0])}, 404

    def get(self, resp):
        try:
            cur = conn.cursor()
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404
            all_vendors = None
            if auth_data['user_group'] == 'multi-vendor':
                all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == auth_data['client_prefix']).first()
                all_vendors = all_vendors.vendor_list
            filters = dict()
            query_to_run_courier = """SELECT cc.courier_name, count(*) FROM weight_discrepency aa
                                        LEFT JOIN shipments bb on aa.shipment_id=bb.id
                                        LEFT JOIN master_couriers cc on bb.courier_id=cc.id
                                        LEFT JOIN orders dd on bb.order_id=dd.id
                                        WHERE aa.raised_date>'2020-04-01'
                                        __CLIENT_FILTER__
                                        GROUP BY courier_name
                                        ORDER BY courier_name"""

            query_to_run_status = """SELECT ee.status, count(*) FROM weight_discrepency aa
                                                    LEFT JOIN shipments bb on aa.shipment_id=bb.id
                                                    LEFT JOIN orders dd on bb.order_id=dd.id
                                                    LEFT JOIN discrepency_status ee on ee.id=aa.status_id
                                                    WHERE aa.raised_date>'2020-04-01'
                                                    __CLIENT_FILTER__
                                                    GROUP BY ee.status
                                                    ORDER BY ee.status"""

            if auth_data['user_group'] == 'client':
                query_to_run_courier = query_to_run_courier.replace("__CLIENT_FILTER__",
                                                    "AND dd.client_prefix='%s'" % auth_data['client_prefix'])
                query_to_run_status = query_to_run_status.replace("__CLIENT_FILTER__",
                                                                    "AND dd.client_prefix='%s'" % auth_data[
                                                                        'client_prefix'])
            elif auth_data['user_group'] in ('super-admin', 'multi-vendor'):
                query_to_run_client = """SELECT cc.client_prefix, count(*) FROM weight_discrepency aa
                                        LEFT JOIN shipments bb on aa.shipment_id=bb.id
                                        LEFT JOIN orders cc on bb.order_id=cc.id
                                        WHERE aa.raised_date>'2020-04-01'
                                        __CLIENT_FILTER__
                                        GROUP BY client_prefix
                                        ORDER BY client_prefix"""
                if all_vendors:
                    query_to_run_client = query_to_run_client.replace("__CLIENT_FILTER__", "AND cc.client_prefix in %s"%str(tuple(all_vendors)))
                    query_to_run_courier = query_to_run_courier.replace("__CLIENT_FILTER__",
                                                                        "AND dd.client_prefix in %s" % str(
                                                                            tuple(all_vendors)))
                    query_to_run_status = query_to_run_status.replace("__CLIENT_FILTER__", "AND dd.client_prefix in %s"%str(tuple(all_vendors)))


                else:
                    query_to_run_client = query_to_run_client.replace("__CLIENT_FILTER__", "")
                    query_to_run_courier = query_to_run_courier.replace("__CLIENT_FILTER__", "")
                    query_to_run_status = query_to_run_status.replace("__CLIENT_FILTER__", "")

                cur.execute(query_to_run_client)
                client_data = cur.fetchall()
                filters['client'] = list()
                for client in client_data:
                    if client[0]:
                        filters['client'].append({client[0]: client[1]})
            else:
                query_to_run_courier = query_to_run_courier.replace("__CLIENT_FILTER__","")
                query_to_run_status = query_to_run_status.replace("__CLIENT_FILTER__","")

            cur.execute(query_to_run_courier)
            courier_data = cur.fetchall()
            filters['courier'] = list()
            for courier in courier_data:
                if courier[0]:
                    filters['courier'].append({courier[0]: courier[1]})

            cur.execute(query_to_run_status)
            status_data = cur.fetchall()
            filters['status'] = list()
            for status in status_data:
                if status[0]:
                    filters['status'].append({status[0]: status[1]})

            return {"success": True, "filters": filters}, 200

        except Exception as e:
            return {"success": False, "msg": ""}, 404


@billing_blueprint.route('/wallet/v1/getcouriercharges', methods=['POST'])
@authenticate_restful
def get_courier_charges(resp):
    response_object = {'status': 'fail'}
    try:
        auth_data = resp.get('data')
        if not auth_data:
            return {"success": False, "msg": "Auth Failed"}, 404
        if auth_data['user_group'] not in ('multi-vendor', 'client', 'super-admin'):
            return {"success": False, "msg": "Invalid user"}, 400

        post_data = request.get_json()
        source_pincode=post_data.get("source_pincode")
        destination_pincode=post_data.get("destination_pincode")
        charged_weight=post_data.get("weight")
        payment=post_data.get("payment")
        order_value=post_data.get("order_value")
        cur = conn.cursor()
        cur_2=conn_2.cursor()
        if source_pincode:
            source_pincode = str(source_pincode)
        else:
            return jsonify(response_object), 400
        if destination_pincode:
            destination_pincode = str(destination_pincode)
        else:
            return jsonify(response_object), 400
        if charged_weight:
            charged_weight = float(charged_weight)
        else:
            return jsonify(response_object), 400
        if payment:
            payment = str(payment)
        else:
            return jsonify(response_object), 400
        if order_value:
            order_value = float(order_value)

        delivery_zone = get_delivery_zone(cur_2, source_pincode, destination_pincode)
        if not delivery_zone:
            return {"success": False, "msg": "Not serviceable"}, 400

        client_prefix=auth_data.get('client_prefix')
        mapped_couriers = list()
        cost_list = list()
        cost_select_tuple = (client_prefix, )
        cur.execute(
            "SELECT __ZONE__, cod_min, cod_ratio, __ZONE_STEP__, bb.courier_name, bb.id from cost_to_clients aa "
            "LEFT JOIN master_couriers bb on aa.courier_id=bb.id "
            "WHERE client_prefix=%s;".replace(
                '__ZONE__', zone_column_mapping[delivery_zone]).replace('__ZONE_STEP__',
                                                                        zone_step_charge_column_mapping[
                                                                            delivery_zone]), cost_select_tuple)
        charge_rate_values = cur.fetchall()
        for crv in charge_rate_values:
            cost_list.append(crv)
            mapped_couriers.append(crv[4])

        courier_name_filter = ""
        if len(mapped_couriers)==1:
            courier_name_filter = "WHERE bb.courier_name != '%s'"
        elif len(mapped_couriers)>1:
            courier_name_filter = "WHERE bb.courier_name not in %s"%str(tuple(mapped_couriers))
        cur.execute(
            "SELECT __ZONE__, cod_min, cod_ratio, __ZONE_STEP__, bb.courier_name, bb.id from client_default_cost aa "
            "LEFT JOIN master_couriers bb on aa.courier_id=bb.id "
            "__COURIER_FILTER__;".replace(
                '__ZONE__', zone_column_mapping[delivery_zone]).replace('__ZONE_STEP__', zone_step_charge_column_mapping[
                                                        delivery_zone]).replace('__COURIER_FILTER__', courier_name_filter))
        charge_rate_values = cur.fetchall()
        for crv in charge_rate_values:
            cost_list.append(crv)

        response_data = list()
        for cost_obj in cost_list:
            cur.execute("select weight_offset, additional_weight_offset from master_couriers where id=%s;",
                        (cost_obj[5],))
            courier_data = cur.fetchone()
            charge_rate = cost_obj[0]
            forward_charge = charge_rate
            per_step_charge = cost_obj[3]
            per_step_charge = 0.0 if per_step_charge is None else per_step_charge
            if courier_data[0] != 0 and courier_data[1] != 0:
                if not per_step_charge:
                    per_step_charge = charge_rate
                if charged_weight > courier_data[0]:
                    forward_charge = charge_rate + math.ceil(
                        (charged_weight - courier_data[0] * 1.0) / courier_data[1]) * per_step_charge
            else:
                multiple = math.ceil(charged_weight / 0.5)
                forward_charge = charge_rate * multiple

            cod_charge = 0
            if payment.lower()=='cod':
                cod_charge = order_value * (cost_obj[2] / 100)
                if cost_obj[1] > cod_charge:
                    cod_charge = cost_obj[1]

            response_data.append({"courier": cost_obj[4],
                                  "cod_charge": cod_charge,
                                  "forward_charge": forward_charge,
                                  "total_charge":cod_charge+forward_charge})

        response_object['data'] = response_data
        response_object['zone'] = delivery_zone
        response_object['status'] = "success"

        return jsonify(response_object), 200
    except Exception as e:
        response_object['message'] = 'failed'
        return jsonify(response_object), 400


@billing_blueprint.route('/wallet/v1/acceptDiscrepency', methods=['POST'])
@authenticate_restful
def accept_discrepency(resp):
    response_object = {'status': 'fail'}
    try:
        cur = conn.cursor()
        auth_data = resp.get('data')
        if not auth_data:
            return {"success": False, "msg": "Auth Failed"}, 404
        if auth_data['user_group'] not in ('multi-vendor', 'client', 'super-admin'):
            return {"success": False, "msg": "Invalid user"}, 400

        data = json.loads(request.data)
        order_ids = data.get('order_ids')
        if not order_ids:
            return jsonify({"success": False, "msg": "please select orders"}), 400

        order_tuple_str = check_client_order_ids(order_ids, auth_data, cur)

        if not order_tuple_str:
            return jsonify({"success": False, "msg": "Invalid order ids"}), 400

        cur.execute("""UPDATE weight_discrepency SET status_id=2 WHERE shipment_id in
                        (SELECT id FROM shipments WHERE order_id in %s)""" % order_tuple_str)

        conn.commit()

        response_object['status']="Success"
        response_object['msg'] = "Discrepency Accepted"

        return jsonify(response_object), 200
    except Exception as e:
        response_object['message'] = 'failed'
        return jsonify(response_object), 400


@billing_blueprint.route('/wallet/v1/raiseDispute', methods=['POST'])
@authenticate_restful
def raise_dispute(resp):
    response_object = {'status': 'fail'}
    try:
        cur = conn.cursor()
        auth_data = resp.get('data')
        if not auth_data:
            return {"success": False, "msg": "Auth Failed"}, 404
        if auth_data['user_group'] not in ('multi-vendor', 'client', 'super-admin'):
            return {"success": False, "msg": "Invalid user"}, 400

        client_prefix = auth_data['client_prefix']

        data = request.values
        order_id = data.get('unique_id')
        remarks = data.get('remarks')
        if not order_id:
            return jsonify({"success": False, "msg": "please select order"}), 400

        file_list = list()
        for i in range(1,6):
            file = request.files.get('file'+str(i))
            if file:
                file_url = process_upload_logo_file(client_prefix, file, bucket="wareiqreconciliation", file_name=str(order_id)+"_file"+str(i), master_bucket="wareiqreconciliation")
                file_list.append(file_url)

        query_to_execute = """UPDATE weight_discrepency SET status_id=3, remarks=%s, files=%s, dispute_date=%s WHERE shipment_id in 
                        (SELECT bb.id FROM orders aa
                        LEFT JOIN shipments bb on aa.id=bb.order_id
                        WHERE aa.id = %s
                        __CLIENT_FILTER__)"""

        if auth_data['user_group'] == 'client':
            query_to_execute = query_to_execute.replace('__CLIENT_FILTER__',
                                                        "AND aa.client_prefix = '%s'" % client_prefix)
        if auth_data['user_group'] == 'multi-vendor':
            cur.execute("SELECT vendor_list FROM multi_vendor WHERE client_prefix='%s';" % client_prefix)
            vendor_list = cur.fetchone()[0]
            query_to_execute = query_to_execute.replace("__CLIENT_FILTER__",
                                                        "AND aa.client_prefix in %s" % str(tuple(vendor_list)))

        else:
            query_to_execute = query_to_execute.replace("__CLIENT_FILTER__", "")

        cur.execute(query_to_execute, (remarks, file_list, datetime.utcnow()+timedelta(hours=5.5), int(order_id)))

        conn.commit()

        response_object['status'] = "Success"
        response_object['msg'] = "Dispute Raised"

        return jsonify(response_object), 200
    except Exception as e:
        response_object['message'] = 'failed'
        return jsonify(response_object), 400


@billing_blueprint.route('/wallet/v1/passbook', methods=['POST'])
@authenticate_restful
def get_passbook(resp):
    response = {"data": list(), "meta": {}, "success": True}
    try:
        auth_data = resp.get('data')
        if not auth_data:
            return {"success": False, "msg": "Auth Failed"}, 404
        if auth_data['user_group'] not in ('client', 'super-admin'):
            return {"success": False, "msg": "Invalid user"}, 400

        post_data = request.get_json()
        filters = post_data.get('filters', {})
        search = post_data.get('search', None)
        page = post_data.get('page', 1)
        page = int(page)
        per_page = post_data.get('per_page', 20)
        per_page = int(per_page)

        passbook_qs = db.session.query(WalletPassbook).filter(WalletPassbook.client_prefix == auth_data.get('client_prefix'))
        if filters.get('categories'):
            passbook_qs = passbook_qs.filter(WalletPassbook.category.in_(filters.get('categories')))
        if search:
            passbook_qs = passbook_qs.filter(or_(WalletPassbook.descr.contains(search), WalletPassbook.ref_no.contains(search)))
        passbook_qs = passbook_qs.order_by(WalletPassbook.txn_time.desc()).paginate(page, per_page, error_out=False)
        data = list()
        for order in passbook_qs.items:
            res_obj = dict()
            res_obj['txn_time'] = order.txn_time.strftime('%Y-%m-%d %I:%M %p')
            res_obj['category'] = order.category
            res_obj['credit'] = order.credit
            res_obj['debit'] = order.debit
            res_obj['closing_balance'] = round(order.closing_balance, 2) if order.closing_balance else None
            res_obj['ref_no'] = order.ref_no
            res_obj['descr'] = order.descr
            data.append(res_obj)

        response['data'] = data
        response['meta']['pagination'] = {'total': passbook_qs.total,
                                          'per_page': passbook_qs.per_page,
                                          'current_page': passbook_qs.page,
                                          'total_pages': passbook_qs.pages}

    except Exception as e:
        return jsonify({
            'success': False
        }), 400

    return jsonify(response), 200


api.add_resource(WalletDeductions, '/wallet/v1/deductions')
api.add_resource(WalletRecharges, '/wallet/v1/payments')
api.add_resource(WalletRemittance, '/wallet/v1/remittance')
api.add_resource(WalletReconciliation, '/wallet/v1/reconciliation')


def get_delivery_zone(cur_2, pick_pincode, del_pincode):
    cur_2.execute("SELECT city from city_pin_mapping where pincode='%s';" % str(pick_pincode).rstrip())
    pickup_city = cur_2.fetchone()
    if not pickup_city:
        return None
    pickup_city = pickup_city[0]
    cur_2.execute("SELECT city from city_pin_mapping where pincode='%s';" % str(del_pincode).rstrip())
    deliver_city = cur_2.fetchone()
    if not deliver_city:
        return None
    deliver_city = deliver_city[0]
    zone_select_tuple = (pickup_city, deliver_city)
    cur_2.execute("SELECT zone_value from city_zone_mapping where zone=%s and city=%s;",
                  zone_select_tuple)
    delivery_zone = cur_2.fetchone()
    if not delivery_zone:
        return None
    delivery_zone = delivery_zone[0]
    if not delivery_zone:
        return None

    if delivery_zone in ('D1', 'D2'):
        delivery_zone = 'D'
    if delivery_zone in ('C1', 'C2'):
        delivery_zone = 'C'

    return delivery_zone


zone_column_mapping = {
    'A': 'zone_a',
    'B': 'zone_b',
    'C': 'zone_c',
    'D': 'zone_d',
    'E': 'zone_e',
}

zone_step_charge_column_mapping = {
    'A': 'a_step',
    'B': 'b_step',
    'C': 'c_step',
    'D': 'd_step',
    'E': 'e_step'
}
