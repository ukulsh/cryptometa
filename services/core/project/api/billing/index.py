import json
import re
import io
import csv
import math
from flask import Blueprint, request, make_response
from flask_restful import Api, Resource
from project import db
from project.api.models import MultiVendor
from project.api.utils import authenticate_restful
from project.api.utilities.db_utils import DbConnection
from project.api.queries import select_wallet_deductions_query, select_wallet_remittance_query, \
    select_wallet_remittance_orders_query

billing_blueprint = Blueprint('billing', __name__)
api = Api(billing_blueprint)

conn = DbConnection.get_db_connection_instance()
conn_2 = DbConnection.get_pincode_db_connection_instance()

DEDUCTIONS_DOWNLOAD_HEADERS = ["Time", "Status", "Courier", "AWB", "order ID", "COD cost", "Forward cost", "Return cost",
                               "Management Fee", "Subtotal", "Total", "Zone", "Weight Charged"]

RECHARGES_DOWNLOAD_HEADERS = ["Payment Time", "Amount", "Transaction ID", "status"]

REMITTANCE_DOWNLOAD_HEADERS = ["Order ID", "Order Date", "Courier", "AWB", "Payment Mode", "Amount", "Delivered Date"]


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
                query_total_recharge = """select COALESCE(sum(recharge_amount), 0) from client_recharges
                                        WHERE recharge_time>'2020-04-01'
                                        AND lower(status)='successful'
                                        __CLIENT_FILTER__
                                        __MV_CLIENT_FILTER__"""
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
                        query_total_recharge = query_total_recharge.replace('__CLIENT_FILTER__', cl_filter.replace('dd.', ''))
                    if 'time' in filters:
                        filter_date_start = filters['time'][0][0:19].replace('T',' ')
                        filter_date_end = filters['time'][1][0:19].replace('T',' ')
                        query_to_execute = query_to_execute.replace("__DATE_TIME_FILTER__", "AND aa.status_time between '%s' and '%s'" %(filter_date_start, filter_date_end))
                if auth_data['user_group'] == 'client':
                    query_to_execute = query_to_execute.replace('__CLIENT_FILTER__', "AND dd.client_prefix = '%s'"%client_prefix)
                    query_total_recharge = query_total_recharge.replace('__CLIENT_FILTER__', "AND client_prefix = '%s'"%client_prefix)
                if auth_data['user_group'] == 'multi-vendor':
                    cur.execute("SELECT vendor_list FROM multi_vendor WHERE client_prefix='%s';" % client_prefix)
                    vendor_list = cur.fetchone()[0]
                    query_to_execute = query_to_execute.replace("__MV_CLIENT_FILTER__",
                                                        "AND dd.client_prefix in %s" % str(tuple(vendor_list)))
                    query_total_recharge = query_total_recharge.replace('__MV_CLIENT_FILTER__', "AND client_prefix in %s"%str(tuple(vendor_list)))

                else:
                    query_to_execute = query_to_execute.replace("__MV_CLIENT_FILTER__", "")
                    query_total_recharge = query_total_recharge.replace('__MV_CLIENT_FILTER__', "")

                query_to_execute = query_to_execute.replace('__CLIENT_FILTER__',"").replace('__COURIER_FILTER__', "").replace('__DATE_TIME_FILTER__', '')
                query_total_recharge = query_total_recharge.replace('__CLIENT_FILTER__', '')
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
                            new_row.append(deduction[0].strftime("%Y-%m-%d %H:%M:%S") if deduction[0] else "N/A")
                            new_row.append(str(deduction[1]))
                            new_row.append(str(deduction[2]))
                            new_row.append(str(deduction[3]))
                            new_row.append(str(deduction[4]))
                            new_row.append(str(deduction[6]))
                            new_row.append(str(deduction[7]))
                            new_row.append(str(deduction[8]))
                            new_row.append(str(deduction[12]))
                            new_row.append(str(deduction[9] + deduction[12]))
                            new_row.append(str((deduction[9] + deduction[12])*1.18))
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

                cur.execute(query_total_recharge)
                total_recharge = cur.fetchone()[0]
                query_to_execute = query_to_execute.replace('__PAGINATION__', "OFFSET %s LIMIT %s"%(str((page-1)*per_page), str(per_page)))

                balance = round(total_recharge-total_deductions*1.18, 1)
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
                    if entry[9] and entry[12] is not None:
                        total_charge = float(entry[9]) + float(entry[12])
                        total_charge = total_charge*1.18
                    ret_obj['amount'] = round(total_charge, 1) if total_charge else None
                    ret_obj['zone'] = entry[10]
                    ret_obj['weight_charged'] = round(entry[11], 2) if entry[11] else None
                    ret_data.append(ret_obj)
                response['data'] = ret_data
                response['balance'] = balance

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
                query_to_execute = """SELECT recharge_time, recharge_amount, transaction_id, status FROM client_recharges aa
                                    WHERE (transaction_id ilike '%__SEARCH_KEY__%' or bank_transaction_id ilike '%__SEARCH_KEY__%')
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


api.add_resource(WalletDeductions, '/wallet/v1/deductions')
api.add_resource(WalletRecharges, '/wallet/v1/payments')
api.add_resource(WalletRemittance, '/wallet/v1/remittance')
