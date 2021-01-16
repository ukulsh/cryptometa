import json, random, string
import math
import re
from datetime import datetime, timedelta
import boto3
import csv
import io
import numpy as np
import os
import pandas as pd
import requests
import time
from flask_cors import cross_origin
from flask import Blueprint, request, jsonify, make_response
from flask_restful import Api, Resource
from reportlab.lib.pagesizes import landscape, A4
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
from sqlalchemy import func, or_, not_, and_
from sqlalchemy.dialects.postgresql import insert

from project import db
from project.api.models import NDRReasons, MultiVendor, NDRShipments, Orders, ClientPickups, MasterCouriers, \
    PickupPoints, Shipments, Products, ShippingAddress, OPAssociation, OrdersPayments, ClientMapping, WarehouseMapping, \
    Manifests, OrderStatus, IVRHistory, OrderPickups, BillingAddress
from project.api.queries import select_orders_list_query, available_warehouse_product_quantity, \
    fetch_warehouse_to_pick_from, select_pickups_list_query, get_selected_product_details
from project.api.utils import authenticate_restful, fill_shiplabel_data_thermal, \
    create_shiplabel_blank_page, fill_shiplabel_data, create_shiplabel_blank_page_thermal, \
    create_invoice_blank_page, fill_invoice_data, generate_picklist, generate_packlist, \
    tracking_get_xpressbees_details, tracking_get_delhivery_details, tracking_get_bluedart_details, \
    tracking_get_ecomxp_details, check_client_order_ids
from project.api.generate_manifest import fill_manifest_data
from project.api.utilities.db_utils import DbConnection

orders_blueprint = Blueprint('orders', __name__)
api = Api(orders_blueprint)


ORDERS_DOWNLOAD_HEADERS = ["Order ID", "Customer Name", "Customer Email", "Customer Phone", "Order Date",
                           "Courier", "Weight", "awb", "Expected Delivery Date", "Status", "Address_one", "Address_two",
                           "City", "State", "Country", "Pincode", "Pickup Point", "Product", "SKU", "Quantity", "Order Type",
                           "Amount", "Manifest Time", "Pickup Date", "Delivered Date", "COD Verfication", "COD Verified Via", "NDR Verfication", "NDR Verified Via","PDD"]

ORDERS_UPLOAD_HEADERS = ["order_id", "customer_name", "customer_email", "customer_phone", "address_one", "address_two",
                         "city", "state", "country", "pincode", "sku", "sku_quantity", "payment_mode", "subtotal", "shipping_charges", "warehouse", "Error"]

conn = DbConnection.get_db_connection_instance()
conn_2 = DbConnection.get_pincode_db_connection_instance()

session = boto3.Session(
    aws_access_key_id='AKIAWRT2R3KC3YZUBFXY',
    aws_secret_access_key='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',
)


class OrderList(Resource):

    method_decorators = {'post': [authenticate_restful]}

    def post(self, resp, type):
        try:
            hide_weights = None
            thirdwatch = None
            cur = conn.cursor()
            response = {'status': 'success', 'data': dict(), "meta": dict()}
            data = json.loads(request.data)
            page = data.get('page', 1)
            per_page = data.get('per_page', 10)
            search_key = data.get('search_key', '')
            search_key_on_customer_detail = data.get('search_key_on_customer_detail', '')
            since_id = data.get('since_id', None)
            filters = data.get('filters', {})
            download_flag = request.args.get("download", None)
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404

            client_prefix = auth_data.get('client_prefix')
            cur.execute("SELECT hide_weights, thirdwatch FROM client_mapping WHERE client_prefix='%s'" % client_prefix)
            try:
                mapping_data  = cur.fetchone()
                hide_weights = mapping_data[0]
                thirdwatch = mapping_data[1]
            except Exception:
                pass

            query_to_run = select_orders_list_query

            if search_key:
                regex_check = "where (aa.channel_order_id ilike '%__SEARCH_KEY__%' or awb ilike '%__SEARCH_KEY__%')"
                query_to_run = query_to_run.replace("__SEARCH_KEY_FILTER__", regex_check)
                query_to_run = query_to_run.replace("__SEARCH_KEY__", search_key)
            else:
                query_to_run = query_to_run.replace("__SEARCH_KEY_FILTER__", "where (1=1)")

            if search_key_on_customer_detail:
                regex_check_customer_details = " AND (customer_name ilike '%__SEARCH_KEY_ON_CUSTOMER_DETAILS__%' or customer_phone ilike '%__SEARCH_KEY_ON_CUSTOMER_DETAILS__%' or customer_email ilike '%__SEARCH_KEY_ON_CUSTOMER_DETAILS__%')"
                query_to_run = query_to_run.replace("__SEARCH_KEY_FILTER_ON_CUSTOMER__", regex_check_customer_details)
                query_to_run = query_to_run.replace("__SEARCH_KEY_ON_CUSTOMER_DETAILS__", search_key_on_customer_detail)

            if auth_data['user_group'] == 'client':
                query_to_run = query_to_run.replace("__CLIENT_FILTER__", "AND aa.client_prefix = '%s'" % client_prefix)
            if auth_data['user_group'] == 'warehouse':
                query_to_run = query_to_run.replace("__PICKUP_FILTER__", "AND ii.warehouse_prefix = '%s'" % auth_data.get('warehouse_prefix'))
            if auth_data['user_group'] == 'multi-vendor':
                cur.execute("SELECT vendor_list FROM multi_vendor WHERE client_prefix='%s';" % client_prefix)
                vendor_list = cur.fetchone()[0]
                query_to_run = query_to_run.replace("__MV_CLIENT_FILTER__", "AND aa.client_prefix in %s"%str(tuple(vendor_list)))
            else:
                query_to_run = query_to_run.replace("__MV_CLIENT_FILTER__", "")

            if since_id:
                query_to_run = query_to_run.replace("__SINCE_ID_FILTER__", "AND id>%s" % str(since_id))

            if type == 'new':
                query_to_run = query_to_run.replace("__TAB_STATUS_FILTER__", "AND aa.status = 'NEW'")
            elif type == 'ready_to_ship':
                query_to_run = query_to_run.replace("__TAB_STATUS_FILTER__", "AND aa.status in ('PICKUP REQUESTED','READY TO SHIP')")
            elif type == 'shipped':
                query_to_run = query_to_run.replace("__TAB_STATUS_FILTER__", "AND aa.status not in ('NEW', 'READY TO SHIP', 'PICKUP REQUESTED','NOT PICKED','CANCELED', 'CLOSED', 'PENDING PAYMENT','NEW - FAILED', 'LOST', 'NOT SHIPPED')")
            elif type == "return":
                query_to_run = query_to_run.replace("__TAB_STATUS_FILTER__", "AND (aa.status_type='RT' or (aa.status_type='DL' and aa.status='RTO'))")
            elif type == "ndr":
                query_to_run = query_to_run.replace("__TAB_STATUS_FILTER__", "AND (rr.id is not null AND aa.status='PENDING' AND aa.status_type!='RT')")
            elif type == 'all':
                pass
            else:
                return {"success": False, "msg": "Invalid URL"}, 404

            if filters:
                if 'status' in filters:
                    if len(filters['status']) == 1:
                        status_tuple = "('"+filters['status'][0]+"')"
                    else:
                        status_tuple = str(tuple(filters['status']))
                    query_to_run = query_to_run.replace("__STATUS_FILTER__", "AND aa.status in %s"% status_tuple)

                if 'courier' in filters:
                    if len(filters['courier']) == 1:
                        courier_tuple = "('"+filters['courier'][0]+"')"
                    else:
                        courier_tuple = str(tuple(filters['courier']))
                    query_to_run = query_to_run.replace("__COURIER_FILTER__", "AND courier_name in %s" %courier_tuple)

                if 'client' in filters and auth_data['user_group'] != 'client':
                    if len(filters['client']) == 1:
                        client_tuple = "('"+filters['client'][0]+"')"
                    else:
                        client_tuple = str(tuple(filters['client']))
                    query_to_run = query_to_run.replace("__CLIENT_FILTER__", "AND aa.client_prefix in %s" % client_tuple)

                if 'pickup_point' in filters:
                    if len(filters['pickup_point']) == 1:
                        pickup_tuple = "('"+filters['pickup_point'][0]+"')"
                    else:
                        pickup_tuple = str(tuple(filters['pickup_point']))
                    query_to_run = query_to_run.replace("__PICKUP_FILTER__", "AND ii.warehouse_prefix in %s" % pickup_tuple)

                if 'ndr_reason' in filters:
                    if len(filters['ndr_reason']) == 1:
                        reason_tuple = "('"+filters['ndr_reason'][0]+"')"
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
                        type_tuple = "('"+filters['order_type'][0]+"')"
                    else:
                        type_tuple = str(tuple(filters['order_type']))
                    query_to_run = query_to_run.replace("__TYPE_FILTER__", "AND upper(payment_mode) in %s" %type_tuple)

                if 'order_date' in filters:
                    filter_date_start = filters['order_date'][0][0:19].replace('T',' ')
                    filter_date_end = filters['order_date'][1][0:19].replace('T',' ')
                    query_to_run = query_to_run.replace("__ORDER_DATE_FILTER__", "AND order_date between '%s' and '%s'" %(filter_date_start, filter_date_end))

                if 'thirdwatch_score' in filters:
                    score_from = float(filters['thirdwatch_score'][0])
                    score_to = float(filters['thirdwatch_score'][1])
                    query_to_run = query_to_run.replace("__THIRDWATCH_SCORE_FILTER__", "AND uu.score between %s and %s" %(score_from, score_to))

                if 'thirdwatch_flag' in filters:
                    if len(filters['thirdwatch_flag']) == 1:
                        flag_tuple = "('"+filters['thirdwatch_flag'][0]+"')"
                    else:
                        flag_tuple = str(tuple(filters['thirdwatch_flag']))
                    query_to_run = query_to_run.replace("__TYPE_FILTER__", "AND lower(uu.flag) in %s" %flag_tuple)

                if 'pickup_time' in filters:
                    filter_date_start = filters['pickup_time'][0][0:19].replace('T',' ')
                    filter_date_end = filters['pickup_time'][1][0:19].replace('T',' ')
                    query_to_run = query_to_run.replace("__PICKUP_TIME_FILTER__", "AND pickup_time between '%s' and '%s'" %(filter_date_start, filter_date_end))

                if 'manifest_time' in filters:
                    filter_date_start = filters['manifest_time'][0][0:19].replace('T',' ')
                    filter_date_end = filters['manifest_time'][1][0:19].replace('T',' ')
                    query_to_run = query_to_run.replace("__MANIFEST_DATE_FILTER__", "AND manifest_time between '%s' and '%s'" %(filter_date_start, filter_date_end))

                if 'delivered_time' in filters:
                    filter_date_start = filters['delivered_time'][0][0:19].replace('T',' ')
                    filter_date_end = filters['delivered_time'][1][0:19].replace('T',' ')
                    query_to_run = query_to_run.replace("__PICKUP_TIME_FILTER__", "AND delivered_time between '%s' and '%s'" %(filter_date_start, filter_date_end))

                if 'edd' in filters:
                    filter_date_start = filters['edd'][0][0:19].replace('T',' ')
                    filter_date_end = filters['edd'][1][0:19].replace('T',' ')
                    query_to_run = query_to_run.replace("__EDD_FILTER__", "AND bb.edd between '%s' and '%s'" %(filter_date_start, filter_date_end))

            if download_flag:
                if not [i for i in ['order_date', 'pickup_time', 'manifest_time', 'delivered_time'] if i in filters]:
                    date_month_ago = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=31)
                    date_month_ago = date_month_ago.strftime("%Y-%m-%d %H:%M:%S")
                    query_to_run = query_to_run.replace('__ORDER_DATE_FILTER__', "AND order_date > '%s' "%date_month_ago)
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
                                new_row.append(order[2].strftime("%Y-%m-%d") if order[2] else "N/A")
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
                filename = str(client_prefix)+"_EXPORT.csv"
                output.headers["Content-Disposition"] = "attachment; filename="+filename
                output.headers["Content-type"] = "text/csv"
                return output

            count_query = "select count(*) from ("+query_to_run.replace('__PAGINATION__', "") +") xx"
            count_query = re.sub(r"""__.+?__""", "", count_query)
            count_query_prefix = '''select count(*) from (select distinct on (aa.order_date, aa.id) aa.channel_order_id as order_id, aa.id as unique_id, aa.order_date'''
            count_query_suffix = 'from orders aa'
            prefix_ind = count_query.find(count_query_prefix)
            suffix_ind = count_query.find(count_query_suffix)
            if prefix_ind == 0 and suffix_ind > 0:
                count_query = count_query[:prefix_ind+len(count_query_prefix)] + ' ' + count_query[suffix_ind:]
            cur.execute(count_query)
            total_count = cur.fetchone()[0]
            query_to_run = query_to_run.replace('__PAGINATION__', "OFFSET %s LIMIT %s" % (str((page-1)*per_page), str(per_page)))
            query_to_run = re.sub(r"""__.+?__""", "", query_to_run)
            cur.execute(query_to_run)
            orders_qs_data = cur.fetchall()
            product_detail_by_order_id = {}
            order_id_data = ','.join([str(it[1]) for it in orders_qs_data])
            if order_id_data:
                update_product_details_query = get_selected_product_details.replace('__FILTERED_ORDER_ID__', order_id_data)
                cur.execute(update_product_details_query)
                product_detail_data = cur.fetchall()
                for it in product_detail_data:
                    product_detail_by_order_id[it[0]] = [it[1], it[2], it[3], it[4], it[5]]

            response_data = list()
            for order in orders_qs_data:
                resp_obj=dict()
                resp_obj['order_id'] = order[0]
                resp_obj['unique_id'] = order[1]
                resp_obj['pickup_point'] = order[26]
                resp_obj['customer_details'] = {"name": order[13],
                                                "email": order[15],
                                                "phone": order[14],
                                                "address_one": order[16],
                                                "address_two": order[17],
                                                "city": order[18],
                                                "state": order[19],
                                                "country": order[20],
                                                "pincode": order[21],
                                                }
                resp_obj['order_date'] = order[2].strftime("%d %b %Y, %I:%M %p") if order[2] else None
                resp_obj['delivered_time'] = order[22].strftime("%d %b %Y, %I:%M %p") if order[22] else None
                resp_obj['manifest_time'] = order[34].strftime("%d %b %Y, %I:%M %p") if order[34] else None
                resp_obj['payment'] = {"mode": order[24], "amount": order[25]}

                resp_obj['product_details'] = list()
                not_shipped = None
                if order[1] in product_detail_by_order_id and product_detail_by_order_id[order[1]][0]:
                    product_data = product_detail_by_order_id[order[1]]
                    for idx, prod in enumerate(product_data[0]):
                        if not product_data[3][idx] or not product_data[4][idx]:
                            not_shipped = "Weight/dimensions not entered for product(s)"
                        resp_obj['product_details'].append({ "name": prod, "sku": product_data[1][idx], "quantity": product_data[2][idx]})

                if not not_shipped and order[12] == "Pincode not serviceable":
                    not_shipped = "Pincode not serviceable"
                elif not order[26]:
                    not_shipped = "Pickup point not assigned"
                elif order[12] and "incorrect phone" in order[12].lower():
                    not_shipped = "Invalid contact number"

                if not_shipped:
                    resp_obj['not_shipped'] = not_shipped
                if order[27]:
                    resp_obj['cod_verification'] = {"confirmed": order[28], "via": order[29]}
                if order[30]:
                    resp_obj['ndr_verification'] = {"confirmed": order[31], "via": order[32]}

                if type=='ndr':
                    resp_obj['ndr_reason'] = order[36]
                    ndr_action = None
                    if order[35] in (1,3,9,11) and order[30]:
                        if order[31] == True and order[32] in ('call','text'):
                            ndr_action = "Cancellation confirmed by customer"
                        elif order[31] == True and order[32] == 'manual':
                            ndr_action = "Cancellation confirmed by seller"
                        elif order[31] == False and order[32] == 'manual':
                            ndr_action = "Re-attempt requested by seller"
                        elif order[31] == False and order[32] in ('call','text'):
                            ndr_action = "Re-attempt requested by customer"
                        elif order[3]=='PENDING':
                            ndr_action = 'take_action'

                    resp_obj['ndr_action'] = ndr_action

                resp_obj['shipping_details'] = {"courier": order[7],
                                                "awb":order[5],
                                                "tracking_link": order[6]}
                resp_obj['dimensions'] = order[10] if not hide_weights else None
                resp_obj['weight'] = order[9] if not hide_weights else None
                resp_obj['volumetric'] = order[11] if not hide_weights else None
                resp_obj['channel_logo'] = order[33]
                if order[8]:
                    resp_obj['edd'] = order[8].strftime('%-d %b')
                if auth_data['user_group'] == 'super-admin':
                    resp_obj['remark'] = order[12]
                if type == "shipped":
                    resp_obj['status_detail'] = order[4]

                resp_obj['status'] = order[3]
                if order[3] in ('NEW','CANCELED','PENDING PAYMENT','READY TO SHIP','PICKUP REQUESTED','NOT PICKED') or not order[5]:
                    resp_obj['status_change'] = True
                if thirdwatch:
                    resp_obj['thirdwatch'] = {"score": order[41], "flag":order[40], "reasons": order[42]}
                response_data.append(resp_obj)

            response['data'] = response_data
            total_pages = math.ceil(total_count/per_page)
            response['meta']['pagination'] = {'total': total_count,
                                              'per_page': per_page,
                                              'current_page': page,
                                              'total_pages': total_pages}
            return response, 200

        except Exception as e:
            conn.rollback()
            return {"success": False, "error": str(e)}, 400


api.add_resource(OrderList, '/orders/<type>')


@orders_blueprint.route('/orders/get_filters', methods=['GET'])
@authenticate_restful
def get_orders_filters(resp):
    response = {"filters":{}, "success": True}
    auth_data = resp.get('data')
    current_tab = request.args.get('tab')
    client_prefix = auth_data.get('client_prefix')
    warehouse_prefix = auth_data.get('warehouse_prefix')
    client_qs = None
    all_vendors = None
    if auth_data['user_group'] == 'multi-vendor':
        all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
        all_vendors = all_vendors.vendor_list
    if current_tab =='ndr':
        type = {"Action Requested": 0,
                "Action Required": 0}
        cur = conn.cursor()
        query_for_type = """select ndr_verified, count(*) from
                            (select * from ndr_shipments aa
                            left join orders cc on aa.order_id=cc.id
                            left join ndr_verification bb on cc.id=bb.order_id
                            where cc.status='PENDING'
                            __CLIENT_FILTER__) xx
                            group by ndr_verified"""
        if auth_data['user_group'] == 'client':
            query_for_type = query_for_type.replace("__CLIENT_FILTER__", "AND cc.client_prefix='%s'"%auth_data['client_prefix'])
        elif all_vendors:
            query_for_type = query_for_type.replace("__CLIENT_FILTER__", "AND cc.client_prefix in %s"%str(tuple(all_vendors)))
        else:
            query_for_type = query_for_type.replace("__CLIENT_FILTER__", "")

        cur.execute(query_for_type)
        all_types = cur.fetchall()

        for type_val in all_types:
            if type_val[0] in (True, False):
                type['Action Requested'] += type_val[1]
            else:
                type['Action Required'] += type_val[1]

        type_list = list()
        for type_val in type:
            type_list.append({type_val:type[type_val]})

        reason_qs = db.session.query(NDRReasons.reason, func.count(NDRReasons.reason)) \
        .join(NDRShipments, NDRReasons.id == NDRShipments.reason_id).join(Orders, Orders.id == NDRShipments.order_id).filter(Orders.status=='PENDING').group_by(NDRReasons.reason)
        if auth_data['user_group'] == 'client':
            reason_qs = reason_qs.filter(Orders.client_prefix == client_prefix)
        if all_vendors:
            reason_qs = reason_qs.filter(Orders.client_prefix.in_(all_vendors))

        reason_qs = reason_qs.order_by(NDRReasons.reason).all()
        response['filters']['ndr_reason'] = [{x[0]: x[1]} for x in reason_qs]
        response['filters']['ndr_type'] = type_list
        if auth_data['user_group'] == 'super-admin':
            client_qs = db.session.query(Orders.client_prefix, func.count(Orders.client_prefix)).join(NDRShipments,
                        Orders.id==NDRShipments.order_id).filter(NDRShipments.reason_id!=None).group_by(Orders.client_prefix).order_by(Orders.client_prefix).all()
            response['filters']['client'] = [{x[0]: x[1]} for x in client_qs]
        elif all_vendors:
            client_qs = db.session.query(Orders.client_prefix, func.count(Orders.client_prefix)).join(NDRShipments,
                                                                                                      Orders.id == NDRShipments.order_id).filter(
                NDRShipments.reason_id != None, Orders.client_prefix.in_(all_vendors)).group_by(Orders.client_prefix).order_by(Orders.client_prefix).all()
            response['filters']['client'] = [{x[0]: x[1]} for x in client_qs]

        return jsonify(response), 200

    status_qs = db.session.query(Orders.status, func.count(Orders.status)).join(ClientPickups,
                        Orders.pickup_data_id==ClientPickups.id).join(PickupPoints, PickupPoints.id==ClientPickups.pickup_id).group_by(Orders.status)
    courier_qs = db.session.query(MasterCouriers.courier_name, func.count(MasterCouriers.courier_name)) \
        .join(Shipments, MasterCouriers.id == Shipments.courier_id).join(Orders, Orders.id == Shipments.order_id) \
        .join(ClientPickups,Orders.pickup_data_id == ClientPickups.id).join(PickupPoints, PickupPoints.id == ClientPickups.pickup_id).group_by(MasterCouriers.courier_name)
    pickup_point_qs = db.session.query(PickupPoints.warehouse_prefix, func.count(PickupPoints.warehouse_prefix)) \
        .join(ClientPickups, PickupPoints.id == ClientPickups.pickup_id).join(Orders, ClientPickups.id == Orders.pickup_data_id).group_by(PickupPoints.warehouse_prefix)

    shipped_filters = ['NEW', 'READY TO SHIP', 'PICKUP REQUESTED','NOT PICKED','CANCELED', 'CLOSED', 'PENDING PAYMENT','NEW - FAILED', 'LOST', 'NOT SHIPPED']
    if auth_data['user_group'] == 'super-admin':
        client_qs = db.session.query(Orders.client_prefix, func.count(Orders.client_prefix))
    elif auth_data['user_group'] == 'warehouse':
        client_qs = db.session.query(Orders.client_prefix, func.count(Orders.client_prefix)).join(ClientPickups,
                        Orders.pickup_data_id==ClientPickups.id).join(PickupPoints,
                        PickupPoints.id==ClientPickups.pickup_id).filter(PickupPoints.warehouse_prefix == warehouse_prefix)
    elif all_vendors:
        client_qs = db.session.query(Orders.client_prefix, func.count(Orders.client_prefix)).filter(Orders.client_prefix.in_(all_vendors))

    if auth_data['user_group'] == 'client':
        status_qs=status_qs.filter(Orders.client_prefix == client_prefix)
        courier_qs = courier_qs.filter(Orders.client_prefix == client_prefix)
        pickup_point_qs = pickup_point_qs.filter(Orders.client_prefix == client_prefix)
    if all_vendors:
        status_qs = status_qs.filter(Orders.client_prefix.in_(all_vendors))
        courier_qs = courier_qs.filter(Orders.client_prefix.in_(all_vendors))
        pickup_point_qs = pickup_point_qs.filter(Orders.client_prefix.in_(all_vendors))
    if auth_data['user_group'] == 'warehouse':
        status_qs = status_qs.filter(PickupPoints.warehouse_prefix == warehouse_prefix)
        courier_qs = courier_qs.filter(PickupPoints.warehouse_prefix == warehouse_prefix)
        pickup_point_qs = pickup_point_qs.filter(PickupPoints.warehouse_prefix == warehouse_prefix)
    if current_tab=="shipped":
        status_qs = status_qs.filter(not_(Orders.status.in_(shipped_filters)))
        courier_qs = courier_qs.filter(not_(Orders.status.in_(shipped_filters)))
        pickup_point_qs = pickup_point_qs.filter(not_(Orders.status.in_(shipped_filters)))
        if client_qs:
            client_qs = client_qs.filter(not_(Orders.status.in_(shipped_filters)))
    if current_tab=="return":
        status_qs = status_qs.filter(or_(Orders.status_type == 'RT', and_(Orders.status_type == 'DL', Orders.status == "RTO")))
        courier_qs = courier_qs.filter(or_(Orders.status_type == 'RT', and_(Orders.status_type == 'DL', Orders.status == "RTO")))
        pickup_point_qs = pickup_point_qs.filter(or_(Orders.status_type == 'RT', and_(Orders.status_type == 'DL', Orders.status == "RTO")))
        if client_qs:
            client_qs = client_qs.filter(or_(Orders.status_type == 'RT', and_(Orders.status_type == 'DL', Orders.status == "RTO")))
    if current_tab=="new":
        status_qs = status_qs.filter(Orders.status=="NEW")
        courier_qs = courier_qs.filter(Orders.status=="NEW")
        pickup_point_qs = pickup_point_qs.filter(Orders.status=="NEW")
        if client_qs:
            client_qs = client_qs.filter(Orders.status=="NEW")
    if current_tab=="ready_to_ship":
        status_qs = status_qs.filter(Orders.status.in_(["READY TO SHIP","PICKUP REQUESTED"]))
        courier_qs = courier_qs.filter(Orders.status.in_(["READY TO SHIP","PICKUP REQUESTED"]))
        pickup_point_qs = pickup_point_qs.filter(Orders.status.in_(["READY TO SHIP","PICKUP REQUESTED"]))
        if client_qs:
            client_qs = client_qs.filter(Orders.status.in_(["READY TO SHIP","PICKUP REQUESTED"]))
    status_qs = status_qs.order_by(Orders.status).all()
    response['filters']['status'] = [{x[0]:x[1]} for x in status_qs]
    courier_qs = courier_qs.order_by(MasterCouriers.courier_name).all()
    response['filters']['courier'] = [{x[0]:x[1]} for x in courier_qs]
    pickup_point_qs = pickup_point_qs.order_by(PickupPoints.warehouse_prefix).all()
    response['filters']['pickup_point'] = [{x[0]: x[1]} for x in pickup_point_qs]
    if client_qs:
        if all_vendors:
            client_qs = client_qs.filter(Orders.client_prefix.in_(all_vendors))
        client_qs = client_qs.group_by(Orders.client_prefix).order_by(Orders.client_prefix).all()
        response['filters']['client'] = [{x[0]: x[1]} for x in client_qs]

    return jsonify(response), 200


class AddOrder(Resource):

    method_decorators = [authenticate_restful]

    def post(self, resp):
        try:
            data = json.loads(request.data)
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404

            order_exists = db.session.query(Orders).filter(Orders.channel_order_id==str(data.get('order_id')).rstrip(), Orders.client_prefix==auth_data.get('client_prefix')).first()
            if order_exists:
                return {"success": False, "msg": "Order ID already exists", "unique_id":order_exists.id}, 400

            delivery_address = ShippingAddress(first_name=data.get('full_name'),
                                               address_one=data.get('address1'),
                                               address_two=data.get('address2'),
                                               city=data.get('city'),
                                               pincode=str(data.get('pincode')),
                                               state=data.get('state'),
                                               country=data.get('country'),
                                               phone=str(data.get('customer_phone'))
                                               )

            bill_obj = None
            if data.get('billing_address'):
                bill_obj = BillingAddress(first_name=data['billing_address'].get('first_name'),
                                last_name=data['billing_address'].get('last_name'),
                                address_one=data['billing_address'].get('address1'),
                                address_two=data['billing_address'].get('address2'),
                                city=data['billing_address'].get('city'),
                                pincode=str(data['billing_address'].get('pincode')),
                                state=data['billing_address'].get('state'),
                                country=data['billing_address'].get('country'),
                                phone=str(data['billing_address'].get('phone') if data['billing_address'].get('phone') else str(data.get('customer_phone'))
                                ),)
                db.session.add(bill_obj)

            pickup_filter = data.get('warehouse')
            pickup_data = None
            if pickup_filter:
                pickup_data = db.session.query(ClientPickups).join(PickupPoints, ClientPickups.pickup_id==PickupPoints.id).filter(PickupPoints.warehouse_prefix==pickup_filter)
                if auth_data.get('user_group') == 'client':
                    pickup_data = pickup_data.filter(ClientPickups.client_prefix==auth_data.get('client_prefix'))
                pickup_data = pickup_data.first()

            chargeable_weight = data.get('weight')
            if chargeable_weight:
                chargeable_weight = float(chargeable_weight)
            new_order = Orders(channel_order_id=str(data.get('order_id')).rstrip(),
                           order_date=datetime.utcnow()+timedelta(hours=5.5),
                           customer_name=data.get('full_name'),
                           customer_email=data.get('customer_email'),
                           customer_phone=data.get('customer_phone'),
                           delivery_address=delivery_address,
                           billing_address=bill_obj,
                           status="NEW",
                           client_prefix=auth_data.get('client_prefix'),
                           pickup_data=pickup_data,
                           chargeable_weight=chargeable_weight,
                           order_id_channel_unique=str(data.get('order_id')).rstrip()
                           )

            if data.get('products'):
                for prod in data.get('products'):
                    if 'sku' in prod:
                        prod_obj = db.session.query(Products).filter(or_(Products.sku == prod['sku'], Products.master_sku==prod['sku']), Products.client_prefix==auth_data.get('client_prefix')).first()
                        if not prod_obj:
                            dimensions = {"length": float(prod.get('length')) if prod.get('length') else None,
                                          "breadth": float(prod.get('breadth')) if prod.get('breadth') else None,
                                          "height": float(prod.get('height')) if prod.get('height') else None}
                            prod_obj = Products(sku=str(prod['product_id']) if prod['product_id'] else str(prod['sku']),
                                                master_sku=str(prod['sku']),
                                                name=str(prod.get('name') if prod.get('name') else prod.get('sku')),
                                                client_prefix=auth_data.get('client_prefix'),
                                                dimensions=dimensions,
                                                weight=float(prod.get('weight')) if prod.get('weight') else None,
                                                price=float(prod.get('price')) if prod.get('price') else None,
                                                )
                            db.session.add(prod_obj)

                    else:
                        prod_obj = db.session.query(Products).filter(Products.id == int(prod['id'])).first()

                    if prod_obj:
                        tax_lines = prod.get('tax_lines')
                        amount = prod.get('amount')
                        op_association = OPAssociation(order=new_order, product=prod_obj, quantity=prod['quantity'], amount=amount, tax_lines=tax_lines)
                        new_order.products.append(op_association)

            if data.get('shipping_charges'):
                total_amount=float(data['total'])+float(data['shipping_charges'])
                shipping_charges = float(data['shipping_charges'])
            else:
                total_amount = float(data['total'])
                shipping_charges = 0

            payment = OrdersPayments(
                payment_mode=data['payment_method'],
                subtotal=float(data['total']),
                amount=total_amount,
                shipping_charges=shipping_charges,
                currency='INR',
                order=new_order
            )

            db.session.add(new_order)
            try:
                db.session.commit()
            except Exception:
                return {"status": "Failed", "msg": "Duplicate order_id"}, 400
            return {'status': 'success', 'msg': "successfully added", "order_id": new_order.channel_order_id, "unique_id": new_order.id}, 200

        except Exception as e:
            if e.args[0].startswith("(psycopg2.IntegrityError) duplicate key value"):
                return {"status": "Failed", "msg": "Duplicate order_id"}, 400
            return {"status":"Failed", "msg":""}, 400

    def get(self, resp):
        auth_data = resp.get('data')
        search_key = request.args.get('search', "")
        if not auth_data:
            return {"success": False, "msg": "Auth Failed"}, 404

        cur = conn.cursor()
        query_to_execute = """SELECT id, name, sku, master_sku FROM products
                              WHERE (name ilike '%__SEARCH_KEY__%'
                              OR sku ilike '%__SEARCH_KEY__%'
                              OR master_sku ilike '%__SEARCH_KEY__%')
                              __CLIENT_FILTER__
                              ORDER BY master_sku
                              LIMIT 10 
                              """.replace('__SEARCH_KEY__', search_key)
        if auth_data['user_group'] != 'super-admin':
            query_to_execute = query_to_execute.replace('__CLIENT_FILTER__', "AND client_prefix='%s'"%auth_data['client_prefix'])
        else:
            query_to_execute = query_to_execute.replace('__CLIENT_FILTER__', "")

        search_tup = tuple()
        try:
            cur.execute(query_to_execute)
            search_tup = cur.fetchall()
        except Exception as e:
            conn.rollback()

        search_list = list()
        for search_obj in search_tup:
            search_dict = dict()
            search_dict['id'] = search_obj[0]
            search_dict['name'] = search_obj[1]
            search_dict['channel_sku'] = search_obj[2]
            search_dict['master_sku'] = search_obj[3]
            search_list.append(search_dict)

        response = {"search_list": search_list}

        if search_key != "":
            return response, 200
        payment_modes = ['prepaid','COD']
        warehouses = [r.warehouse_prefix for r in db.session.query(PickupPoints.warehouse_prefix)
            .join(ClientPickups, ClientPickups.pickup_id==PickupPoints.id)
            .filter(ClientPickups.client_prefix==auth_data.get('client_prefix'))
            .order_by(PickupPoints.warehouse_prefix)]

        response['payment_modes'] = payment_modes
        response['warehouses'] = warehouses
        return response, 200


api.add_resource(AddOrder, '/orders/add')


@orders_blueprint.route('/orders/v1/upload', methods=['POST'])
@authenticate_restful
def upload_orders(resp):
    auth_data = resp.get('data')
    if not auth_data:
        return {"success": False, "msg": "Auth Failed"}, 404

    myfile = request.files['myfile']

    data_xlsx = pd.read_csv(myfile)
    failed_ids = list()

    si = io.StringIO()
    cw = csv.writer(si)
    cw.writerow(ORDERS_UPLOAD_HEADERS)

    def process_row(row, failed_ids):
        row_data = row[1]
        try:
            order_exists = db.session.query(Orders).filter(Orders.channel_order_id==str(row_data.order_id).rstrip(), Orders.client_prefix==auth_data.get('client_prefix')).first()
            if order_exists:
                failed_ids.append(str(row_data.order_id).rstrip())
                cw.writerow(list(row_data.values)+["Order ID already exists. Please use a different ID."])
                return

            delivery_address = ShippingAddress(first_name=str(row_data.customer_name),
                                               address_one=str(row_data.address_one),
                                               address_two=str(row_data.address_two),
                                               city=str(row_data.city),
                                               pincode=str(row_data.pincode),
                                               state=str(row_data.state),
                                               country=str(row_data.country),
                                               phone=str(row_data.customer_phone))

            pickup_filter = str(row_data.warehouse)
            if pickup_filter:
                pickup_data = db.session.query(ClientPickups).join(PickupPoints, ClientPickups.pickup_id==PickupPoints.id).filter(
                    PickupPoints.warehouse_prefix == pickup_filter).first()
            else:
                pickup_data = None

            new_order = Orders(channel_order_id=str(row_data.order_id).rstrip(),
                               order_date=datetime.now()+timedelta(hours=5.5),
                               customer_name=str(row_data.customer_name),
                               customer_email=str(row_data.customer_email),
                               customer_phone=str(row_data.customer_phone),
                               delivery_address=delivery_address,
                               status="NEW",
                               client_prefix=auth_data.get('client_prefix'),
                               pickup_data=pickup_data
                               )

            sku = list()
            sku_quantity = list()
            if row_data.sku:
                sku = str(row_data.sku).split('|')
                sku_quantity = str(row_data.sku_quantity).split('|')
                for idx, sku_str in enumerate(sku):
                    prod_obj = db.session.query(Products).filter(or_(Products.sku == sku_str.strip(), Products.master_sku==sku_str.strip()), Products.client_prefix==auth_data.get('client_prefix')).first()
                    if prod_obj:
                        op_association = OPAssociation(order=new_order, product=prod_obj, quantity=int(sku_quantity[idx].strip()))
                        new_order.products.append(op_association)
                    else:
                        failed_ids.append(str(row_data.order_id).rstrip())
                        cw.writerow(list(row_data.values) + ["One or more SKU not found. Please add SKU in products tab."])
                        db.session.rollback()
                        return

            subtotal = float(row_data.subtotal) if not np.isnan(row_data.subtotal) else 0
            shipping_charges = float(row_data.shipping_charges) if not np.isnan(row_data.shipping_charges) else 0
            payment = OrdersPayments(
                payment_mode=str(row_data.payment_mode),
                subtotal=subtotal,
                amount=subtotal+shipping_charges,
                shipping_charges=shipping_charges,
                currency='INR',
                order=new_order
            )

            db.session.add(new_order)
            db.session.commit()

        except Exception as e:
            failed_ids.append(str(row_data.order_id).rstrip())
            cw.writerow(list(row_data.values) + [str(e.args[0])])
            db.session.rollback()

    for row in data_xlsx.iterrows():
        process_row(row, failed_ids)

    if failed_ids:
        output = make_response(si.getvalue())
        filename = "failed_uploads.csv"
        output.headers["Content-Disposition"] = "attachment; filename=" + filename
        output.headers["Content-type"] = "text/csv"
        return output

    return jsonify({
        'status': 'success',
        "failed_ids": failed_ids
    }), 200


@orders_blueprint.route('/orders/v1/download/shiplabels', methods=['POST'])
@authenticate_restful
def download_shiplabels(resp):
    data = json.loads(request.data)
    auth_data = resp.get('data')
    if not auth_data:
        return jsonify({"success": False, "msg": "Auth Failed"}), 404

    order_ids = data['order_ids']
    orders_qs = db.session.query(Orders, ClientMapping).outerjoin(ClientMapping, Orders.client_prefix==ClientMapping.client_prefix).filter(Orders.id.in_(order_ids), Orders.delivery_address!=None,
                                                Orders.shipments!=None)

    if auth_data['user_group'] == 'client':
        orders_qs = orders_qs.filter(Orders.client_prefix==auth_data.get('client_prefix'))
    orders_qs = orders_qs.order_by(Orders.id).all()
    if not orders_qs:
        return jsonify({"success": False, "msg": "No valid order ID"}), 404

    shiplabel_url, failed_ids = shiplabel_download_util(orders_qs, auth_data)

    return jsonify({
        'status': 'success',
        'url': shiplabel_url,
        "failed_ids": failed_ids
    }), 200


def shiplabel_download_util(orders_qs, auth_data):
    shiplabel_type = "A4"
    if auth_data['user_group'] in ('client', 'super-admin', 'multi-vendor'):
        qs = db.session.query(ClientMapping).filter(
            ClientMapping.client_prefix == auth_data.get('client_prefix')).first()
        if qs and qs.shipping_label:
            shiplabel_type = qs.shipping_label
    if auth_data['user_group'] == 'warehouse':
        qs = db.session.query(WarehouseMapping).filter(
            WarehouseMapping.warehouse_prefix == auth_data.get('warehouse_prefix')).first()
        if qs and qs.shiplabel_type:
            shiplabel_type = qs.shiplabel_type

    file_pref = auth_data['client_prefix'] if auth_data['client_prefix'] else auth_data['warehouse_prefix']
    file_name = "shiplabels_" + str(file_pref) + "_" + str(datetime.now().strftime("%d_%b_%Y_%H_%M_%S")) + ".pdf"
    if shiplabel_type == 'TH1':
        c = canvas.Canvas(file_name, pagesize=(288, 432))
        create_shiplabel_blank_page_thermal(c)
    else:
        c = canvas.Canvas(file_name, pagesize=landscape(A4))
        create_shiplabel_blank_page(c)
    failed_ids = dict()
    idx = 0
    for ixx, order in enumerate(orders_qs):
        try:
            if not order[0].shipments or not order[0].shipments[0].awb:
                continue
            if shiplabel_type == 'TH1':
                try:
                    fill_shiplabel_data_thermal(c, order[0], order[1])
                except Exception:
                    pass

                if idx != len(orders_qs) - 1:
                    c.showPage()
                    create_shiplabel_blank_page_thermal(c)

            elif shiplabel_type == 'A41':
                offset = 3.913
                try:
                    fill_shiplabel_data(c, order[0], offset, order[1])
                except Exception:
                    pass
                c.setFillColorRGB(1, 1, 1)
                c.rect(6.680 * inch, -1.0 * inch, 10 * inch, 10 * inch, fill=1)
                c.rect(-1.0 * inch, -1.0 * inch, 3.907 * inch, 10 * inch, fill=1)
                if idx != len(orders_qs) - 1:
                    c.showPage()
                    create_shiplabel_blank_page(c)
            else:
                offset_dict = {0: 0.20, 1: 3.913, 2: 7.676}
                try:
                    fill_shiplabel_data(c, order[0], offset_dict[idx % 3], order[1])
                except Exception:
                    pass
                if idx % 3 == 2 and ixx != (len(orders_qs) - 1):
                    c.showPage()
                    create_shiplabel_blank_page(c)
            idx += 1
        except Exception as e:
            failed_ids[order[0].channel_order_id] = str(e.args[0])
            pass

    if not (shiplabel_type in ('A41', 'TH1')):
        c.setFillColorRGB(1, 1, 1)
        if idx % 3 == 1:
            c.rect(2.917 * inch, -1.0 * inch, 10 * inch, 10 * inch, fill=1)
        if idx % 3 == 2:
            c.rect(6.680 * inch, -1.0 * inch, 10 * inch, 10 * inch, fill=1)

    c.save()
    s3 = session.resource('s3')
    bucket = s3.Bucket("wareiqshiplabels")
    bucket.upload_file(file_name, file_name, ExtraArgs={'ACL': 'public-read'})
    shiplabel_url = "https://wareiqshiplabels.s3.us-east-2.amazonaws.com/" + file_name
    os.remove(file_name)
    return shiplabel_url, failed_ids


@orders_blueprint.route('/orders/v1/request_pickups', methods=['POST'])
@authenticate_restful
def request_pickups(resp):
    try:
        data = json.loads(request.data)
        auth_data = resp.get('data')
        if not auth_data:
            return jsonify({"success": False, "msg": "Auth Failed"}), 401

        if auth_data['user_group'] != 'client':
            return jsonify({"success": False, "msg": "Not allowed"}), 400

        order_ids = data['order_ids']
        orders_qs = db.session.query(Orders, Shipments).outerjoin(Shipments, Orders.id==Shipments.order_id).filter(Orders.id.in_(order_ids),
                                                    Orders.shipments!=None).all() #todo: client/wh filters here

        pur_dict = dict()

        for order in orders_qs:
            if order[0].pickup_data_id not in pur_dict:
                pur_dict[order[0].pickup_data_id] = {order[1].courier_id: [order[0]]}
            elif order[1].courier_id not in pur_dict[order[0].pickup_data_id]:
                pur_dict[order[0].pickup_data_id][order[1].courier_id] = [order[0]]
            else:
                pur_dict[order[0].pickup_data_id][order[1].courier_id].append(order[0])

        pickup_time_ist = datetime.utcnow() + timedelta(hours=5.5)
        if pickup_time_ist.hour>13:
            pickup_time_ist = pickup_time_ist + timedelta(days=1)
        pickup_time_str = pickup_time_ist.strftime("%Y-%m-%d")
        for pickup_data_id, courier_dict in pur_dict.items():
            for courier_id, order_list in courier_dict.items():
                manifest_qs = db.session.query(Manifests).filter(Manifests.client_pickup_id==pickup_data_id,
                                                                 Manifests.courier_id==courier_id,
                                                                 Manifests.pickup_date>=pickup_time_str).first()
                if not manifest_qs:
                    manifest_id_str = pickup_time_ist.strftime('%Y_%m_%d_') + ''.join(
                        random.choices(string.ascii_uppercase, k=8)) + "_" + str(auth_data.get('client_prefix'))

                    manifest_qs = Manifests(manifest_id=manifest_id_str,
                                            warehouse_prefix=order_list[0].pickup_data.pickup.warehouse_prefix,
                                            courier_id=courier_id,
                                            client_pickup_id=pickup_data_id,
                                            pickup_id=order_list[0].pickup_data.pickup.id,
                                            pickup_date=pickup_time_ist.replace(hour=13, minute=0, second=0),
                                            manifest_url="",
                                            total_scheduled=len(order_list)
                                            )

                    db.session.add(manifest_qs)
                    db.session.flush()

                manifest_id = manifest_qs.id

                for order in order_list:
                    stmt = insert(OrderPickups).values(manifest_id=manifest_id, order_id=order.id, picked=False)
                    stmt = stmt.on_conflict_do_nothing()
                    db.session.execute(stmt)
                    order.status = "PICKUP REQUESTED"

        db.session.commit()
    except Exception as e:
        return jsonify({
            'status': 'failed'
        }), 400

    return jsonify({
        'status': 'success'
    }), 200


@orders_blueprint.route('/orders/v1/<pickup_id>/pick_orders', methods=['GET'])
@authenticate_restful
def pick_orders(resp, pickup_id):
    response = list()
    try:
        auth_data = resp.get('data')
        if not auth_data:
            return jsonify({"success": False, "msg": "Auth Failed"}), 401

        manifest_id=int(pickup_id)

        order_qs = db.session.query(OrderPickups).filter(OrderPickups.manifest_id==manifest_id).order_by(OrderPickups.pickup_time.desc()).all()
        for order in order_qs:
            res_obj = dict()
            res_obj['unique_id'] = order.order_id
            res_obj['order_id'] = order.order.channel_order_id
            res_obj['awb'] = order.order.shipments[0].awb if order.order.shipments[0] else None
            res_obj['picked'] = order.picked
            res_obj['picked_time'] = order.pickup_time
            response.append(res_obj)

    except Exception as e:
        return jsonify({
            'status': 'failed'
        }), 400

    return jsonify({
        'status': 'success',
        'data': response
    }), 200


@orders_blueprint.route('/orders/v1/<pickup_id>/download', methods=['GET'])
@authenticate_restful
def pickup_download(resp, pickup_id):
    try:
        auth_data = resp.get('data')
        if not auth_data:
            return jsonify({"success": False, "msg": "Auth Failed"}), 401

        flag = request.args.get('flag')
        if not flag:
            return jsonify({"success": False, "msg": "Bad request"}), 400

        manifest_id=int(pickup_id)
        download_url = ""

        if flag=="labels":
            orders_qs = db.session.query(Orders, ClientMapping).outerjoin(ClientMapping,
                                                              Orders.client_prefix == ClientMapping.client_prefix).outerjoin(
                OrderPickups, Orders.id == OrderPickups.order_id).filter(OrderPickups.manifest_id == manifest_id).all()
            if not orders_qs:
                return jsonify({"success": False, "msg": "No valid order ID"}), 400

            download_url, failed_ids = shiplabel_download_util(orders_qs, auth_data)

        elif flag=="invoice":
            orders_qs = db.session.query(Orders, ClientMapping).outerjoin(ClientMapping,
                                                              Orders.client_prefix == ClientMapping.client_prefix).outerjoin(
                OrderPickups, Orders.id == OrderPickups.order_id).filter(OrderPickups.manifest_id == manifest_id).all()
            if not orders_qs:
                return jsonify({"success": False, "msg": "No valid order ID"}), 400

            download_url, failed_ids = download_invoice_util(orders_qs, auth_data)

        elif flag=="picklist":
            orders_qs = db.session.query(Orders).outerjoin(
                OrderPickups, Orders.id == OrderPickups.order_id).filter(OrderPickups.manifest_id == manifest_id).all()
            if not orders_qs:
                return jsonify({"success": False, "msg": "No valid order ID"}), 400

            download_url = download_picklist_util(orders_qs, auth_data)

        elif flag=="packlist":
            orders_qs = db.session.query(Orders).outerjoin(
                OrderPickups, Orders.id == OrderPickups.order_id).filter(OrderPickups.manifest_id == manifest_id).all()
            if not orders_qs:
                return jsonify({"success": False, "msg": "No valid order ID"}), 400

            download_url = download_packlist_util(orders_qs, auth_data)

        elif flag=="manifest":
            orders_qs = db.session.query(Orders).outerjoin(
                OrderPickups, Orders.id == OrderPickups.order_id).filter(OrderPickups.manifest_id == manifest_id).all()
            if not orders_qs:
                return jsonify({"success": False, "msg": "No valid order ID"}), 400

            download_url = download_manifest_util(orders_qs, auth_data)

    except Exception as e:
        return jsonify({
            'status': 'failed'
        }), 400

    return jsonify({
        'status': 'success',
        'download_url': download_url
    }), 200


@orders_blueprint.route('/orders/v1/download/invoice', methods=['POST'])
@authenticate_restful
def download_invoice(resp):
    data = json.loads(request.data)
    auth_data = resp.get('data')
    if not auth_data:
        return jsonify({"success": False, "msg": "Auth Failed"}), 404

    order_ids = data['order_ids']
    orders_qs = db.session.query(Orders, ClientMapping).join(ClientMapping, Orders.client_prefix==ClientMapping.client_prefix,
          isouter=True).filter(Orders.id.in_(order_ids), Orders.delivery_address != None)

    if auth_data['user_group'] == 'client':
        orders_qs = orders_qs.filter(Orders.client_prefix==auth_data.get('client_prefix'))
    orders_qs = orders_qs.order_by(Orders.channel_order_id).all()
    if not orders_qs:
        return jsonify({"success": False, "msg": "No valid order ID"}), 404

    invoice_url, failed_ids = download_invoice_util(orders_qs, auth_data)

    return jsonify({
        'status': 'success',
        'url': invoice_url,
        "failed_ids": failed_ids
    }), 200


def download_invoice_util(orders_qs, auth_data):
    file_pref = auth_data['client_prefix'] if auth_data['client_prefix'] else auth_data['warehouse_prefix']
    file_name = "invoice_" + str(file_pref) + "_" + str(datetime.now().strftime("%d_%b_%Y_%H_%M_%S")) + ".pdf"
    c = canvas.Canvas(file_name, pagesize=A4)
    create_invoice_blank_page(c)
    failed_ids = dict()
    idx = 0
    for order in orders_qs:
        try:
            try:
                fill_invoice_data(c, order[0], order[1])
            except Exception:
                pass
            if idx != len(orders_qs) - 1:
                c.showPage()
                create_invoice_blank_page(c)
            idx += 1
        except Exception as e:
            failed_ids[order.channel_order_id] = str(e.args[0])
            pass

    c.save()
    s3 = session.resource('s3')
    bucket = s3.Bucket("wareiqinvoices")
    bucket.upload_file(file_name, file_name, ExtraArgs={'ACL': 'public-read'})
    invoice_url = "https://wareiqinvoices.s3.us-east-2.amazonaws.com/" + file_name
    os.remove(file_name)
    return invoice_url, failed_ids


@orders_blueprint.route('/orders/v1/download/picklist', methods=['POST'])
@authenticate_restful
def download_picklist(resp):
    data = json.loads(request.data)
    auth_data = resp.get('data')
    if not auth_data:
        return jsonify({"success": False, "msg": "Auth Failed"}), 404

    order_ids = data['order_ids']
    orders_qs = db.session.query(Orders).filter(Orders.id.in_(order_ids))

    if auth_data['user_group'] == 'client':
        orders_qs = orders_qs.filter(Orders.client_prefix==auth_data.get('client_prefix'))
    orders_qs = orders_qs.order_by(Orders.id).all()
    if not orders_qs:
        return jsonify({"success": False, "msg": "No valid order ID"}), 404

    invoice_url = download_picklist_util(orders_qs, auth_data)

    return jsonify({
        'status': 'success',
        'url': invoice_url,
    }), 200


def download_picklist_util(orders_qs, auth_data):
    products_dict = dict()
    order_count = dict()

    for order in orders_qs:
        if order.client_prefix not in products_dict:
            products_dict[order.client_prefix] = dict()
            order_count[order.client_prefix] = 1
        else:
            order_count[order.client_prefix] += 1
            pass
        for prod in order.products:
            if prod.product.combo:
                for new_prod in prod.product.combo:
                    if new_prod.combo_prod_id not in products_dict[order.client_prefix]:
                        sku = new_prod.combo_prod.master_sku if new_prod.combo_prod.master_sku else new_prod.combo_prod.sku
                        products_dict[order.client_prefix][new_prod.combo_prod_id] = {"sku": sku,
                                                                                      "name": new_prod.combo_prod.name,
                                                                                      "quantity": prod.quantity * new_prod.quantity}
                    else:
                        products_dict[order.client_prefix][new_prod.combo_prod_id][
                            'quantity'] += prod.quantity * new_prod.quantity
            else:
                if prod.product_id not in products_dict[order.client_prefix]:
                    sku = prod.product.master_sku if prod.product.master_sku else prod.product.sku
                    products_dict[order.client_prefix][prod.product_id] = {"sku": sku, "name": prod.product.name,
                                                                           "quantity": prod.quantity}
                else:
                    products_dict[order.client_prefix][prod.product_id]['quantity'] += prod.quantity

    file_pref = auth_data['client_prefix'] if auth_data['client_prefix'] else auth_data['warehouse_prefix']
    file_name = "picklist_" + str(file_pref) + "_" + str(datetime.now().strftime("%d_%b_%Y_%H_%M_%S")) + ".pdf"
    c = canvas.Canvas(file_name, pagesize=A4)
    c = generate_picklist(c, products_dict, order_count)

    c.save()
    s3 = session.resource('s3')
    bucket = s3.Bucket("wareiqpicklist")
    bucket.upload_file(file_name, file_name, ExtraArgs={'ACL': 'public-read'})
    invoice_url = "https://wareiqpicklist.s3.us-east-2.amazonaws.com/" + file_name
    os.remove(file_name)
    return invoice_url


@orders_blueprint.route('/orders/v1/download/packlist', methods=['POST'])
@authenticate_restful
def download_packlist(resp):
    data = json.loads(request.data)
    auth_data = resp.get('data')
    if not auth_data:
        return jsonify({"success": False, "msg": "Auth Failed"}), 404

    order_ids = data['order_ids']
    orders_qs = db.session.query(Orders).filter(Orders.id.in_(order_ids))

    if auth_data['user_group'] == 'client':
        orders_qs = orders_qs.filter(Orders.client_prefix==auth_data.get('client_prefix'))
    orders_qs = orders_qs.order_by(Orders.id).all()
    if not orders_qs:
        return jsonify({"success": False, "msg": "No valid order ID"}), 404

    invoice_url = download_packlist_util(orders_qs, auth_data)

    return jsonify({
        'status': 'success',
        'url': invoice_url,
    }), 200


def download_packlist_util(orders_qs, auth_data):
    orders_dict = dict()
    order_count = dict()

    for order in orders_qs:
        if order.client_prefix not in orders_dict:
            orders_dict[order.client_prefix] = {order.channel_order_id: dict()}
            order_count[order.client_prefix] = 1
        else:
            orders_dict[order.client_prefix][order.channel_order_id] = dict()
            order_count[order.client_prefix] += 1
            pass
        for prod in order.products:
            if prod.product.combo:
                for new_prod in prod.product.combo:
                    sku = new_prod.combo_prod.master_sku if new_prod.combo_prod.master_sku else new_prod.combo_prod.sku
                    if new_prod.combo_prod_id not in orders_dict[order.client_prefix][order.channel_order_id]:
                        orders_dict[order.client_prefix][order.channel_order_id][new_prod.combo_prod_id] = {"sku": sku,
                                                                                                            "name": new_prod.combo_prod.name,
                                                                                                            "quantity": prod.quantity * new_prod.quantity}
                    else:
                        orders_dict[order.client_prefix][order.channel_order_id][new_prod.combo_prod_id][
                            'quantity'] += prod.quantity * new_prod.quantity
            else:
                sku = prod.product.master_sku if prod.product.master_sku else prod.product.sku
                if prod.product_id not in orders_dict[order.client_prefix][order.channel_order_id]:
                    orders_dict[order.client_prefix][order.channel_order_id][prod.product_id] = {"sku": sku,
                                                                                                 "name": prod.product.name,
                                                                                                 "quantity": prod.quantity}
                else:
                    orders_dict[order.client_prefix][order.channel_order_id][prod.product_id][
                        'quantity'] += prod.quantity

    file_pref = auth_data['client_prefix'] if auth_data['client_prefix'] else auth_data['warehouse_prefix']
    file_name = "packlist_" + str(file_pref) + "_" + str(datetime.now().strftime("%d_%b_%Y_%H_%M_%S")) + ".pdf"
    c = canvas.Canvas(file_name, pagesize=A4)
    c = generate_packlist(c, orders_dict, order_count)

    c.save()
    s3 = session.resource('s3')
    bucket = s3.Bucket("wareiqpacklist")
    bucket.upload_file(file_name, file_name, ExtraArgs={'ACL': 'public-read'})
    invoice_url = "https://wareiqpacklist.s3.us-east-2.amazonaws.com/" + file_name
    os.remove(file_name)
    return invoice_url


@orders_blueprint.route('/orders/v1/<order_id>/cancel', methods=['GET'])
@authenticate_restful
def cancel_order_channel(resp, order_id):
    auth_data = resp.get('data')
    if not auth_data:
        return jsonify({"success": False, "msg": "Auth Failed"}), 404

    orders_qs = db.session.query(Orders).filter(Orders.order_id_channel_unique == str(order_id), Orders.client_prefix==auth_data.get('client_prefix')).all()

    for order in orders_qs:
        order.status = 'CANCELED'
        if order.shipments and order.shipments[0].awb:
            if order.shipments[0].courier.id in (
            1, 2, 8, 11, 12):  # Cancel on delhievry #todo: cancel on other platforms too
                cancel_body = json.dumps({"waybill": order.shipments[0].awb, "cancellation": "true"})
                headers = {"Authorization": "Token " + order.shipments[0].courier.api_key,
                           "Content-Type": "application/json"}
                req_can = requests.post("https://track.delhivery.com/api/p/edit", headers=headers, data=cancel_body)
            if order.shipments[0].courier.id in (5, 13):  # Cancel on Xpressbees
                cancel_body = json.dumps(
                    {"AWBNumber": order.shipments[0].awb, "XBkey": order.shipments[0].courier.api_key,
                     "RTOReason": "Cancelled by seller"})
                headers = {"Authorization": "Basic " + order.shipments[0].courier.api_key,
                           "Content-Type": "application/json"}
                req_can = requests.post("http://xbclientapi.xbees.in/POSTShipmentService.svc/RTONotifyShipment",
                                        headers=headers, data=cancel_body)
        if order.orders_invoice:
            for invoice_obj in order.orders_invoice:
                invoice_obj.cancelled=True
        db.session.query(OrderStatus).filter(OrderStatus.order_id == order.id).delete()

    db.session.commit()

    return jsonify({'status': 'success'}), 200


@orders_blueprint.route('/orders/v1/download/manifest', methods=['POST'])
@authenticate_restful
def download_manifests(resp):
    data = json.loads(request.data)
    auth_data = resp.get('data')
    if not auth_data:
        return jsonify({"success": False, "msg": "Auth Failed"}), 404

    order_ids = data['order_ids']
    orders_qs = db.session.query(Orders).filter(Orders.id.in_(order_ids), Orders.shipments!=None)

    if auth_data['user_group'] == 'client':
        orders_qs = orders_qs.filter(Orders.client_prefix==auth_data.get('client_prefix'))
    orders_qs = orders_qs.order_by(Orders.id).all()
    if not orders_qs:
        return jsonify({"success": False, "msg": "No valid order ID"}), 404

    manifest_url = download_manifest_util(orders_qs, auth_data)

    return jsonify({
        'status': 'success',
        'url': manifest_url,
    }), 200


def download_manifest_util(orders_qs, auth_data):
    orders_list = list()
    warehouse = auth_data['client_prefix'] if auth_data['client_prefix'] else auth_data['warehouse_prefix']
    courier = None
    store = None
    for order in orders_qs:
        if not order.shipments or not order.shipments[0].awb:
            continue
        if not store and order.pickup_data:
            store = order.pickup_data.pickup.warehouse_prefix
        if not courier:
            courier = order.shipments[0].courier.courier_name

        prod_names = list()
        prod_quan = list()

        for prod in order.products:
            prod_names.append(prod.product.name)
            prod_quan.append(prod.quantity)

        order_tuple = (
        order.channel_order_id, order.order_date, order.client_prefix, order.shipments[0].weight, None, None,
        None, prod_names, prod_quan, order.payments[0].payment_mode, order.payments[0].amount,
        order.delivery_address.first_name, order.delivery_address.last_name, order.delivery_address.address_one,
        order.delivery_address.address_two, order.delivery_address.city, order.delivery_address.pincode,
        order.delivery_address.state,
        order.delivery_address.country, order.delivery_address.phone, order.shipments[0].awb, None, None)

        orders_list.append(order_tuple)

    manifest_url = fill_manifest_data(orders_list, courier, store, warehouse)
    return manifest_url


@orders_blueprint.route('/orders/v1/bulkcancel', methods=['POST'])
@authenticate_restful
def bulk_cancel_orders(resp):
    cur = conn.cursor()
    auth_data = resp.get('data')
    if not auth_data:
        return jsonify({"success": False, "msg": "Auth Failed"}), 404
    if auth_data['user_group'] not in ('client', 'super-admin', 'multi-vendor'):
        return jsonify({"success":False, "msg": "invalid user"}), 400
    data = json.loads(request.data)
    order_ids=data.get('order_ids')
    if not order_ids:
        return jsonify({"success": False, "msg": "please select orders"}), 400

    order_tuple_str = check_client_order_ids(order_ids, auth_data, cur)

    if not order_tuple_str:
        return jsonify({"success": False, "msg": "Invalid order ids"}), 400

    cur.execute("UPDATE orders SET status='CANCELED' WHERE id in %s"%order_tuple_str)
    cur.execute("UPDATE orders_invoice SET cancelled=true WHERE order_id in %s"%order_tuple_str)

    conn.commit()

    return jsonify({"success": True, "msg": "Cancelled orders successfully"}), 200


@orders_blueprint.route('/orders/v1/bulkdelivered', methods=['POST'])
@authenticate_restful
def bulk_delivered_orders(resp):
    cur = conn.cursor()
    auth_data = resp.get('data')
    if not auth_data:
        return jsonify({"success": False, "msg": "Auth Failed"}), 404
    if auth_data['user_group'] not in ('client', 'super-admin', 'multi-vendor'):
        return jsonify({"success":False, "msg": "invalid user"}), 400
    data = json.loads(request.data)
    order_ids=data.get('order_ids')
    if not order_ids:
        return jsonify({"success": False, "msg": "please select orders"}), 400
    if len(order_ids)==1:
        order_tuple_str = "("+str(order_ids[0])+")"
    else:
        order_tuple_str = str(tuple(order_ids))

    query_to_run = """SELECT array_agg(aa.id) FROM orders aa
                        LEFT JOIN shipments bb on aa.id=bb.order_id
                        WHERE aa.id in __ORDER_IDS__
                        AND bb.courier_id in (3,19)
                        __CLIENT_FILTER__;""".replace("__ORDER_IDS__", order_tuple_str)

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
        return jsonify({"success": False, "msg": "invalid order ids"}), 400

    if len(order_ids)==1:
        order_tuple_str = "("+str(order_ids[0])+")"
    else:
        order_tuple_str = str(tuple(order_ids))

    cur.execute("UPDATE orders SET status='DELIVERED' WHERE id in %s"%order_tuple_str)

    conn.commit()

    return jsonify({"success": True, "msg": "Cancelled orders successfully"}), 200


@orders_blueprint.route('/orders/v1/bulkAssignPickup', methods=['POST'])
@authenticate_restful
def bulk_assign_pickups(resp):
    cur = conn.cursor()
    auth_data = resp.get('data')
    if not auth_data:
        return jsonify({"success": False, "msg": "Auth Failed"}), 404
    if auth_data['user_group'] not in ('client', 'super-admin', 'multi-vendor'):
        return jsonify({"success":False, "msg": "invalid user"}), 400
    data = json.loads(request.data)
    order_ids=data.get('order_ids')
    warehouse_prefix=data.get('pickup_point')
    if not order_ids:
        return jsonify({"success": False, "msg": "please select orders"}), 400
    if len(order_ids)==1:
        order_tuple_str = "("+str(order_ids[0])+")"
    else:
        order_tuple_str = str(tuple(order_ids))

    query_to_run = """SELECT aa.id, bb.id FROM orders aa 
    LEFT JOIN client_pickups bb on aa.client_prefix=bb.client_prefix
    LEFT JOIN pickup_points cc on bb.pickup_id=cc.id 
    WHERE aa.id in __ORDER_IDS__ __CLIENT_FILTER__
    AND cc.warehouse_prefix='__WH_PREFIX__';""".replace("__ORDER_IDS__", order_tuple_str).replace('__WH_PREFIX__', warehouse_prefix)

    if auth_data['user_group'] == 'client':
        query_to_run = query_to_run.replace('__CLIENT_FILTER__', "AND aa.client_prefix='%s'"%auth_data['client_prefix'])
    elif auth_data['user_group'] == 'multi-vendor':
        cur.execute("SELECT vendor_list FROM multi_vendor WHERE client_prefix='%s';" % auth_data['client_prefix'])
        vendor_list = cur.fetchone()[0]
        query_to_run = query_to_run.replace("__CLIENT_FILTER__",
                                            "AND aa.client_prefix in %s" % str(tuple(vendor_list)))
    else:
        query_to_run = query_to_run.replace("__CLIENT_FILTER__","")

    cur.execute(query_to_run)
    orders_data = cur.fetchall()

    if not orders_data:
        return jsonify({"success":False, "msg": "invalid orders selected"}), 400

    values_str = ""

    for idx, order_data in enumerate(orders_data):
        if idx<len(orders_data)-1:
            values_str += "("+str(order_data[0])+","+str(order_data[1])+"),"
        else:
            values_str += "("+str(order_data[0])+","+str(order_data[1])+")"

    update_query= """UPDATE orders as aa SET
                    pickup_data_id = cc.pickup_data_id
                FROM (VALUES
                    __VALUES_STR__  
                ) as cc(order_id, pickup_data_id) 
                WHERE cc.order_id = aa.id;""".replace('__VALUES_STR__', values_str)

    cur.execute(update_query)

    conn.commit()

    return jsonify({"success": True, "msg": "assigned pickups successfully"}), 200


@orders_blueprint.route('/orders/v1/manifests', methods=['POST'])
@authenticate_restful
def get_manifests(resp):
    cur = conn.cursor()
    response = {'status': 'success', 'data': dict(), "meta": dict()}
    auth_data = resp.get('data')
    data = json.loads(request.data)
    page = data.get('page', 1)
    per_page = data.get('per_page', 10)
    filters = data.get('filters', {})
    if not auth_data:
        return jsonify({"success": False, "msg": "Auth Failed"}), 404

    client_prefix = auth_data.get('client_prefix')
    query_to_run = select_pickups_list_query
    if auth_data['user_group'] == 'client':
        query_to_run = query_to_run.replace("__CLIENT_FILTER__", "AND cc.client_prefix = '%s'" % client_prefix)
    if auth_data['user_group'] == 'warehouse':
        query_to_run = query_to_run.replace("__PICKUP_FILTER__",
                                            "AND dd.warehouse_prefix = '%s'" % auth_data.get('warehouse_prefix'))
    if auth_data['user_group'] == 'multi-vendor':
        cur.execute("SELECT vendor_list FROM multi_vendor WHERE client_prefix='%s';" % client_prefix)
        vendor_list = cur.fetchone()[0]
        query_to_run = query_to_run.replace("__MV_CLIENT_FILTER__",
                                            "AND cc.client_prefix in %s" % str(tuple(vendor_list)))
    else:
        query_to_run = query_to_run.replace("__MV_CLIENT_FILTER__", "")

    if filters:
        if 'courier' in filters:
            if len(filters['courier']) == 1:
                courier_tuple = "('" + filters['courier'][0] + "')"
            else:
                courier_tuple = str(tuple(filters['courier']))
            query_to_run = query_to_run.replace("__COURIER_FILTER__", "AND bb.courier_name in %s" % courier_tuple)
        if 'client' in filters and auth_data['user_group'] != 'client':
            if len(filters['client']) == 1:
                client_tuple = "('" + filters['client'][0] + "')"
            else:
                client_tuple = str(tuple(filters['client']))
            query_to_run = query_to_run.replace("__CLIENT_FILTER__", "AND cc.client_prefix in %s" % client_tuple)

        if 'pickup_point' in filters:
            if len(filters['pickup_point']) == 1:
                pickup_tuple = "('" + filters['pickup_point'][0] + "')"
            else:
                pickup_tuple = str(tuple(filters['pickup_point']))
            query_to_run = query_to_run.replace("__PICKUP_FILTER__", "AND dd.warehouse_prefix in %s" % pickup_tuple)

        if 'pickup_time' in filters:
            filter_date_start = filters['pickup_time'][0][0:19].replace('T', ' ')
            filter_date_end = filters['pickup_time'][1][0:19].replace('T', ' ')
            query_to_run = query_to_run.replace("__PICKUP_TIME_FILTER__", "AND aa.pickup_date between '%s' and '%s'" % (
            filter_date_start, filter_date_end))

    count_query = "select count(*) from (" + query_to_run.replace('__PAGINATION__', "") + ") xx"
    count_query = re.sub(r"""__.+?__""", "", count_query)
    cur.execute(count_query)
    total_count = cur.fetchone()[0]
    query_to_run = query_to_run.replace('__PAGINATION__',
                                        "OFFSET %s LIMIT %s" % (str((page - 1) * per_page), str(per_page)))
    query_to_run = re.sub(r"""__.+?__""", "", query_to_run)
    cur.execute(query_to_run)
    orders_qs_data = cur.fetchall()

    return_data = list()
    for order in orders_qs_data:
        resp_obj = dict()
        resp_obj['manifest_id'] = order[1]
        resp_obj['pickup_id'] = order[0]
        resp_obj['courier'] = order[2]
        resp_obj['pickup_point'] = order[6]
        resp_obj['total_scheduled'] = order[4] if order[4] else order[8]
        resp_obj['total_picked'] = order[3] if order[3] else order[7]
        resp_obj['pickup_date'] = order[5]
        resp_obj['manifest_url'] = order[9]
        return_data.append(resp_obj)

    response['data'] = return_data

    total_pages = math.ceil(total_count / per_page)
    response['meta']['pagination'] = {'total': total_count,
                                      'per_page': per_page,
                                      'current_page': page,
                                      'total_pages': total_pages}

    return jsonify(response), 200


class OrderDetails(Resource):

    method_decorators = [authenticate_restful]

    def get(self, resp, order_id):
        try:
            response = {"status": "success"}
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404

            order = db.session.query(Orders).filter(Orders.id == int(order_id))
            if auth_data['user_group'] == 'client':
                order = order.filter(Orders.client_prefix==auth_data['client_prefix'])

            order = order.first()

            if order:
                resp_obj = dict()
                resp_obj['order_id'] = order.channel_order_id
                resp_obj['unique_id'] = order.id
                resp_obj['customer_details'] = {"name": order.customer_name,
                                                "email": order.customer_email,
                                                "phone": order.customer_phone,
                                                "address_one": order.delivery_address.address_one,
                                                "address_two": order.delivery_address.address_two,
                                                "city": order.delivery_address.city,
                                                "country": order.delivery_address.country,
                                                "state": order.delivery_address.state,
                                                "pincode": order.delivery_address.pincode}
                resp_obj['order_date'] = order.order_date.strftime("%d %b %Y, %I:%M %p")
                resp_obj['payment'] = {"mode": order.payments[0].payment_mode,
                                       "amount": order.payments[0].amount}
                resp_obj['product_details'] = list()
                for prod in order.products:
                    resp_obj['product_details'].append(
                        {"name": prod.product.name,
                         "sku": prod.product.master_sku,
                         "quantity": prod.quantity,
                         "id": prod.product.id,
                         "total": prod.amount}
                    )

                resp_obj['shipping_details'] = dict()
                resp_obj['dimensions'] = None
                resp_obj['weight'] = None
                resp_obj['volumetric'] = None
                if order.shipments and order.shipments[0].courier:
                    resp_obj['shipping_details'] = {"courier": order.shipments[0].courier.courier_name,
                                                    "awb": order.shipments[0].awb}
                    resp_obj['dimensions'] = order.shipments[0].dimensions
                    resp_obj['weight'] = order.shipments[0].weight
                    resp_obj['volumetric'] = order.shipments[0].volumetric_weight

                if order.pickup_data:
                    resp_obj['pickup_point'] = order.pickup_data.pickup.warehouse_prefix

                resp_obj['status'] = order.status
                resp_obj['remark'] = None
                if auth_data['user_group'] == 'super-admin' and order.shipments:
                    resp_obj['remark'] = order.shipments[0].remark

                if not order.exotel_data or order.status not in ('NEW','CANCELED','PENDING PAYMENT'):
                    pass
                elif order.exotel_data[0].cod_verified == None:
                    resp_obj['cod_verification'] = None
                elif order.exotel_data[0].cod_verified == False:
                    resp_obj['cod_verification'] = False
                else:
                    resp_obj['cod_verification'] = True

                if not order.ndr_verification:
                    pass
                elif order.ndr_verification[0].ndr_verified == None:
                    resp_obj['ndr_verification'] = None
                elif order.ndr_verification[0].ndr_verified == False:
                    resp_obj['ndr_verification'] = False
                else:
                    resp_obj['ndr_verification'] = True

                if order.status in ('NEW','CANCELED','PENDING PAYMENT','READY TO SHIP','PICKUP REQUESTED','NOT PICKED') or not order.shipments:
                    resp_obj['status_change'] = True

                response['data'] = resp_obj

                return response, 200
            else:
                response["status"] = "Failed"
                response["msg"] = "No order with given ID found"
                return response, 404

        except Exception as e:
            return {"status": "Failed", "msg": str(e.args)}, 400

    def patch(self, resp, order_id):
        try:
            data = json.loads(request.data)
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404

            order = db.session.query(Orders).filter(Orders.id==int(order_id)).first()

            if not order:
                return {"success": False, "msg": "No order found for given id"}, 400

            if data.get('full_name'):
                order.customer_name =data.get('full_name')
                order.delivery_address.first_name = data.get('full_name')
                order.delivery_address.last_name = ""
            if data.get('customer_email'):
                order.customer_email =data.get('customer_email')
            if data.get('customer_phone'):
                order.customer_phone =data.get('customer_phone')
                order.delivery_address.phone = data.get('customer_phone')
            if data.get('address1'):
                order.delivery_address.address_one = data.get('address1')
            if data.get('address2'):
                order.delivery_address.address_two = data.get('address2')
            if data.get('city'):
                order.delivery_address.city = data.get('city')
            if data.get('pincode'):
                order.delivery_address.pincode = data.get('pincode')
            if data.get('state'):
                order.delivery_address.state = data.get('state')
            if data.get('country'):
                order.delivery_address.country = data.get('country')
            if data.get('payment_method'):
                order.payments[0].payment_mode = data.get('payment_method')
            if data.get('total'):
                order.payments[0].subtotal = float(data.get('total'))
                order.payments[0].shipping_charges = float(data.get('shipping_charges'))
                order.payments[0].amount = float(data['total'])+float(data['shipping_charges'])
            if data.get('awb') and order.shipments:
                order.shipments[0].awb = data.get('awb')

            if 'cod_verification' in data:
                if order.exotel_data:
                    order.exotel_data[0].cod_verified = data.get('cod_verification')
                    order.exotel_data[0].verified_via = 'manual'
                    order.exotel_data[0].verification_time = datetime.utcnow() + timedelta(hours=5.5)
                if data.get('cod_verification') == False:
                    order.status = 'CANCELED'
                    if order.shipments and order.shipments[0].awb:
                        if order.shipments[0].courier.id in (1,2,8,11,12):  #Cancel on delhievry #todo: cancel on other platforms too
                            cancel_body = json.dumps({"waybill": order.shipments[0].awb, "cancellation": "true"})
                            headers = {"Authorization": "Token " + order.shipments[0].courier.api_key,
                                        "Content-Type": "application/json"}
                            req_can = requests.post("https://track.delhivery.com/api/p/edit", headers=headers, data=cancel_body)
                        if order.shipments[0].courier.id in (5,13):  #Cancel on Xpressbees
                            cancel_body = json.dumps({"AWBNumber": order.shipments[0].awb, "XBkey": order.shipments[0].courier.api_key, "RTOReason": "Cancelled by seller"})
                            headers = {"Authorization": "Basic " + order.shipments[0].courier.api_key,
                                        "Content-Type": "application/json"}
                            req_can = requests.post("http://xbclientapi.xbees.in/POSTShipmentService.svc/RTONotifyShipment", headers=headers, data=cancel_body)
                    db.session.query(OrderStatus).filter(OrderStatus.order_id == int(order_id)).delete()
                    if order.client_channel and order.client_channel.channel_id == 6 and order.order_id_channel_unique: #cancel on magento
                        cancel_header = {'Content-Type': 'application/json',
                                      'Authorization': 'Bearer ' + order.client_channel.api_key}
                        cancel_data = {
                                      "entity": {
                                        "entity_id": int(order.order_id_channel_unique),
                                        "status": "canceled"
                                      }
                                    }
                        cancel_url = order.client_channel.shop_url + "/rest/V1/orders/%s/cancel"%str(order.order_id_channel_unique)
                        req_ful = requests.post(cancel_url, data=json.dumps(cancel_data),
                                                headers=cancel_header, verify=False)

                    if order.orders_invoice: #cancel invoice
                        for invoice_obj in order.orders_invoice:
                            invoice_obj.cancelled = True

                elif data.get('cod_verification') == True:
                    if order.shipments and order.shipments and order.shipments[0].awb:
                        order.status = 'READY TO SHIP'
                    else:
                        order.status = 'NEW'

            if 'ndr_verification' in data and order.ndr_verification:
                order.ndr_verification[0].ndr_verified = data.get('ndr_verification')
                order.ndr_verification[0].verified_via = 'manual'
                order.ndr_verification[0].verification_time = datetime.utcnow() + timedelta(hours=5.5)

            if 'pickup_point' in data:
                client_pickup = db.session.query(ClientPickups).join(PickupPoints,
                                         ClientPickups.pickup_id==PickupPoints.id).filter(ClientPickups.client_prefix==order.client_prefix, PickupPoints.warehouse_prefix==data.get('pickup_point')).first()
                if client_pickup:
                    order.pickup_data = client_pickup
                    order.status = 'NEW'
                    db.session.query(OrderStatus).filter(OrderStatus.order_id == int(order_id)).delete()
                    db.session.query(Shipments).filter(Shipments.order_id == int(order_id)).delete()
                    if order.shipments and order.shipments[0].awb:
                        if order.shipments[0].courier.id in (1,2,8,11,12):  #Cancel on delhievry #todo: cancel on other platforms too
                            cancel_body = json.dumps({"waybill": order.shipments[0].awb, "cancellation": "true"})
                            headers = {"Authorization": "Token " + order.shipments[0].courier.api_key,
                                        "Content-Type": "application/json"}
                            req_can = requests.post("https://track.delhivery.com/api/p/edit", headers=headers, data=cancel_body)
                        if order.shipments[0].courier.id in (5,13):  #Cancel on Xpressbees
                            cancel_body = json.dumps({"AWBNumber": order.shipments[0].awb, "XBkey": order.shipments[0].courier.api_key, "RTOReason": "Cancelled by seller"})
                            headers = {"Authorization": "Basic " + order.shipments[0].courier.api_key,
                                        "Content-Type": "application/json"}
                            req_can = requests.post("http://xbclientapi.xbees.in/POSTShipmentService.svc/RTONotifyShipment", headers=headers, data=cancel_body)

                    if order.orders_invoice:
                        for invoice_obj in order.orders_invoice:
                            invoice_obj.cancelled = True

            db.session.commit()
            return {'status': 'success', 'msg': "successfully updated"}, 200

        except Exception as e:
            return {'status': 'Failed'}, 200


api.add_resource(OrderDetails, '/orders/v1/order/<order_id>')


class CreateReturn(Resource):

    method_decorators = [authenticate_restful]

    def post(self, resp, order_id):
        try:
            data = json.loads(request.data)
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404

            order = db.session.query(Orders).filter(Orders.id==int(order_id)).first()

            if not order:
                return {"success": False, "msg": "No order found for given id"}, 400

            new_order = Orders(channel_order_id="R_"+str(order.channel_order_id),
                               order_date=datetime.utcnow() + timedelta(hours=5.5),
                               customer_name=order.customer_name,
                               customer_email=order.customer_email,
                               customer_phone=order.customer_phone,
                               delivery_address=order.delivery_address,
                               billing_address=order.billing_address,
                               status="NEW",
                               client_prefix=auth_data.get('client_prefix') if auth_data['user_group'] != 'super-admin' else order.client_prefix,
                               pickup_data=order.pickup_data,
                               )

            if data.get('products'):
                for prod in data.get('products'):
                    prod_obj = db.session.query(Products).filter(Products.id == prod['id']).first()

                    if prod_obj:
                        op_association = OPAssociation(order=new_order, product=prod_obj, quantity=prod['quantity'])
                        new_order.products.append(op_association)

            payment = OrdersPayments(
                payment_mode="Pickup",
                subtotal=order.payments[0].subtotal,
                amount=order.payments[0].amount,
                shipping_charges=order.payments[0].shipping_charges,
                currency='INR',
                order=new_order
            )

            db.session.add(new_order)
            try:
                db.session.commit()
            except Exception:
                return {"status": "Failed", "msg": "Duplicate order_id"}, 400

            return {"status": "Success", "msg": "Successfully created"}, 201

        except Exception as e:
            return {'status': 'Failed'}, 200


api.add_resource(CreateReturn, '/orders/v1/create_return/<order_id>')


class ShipOrders(Resource):

    method_decorators = [authenticate_restful]

    def post(self, resp):
        try:
            data = json.loads(request.data)
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404
            if auth_data['user_group'] != 'super-admin':
                return {"success": False, "msg": "User not Admin"}, 404

            courier_name = data.get('courier')
            order_ids = data.get('order_ids')

            request_body = {"courier_name": courier_name, "order_ids": order_ids}

            res = requests.post("https://2qrojivwz2.execute-api.us-east-2.amazonaws.com/default/ShipOrders", data=json.dumps(request_body))

            if not res.json():
                return {'status': 'success', 'msg': "successfully shipped"}, 200
            else:
                return {'status': 'Failed', 'msg': "some error occurred"}, 400

        except Exception as e:
            return {"status":"Failed", "msg":""}, 400

    def get(self, resp):
        cur = conn.cursor()
        auth_data = resp.get('data')
        if not auth_data:
            return {"success": False, "msg": "Auth Failed"}, 404
        if auth_data['user_group'] != 'super-admin':
            return {"success": False, "msg": "User not Admin"}, 404

        cur.execute("""SELECT array_agg(courier_name) FROM
                        (SELECT courier_name FROM master_couriers WHERE integrated=true ORDER BY courier_name) xx""")

        response = {"couriers":cur.fetchone()[0], "success": True}

        return response, 200


api.add_resource(ShipOrders, '/orders/v1/ship_orders')


@orders_blueprint.route('/orders/v1/track/<awb>', methods=['GET'])
@cross_origin()
def track_order(awb):
    try:
        shipment = db.session.query(Shipments).filter(Shipments.awb==awb).first()
        req_obj = None
        if not shipment:
            req_obj = requests.get("https://track.delhivery.com/api/status/packages/json/?waybill=%s&token=d6ce40e10b52b5ca74805a6e2fb45083f0194185"%str(awb)).json()
            if 'ShipmentData' not in req_obj or not req_obj['ShipmentData']:
                return jsonify({"success": False, "msg": "tracking id not found"}), 400

        details = request.args.get('details')
        if details:
            if shipment and shipment.courier.courier_name.startswith('Xpressbees'): #Xpressbees details of status
                try:
                    return_details = tracking_get_xpressbees_details(shipment, awb)
                    return jsonify({"success": True, "data": return_details}), 200
                except Exception as e:
                    return jsonify({"success": False, "msg": "Details not available"}), 400

            if shipment and shipment.courier.courier_name.startswith('Delhivery'): #Delhivery details of status
                try:
                    return_details = tracking_get_delhivery_details(shipment, awb)
                    return jsonify({"success": True, "data": return_details}), 200
                except Exception as e:
                    return jsonify({"success": False, "msg": "Details not available"}), 400

            if shipment and shipment.courier.courier_name.startswith('Bluedart'): #Bluedart details of status
                try:
                    return_details = tracking_get_bluedart_details(shipment, awb)
                    return jsonify({"success": True, "data": return_details}), 200
                except Exception as e:
                    return jsonify({"success": False, "msg": "Details not available"}), 400

            if shipment and shipment.courier.courier_name.startswith('Ecom'): #Ecom details of status
                try:
                    return_details = tracking_get_ecomxp_details(shipment, awb)
                    return jsonify({"success": True, "data": return_details}), 200
                except Exception as e:
                    return jsonify({"success": False, "msg": "Details not available"}), 400

        if req_obj and 'ShipmentData' in req_obj and req_obj['ShipmentData']:
            response = dict()
            last_status = req_obj['ShipmentData'][0]['Shipment']['Status']['Status']
            response['tracking_id'] = awb
            response['status'] = last_status
            response['logo_url'] = None
            response['theme_color'] = None
            response['remark'] = req_obj['ShipmentData'][0]['Shipment']['Status']['Instructions']
            response['order_id'] = req_obj['ShipmentData'][0]['Shipment']['ReferenceNo']
            if len(req_obj['ShipmentData'][0]['Shipment']['PickUpDate'])!=19:
                status_time = datetime.strptime(req_obj['ShipmentData'][0]['Shipment']['PickUpDate'], '%Y-%m-%dT%H:%M:%S.%f')
            else:
                status_time = datetime.strptime(req_obj['ShipmentData'][0]['Shipment']['PickUpDate'], '%Y-%m-%dT%H:%M:%S')

            response['placed_on'] = status_time.strftime("%d %b %Y, %H:%M:%S")
            response['get_details'] = True
            if 'expectedDate' in req_obj['ShipmentData'][0]['Shipment']:
                response['arriving_on'] = req_obj['ShipmentData'][0]['Shipment']['expectedDate'][:10]
            else:
                response['arriving_on'] = None
            picked_obj = {'status': 'Picked', 'city': None, 'time': None}
            in_transit_obj = {'status': 'In Transit', 'city': None, 'time': None}
            ofd_obj = {'status': 'Out for delivery', 'city': None, 'time': None}
            del_obj = {'status': 'Delivered', 'city': None, 'time': None}
            for order_status in req_obj['ShipmentData'][0]['Shipment']['Scans']:
                status_dict = dict()
                if 'Picked Up' in order_status['ScanDetail']['Instructions']:
                    status_dict['status'] = 'Picked'
                    status_dict['city'] = order_status['ScanDetail']['ScannedLocation']
                    if len(order_status['ScanDetail']['StatusDateTime'])!=19:
                        status_time = datetime.strptime(order_status['ScanDetail']['StatusDateTime'],
                                                                 '%Y-%m-%dT%H:%M:%S.%f')
                    else:
                        status_time = datetime.strptime(order_status['ScanDetail']['StatusDateTime'],
                                                        '%Y-%m-%dT%H:%M:%S')
                    status_dict['time'] = status_time.strftime("%d %b %Y, %H:%M:%S")
                    picked_obj = status_dict
                elif order_status['ScanDetail']['Scan'] == 'In Transit':
                    status_dict['status'] = 'In Transit'
                    status_dict['city'] = order_status['ScanDetail']['ScannedLocation']
                    if len(order_status['ScanDetail']['StatusDateTime']) != 19:
                        status_time = datetime.strptime(order_status['ScanDetail']['StatusDateTime'],
                                                        '%Y-%m-%dT%H:%M:%S.%f')
                    else:
                        status_time = datetime.strptime(order_status['ScanDetail']['StatusDateTime'],
                                                        '%Y-%m-%dT%H:%M:%S')
                    status_dict['time'] = status_time.strftime("%d %b %Y, %H:%M:%S")
                    in_transit_obj = status_dict
                elif order_status['ScanDetail']['Scan'] == 'Dispatched':
                    status_dict['status'] = 'Out for delivery'
                    status_dict['city'] = order_status['ScanDetail']['ScannedLocation']
                    if len(order_status['ScanDetail']['StatusDateTime']) != 19:
                        status_time = datetime.strptime(order_status['ScanDetail']['StatusDateTime'],
                                                        '%Y-%m-%dT%H:%M:%S.%f')
                    else:
                        status_time = datetime.strptime(order_status['ScanDetail']['StatusDateTime'],
                                                        '%Y-%m-%dT%H:%M:%S')
                    status_dict['time'] = status_time.strftime("%d %b %Y, %H:%M:%S")
                    ofd_obj = status_dict
                elif 'Delivered' in order_status['ScanDetail']['Instructions']:
                    status_dict['status'] = 'Delivered'
                    status_dict['city'] = order_status['ScanDetail']['ScannedLocation']
                    if len(order_status['ScanDetail']['StatusDateTime']) != 19:
                        status_time = datetime.strptime(order_status['ScanDetail']['StatusDateTime'],
                                                        '%Y-%m-%dT%H:%M:%S.%f')
                    else:
                        status_time = datetime.strptime(order_status['ScanDetail']['StatusDateTime'],
                                                        '%Y-%m-%dT%H:%M:%S')
                    status_dict['time'] = status_time.strftime("%d %b %Y, %H:%M:%S")
                    del_obj = status_dict

            response['order_track'] = [picked_obj, in_transit_obj, ofd_obj, del_obj]

            return_response = jsonify({"success": True, "data": response})

            return return_response, 200

        order_statuses = db.session.query(OrderStatus).filter(OrderStatus.shipment==shipment)\
            .order_by(OrderStatus.status_time).all()
        if not order_statuses:
            return jsonify({"success": False, "msg": "tracking not available for this id"}), 400

        client_obj = db.session.query(ClientMapping).filter(ClientMapping.client_prefix==order_statuses[-1].order.client_prefix).first()

        response = dict()
        last_status = order_statuses[-1].status
        response['tracking_id'] = awb
        response['status'] = last_status
        response['logo_url'] = None
        response['theme_color'] = None
        response['products'] = list()
        for op_ass in shipment.order.products:
            prod_obj = {"name": op_ass.product.name, "quantity": op_ass.quantity}
            response['products'].append(prod_obj)
        response['destination_city'] = None
        if shipment.order.status not in ('DELIVERED','RTO') and shipment.order.status_type!='RT':
            response['destination_city'] = shipment.order.delivery_address.city
        if client_obj:
            response['logo_url'] = client_obj.client_logo
            response['theme_color'] = client_obj.theme_color
        response['remark'] = order_statuses[-1].status_text
        response['order_id'] = order_statuses[-1].order.channel_order_id
        response['placed_on'] = order_statuses[-1].order.order_date.strftime("%d %b %Y, %I:%M %p")
        response['get_details'] = True
        response['order_track'] = list()
        if shipment.edd:
            response['arriving_on'] = shipment.edd.strftime("%d %b")
        else:
            response['arriving_on'] = None
        for order_status in order_statuses:
            status_dict = dict()
            status_dict['status'] = order_status.status
            status_dict['city'] = order_status.location_city
            status_dict['time'] = order_status.status_time.strftime("%d %b %Y, %I:%M %p")
            response['order_track'].append(status_dict)

        addition_statuses = list()
        if last_status == "Received":
            addition_statuses = ["Picked", "In Transit", "Out for delivery", "Delivered"]
        elif last_status == "Picked":
            addition_statuses = ["In Transit", "Out for delivery", "Delivered"]
        elif last_status == "In Transit":
            addition_statuses = ["Out for delivery", "Delivered"]
        elif last_status == "Out for delivery":
            addition_statuses = ["Delivered"]

        for add_status in addition_statuses:
            status_dict = dict()
            status_dict['status'] = add_status
            status_dict['city'] = None
            status_dict['time'] = None
            response['order_track'].append(status_dict)

        return_response = jsonify({"success": True, "data": response})

        return return_response, 200
    except Exception as e:
        return jsonify({"success": False, "msg": str(e.args[0])}), 404


@orders_blueprint.route('/orders/v1/ivrcalls/call', methods=['GET'])
@authenticate_restful
def ivr_call(resp):
    try:
        order_id = request.args.get('unique_id')
        auth_data = resp.get('data')
        from_no = auth_data.get("phone_no")
        if not from_no:
            return jsonify({"success": False, "msg": "From number not found"}), 404

        order = db.session.query(Orders).filter(Orders.id==int(order_id)).first()
        if not order or not order.customer_phone:
            return jsonify({"success": False, "msg": "To number not found"}), 404

        ivr_obj = IVRHistory(order=order,
                             from_no=from_no,
                             to_no=str(order.customer_phone),
                             status="new",
                             call_time=datetime.utcnow() + timedelta(hours=5.5))

        db.session.add(ivr_obj)
        db.session.commit()
        ivr_id=ivr_obj.id

        ivr_url = "https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Calls/connect"
        call_data = {
            'From': from_no,
            'To': str(order.customer_phone),
            'CallerId': '08047188642',
            'CallType': 'trans',
            'StatusCallback': 'http://track.wareiq.com/orders/v1/ivrcalls/passthru/%s'%str(ivr_id),
            'MaxRetries': 1
        }

        req = requests.post(ivr_url, data=call_data)

        if req.status_code!=200:
            ivr_obj.status = "failed"
            db.session.commit()
        return jsonify({"success": True, "data": {"from_no": from_no, "to_no": order.customer_phone}}), 200

    except Exception as e:
        return jsonify({"success": False, "msg": str(e.args[0])}), 404


@orders_blueprint.route('/orders/v1/ivrcalls/call_history', methods=['GET'])
@authenticate_restful
def ivr_call_history(resp):
    try:
        order_id = request.args.get('unique_id')
        auth_data = resp.get('data')
        if not auth_data:
            return {"success": False, "msg": "Auth Failed"}, 404
        if not order_id:
            return jsonify({"success": False, "msg": "order not found"}), 404

        ivr_qs = db.session.query(IVRHistory).filter(IVRHistory.order_id==int(order_id)).order_by(IVRHistory.call_time.desc()).all()
        call_list = list()
        for ivr_call in ivr_qs:
            call_obj = dict()
            call_obj['from_no'] = ivr_call.from_no
            call_obj['to_no'] = ivr_call.to_no
            call_obj['call_time'] = ivr_call.call_time.strftime("%d %b %Y, %I:%M %p")
            call_obj['status'] = ivr_call.status
            call_obj['recording_url'] = ivr_call.recording_url
            call_list.append(call_obj)

        return jsonify({"success": True, "data": call_list}), 200

    except Exception as e:
        return jsonify({"success": False, "msg": str(e.args[0])}), 404


@orders_blueprint.route('/orders/v1/ivrcalls/passthru/<ivr_id>', methods=['POST'])
def ivr_passthru(ivr_id):
    try:
        ivr_obj = db.session.query(IVRHistory).filter(IVRHistory.id==int(ivr_id)).first()
        if not ivr_obj:
            return jsonify({"success": False, "msg": "IVR details not found"}), 404

        recording_url = request.form.get('RecordingUrl')
        call_sid = request.form.get('CallSid')
        status = request.form.get('Status')

        ivr_obj.call_sid = call_sid
        ivr_obj.recording_url = recording_url
        ivr_obj.status = status
        db.session.commit()
        return jsonify({"success": True}), 200

    except Exception as e:
        return jsonify({"success": False, "msg": str(e.args[0])}), 404


class PincodeServiceabilty(Resource):

    method_decorators = [authenticate_restful]

    def post(self, resp):
        try:
            cur = conn.cursor()
            cur_2 = conn_2.cursor()
            auth_data = resp.get('data')
            data = json.loads(request.data)
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 400

            del_pincode = data.get("pincode")
            cod_available = False

            sku_list = data.get("sku_list")
            if not del_pincode:
                return {"success": False, "msg": "Pincode not provided"}, 400

            covid_zone = None
            city = None
            state = None
            try:
                cod_req = requests.get(
                    "https://track.delhivery.com/c/api/pin-codes/json/?filter_codes=%s&token=d6ce40e10b52b5ca74805a6e2fb45083f0194185" % str(
                        del_pincode)).json()
                if not cod_req.get('delivery_codes'):
                    return {"success": False, "msg": "Pincode not serviceable"}, 400

                if cod_req['delivery_codes'][0]['postal_code']['cod'].lower() == 'y':
                    cod_available = True
                covid_zone = cod_req['delivery_codes'][0]['postal_code']['covid_zone']
                city = cod_req['delivery_codes'][0]['postal_code']['district']
                state = cod_req['delivery_codes'][0]['postal_code']['state_code']
            except Exception:
                pass
            if not sku_list:
                return {"success": True, "data": {"cod_available": cod_available, "covid_zone": covid_zone, "city": city, "state":state}}, 200

            sku_string = "('"

            for value in sku_list:
                sku_string += value['sku'] + "','"
            sku_string = sku_string.rstrip("'").rstrip(",")
            sku_string += ")"
            sku_dict = dict()

            cur.execute("""select cc.sku, cc.master_sku from products_combos aa
                            left join products bb on aa.combo_id=bb.id
                            left join products cc on aa.combo_prod_id=cc.id
                            WHERE (bb.sku in __SKU_STR__ or bb.master_sku in __SKU_STR__) 
                            and bb.client_prefix='__CLIENT__'""".replace('__SKU_STR__', sku_string).replace('__CLIENT__', auth_data[
                                                                                                            'client_prefix']))
            sku_tuple = cur.fetchall()
            if not sku_tuple:
                cur.execute("SELECT sku, master_sku FROM products WHERE (sku in __SKU_STR__ or master_sku in __SKU_STR__) and client_prefix='__CLIENT__'".replace('__SKU_STR__', sku_string).replace('__CLIENT__', auth_data[
                                                                                                            'client_prefix']))
                sku_tuple = cur.fetchall()

                for sku in sku_list:
                    [accept_sku] = [a[0] for a in sku_tuple if sku['sku'] in a]
                    sku_dict[str(accept_sku)] = sku['quantity']
            else:
                for sku in sku_tuple:
                    sku_dict[str(sku[0])] = 1 #defaulting qty to one for combos

            sku_string = "('"

            for key, value in sku_dict.items():
                sku_string += key + "','"
            sku_string = sku_string.rstrip("'")
            sku_string = sku_string.rstrip(",")
            sku_string += ")"

            no_sku = len(sku_list)
            try:
                cur.execute(
                    available_warehouse_product_quantity.replace('__SKU_STR__', sku_string).replace('__CLIENT_PREFIX__',
                                                                                                    auth_data[
                                                                                                        'client_prefix']))
            except Exception:
                conn.rollback()
                return {"success": False, "msg": "",
                        "cod_available": cod_available,
                        "label_url":"https://logourls.s3.amazonaws.com/wareiq_standard.jpeg"}, 400

            prod_wh_tuple = cur.fetchall()
            wh_dict = dict()
            courier_id = 1
            courier_id_weight = 0.0
            for prod_wh in prod_wh_tuple:
                if sku_dict[prod_wh[2]] <= prod_wh[3]:
                    if prod_wh[0] not in wh_dict:
                        wh_dict[prod_wh[0]] = {"pincode": prod_wh[6], "count": 1}
                    else:
                        wh_dict[prod_wh[0]]['count'] += 1

            if not wh_dict:
                return {"success": False, "msg": "One or more SKUs not serviceable",
                        "cod_available": cod_available,
                        "label_url":"https://logourls.s3.amazonaws.com/wareiq_standard.jpeg"}, 400

            warehouse_pincode_str = ""
            highest_num_loc = list()
            for key, value in wh_dict.items():
                if not highest_num_loc or value['count'] > highest_num_loc[1]:
                    highest_num_loc = [key, value['count'], value['pincode']]
                if value['count'] == no_sku:
                    warehouse_pincode_str += "('" + key + "','" + str(value['pincode']) + "'),"

            if not warehouse_pincode_str:
                warehouse_pincode_str = "('" + str(highest_num_loc[0]) + "','" + str(str(highest_num_loc[2])) + "'),"

            warehouse_pincode_str = warehouse_pincode_str.rstrip(',')

            if courier_id in (8, 11, 12):
                courier_id = 1

            try:
                cur_2.execute(fetch_warehouse_to_pick_from.replace('__WAREHOUSE_PINCODES__', warehouse_pincode_str).replace(
                    '__COURIER_ID__', str(courier_id)).replace('__DELIVERY_PINCODE__', str(del_pincode)))
            except Exception:
                conn_2.rollback()
                return {"success": False, "msg": "",
                        "cod_available": cod_available,
                        "label_url": "https://logourls.s3.amazonaws.com/wareiq_standard.jpeg"}, 400

            final_wh = cur_2.fetchone()

            if not final_wh or final_wh[1] is None:
                return {"success": True, "data": {"cod_available": cod_available, "covid_zone": covid_zone,
                        "label_url":"https://logourls.s3.amazonaws.com/wareiq_standard.jpeg"}}, 200

            current_time = datetime.utcnow() + timedelta(hours=5.5)
            order_before = current_time
            if current_time.hour >= 14:
                order_before = order_before + timedelta(days=1)
                order_before = order_before.replace(hour=14, minute=0, second=0)
                days_for_delivery = final_wh[1] + 1
                if days_for_delivery == 1:
                    days_for_delivery = 2
            else:
                order_before = order_before.replace(hour=14, minute=0, second=0)
                days_for_delivery = final_wh[1]
                if days_for_delivery == 0:
                    days_for_delivery = 1

            days_for_delivery += 1
            if days_for_delivery == 1:
                label_url = "https://logourls.s3.amazonaws.com/wareiq_next_day.jpeg"
            elif days_for_delivery == 2:
                label_url = "https://logourls.s3.amazonaws.com/wareiq_two_days.jpeg"
            else:
                label_url = "https://logourls.s3.amazonaws.com/wareiq_standard.jpeg"

            delivered_by = datetime.utcnow() + timedelta(hours=5.5) + timedelta(
                days=days_for_delivery)

            delivery_zone = final_wh[2]
            if delivery_zone in ('D1', 'D2'):
                delivery_zone = 'D'
            if delivery_zone in ('C1', 'C2'):
                delivery_zone = 'C'

            sku_wise_list = list()
            for key, value in sku_dict.items():
                sku_wise_list.append({"sku":key, "quantity":value, "warehouse": final_wh[0],
                                      "delivery_date": delivered_by.strftime('%d-%m-%Y'),
                                      "delivery_zone": delivery_zone})

            cod_disabled_sku = ["NM_CSK_H8012_Amsterdam_Yellow_20",
                                "NM_CSK_H8012_Amsterdam_Yellow_24",
                                "NM_CSK_H8012_Amsterdam_Yellow_28",
                                "NM_CSK_H8012_Amsterdam_Yellow_S3",
                                "NM_CSK_H8012_Amsterdam_Yellow_20-24",
                                "NM_CSK_H8012_Amsterdam_Yellow_24-28",
                                "NM_CSK_A849_Bruges_Yellow_20",
                                "NM_CSK_A849_Bruges_Yellow_24",
                                "NM_CSK_A849_Bruges_Yellow_28",
                                "NM_CSK_A849_Bruges_Yellow_S3",
                                "NM_CSK_A849_Bruges_Yellow_20-24",
                                "NM_CSK_A849_Bruges_Yellow_24-28",
                                "NM_CSK_PP03_Nicobar_Yellow & Navy Blue_20",
                                "NM_CSK_PP03_Nicobar_Yellow & Navy Blue_24",
                                "NM_CSK_PP03_Nicobar_Yellow & Navy Blue_28",
                                "NM_CSK_PP03_Nicobar_Yellow & Navy Blue_S3",
                                "NM_CSK_PP03_Nicobar_Yellow & Navy Blue_20-24",
                                "NM_CSK_PP03_Nicobar_Yellow & Navy Blue_24-28"]

            for sku_ob in sku_wise_list:
                if sku_ob['sku'] in cod_disabled_sku:
                    cod_available = False
                    break

            return_data = {"warehouse": final_wh[0],
                           "delivery_date": delivered_by.strftime('%d-%m-%Y'),
                           "cod_available": cod_available,
                           "order_before": order_before.strftime('%d-%m-%Y %H:%M:%S'),
                           "delivery_zone": delivery_zone,
                           "label_url": label_url,
                           "sku_wise": sku_wise_list,
                           "covid_zone": covid_zone,
                           "city": city,
                           "state":state}

            return {"success": True, "data": return_data}, 200

        except Exception as e:
            return {"success": False, "msg": ""}, 400

    def get(self, resp):
        try:
            auth_data = resp.get('data')
            pincode = request.args.get('pincode')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 400
            if not pincode:
                return {"success": False, "msg": "Pincode not provided"}, 401

            cod_req = requests.get(
                "https://track.delhivery.com/c/api/pin-codes/json/?filter_codes=%s&token=d6ce40e10b52b5ca74805a6e2fb45083f0194185" % str(
                    pincode)).json()
            if not cod_req.get('delivery_codes'):
                return {"success": False, "msg": "Pincode not serviceable"}, 400

            cod_available = False
            reverse_pickup = False
            if cod_req['delivery_codes'][0]['postal_code']['cod'].lower() == 'y':
                cod_available = True
            if cod_req['delivery_codes'][0]['postal_code']['pickup'].lower() == 'y':
                reverse_pickup = True

            return {"success": True, "data": {"serviceable": True, "cod_available": cod_available, "reverse_pickup": reverse_pickup}}, 200

        except Exception as e:
            return {"success": False, "msg": ""}, 400


api.add_resource(PincodeServiceabilty, '/orders/v1/serviceability')


class GetShipmentData(Resource):

    method_decorators = [authenticate_restful]

    def post(self, resp):
        try:
            data = json.loads(request.data)
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404

            channel_order_ids = data.get('order_nos')
            if not channel_order_ids:
                return {"success": False, "msg": "orders_nos is mandatory"}, 400

            if len(channel_order_ids)>20:
                return {"success": False, "msg": "Max 20 orders allowed at a time"}, 400

            orders_qs = db.session.query(Orders).filter(Orders.order_id_channel_unique.in_(channel_order_ids), Orders.client_prefix==auth_data.get('client_prefix')).all()

            if not orders_qs:
                return {"success": False, "msg": "Orders not found for given ids"}, 400

            return_data = list()
            orders_available_list = list()

            for order in orders_qs:
                ret_obj = dict()
                ret_obj['order_no'] = order.order_id_channel_unique
                if order.shipments and order.shipments[0].courier:
                    ret_obj['status'] = 'success'
                    ret_obj['sort_code'] = order.shipments[0].routing_code
                    ret_obj['awb'] = order.shipments[0].awb
                    ret_obj['courier'] = order.shipments[0].courier.courier_name
                else:
                    ret_obj['status'] = 'failure'
                    ret_obj['msg'] = 'order not shipped yet'

                ret_obj['WH'] = order.pickup_data.pickup.warehouse_prefix if order.pickup_data else None
                return_data.append(ret_obj)
                orders_available_list.append(order.order_id_channel_unique)

            for order_no in channel_order_ids:
                if order_no not in orders_available_list:
                    ret_obj = dict()
                    ret_obj['order_no'] = order_no
                    ret_obj['status'] = 'failure'
                    ret_obj['msg'] = 'order not found'
                    return_data.append(ret_obj)

            return {"success": True, "data": return_data}, 200

        except Exception as e:
            return {"status":"Failed", "msg":""}, 400


api.add_resource(GetShipmentData, '/orders/v1/shipments')


class TrackShipments(Resource):

    method_decorators = [authenticate_restful]

    def post(self, resp):
        try:
            data = json.loads(request.data)
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404

            return_data = list()
            awbs = data.get('awbs')
            if not awbs:
                return {"status": "Failed", "msg": "awbs value is mandatory"}, 400

            if len(awbs)>50:
                return {"status": "Failed", "msg": "max allowed awbs 50"}, 400

            shipment_qs = db.session.query(Shipments).filter(Shipments.awb.in_(awbs)).all()
            found_awbs = list()
            for shipment in shipment_qs:
                try:
                    found_awbs.append(shipment.awb)
                    order_statuses = db.session.query(OrderStatus).filter(OrderStatus.shipment == shipment) \
                        .order_by(OrderStatus.status_time).all()
                    if not order_statuses:
                        return_data.append({"awb": shipment.awb, "status": "failure", "msg": "tracking not available for this id"})

                    client_obj = db.session.query(ClientMapping).filter(
                        ClientMapping.client_prefix == order_statuses[-1].order.client_prefix).first()

                    response = dict()
                    last_status = order_statuses[-1].status
                    response['awb'] = shipment.awb
                    response['success'] = True
                    response['status'] = last_status
                    response['logo_url'] = None
                    response['theme_color'] = None
                    response['products'] = list()
                    for op_ass in shipment.order.products:
                        prod_obj = {"name": op_ass.product.name, "quantity": op_ass.quantity}
                        response['products'].append(prod_obj)
                    response['destination_city'] = None
                    if shipment.order.status not in ('DELIVERED', 'RTO') and shipment.order.status_type != 'RT':
                        response['destination_city'] = shipment.order.delivery_address.city
                    if client_obj:
                        response['logo_url'] = client_obj.client_logo
                        response['theme_color'] = client_obj.theme_color
                    response['remark'] = order_statuses[-1].status_text
                    response['order_id'] = order_statuses[-1].order.channel_order_id
                    response['placed_on'] = order_statuses[-1].order.order_date.strftime("%d %b %Y, %I:%M %p")
                    response['get_details'] = True
                    response['order_track'] = list()
                    if shipment.edd:
                        response['arriving_on'] = shipment.edd.strftime("%d %b")
                    else:
                        response['arriving_on'] = None
                    for order_status in order_statuses:
                        status_dict = dict()
                        status_dict['status'] = order_status.status
                        status_dict['city'] = order_status.location_city
                        status_dict['time'] = order_status.status_time.strftime("%d %b %Y, %I:%M %p")
                        response['order_track'].append(status_dict)

                    return_data.append(response)

                except Exception as e:
                    return_data.append({"awb": shipment.awb, "success": False, "msg": "something went wrong"})

            for awb in awbs:
                if awb not in found_awbs:
                    return_data.append({"awb": awb, "success": False, "msg": "awb not found"})

            return {"success":True, "data":return_data}, 200

        except Exception as e:
            return {"status":"Failed", "msg":""}, 400


api.add_resource(TrackShipments, '/orders/v1/shipments/track')


@orders_blueprint.route('/orders/v1/getPickupPoints', methods=['GET'])
@authenticate_restful
def get_pickup_points(resp):
    response = {'pickup_points': [], 'status': 'fail'}
    try:
        cur = conn.cursor()
        auth_data = resp.get('data')
        client_prefix = auth_data.get('client_prefix')
        pickup_points_select_query = """select array_agg(warehouse_prefix) from
                                                    (select distinct bb.warehouse_prefix from client_pickups aa
                                                    left join pickup_points bb on aa.pickup_id=bb.id
                                                    __CLIENT_FILTER__
                                                    order by bb.warehouse_prefix) xx"""
        if auth_data['user_group'] == 'super-admin':
            pickup_points_select_query = pickup_points_select_query.replace("__CLIENT_FILTER__", "")
        elif auth_data['user_group'] == 'client':
            pickup_points_select_query = pickup_points_select_query.replace("__CLIENT_FILTER__",
                                                                            "where aa.client_prefix='%s'" % str(
                                                                                client_prefix))
        elif auth_data['user_group'] == 'multi-vendor':
            pickup_points_select_query = pickup_points_select_query.replace("__CLIENT_FILTER__",
                                                                            "where aa.client_prefix in (select unnest(vendor_list) from multi_vendor where client_prefix='%s')" % str(
                                                                                client_prefix))
        else:
            pickup_points_select_query = None

        if pickup_points_select_query:
            cur.execute(pickup_points_select_query)
            all_pickups = cur.fetchone()[0]
            response['pickup_points'] = all_pickups
        response['status'] = 'success'
        return jsonify(response), 200
    except Exception:
        response['message'] = 'failed while getting pickup-points'
        return jsonify(response), 400