# services/core/project/api/core.py

import requests, json, math, pytz, psycopg2
import boto3, os, csv, io, smtplib
import pandas as pd
import numpy as np
import re
from flask_cors import cross_origin
from datetime import datetime, timedelta
from sqlalchemy import or_, func, not_, and_
from flask import Blueprint, request, jsonify, make_response
from flask_restful import Resource, Api
from reportlab.lib.pagesizes import landscape, A4
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
from psycopg2.extras import RealDictCursor
from .generate_manifest import fill_manifest_data
from .worker import celery

from project import db
from .queries import product_count_query, available_warehouse_product_quantity, fetch_warehouse_to_pick_from, \
    select_product_list_query, select_orders_list_query, select_wallet_deductions_query, select_wallet_remittance_query, \
    select_wallet_remittance_orders_query
from project.api.models import Products, ProductQuantity, InventoryUpdate, WarehouseMapping, NDRReasons, MultiVendor, \
    Orders, OrdersPayments, PickupPoints, MasterChannels, ClientPickups, CodVerification, NDRVerification, NDRShipments,\
    MasterCouriers, Shipments, OPAssociation, ShippingAddress, Manifests, ClientCouriers, OrderStatus, DeliveryCheck, ClientMapping, IVRHistory
from project.api.utils import authenticate_restful, get_products_sort_func, fill_shiplabel_data_thermal, \
    get_orders_sort_func, create_shiplabel_blank_page, fill_shiplabel_data, create_shiplabel_blank_page_thermal, \
    create_invoice_blank_page, fill_invoice_data, generate_picklist, generate_packlist

core_blueprint = Blueprint('core', __name__)
api = Api(core_blueprint)

session = boto3.Session(
    aws_access_key_id='AKIAWRT2R3KC3YZUBFXY',
    aws_secret_access_key='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',
)

conn = psycopg2.connect(host=os.environ.get('DATABASE_HOST'), database=os.environ.get('DATABASE_NAME'), user=os.environ.get('DATABASE_USER'), password=os.environ.get('DATABASE_PASSWORD'))
conn_2 = psycopg2.connect(host=os.environ.get('DATABASE_HOST_PINCODE'), database=os.environ.get('DATABASE_NAME'), user=os.environ.get('DATABASE_USER'), password=os.environ.get('DATABASE_PASSWORD'))

email_server = smtplib.SMTP_SSL('smtpout.secureserver.net', 465)
email_server.login("noreply@wareiq.com", "Berlin@123")


ORDERS_DOWNLOAD_HEADERS = ["Order ID", "Customer Name", "Customer Email", "Customer Phone", "Order Date",
                            "Courier", "Weight", "awb", "Expected Delivery Date", "Status", "Address_one", "Address_two",
                           "City", "State", "Country", "Pincode", "Pickup Point", "Product", "SKU", "Quantity", "Order Type",
                           "Amount", "Pickup Date", "Delivered Date", "COD Verfication", "COD Verified Via", "NDR Verfication", "NDR Verified Via"]

PRODUCTS_DOWNLOAD_HEADERS = ["S. No.", "Product Name", "Channel SKU", "Master SKU", "Price", "Total Quantity",
                             "Available Quantity", "Current Quantity", "Inline Quantity", "RTO Quantity", "Dimensions", "Weight"]

DEDUCTIONS_DOWNLOAD_HEADERS = ["Time", "Status", "Courier", "AWB", "order ID", "COD cost", "Forward cost", "Return cost",
                              "Management Fee", "Subtotal", "Total", "Zone", "Weight Charged"]

REMITTANCE_DOWNLOAD_HEADERS = ["Order ID", "Order Date", "Courier", "AWB", "Payment Mode", "Amount", "Delivered Date"]

RECHARGES_DOWNLOAD_HEADERS = ["Payment Time", "Amount", "Transaction ID", "status"]

ORDERS_UPLOAD_HEADERS = ["order_id", "customer_name", "customer_email", "customer_phone", "address_one", "address_two",
                         "city", "state", "country", "pincode", "sku", "sku_quantity", "payment_mode", "subtotal", "shipping_charges", "warehouse", "Error"]
'''
class ProductList(Resource):

    method_decorators = {'post': [authenticate_restful]}

    def post(self, resp, type):
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            response = {'status':'success', 'data': dict(), "meta": dict()}
            data = json.loads(request.data)
            page = data.get('page', 1)
            per_page = data.get('per_page', 10)
            if int(per_page) > 250:
                return {"success": False, "error": "upto 250 results allowed per page"}, 401
            sort = data.get('sort', "desc")
            sort_by = data.get('sort_by', 'available_quantity')
            search_key = data.get('search_key', '')
            filters = data.get('filters', {})
            download_flag = request.args.get("download", None)
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404
            client_prefix = auth_data.get('client_prefix')
            query_to_execute = select_product_list_query
            if auth_data['user_group'] == 'client':
                query_to_execute = query_to_execute.replace('__CLIENT_FILTER__', "AND aa.client_prefix in ('%s')"%client_prefix)
            if auth_data['user_group'] == 'warehouse':
                query_to_execute = query_to_execute.replace('__WAREHOUSE_FILTER__', "WHERE warehouse_prefix in ('%s')"%auth_data.get('warehouse_prefix'))
                query_to_execute = query_to_execute.replace('__JOIN_TYPE__', "JOIN")
            if auth_data['user_group'] == 'multi-vendor':
                cur.execute("SELECT vendor_list FROM multi_vendor WHERE client_prefix='%s';"%client_prefix)
                vendor_list = cur.fetchone()['vendor_list']
                query_to_execute = query_to_execute.replace('__MV_CLIENT_FILTER__', "AND aa.client_prefix in %s"%str(tuple(vendor_list)))
            else:
                query_to_execute = query_to_execute.replace('__MV_CLIENT_FILTER__', "")

            if filters:
                if 'warehouse' in filters:
                    if len(filters['warehouse'])==1:
                        wh_filter = "WHERE warehouse_prefix in ('%s')"%filters['warehouse'][0]
                    else:
                        wh_filter = "WHERE warehouse_prefix in %s"%str(tuple(filters['warehouse']))

                    query_to_execute = query_to_execute.replace('__WAREHOUSE_FILTER__', wh_filter)
                    query_to_execute = query_to_execute.replace('__JOIN_TYPE__', "JOIN")

                if 'client' in filters:
                    if len(filters['client'])==1:
                        cl_filter = "AND aa.client_prefix in ('%s')"%filters['client'][0]
                    else:
                        cl_filter = "AND aa.client_prefix in %s"%str(tuple(filters['client']))

                    query_to_execute = query_to_execute.replace('__CLIENT_FILTER__', cl_filter)

            if type != 'all':
                return {"success": False, "msg": "Invalid URL"}, 404

            query_to_execute = query_to_execute.replace('__JOIN_TYPE__', "LEFT JOIN")
            query_to_execute = query_to_execute.replace('__CLIENT_FILTER__',"").replace('__WAREHOUSE_FILTER__', "")
            if sort.lower() == 'desc':
                sort = "DESC NULLS LAST"
            query_to_execute = query_to_execute.replace('__ORDER_BY__', sort_by).replace('__ORDER_TYPE__', sort)
            query_to_execute = query_to_execute.replace('__SEARCH_KEY__', search_key)
            if download_flag:
                s_no = 1
                query_to_run = query_to_execute.replace('__PAGINATION__', "")
                query_to_run = re.sub(r"""__.+?__""", "", query_to_run)
                cur.execute(query_to_run)
                products_qs_data = cur.fetchall()
                si = io.StringIO()
                cw = csv.writer(si)
                cw.writerow(PRODUCTS_DOWNLOAD_HEADERS)
                for product in products_qs_data:
                    try:
                        new_row = list()
                        new_row.append(str(s_no))
                        new_row.append(str(product['product_name']))
                        new_row.append(str(product['channel_sku']))
                        new_row.append(str(product['master_sku']))
                        new_row.append(str(product['price']))
                        new_row.append(str(product['total_quantity']))
                        new_row.append(str(product['available_quantity']))
                        new_row.append(str(product['current_quantity']))
                        new_row.append(str(product['inline_quantity']))
                        new_row.append(str(product['rto_quantity']))
                        new_row.append(str(product['dimensions']))
                        new_row.append(str(product['weight']))
                        cw.writerow(new_row)
                        s_no += 1
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
            response['data'] = cur.fetchall()

            total_pages = math.ceil(total_count/per_page)
            response['meta']['pagination'] = {'total': total_count,
                                              'per_page':per_page,
                                              'current_page': page,
                                              'total_pages':total_pages}

            return response, 200
        except Exception as e:
            return {"success": False, "error":str(e.args[0])}, 404


api.add_resource(ProductList, '/products/<type>')


@core_blueprint.route('/products/v1/get_filters', methods=['GET'])
@authenticate_restful
def get_products_filters(resp):
    response = {"filters":{}, "success": True}
    auth_data = resp.get('data')
    current_tab = request.args.get('tab')
    client_prefix = auth_data.get('client_prefix')
    all_vendors = None
    if auth_data['user_group'] == 'multi-vendor':
        all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
        all_vendors = all_vendors.vendor_list
    warehouse_qs = db.session.query(ProductQuantity.warehouse_prefix, func.count(ProductQuantity.warehouse_prefix))\
                .join(Products, Products.id == ProductQuantity.product_id)
    if auth_data['user_group'] == 'client':
        warehouse_qs = warehouse_qs.filter(Products.client_prefix == client_prefix)
    if auth_data['user_group'] == 'warehouse':
        warehouse_qs = warehouse_qs.filter(ProductQuantity.warehouse_prefix == auth_data.get('warehouse_prefix'))
    if all_vendors:
        warehouse_qs = warehouse_qs.filter(Products.client_prefix.in_(all_vendors))
    if current_tab == 'active':
        warehouse_qs = warehouse_qs.filter(Products.active == True)
    elif current_tab =='inactive':
        warehouse_qs = warehouse_qs.filter(Products.active == False)
    warehouse_qs = warehouse_qs.group_by(ProductQuantity.warehouse_prefix)
    response['filters']['warehouse'] = [{x[0]: x[1]} for x in warehouse_qs]
    if auth_data['user_group'] in ('super-admin','warehouse'):
        client_qs = db.session.query(Products.client_prefix, func.count(Products.client_prefix)).join(ProductQuantity, ProductQuantity.product_id == Products.id).group_by(Products.client_prefix)
        if auth_data['user_group'] == 'warehouse':
            client_qs = client_qs.filter(ProductQuantity.warehouse_prefix == auth_data.get('warehouse_prefix'))
        response['filters']['client'] = [{x[0]: x[1]} for x in client_qs]
    if all_vendors:
        client_qs = db.session.query(Products.client_prefix, func.count(Products.client_prefix)).join(ProductQuantity,
                                                                                                      ProductQuantity.product_id == Products.id).filter(
            Products.client_prefix.in_(all_vendors)).group_by(Products.client_prefix)
        response['filters']['client'] = [{x[0]: x[1]} for x in client_qs]

    return jsonify(response), 200


class ProductUpdate(Resource):

    method_decorators = [authenticate_restful]

    def patch(self, resp, product_id):
        try:
            data = json.loads(request.data)
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404

            product = db.session.query(Products).filter(Products.id==int(product_id)).first()

            if not product:
                return {"success": False, "msg": "No product found for given id"}, 400

            if data.get('product_name'):
                product.name =data.get('product_name')
            if data.get('master_sku'):
                product.master_sku =data.get('master_sku')
            if data.get('price'):
                product.price = float(data.get('price'))
            if data.get('dimensions'):
                product.dimensions = data.get('dimensions')
            if data.get('weight'):
                product.weight = data.get('weight')

            db.session.commit()
            return {'status': 'success', 'msg': "successfully updated"}, 200

        except Exception as e:
            return {'status': 'Failed'}, 200


api.add_resource(ProductUpdate, '/products/v1/product/<product_id>')


@core_blueprint.route('/products/v1/details', methods=['GET'])
@authenticate_restful
def get_products_details(resp):
    try:
        cur = conn.cursor()
        auth_data = resp.get('data')
        client_prefix = auth_data.get('client_prefix')
        sku = request.args.get('sku')
        if not sku:
            return jsonify({"success": False, "msg": "SKU not provided"}), 400

        query_to_run = """SELECT name, sku as channel_sku, master_sku, weight, dimensions, price, bb.warehouse_prefix as warehouse, 
                            bb.approved_quantity as total_quantity, bb.current_quantity, bb.available_quantity, bb.inline_quantity, bb.rto_quantity
                            from products aa
                            left join products_quantity bb on aa.id=bb.product_id
                            WHERE client_prefix='%s'
                            and (sku='%s' or master_sku='%s')
                            __WAREHOUSE_FILTER__"""%(client_prefix, sku, sku)
        warehouse = request.args.get('warehouse')
        if warehouse:
            query_to_run  = query_to_run.replace('__WAREHOUSE_FILTER__', "and warehouse_prefix='%s'"%warehouse)
            cur.execute(query_to_run)
            ret_tuple = cur.fetchone()
            if not ret_tuple:
                return jsonify({"success": False, "msg": "SKU, warehouse combination not found"}), 400

            ret_obj = {"name":ret_tuple[0],
                       "channel_sku": ret_tuple[1],
                       "master_sku": ret_tuple[2],
                       "weight": ret_tuple[3],
                       "dimensions": ret_tuple[4],
                       "price": ret_tuple[5],
                       "warehouse": ret_tuple[6],
                       "total_quantity": ret_tuple[7],
                       "current_quantity": ret_tuple[8],
                       "available_quantity": ret_tuple[9],
                       "inline_quantity": ret_tuple[10],
                       "rto_quantity": ret_tuple[11],
                       }
            return jsonify({"success": True, "data": ret_obj}), 200

        query_to_run = query_to_run.replace('__WAREHOUSE_FILTER__', "")
        cur.execute(query_to_run)
        ret_tuple_all = cur.fetchall()
        if not ret_tuple_all:
            return jsonify({"success": False, "msg": "SKU, warehouse combination not found"}), 400

        ret_list = list()

        for ret_tuple in ret_tuple_all:
            ret_obj = {"name": ret_tuple[0],
                       "channel_sku": ret_tuple[1],
                       "master_sku": ret_tuple[2],
                       "weight": ret_tuple[3],
                       "dimensions": ret_tuple[4],
                       "price": ret_tuple[5],
                       "warehouse": ret_tuple[6],
                       "total_quantity": ret_tuple[7],
                       "current_quantity": ret_tuple[8],
                       "available_quantity": ret_tuple[9],
                       "inline_quantity": ret_tuple[10],
                       "rto_quantity": ret_tuple[11],
                       }

            ret_list.append(ret_obj)

        return jsonify({"success": True, "data": ret_list}), 200

    except Exception as e:
        return jsonify({"success": False}), 400
'''
'''
class OrderList(Resource):

    method_decorators = {'post': [authenticate_restful]}

    def post(self, resp, type):
        try:
            hide_weights = None
            cur = conn.cursor()
            response = {'status':'success', 'data': dict(), "meta": dict()}
            data = json.loads(request.data)
            page = data.get('page', 1)
            per_page = data.get('per_page', 10)
            search_key = data.get('search_key', '')
            since_id = data.get('since_id', None)
            filters = data.get('filters', {})
            download_flag = request.args.get("download", None)
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404

            client_prefix = auth_data.get('client_prefix')
            cur.execute("SELECT hide_weights FROM client_mapping WHERE client_prefix='%s'"%client_prefix)
            try:
                hide_weights = cur.fetchone()[0]
            except Exception:
                pass
            pickup_points_select_query = """select array_agg(warehouse_prefix) from
                                            (select distinct bb.warehouse_prefix from client_pickups aa
                                            left join pickup_points bb on aa.pickup_id=bb.id
                                            __CLIENT_FILTER__
                                            order by bb.warehouse_prefix) xx"""
            if auth_data['user_group'] == 'super-admin':
                pickup_points_select_query = pickup_points_select_query.replace("__CLIENT_FILTER__","")
            elif auth_data['user_group'] == 'client':
                pickup_points_select_query = pickup_points_select_query.replace("__CLIENT_FILTER__","where aa.client_prefix='%s'"%str(client_prefix))
            elif auth_data['user_group'] == 'multi-vendor':
                pickup_points_select_query = pickup_points_select_query.replace("__CLIENT_FILTER__","where aa.client_prefix in (select unnest(vendor_list) from multi_vendor where client_prefix='%s')"%str(client_prefix))
            else:
                pickup_points_select_query = None

            if pickup_points_select_query:
                try:
                    cur.execute(pickup_points_select_query)
                    all_pickups = cur.fetchone()[0]
                    response['pickup_points'] = all_pickups
                except Exception:
                    pass

            query_to_run = select_orders_list_query
            if auth_data['user_group'] == 'client':
                query_to_run = query_to_run.replace("__CLIENT_FILTER__", "AND aa.client_prefix = '%s'"%client_prefix)
            if auth_data['user_group'] == 'warehouse':
                query_to_run = query_to_run.replace("__PICKUP_FILTER__", "AND ii.warehouse_prefix = '%s'" % auth_data.get('warehouse_prefix'))
            if auth_data['user_group'] == 'multi-vendor':
                cur.execute("SELECT vendor_list FROM multi_vendor WHERE client_prefix='%s';" % client_prefix)
                vendor_list = cur.fetchone()[0]
                query_to_run = query_to_run.replace("__MV_CLIENT_FILTER__", "AND aa.client_prefix in %s"%str(tuple(vendor_list)))
            else:
                query_to_run = query_to_run.replace("__MV_CLIENT_FILTER__", "")

            if since_id:
                query_to_run = query_to_run.replace("__SINCE_ID_FILTER__", "AND id>%s"%str(since_id))
            query_to_run = query_to_run.replace("__SEARCH_KEY__", search_key)

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
                    query_to_run = query_to_run.replace("__STATUS_FILTER__", "AND aa.status in %s"%status_tuple)
                if 'courier' in filters:
                    if len(filters['courier']) == 1:
                        courier_tuple = "('"+filters['courier'][0]+"')"
                    else:
                        courier_tuple = str(tuple(filters['courier']))
                    query_to_run = query_to_run.replace("__COURIER_FILTER__", "AND courier_name in %s"%courier_tuple)
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

            if download_flag:
                if not [i for i in ['order_date', 'pickup_time', 'manifest_time', 'delivered_time'] if i in filters]:
                    date_month_ago = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=31)
                    date_month_ago = date_month_ago.strftime("%Y-%m-%d %H:%M:%S")
                    query_to_run = query_to_run.replace('__ORDER_DATE_FILTER__', "AND order_date > '%s' "%date_month_ago)
                    query_to_run = query_to_run.replace('__PAGINATION__', "")
                query_to_run = re.sub(r"""__.+?__""", "", query_to_run)
                cur.execute(query_to_run)
                orders_qs_data = cur.fetchall()
                si = io.StringIO()
                cw = csv.writer(si)
                cw.writerow(ORDERS_DOWNLOAD_HEADERS)
                for order in orders_qs_data:
                    try:
                        if order[28]:
                            for idx, val in enumerate(order[28]):
                                new_row = list()
                                new_row.append(str(order[0]))
                                new_row.append(str(order[14]))
                                new_row.append(str(order[16]))
                                new_row.append(str(order[15]))
                                new_row.append(order[2].strftime("%Y-%m-%d") if order[2] else "N/A")
                                new_row.append(str(order[8]))
                                new_row.append(str(order[10]) if not hide_weights else "")
                                new_row.append(str(order[6]))
                                new_row.append(order[9].strftime("%Y-%m-%d") if order[9] else "N/A")
                                new_row.append(str(order[3]))
                                new_row.append(str(order[17]))
                                new_row.append(str(order[18]))
                                new_row.append(str(order[19]))
                                new_row.append(str(order[20]))
                                new_row.append(str(order[21]))
                                new_row.append(str(order[22]))
                                new_row.append(order[27])
                                new_row.append(str(val))
                                new_row.append(str(order[29][idx]))
                                new_row.append(str(order[30][idx]))
                                new_row.append(str(order[25]))
                                new_row.append(order[26])
                                new_row.append(order[24].strftime("%Y-%m-%d %H:%M:%S") if order[24] else "N/A")
                                new_row.append(order[23].strftime("%Y-%m-%d %H:%M:%S") if order[23] else "N/A")
                                if order[31] and order[32] is not None:
                                    new_row.append("Confirmed" if order[32] else "Cancelled")
                                    new_row.append(str(order[33]))
                                else:
                                    new_row.append("N/A")
                                    new_row.append("N/A")
                                if order[34] and order[35] is not None:
                                    new_row.append("Cancelled" if order[35] else "Re-attempt")
                                    new_row.append(str(order[36]))
                                else:
                                    new_row.append("N/A")
                                    new_row.append("N/A")
                                if auth_data.get('user_group') == 'super-admin':
                                    new_row.append(order[42])
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
            cur.execute(count_query)
            total_count = cur.fetchone()[0]
            query_to_run = query_to_run.replace('__PAGINATION__', "OFFSET %s LIMIT %s"%(str((page-1)*per_page), str(per_page)))
            query_to_run = re.sub(r"""__.+?__""", "", query_to_run)
            cur.execute(query_to_run)
            orders_qs_data = cur.fetchall()

            response_data = list()
            for order in orders_qs_data:
                resp_obj=dict()
                resp_obj['order_id'] = order[0]
                resp_obj['unique_id'] = order[1]
                resp_obj['pickup_point'] = order[27]
                resp_obj['customer_details'] = {"name":order[14],
                                                "email":order[16],
                                                "phone":order[15],
                                                "address_one":order[17],
                                                "address_two":order[18],
                                                "city":order[19],
                                                "state":order[20],
                                                "country":order[21],
                                                "pincode":order[22],
                                                }
                resp_obj['order_date'] = order[2].strftime("%d %b %Y, %I:%M %p") if order[2] else None
                resp_obj['delivered_time'] = order[23].strftime("%d %b %Y, %I:%M %p") if order[23] else None
                resp_obj['manifest_time'] = order[38].strftime("%d %b %Y, %I:%M %p") if order[38] else None
                resp_obj['payment'] = {"mode": order[25],
                                           "amount": order[26]}

                resp_obj['product_details'] = list()
                not_shipped = None
                if order[28]:
                    for idx, prod in enumerate(order[28]):
                        if not order[43][idx] or not order[44][idx]:
                            not_shipped = "Weight/dimensions not entered for product(s)"
                        resp_obj['product_details'].append(
                            {"name": prod,
                             "sku": order[29][idx],
                             "quantity": order[30][idx]}
                        )

                if not not_shipped and order[13] == "Pincode not serviceable":
                    not_shipped = "Pincode not serviceable"
                elif not order[27]:
                    not_shipped = "Pickup point not assigned"

                if not_shipped:
                    resp_obj['not_shipped'] = not_shipped
                if order[31]:
                    resp_obj['cod_verification'] = {"confirmed": order[32], "via": order[33]}
                if order[34]:
                    resp_obj['ndr_verification'] = {"confirmed": order[35], "via": order[36]}

                if type=='ndr':
                    resp_obj['ndr_reason'] = order[40]
                    ndr_action = None
                    if order[39] in (1,3,9,11) and order[34]:
                        if order[35] == True and order[36] in ('call','text'):
                            ndr_action = "Cancellation confirmed by customer"
                        elif order[35] == True and order[36] == 'manual':
                            ndr_action = "Cancellation confirmed by seller"
                        elif order[35] == False and order[36] == 'manual':
                            ndr_action = "Re-attempt requested by seller"
                        elif order[35] == False and order[36] in ('call','text'):
                            ndr_action = "Re-attempt requested by customer"
                        elif order[3]=='PENDING':
                            ndr_action = 'take_action'

                    resp_obj['ndr_action'] = ndr_action

                resp_obj['shipping_details'] = {"courier": order[8],
                                                "awb":order[6],
                                                "tracking_link": order[7]}
                resp_obj['dimensions'] = order[11] if not hide_weights else None
                resp_obj['weight'] = order[10] if not hide_weights else None
                resp_obj['volumetric'] = order[12] if not hide_weights else None
                resp_obj['channel_logo'] = order[37]
                if order[9]:
                    resp_obj['edd'] = order[9].strftime('%-d %b')
                if auth_data['user_group'] == 'super-admin':
                    resp_obj['remark'] = order[13]
                if type == "shipped":
                    resp_obj['status_detail'] = order[5]

                resp_obj['status'] = order[3]
                if order[3] in ('NEW','CANCELED','PENDING PAYMENT','READY TO SHIP','PICKUP REQUESTED','NOT PICKED') or not order[6]:
                    resp_obj['status_change'] = True
                response_data.append(resp_obj)

            response['data'] = response_data
            total_pages = math.ceil(total_count/per_page)
            response['meta']['pagination'] = {'total': total_count,
                                              'per_page':per_page,
                                              'current_page': page,
                                              'total_pages':total_pages}
            return response, 200

        except Exception as e:
            conn.rollback()
            return {"success": False, "error": str(e)}, 400


api.add_resource(OrderList, '/orders/<type>')


@core_blueprint.route('/orders/get_filters', methods=['GET'])
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
                            return {"status": "Failed", "msg": "One or more SKU(s) not found"}, 400

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


@core_blueprint.route('/orders/v1/upload', methods=['POST'])
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


@core_blueprint.route('/orders/v1/download/shiplabels', methods=['POST'])
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

    sl_type_pref = auth_data.get('warehouse_prefix')
    if not sl_type_pref:
        sl_type_pref = auth_data.get('client_prefix')

    shiplabel_type = db.session.query(WarehouseMapping).filter(WarehouseMapping.warehouse_prefix==sl_type_pref).first()
    file_pref = auth_data['client_prefix'] if auth_data['client_prefix'] else auth_data['warehouse_prefix']
    file_name = "shiplabels_"+str(file_pref)+"_"+str(datetime.now().strftime("%d_%b_%Y_%H_%M_%S"))+".pdf"
    if shiplabel_type and shiplabel_type.shiplabel_type=='TH1':
        c = canvas.Canvas(file_name, pagesize=(288, 432))
        create_shiplabel_blank_page_thermal(c)
    else:
        c = canvas.Canvas(file_name, pagesize=landscape(A4))
        create_shiplabel_blank_page(c)
    failed_ids = dict()
    idx=0
    for ixx, order in enumerate(orders_qs):
        try:
            if not order[0].shipments or not order[0].shipments[0].awb:
                continue
            if shiplabel_type and shiplabel_type.shiplabel_type=='TH1':
                try:
                    fill_shiplabel_data_thermal(c, order[0], order[1])
                except Exception:
                    pass

                if idx != len(orders_qs) - 1:
                    c.showPage()
                    create_shiplabel_blank_page_thermal(c)

            elif shiplabel_type and shiplabel_type.shiplabel_type=='A41':
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
                offset_dict = {0:0.20, 1:3.913, 2:7.676}
                try:
                    fill_shiplabel_data(c, order[0], offset_dict[idx%3], order[1])
                except Exception:
                    pass
                if idx%3==2 and ixx!=(len(orders_qs)-1):
                    c.showPage()
                    create_shiplabel_blank_page(c)
            idx += 1
        except Exception as e:
            failed_ids[order[0].channel_order_id] = str(e.args[0])
            pass

    if not (shiplabel_type and shiplabel_type.shiplabel_type in ('A41','TH1')):
        c.setFillColorRGB(1, 1, 1)
        if idx%3==1:
            c.rect(2.917 * inch, -1.0 * inch, 10 * inch, 10*inch, fill=1)
        if idx%3==2:
            c.rect(6.680 * inch, -1.0 * inch, 10 * inch, 10*inch, fill=1)

    c.save()
    s3 = session.resource('s3')
    bucket = s3.Bucket("wareiqshiplabels")
    bucket.upload_file(file_name, file_name, ExtraArgs={'ACL':'public-read'})
    shiplabel_url = "https://wareiqshiplabels.s3.us-east-2.amazonaws.com/" + file_name
    os.remove(file_name)

    return jsonify({
        'status': 'success',
        'url': shiplabel_url,
        "failed_ids": failed_ids
    }), 200


@core_blueprint.route('/orders/v1/download/invoice', methods=['POST'])
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
    orders_qs = orders_qs.order_by(Orders.id).all()
    if not orders_qs:
        return jsonify({"success": False, "msg": "No valid order ID"}), 404

    file_pref = auth_data['client_prefix'] if auth_data['client_prefix'] else auth_data['warehouse_prefix']
    file_name = "invoice_"+str(file_pref)+"_"+str(datetime.now().strftime("%d_%b_%Y_%H_%M_%S"))+".pdf"
    c = canvas.Canvas(file_name, pagesize=A4)
    create_invoice_blank_page(c)
    failed_ids = dict()
    idx=0
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
    bucket.upload_file(file_name, file_name, ExtraArgs={'ACL':'public-read'})
    invoice_url = "https://wareiqinvoices.s3.us-east-2.amazonaws.com/" + file_name
    os.remove(file_name)

    return jsonify({
        'status': 'success',
        'url': invoice_url,
        "failed_ids": failed_ids
    }), 200


@core_blueprint.route('/orders/v1/download/picklist', methods=['POST'])
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
                        products_dict[order.client_prefix][new_prod.combo_prod_id] = {"sku": sku, "name": new_prod.combo_prod.name,
                                                                               "quantity": prod.quantity * new_prod.quantity}
                    else:
                        products_dict[order.client_prefix][new_prod.combo_prod_id]['quantity'] += prod.quantity*new_prod.quantity
            else:
                if prod.product_id not in products_dict[order.client_prefix]:
                    sku = prod.product.master_sku if prod.product.master_sku else prod.product.sku
                    products_dict[order.client_prefix][prod.product_id] = {"sku": sku, "name": prod.product.name, "quantity": prod.quantity}
                else:
                    products_dict[order.client_prefix][prod.product_id]['quantity'] += prod.quantity

    file_pref = auth_data['client_prefix'] if auth_data['client_prefix'] else auth_data['warehouse_prefix']
    file_name = "picklist_"+str(file_pref)+"_"+str(datetime.now().strftime("%d_%b_%Y_%H_%M_%S"))+".pdf"
    c = canvas.Canvas(file_name, pagesize=A4)
    c = generate_picklist(c, products_dict, order_count)

    c.save()
    s3 = session.resource('s3')
    bucket = s3.Bucket("wareiqpicklist")
    bucket.upload_file(file_name, file_name, ExtraArgs={'ACL':'public-read'})
    invoice_url = "https://wareiqpicklist.s3.us-east-2.amazonaws.com/" + file_name
    os.remove(file_name)

    return jsonify({
        'status': 'success',
        'url': invoice_url,
    }), 200


@core_blueprint.route('/orders/v1/download/packlist', methods=['POST'])
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
                        orders_dict[order.client_prefix][order.channel_order_id][new_prod.combo_prod_id] = {"sku": sku, "name": new_prod.combo_prod.name,
                                                                               "quantity": prod.quantity * new_prod.quantity}
                    else:
                        orders_dict[order.client_prefix][order.channel_order_id][new_prod.combo_prod_id]['quantity'] += prod.quantity * new_prod.quantity
            else:
                sku = prod.product.master_sku if prod.product.master_sku else prod.product.sku
                if prod.product_id not in orders_dict[order.client_prefix][order.channel_order_id]:
                    orders_dict[order.client_prefix][order.channel_order_id][prod.product_id] = {"sku": sku, "name": prod.product.name, "quantity": prod.quantity}
                else:
                    orders_dict[order.client_prefix][order.channel_order_id][prod.product_id]['quantity'] += prod.quantity


    file_pref = auth_data['client_prefix'] if auth_data['client_prefix'] else auth_data['warehouse_prefix']
    file_name = "packlist_"+str(file_pref)+"_"+str(datetime.now().strftime("%d_%b_%Y_%H_%M_%S"))+".pdf"
    c = canvas.Canvas(file_name, pagesize=A4)
    c = generate_packlist(c, orders_dict, order_count)

    c.save()
    s3 = session.resource('s3')
    bucket = s3.Bucket("wareiqpacklist")
    bucket.upload_file(file_name, file_name, ExtraArgs={'ACL':'public-read'})
    invoice_url = "https://wareiqpacklist.s3.us-east-2.amazonaws.com/" + file_name
    os.remove(file_name)

    return jsonify({
        'status': 'success',
        'url': invoice_url,
    }), 200


@core_blueprint.route('/orders/v1/download/manifest', methods=['POST'])
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

        order_tuple = (order.channel_order_id, order.order_date, order.client_prefix, order.shipments[0].weight, None, None,
                       None, prod_names, prod_quan, order.payments[0].payment_mode, order.payments[0].amount,
                       order.delivery_address.first_name, order.delivery_address.last_name, order.delivery_address.address_one,
                       order.delivery_address.address_two, order.delivery_address.city, order.delivery_address.pincode, order.delivery_address.state,
                       order.delivery_address.country, order.delivery_address.phone, order.shipments[0].awb, None, None)

        orders_list.append(order_tuple)

    return jsonify({
        'status': 'success',
        'url': fill_manifest_data(orders_list, courier, store, warehouse),
    }), 200


@core_blueprint.route('/orders/v1/manifests', methods=['POST'])
@authenticate_restful
def get_manifests(resp):
    response = {'status': 'success', 'data': dict(), "meta": dict()}
    auth_data = resp.get('data')
    data = json.loads(request.data)
    page = data.get('page', 1)
    per_page = data.get('per_page', 10)
    if not auth_data:
        return jsonify({"success": False, "msg": "Auth Failed"}), 404

    return_data = list()
    manifest_qs = db.session.query(Manifests).join(ClientPickups, Manifests.client_pickup_id==ClientPickups.id).join(PickupPoints, Manifests.pickup_id==PickupPoints.id)
    if auth_data['user_group'] == 'client':
        manifest_qs= manifest_qs.filter(ClientPickups.client_prefix==auth_data['client_prefix'])
    if auth_data['user_group'] == 'multi-vendor':
        vendor_list = db.session.query(MultiVendor).filter(MultiVendor.client_prefix==auth_data['client_prefix']).first()
        manifest_qs= manifest_qs.filter(ClientPickups.client_prefix.in_(vendor_list.vendor_list))
    if auth_data['user_group'] == 'warehouse':
        manifest_qs= manifest_qs.filter(PickupPoints.warehouse_prefix==auth_data['warehouse_prefix'])
    manifest_qs = manifest_qs.order_by(Manifests.pickup_date.desc(), Manifests.total_scheduled.desc())

    manifest_qs_data = manifest_qs.limit(per_page).offset((page - 1) * per_page).all()

    for manifest in manifest_qs_data:
        manifest_dict = dict()
        manifest_dict['manifest_id'] = manifest.manifest_id
        manifest_dict['courier'] = manifest.courier.courier_name
        manifest_dict['pickup_point'] = manifest.pickup.warehouse_prefix
        manifest_dict['total_scheduled'] = manifest.total_scheduled
        manifest_dict['total_picked'] = manifest.total_picked
        manifest_dict['pickup_date'] = manifest.pickup_date
        manifest_dict['manifest_url'] = manifest.manifest_url
        return_data.append(manifest_dict)

    response['data'] = return_data

    total_count = manifest_qs.count()
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


@core_blueprint.route('/orders/v1/track/<awb>', methods=['GET'])
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
            if shipment and shipment.courier_id in (5,13): #Xpressbees details of status
                xpressbees_url = "http://xbclientapi.xbees.in/TrackingService.svc/GetShipmentSummaryDetails"
                try:
                    body = {"AWBNo": awb, "XBkey": shipment.courier.api_key}
                    return_details = dict()
                    req = requests.post(xpressbees_url, json=body).json()
                    for each_scan in req[0]['ShipmentSummary']:
                        return_details_obj = dict()
                        return_details_obj['status'] = each_scan['Status']
                        if each_scan['Comment']:
                            return_details_obj['status'] += " - " + each_scan['Comment']
                        return_details_obj['city'] = each_scan['Location']
                        if each_scan['Location']:
                            return_details_obj['city'] = each_scan['Location'].split(", ")[1]
                        status_time = each_scan['StatusDate'] + " " + each_scan['StatusTime']
                        if status_time:
                            status_time = datetime.strptime(status_time, '%d-%m-%Y %H%M')

                        time_str = status_time.strftime("%d %b %Y, %H:%M:%S")
                        return_details_obj['time'] = time_str
                        if time_str[:11] not in return_details:
                            return_details[time_str[:11]] = [return_details_obj]
                        else:
                            return_details[time_str[:11]].append(return_details_obj)

                        for key in return_details:
                            return_details[key] = sorted(return_details[key], key=lambda k: k['time'], reverse=True)
                    return jsonify({"success": True, "data": return_details}), 200
                except Exception as e:
                    return jsonify({"success": False, "msg": "Details not available"}), 400

            if shipment and shipment.courier_id in (1,2,8,11,12): #Delhivery details of status
                delhivery_url = "https://track.delhivery.com/api/status/packages/json/?waybill=%s&token=%s" \
                                % (str(awb), shipment.courier.api_key)
            else:
                delhivery_url = "https://track.delhivery.com/api/status/packages/json/?waybill=%s&token=%s" \
                                % (str(awb), "d6ce40e10b52b5ca74805a6e2fb45083f0194185")
            try:
                return_details = dict()
                req = requests.get(delhivery_url).json()
                for each_scan in req['ShipmentData'][0]['Shipment']["Scans"]:
                    return_details_obj = dict()
                    return_details_obj['status'] = each_scan['ScanDetail']['Scan'] + \
                                                   ' - ' + each_scan['ScanDetail']['Instructions']
                    return_details_obj['city'] = each_scan['ScanDetail']['CityLocation']
                    status_time = each_scan['ScanDetail']['StatusDateTime']
                    if status_time:
                        if len(status_time) == 19:
                            status_time = datetime.strptime(status_time, '%Y-%m-%dT%H:%M:%S')
                        else:
                            status_time = datetime.strptime(status_time, '%Y-%m-%dT%H:%M:%S.%f')
                    time_str = status_time.strftime("%d %b %Y, %H:%M:%S")
                    return_details_obj['time'] = time_str
                    if time_str[:11] not in return_details:
                        return_details[time_str[:11]] = [return_details_obj]
                    else:
                        return_details[time_str[:11]].append(return_details_obj)

                    for key in return_details:
                        return_details[key] = sorted(return_details[key], key=lambda k: k['time'], reverse=True)
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
            status_time = datetime.strptime(req_obj['ShipmentData'][0]['Shipment']['PickUpDate'], '%Y-%m-%dT%H:%M:%S.%f')
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
                    status_time = datetime.strptime(order_status['ScanDetail']['StatusDateTime'],
                                                             '%Y-%m-%dT%H:%M:%S.%f')
                    status_dict['time'] = status_time.strftime("%d %b %Y, %H:%M:%S")
                    picked_obj = status_dict
                elif order_status['ScanDetail']['Scan'] == 'In Transit':
                    status_dict['status'] = 'In Transit'
                    status_dict['city'] = order_status['ScanDetail']['ScannedLocation']
                    status_time = datetime.strptime(order_status['ScanDetail']['StatusDateTime'],
                                                             '%Y-%m-%dT%H:%M:%S.%f')
                    status_dict['time'] = status_time.strftime("%d %b %Y, %H:%M:%S")
                    in_transit_obj = status_dict
                elif order_status['ScanDetail']['Scan'] == 'Dispatched':
                    status_dict['status'] = 'Out for delivery'
                    status_dict['city'] = order_status['ScanDetail']['ScannedLocation']
                    status_time = datetime.strptime(order_status['ScanDetail']['StatusDateTime'],
                                                             '%Y-%m-%dT%H:%M:%S.%f')
                    status_dict['time'] = status_time.strftime("%d %b %Y, %H:%M:%S")
                    ofd_obj = status_dict
                elif 'Delivered' in order_status['ScanDetail']['Instructions']:
                    status_dict['status'] = 'Delivered'
                    status_dict['city'] = order_status['ScanDetail']['ScannedLocation']
                    status_time = datetime.strptime(order_status['ScanDetail']['StatusDateTime'],
                                                             '%Y-%m-%dT%H:%M:%S.%f')
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

'''
class CodVerificationGather(Resource):

    def get(self, ver_type):
        try:
            order_id = request.args.get('CustomField')
            if order_id:
                order_id = int(order_id)
                order = db.session.query(Orders).filter(Orders.id == order_id).first()
                client_name = db.session.query(ClientMapping).filter(ClientMapping.client_prefix == order.client_prefix).first()
                if client_name:
                    client_name = client_name.client_name
                else:
                    client_name = order.client_prefix.lower()
                if ver_type=="cod":
                    gather_prompt_text = "Hello %s, You recently placed an order from %s with amount %s." \
                                     " Press 1 to confirm your order or, 0 to cancel." % (order.customer_name,
                                                                                         client_name,
                                                                                         str(order.payments[0].amount))

                    repeat_prompt_text = "It seems that you have not provided any input, please try again. Order from %s, " \
                                     "Order ID %s. Press 1 to confirm your order or, 0 to cancel." % (
                                     client_name,
                                     order.channel_order_id)
                elif ver_type=="ndr":
                    gather_prompt_text = "Hello %s, You recently cancelled your order from %s with amount %s." \
                                         " Press 1 to confirm cancellation or, 0 to re-attempt." % (order.customer_name,
                                                                                             client_name,
                                                                                             str(order.payments[0].amount))

                    repeat_prompt_text = "It seems that you have not provided any input, please try again. Order from %s, " \
                                         "Order ID %s. Press 1 to confirm cancellation or, 0 to re-attempt." % (
                                             client_name,
                                             order.channel_order_id)
                else:
                    return {"success": False, "msg": "Order not found"}, 400

                response = {
                    "gather_prompt": {
                        "text": gather_prompt_text,
                    },
                    "max_input_digits": 1,
                    "repeat_menu": 2,
                    "repeat_gather_prompt": {
                        "text": repeat_prompt_text
                    }
                }
                return response, 200
            else:
                return {"success": False, "msg": "Order not found"}, 400
        except Exception as e:
            return {'success': False}, 404


api.add_resource(CodVerificationGather, '/core/v1/verification/gather/<ver_type>')


@core_blueprint.route('/core/v1/passthru/<type>', methods=['GET'])
def verification_passthru(type):
    try:
        order_id = request.args.get('CustomField')
        digits = request.args.get('digits')
        recording_url = request.args.get('RecordingUrl')
        call_sid = request.args.get('CallSid')
        if digits:
            digits = digits.replace('"', '')
        if request.user_agent.browser == 'safari' and request.user_agent.platform=='iphone' and request.user_agent.version=='13.0.1':
            return jsonify({"success": True}), 200
        if order_id:
            order_id = int(order_id)
            if type=='cod':
                cod_ver = db.session.query(CodVerification).filter(CodVerification.order_id==order_id).first()
                if digits=="0" and cod_ver.order.status=='NEW':
                    cod_ver.order.status='CANCELED'
            elif type=='delivery':
                cod_ver = db.session.query(DeliveryCheck).filter(DeliveryCheck.order_id==order_id).first()
            elif type=='ndr':
                cod_ver = db.session.query(NDRVerification).filter(NDRVerification.order_id==order_id).first()
            else:
                return jsonify({"success": False, "msg": "Not found"}), 400
            cod_verified = None
            if call_sid:
                verified_via = 'call'
            else:
                verified_via = 'text'
                cod_ver.click_browser = request.user_agent.browser
                cod_ver.click_platform = request.user_agent.platform
                cod_ver.click_string = request.user_agent.string
                cod_ver.click_version = request.user_agent.version

            if digits=="1" or digits==None:
                cod_verified = True
            elif digits=="0":
                cod_verified = False
            cod_ver.call_sid = call_sid
            cod_ver.recording_url = recording_url
            if type == 'cod':
                cod_ver.cod_verified = cod_verified
            elif type=='delivery':
                cod_ver.del_verified = cod_verified
            elif type == 'ndr':
                if verified_via=='text':
                    cod_ver.ndr_verified = False
                else:
                    cod_ver.ndr_verified = cod_verified

            cod_ver.verified_via = verified_via

            current_time = datetime.now()
            cod_ver.verification_time = current_time

            db.session.commit()

            return jsonify({"success": True}), 200
        else:
            return jsonify({"success": False, "msg": "No Order"}), 400
    except Exception as e:
        return jsonify({"success": False, "msg": str(e.args[0])}), 404

'''
@core_blueprint.route('/orders/v1/ivrcalls/call', methods=['GET'])
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
            'CallerId': '08047192710',
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


@core_blueprint.route('/orders/v1/ivrcalls/call_history', methods=['GET'])
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


@core_blueprint.route('/orders/v1/ivrcalls/passthru/<ivr_id>', methods=['POST'])
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
                return {"success": False, "msg": "Auth Failed"}, 404

            del_pincode = data.get("pincode")
            cod_available = False

            sku_list = data.get("sku_list")
            if not del_pincode:
                return {"success": False, "msg": "Pincode not provided"}, 404

            covid_zone = None
            city = None
            state = None
            try:
                cod_req = requests.get(
                    "https://track.delhivery.com/c/api/pin-codes/json/?filter_codes=%s&token=d6ce40e10b52b5ca74805a6e2fb45083f0194185" % str(
                        del_pincode)).json()
                if not cod_req.get('delivery_codes'):
                    return {"success": False, "msg": "Pincode not serviceable"}, 404

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
            cur.execute("SELECT sku, master_sku FROM products WHERE (sku in __SKU_STR__ or master_sku in __SKU_STR__) and client_prefix='__CLIENT__'".replace('__SKU_STR__', sku_string).replace('__CLIENT__', auth_data[
                                                                                                        'client_prefix']))
            sku_tuple = cur.fetchall()

            sku_dict = dict()
            for sku in sku_list:
                [accept_sku] = [a[0] for a in sku_tuple if sku['sku'] in a]
                sku_dict[str(accept_sku)] = sku['quantity']

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
                        "label_url":"https://logourls.s3.amazonaws.com/wareiq_standard.jpeg"}, 404

            prod_wh_tuple = cur.fetchall()
            wh_dict = dict()
            courier_id = 1
            courier_id_weight = 0.0
            for prod_wh in prod_wh_tuple:
                if auth_data['client_prefix']=='NASHER' and prod_wh[5] > courier_id_weight:
                    courier_id = prod_wh[4]
                    courier_id_weight = prod_wh[5]
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
                        "label_url": "https://logourls.s3.amazonaws.com/wareiq_standard.jpeg"}, 404

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
            return {"success": False, "msg": ""}, 404

    def get(self, resp):
        try:
            auth_data = resp.get('data')
            pincode = request.args.get('pincode')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404
            if not pincode:
                return {"success": False, "msg": "Pincode not provided"}, 401

            cod_req = requests.get(
                "https://track.delhivery.com/c/api/pin-codes/json/?filter_codes=%s&token=d6ce40e10b52b5ca74805a6e2fb45083f0194185" % str(
                    pincode)).json()
            if not cod_req.get('delivery_codes'):
                return {"success": False, "msg": "Pincode not serviceable"}, 404

            cod_available = False
            reverse_pickup = False
            if cod_req['delivery_codes'][0]['postal_code']['cod'].lower() == 'y':
                cod_available = True
            if cod_req['delivery_codes'][0]['postal_code']['pickup'].lower() == 'y':
                reverse_pickup = True

            return {"success": True, "data": {"serviceable": True, "cod_available": cod_available, "reverse_pickup": reverse_pickup}}, 200

        except Exception as e:
            return {"success": False, "msg": ""}, 404


api.add_resource(PincodeServiceabilty, '/orders/v1/serviceability')
'''
'''
class UpdateInventory(Resource):

    method_decorators = [authenticate_restful]

    def post(self, resp):
        try:
            cur = conn.cursor()
            auth_data = resp.get('data')
            data = json.loads(request.data)
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404

            sku_list = data.get("sku_list")
            failed_list = list()
            current_quantity = list()
            for sku_obj in sku_list:
                try:
                    warehouse = sku_obj.get('warehouse')
                    if not warehouse:
                        sku_obj['error'] = "Warehouse not provided."
                        failed_list.append(sku_obj)
                        continue
                    sku = sku_obj.get('sku')
                    if not sku:
                        sku_obj['error'] = "SKU not provided."
                        failed_list.append(sku_obj)
                        continue

                    sku = str(sku)

                    type = sku_obj.get('type')
                    if not type or str(type).lower() not in ('add', 'subtract', 'replace'):
                        sku_obj['error'] = "Invalid type"
                        failed_list.append(sku_obj)
                        continue

                    quantity = sku_obj.get('quantity')
                    if quantity is None:
                        sku_obj['error'] = "Invalid Quantity"
                        failed_list.append(sku_obj)
                        continue

                    quantity = int(quantity)

                    quan_obj = db.session.query(ProductQuantity).join(Products, ProductQuantity.product_id==Products.id)\
                        .filter(ProductQuantity.warehouse_prefix==warehouse).filter(
                        or_(Products.sku==sku, Products.master_sku==sku))

                    if auth_data.get('user_group') != 'super-admin':
                        quan_obj = quan_obj.filter(Products.client_prefix==auth_data['client_prefix'])

                    quan_obj = quan_obj.first()

                    if not quan_obj:
                        prod_obj = db.session.query(Products).filter(or_(Products.sku==sku, Products.master_sku==sku))
                        if auth_data.get('user_group') != 'super-admin':
                            prod_obj = prod_obj.filter(Products.client_prefix == auth_data['client_prefix'])

                        prod_obj = prod_obj.first()
                        if not prod_obj:
                            sku_obj['error'] = "Warehouse sku combination not found."
                            failed_list.append(sku_obj)
                            continue
                        else:
                            quan_obj = ProductQuantity(product=prod_obj,
                                                       total_quantity=0,
                                                       approved_quantity=0,
                                                       available_quantity=0,
                                                       inline_quantity=0,
                                                       rto_quantity=0,
                                                       current_quantity=0,
                                                       warehouse_prefix=warehouse,
                                                       status="APPROVED",
                                                       date_created=datetime.now())
                            db.session.add(quan_obj)

                    update_obj = InventoryUpdate(product=quan_obj.product,
                                                 warehouse_prefix=warehouse,
                                                 user=auth_data['email'] if auth_data.get('email') else auth_data[
                                                     'client_prefix'],
                                                 remark=sku_obj.get('remark', None),
                                                 quantity=int(quantity),
                                                 type=str(type).lower(),
                                                 date_created=datetime.utcnow() + timedelta(hours=5.5))

                    shipped_quantity=0
                    dto_quantity=0
                    try:
                        cur.execute("""  select COALESCE(sum(quantity), 0) from op_association aa
                                left join orders bb on aa.order_id=bb.id
                                left join client_pickups cc on bb.pickup_data_id=cc.id
                                left join pickup_points dd on cc.pickup_id=dd.id
                                left join products ee on aa.product_id=ee.id
                                where status in ('DELIVERED','DISPATCHED','IN TRANSIT','ON HOLD','PENDING','LOST')
                                and dd.warehouse_prefix='__WAREHOUSE__'
                                and ee.master_sku='__SKU__';""".replace('__WAREHOUSE__', warehouse).replace('__SKU__', sku))
                        shipped_quantity_obj = cur.fetchone()
                        if shipped_quantity_obj is not None:
                            shipped_quantity = shipped_quantity_obj[0]
                    except Exception:
                        conn.rollback()

                    try:
                        cur.execute("""  select COALESCE(sum(quantity), 0) from op_association aa
                                left join orders bb on aa.order_id=bb.id
                                left join client_pickups cc on bb.pickup_data_id=cc.id
                                left join pickup_points dd on cc.pickup_id=dd.id
                                left join products ee on aa.product_id=ee.id
                                where status in ('DTO')
                                and dd.warehouse_prefix='__WAREHOUSE__'
                                and ee.master_sku='__SKU__';""".replace('__WAREHOUSE__', warehouse).replace('__SKU__', sku))
                        dto_quantity_obj = cur.fetchone()
                        if dto_quantity_obj is not None:
                            dto_quantity = dto_quantity_obj[0]
                    except Exception:
                        conn.rollback()

                    if str(type).lower() == 'add':
                        quan_obj.total_quantity = quan_obj.total_quantity+quantity
                        quan_obj.approved_quantity = quan_obj.approved_quantity+quantity
                    elif str(type).lower() == 'subtract':
                        quan_obj.total_quantity = quan_obj.total_quantity - quantity
                        quan_obj.approved_quantity = quan_obj.approved_quantity - quantity
                    elif str(type).lower() == 'replace':
                        quan_obj.total_quantity = quantity + shipped_quantity - dto_quantity
                        quan_obj.approved_quantity = quantity + shipped_quantity - dto_quantity
                    else:
                        continue

                    current_quantity.append({"warehouse": warehouse, "sku": sku,
                                             "current_quantity": quan_obj.approved_quantity- shipped_quantity+dto_quantity})

                except Exception:
                    failed_list.append(sku_obj)
                    continue

                db.session.add(update_obj)
                db.session.commit()

            return {"success": True if not failed_list else False, "failed_list": failed_list, "current_quantity": current_quantity}, 200

        except Exception as e:
            return {"success": False, "msg": str(e.args[0])}, 400

    def get(self, resp):
        try:
            cur = conn.cursor()
            auth_data = resp.get('data')
            sku = request.args.get('sku')
            search_key = request.args.get('search', '')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404

            if not sku:
                query_to_run = """select array_agg(master_sku) from 
                                (SELECT master_sku from products WHERE master_sku ilike '%__SEARCH_KEY__%' __CLIENT_FILTER__ ORDER BY master_sku LIMIT 10) ss""".replace('__SEARCH_KEY__', search_key)
                if auth_data['user_group'] != 'super-admin':
                    query_to_run = query_to_run.replace("__CLIENT_FILTER__", "AND client_prefix='%s'"%auth_data['client_prefix'])
                else:
                    query_to_run = query_to_run.replace("__CLIENT_FILTER__", "")

                cur.execute(query_to_run)

                return {"success": True, "sku_list": cur.fetchone()[0]}, 200

            else:
                query_to_run = """select array_agg(warehouse_prefix) from
                                    (select distinct(warehouse_prefix) from products_quantity WHERE product_id in
                                    (select id from products where master_sku='%s') 
                                    ORDER BY warehouse_prefix) ss"""%(str(sku))

                cur.execute(query_to_run)

                return {"success": True, "warehouse_list": cur.fetchone()[0], "sku":sku}, 200

        except Exception as e:
            return {"success": False, "msg": ""}, 404


api.add_resource(UpdateInventory, '/products/v1/update_inventory')


class AddSKU(Resource):

    method_decorators = [authenticate_restful]

    def post(self, resp):
        try:
            auth_data = resp.get('data')
            data = json.loads(request.data)
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404

            product_name = data.get('name')
            sku = data.get('sku')
            dimensions = data.get('dimensions')
            if dimensions:
                dimensions = {"length": float(dimensions['length']), "breadth": float(dimensions['breadth']), "height":  float(dimensions['height'])}
            weight = data.get('weight')
            price = float(data.get('price', 0))
            client = data.get('client')
            warehouse_list= data.get('warehouse_list', [])
            if auth_data['user_group'] != 'super-admin':
                client = auth_data['client_prefix']

            prod_obj_x = db.session.query(Products).filter(Products.client_prefix==client, Products.master_sku==sku).first()
            if prod_obj_x:
                return {"success": False, "msg": "SKU already exists"}, 400

            prod_obj_x = Products(name=product_name,
                                  sku=sku,
                                  master_sku=sku,
                                  dimensions=dimensions,
                                  weight=weight,
                                  price=price,
                                  client_prefix=client,
                                  active=True,
                                  channel_id=4,
                                  date_created=datetime.now()
                                  )

            for wh_obj in warehouse_list:
                prod_quan_obj = ProductQuantity(product=prod_obj_x,
                                                total_quantity=int(wh_obj['quantity']),
                                                approved_quantity=int(wh_obj['quantity']),
                                                available_quantity=int(wh_obj['quantity']),
                                                inline_quantity=0,
                                                rto_quantity=0,
                                                current_quantity=int(wh_obj['quantity']),
                                                warehouse_prefix=wh_obj['warehouse'],
                                                status="APPROVED",
                                                date_created=datetime.now()
                                                )
                db.session.add(prod_quan_obj)

            db.session.commit()
            return {"success": True, "msg": "Successfully added"}, 201

        except Exception as e:
            return {"success": False, "msg": ""}, 404

    def get(self, resp):
        try:
            cur = conn.cursor()
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404

            query_to_run = """select array_agg(warehouse_prefix) from 
                            (select bb.warehouse_prefix from client_pickups aa
                            left join pickup_points bb
                            on aa.pickup_id=bb.id
                            __CLIENT_FILTER__
                            order by warehouse_prefix) ss"""
            if auth_data['user_group'] != 'super-admin':
                query_to_run = query_to_run.replace("__CLIENT_FILTER__", "WHERE aa.client_prefix='%s'"%auth_data['client_prefix'])
            else:
                query_to_run = query_to_run.replace("__CLIENT_FILTER__", "")

            cur.execute(query_to_run)

            return {"success": True, "warehouses": cur.fetchone()[0]}, 200

        except Exception as e:
            return {"success": False, "msg": ""}, 404


api.add_resource(AddSKU, '/products/v1/add_sku')
'''

'''
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


api.add_resource(WalletDeductions, '/wallet/v1/deductions')


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


api.add_resource(WalletRecharges, '/wallet/v1/payments')


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


api.add_resource(WalletRemittance, '/wallet/v1/remittance')
'''

@core_blueprint.route('/core/dev', methods=['POST'])
def ping_dev():
    from .fetch_orders import lambda_handler
    lambda_handler()
    myfile = request.files['myfile']
    import json, requests
    data_xlsx = pd.read_excel(myfile)

    iter_rw = data_xlsx.iterrows()
    source_items = list()
    sku_list = list()
    for row in iter_rw:
        try:

            sku = str(row[1].SKU)
            del_qty = int(row[1].Qty)
            # cb_qty = int(row[1].CBQT)
            # mh_qty = int(row[1].MHQT)
            """
            source_items.append({
                "sku": sku,
                "source_code": "default",
                "quantity": del_qty + cb_qty + mh_qty,
                "status": 1
            })
            """

            """
            data = {"sku_list": [{"sku": sku,
                                  "warehouse": "DLWHEC",
                                  "quantity": del_qty,
                                  "type": "replace",
                                  "remark": "30th aug resync"}]}
            req = requests.post("http://track.wareiq.com/products/v1/update_inventory", headers=headers,
                                data=json.dumps(data))

            """
            sku_list.append({"sku": sku,
                             "warehouse": "HOLISOLBL",
                             "quantity": del_qty,
                             "type": "add",
                             "remark": "7 sep inbound"})

            """

            data = {"sku_list": [{"sku": sku,
                                  "warehouse": "MHWHECB2C",
                                  "quantity": mh_qty,
                                  "type": "replace",
                                  "remark": "30th aug resync"}]}
            req = requests.post("http://track.wareiq.com/products/v1/update_inventory", headers=headers,
                                data=json.dumps(data))

            combo = str(row[1].SKU)
            combo_prod = str(row[1].childsku)
            combo = db.session.query(Products).filter(Products.master_sku == combo,
                                                      Products.client_prefix == 'URBANGABRU').first()
            combo_prod = db.session.query(Products).filter(Products.master_sku == combo_prod,
                                                           Products.client_prefix == 'URBANGABRU').first()
            if combo and combo_prod:
                combo_obj = ProductsCombos(combo=combo,
                                           combo_prod=combo_prod,
                                           quantity=int(row[1].qty))
                db.session.add(combo_obj)
            else:
                pass

            if row[0]%200==0:
                headers = {
                    'Authorization': "Bearer " + "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE2MDE3NDQwNzksImlhdCI6MTU5OTE1MjA3OSwic3ViIjo5fQ.lPSECo8JK0zJgv6oAO0fLyJ5JvsnJjVHp-97cKNO6E0",
                    'Content-Type': 'application/json'}

                data = {"sku_list": sku_list}
                req = requests.post("http://track.wareiq.com/products/v1/update_inventory", headers=headers,
                                    data=json.dumps(data))

                sku_list = list()
            """

        except Exception as e:
            pass

    headers = {
        'Authorization': "Bearer " + "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE2MDI0MzYzMzMsImlhdCI6MTU5OTg0NDMzMywic3ViIjoxMTN9.k0NyjjiTO3BrPDN2RBtMwOytGMANZ5Rfppv1hkaO1TI",
        'Content-Type': 'application/json'}

    data = {"sku_list": sku_list}
    req = requests.post("https://track.wareiq.com/products/v1/update_inventory", headers=headers,
                        data=json.dumps(data))
    return 0
    from .fetch_orders import lambda_handler
    lambda_handler()
    from .models import Orders, ReturnPoints, ClientPickups, Products, ProductQuantity
    data_xlsx = pd.read_excel(myfile)
    import json, re
    count = 0
    iter_rw = data_xlsx.iterrows()
    for row in iter_rw:
        try:
            sku = str(row[1].SKU)
            name = str(row[1].Name)
            mrp = float(row[1].Price)
            weight = float(row[1].Weight)
            dimensions = {"length": float(row[1].Length), "breadth": float(row[1].Breadth),
                          "height": float(row[1].Height)}
            prod_obj = Products(name=name,
                                sku=str(sku),
                                master_sku=str(sku),
                                dimensions=dimensions,
                                weight=weight,
                                price=mrp,
                                client_prefix='LOTUSORGANICS',
                                active=True,
                                channel_id=4,
                                inactive_reason=None,
                                date_created=datetime.now()
                                )

            db.session.add(prod_obj)

        except Exception as e:
            pass
    from .tasks import add
    add.delay(1,2)
    return 0

    return 0
    since_id = "1"
    count = 250
    while count == 250:
        create_fulfillment_url = "https://f2e810c7035e1653f0191cb8f5da58f6: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@rattleandco.myshopify.com/admin/api/2020-07/products.json?limit=250&since_id=%s"%since_id
        qs = requests.get(create_fulfillment_url)
        for prod in qs.json()['products']:
            for prod_obj in prod['variants']:
                prod_obj_x = db.session.query(Products).filter(Products.sku == str(prod_obj['id'])).first()
                if prod_obj_x:
                    prod_obj_x.master_sku = prod_obj['sku']
                else:
                    prod_name = prod['title']
                    if prod_obj['title'] != 'Default Title':
                        prod_name += " - " + prod_obj['title']
                    prod_obj_x = Products(name=prod_name,
                                          sku=str(prod_obj['id']),
                                          master_sku=str(prod_obj['sku']),
                                          dimensions=None,
                                          weight=None,
                                          price=float(prod_obj['price']),
                                          client_prefix='RATTLEANDCO',
                                          active=True,
                                          channel_id=1,
                                          date_created=datetime.now()
                                          )

                    db.session.add(prod_obj_x)
                    # prod_quan_obj = ProductQuantity(product=prod_obj_x,
                    #                                 total_quantity=0,
                    #                                 approved_quantity=0,
                    #                                 available_quantity=0,
                    #                                 inline_quantity=0,
                    #                                 rto_quantity=0,
                    #                                 current_quantity=0,
                    #                                 warehouse_prefix="QSDWARKA",
                    #                                 status="APPROVED",
                    #                                 date_created=datetime.now()
                    #                                 )
                    #
                    # db.session.add(prod_quan_obj)

        count = len(qs.json()['products'])
        since_id = str(qs.json()['products'][-1]['id'])


    import requests

    since_id = "1"
    count = 250
    myfile = request.files['myfile']
    while count==250:
        create_fulfillment_url = "https://f2e810c7035e1653f0191cb8f5da58f6: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@rattleandco.myshopify.com/admin/api/2020-07/products.json?limit=250&since_id=%s"%since_id
        qs = requests.get(create_fulfillment_url)
        for prod in qs.json()['products']:
            for prod_obj in prod['variants']:
                prod_name = prod['title']
                quan = 0
                if not prod_obj['sku']:
                    continue
                master_sku = str(prod_obj['sku'])
                data_xlsx = pd.read_excel(myfile)
                iter_rw = data_xlsx.iterrows()
                for row in iter_rw:
                    try:
                        if master_sku == str(row[1].SKU):
                            quan = str(row[1].quan)
                            break
                    except Exception as e:
                        pass
                if prod_obj['title'] != 'Default Title':
                    prod_name += " - " + prod_obj['title']
                if not quan:
                    continue
                prod_obj_x = db.session.query(Products).filter(Products.sku == str(prod_obj['id'])).first()
                if not prod_obj_x:
                    prod_obj_x = Products(name=prod_name,
                                          sku=str(prod_obj['id']),
                                          master_sku=master_sku,
                                          dimensions=None,
                                          weight=None,
                                          price=float(prod_obj['price']),
                                          client_prefix='SPORTSQVEST',
                                          active=True,
                                          channel_id=1,
                                          date_created=datetime.now()
                                          )

                    db.session.add(prod_obj_x)

                if quan:
                    prod_quan_obj = ProductQuantity(product=prod_obj_x,
                                                    total_quantity=quan,
                                                    approved_quantity=quan,
                                                    available_quantity=quan,
                                                    inline_quantity=0,
                                                    rto_quantity=0,
                                                    current_quantity=quan,
                                                    warehouse_prefix="QSBHIWANDI",
                                                    status="APPROVED",
                                                    date_created=datetime.now()
                                                    )

                    db.session.add(prod_quan_obj)
        count = len(qs.json()['products'])
        since_id = str(qs.json()['products'][-1]['id'])
        db.session.commit()

    db.session.commit()
    import requests, json

    return 0
    import requests
    create_fulfillment_url = "https://7e589aaa86d4fd54f88efc3daedf0615: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@shri-wellness-india.myshopify.com/admin/api/2020-07/orders.json"
    qs = requests.get(create_fulfillment_url)
    from .create_shipments import lambda_handler
    lambda_handler()
    return 0
    from requests_oauthlib.oauth1_session import OAuth1Session
    auth_session = OAuth1Session("ck_43f358286bc3a3a30ffd00e22d2282db07ed7f5d",
                                 client_secret="cs_970ec6a2707c17fc2d04cc70e87972faf3c98918")
    url = '%s/wp-json/wc/v3/orders/%s' % ("https://bleucares.com", str(6613))
    status_mark = "completed"
    if not status_mark:
        status_mark = "completed"
    r = auth_session.post(url, data={"status": status_mark})
    from woocommerce import API
    wcapi = API(
        url="https://www.zladeformen.com",
        consumer_key="ck_cd462226a5d5c21c5936c7f75e1afca25b9853a6",
        consumer_secret="cs_c897bf3e770e15f518cba5c619b32671b7cc527c",
        version="wc/v3"
    )
    r = wcapi.get('orders/117929')
    return 0

    from .fetch_orders import lambda_handler
    lambda_handler()

    #push magento inventory
    cur = conn.cursor()

    cur.execute("""select master_sku, GREATEST(available_quantity, 0) as available_quantity from
                    (select master_sku, sum(available_quantity) as available_quantity from products_quantity aa
                    left join products bb on aa.product_id=bb.id
                    where bb.client_prefix='KAMAAYURVEDA'
                    group by master_sku
                    order by available_quantity) xx""")

    all_quan = cur.fetchall()
    source_items = list()
    for quan in all_quan:
        source_items.append({
            "sku": quan[0],
            "source_code": "default",
            "quantity": quan[1],
            "status": 1
        })

    magento_url = "https://www.kamaayurveda.com/rest/default/V1/inventory/source-items"
    body = {
        "sourceItems": source_items}
    headers = {'Authorization': "Bearer q4ldi2wasczvm7l8caeyozgkxf4qanfr",
               'Content-Type': 'application/json'}
    r = requests.post(magento_url, headers=headers, data=json.dumps(body))




    return 0
    import requests, json
    magento_orders_url = """%s/V1/orders/12326""" % (
        "https://magento.feelmighty.com/rest")
    headers = {'Authorization': "Bearer " + "f3ekur9ci3gc0cb63y743dvcy3ptyxe5",
               'Content-Type': 'application/json'}
    data = requests.get(magento_orders_url, headers=headers)

    return 0




    return 0



    from .fetch_orders import lambda_handler
    lambda_handler()
    from .models import PickupPoints, ReturnPoints, ClientPickups, Products, ProductQuantity, ProductsCombos, Orders


    db.session.commit()
    import requests
    myfile = request.files['myfile']

    db.session.commit()
    import requests
    req = requests.get("https://api.ecomexpress.in/apiv2/fetch_awb/?username=warelqlogisticspvtltd144004_pro&password=LdGvdcTFv6n4jGMT&count=10000&type=PPD")

    from requests_oauthlib import OAuth1Session
    auth_session = OAuth1Session("ck_cd462226a5d5c21c5936c7f75e1afca25b9853a6",
                                 client_secret="cs_c897bf3e770e15f518cba5c619b32671b7cc527c")

    order_qs = db.session.query(Orders).filter(Orders.client_prefix=='ZLADE', Orders.status.in_(['DELIVERED'])).all()
    for order in order_qs:
        if order.order_id_channel_unique:
            url = '%s/wp-json/wc/v3/orders/%s' % ("https://www.zladeformen.com", order.order_id_channel_unique)
            r = auth_session.post(url, data={"status": "completed"})

    import requests, json



    return 0


    count = 0
    return 0

    import requests
    create_fulfillment_url = "https://87c506a89d76c5815d7f1c4f782a4bef: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@perfour.myshopify.com/admin/api/2020-07/orders.json"
    qs = requests.get(create_fulfillment_url)
    from .fetch_orders import lambda_handler
    lambda_handler()


    return 0

    from .models import PickupPoints, ReturnPoints, ClientPickups, Products, ProductQuantity
    import requests, json, xmltodict
    req = requests.get("https://plapi.ecomexpress.in/track_me/api/mawbd/?awb=8636140444,8636140446,8636140435&username=warelqlogisticspvtltd144004_pro&password=LdGvdcTFv6n4jGMT")
    req = json.loads(json.dumps(xmltodict.parse(req.content)))
    cancel_body = json.dumps({"AWBNumber": "14201720011569", "XBkey": "NJlG1ISTUa2017XzrCG6OoJng",
                              "RTOReason": "Cancelled by seller"})
    headers = {"Authorization": "Basic " + "NJlG1ISTUa2017XzrCG6OoJng",
               "Content-Type": "application/json"}
    req_can = requests.post("http://xbclientapi.xbees.in/POSTShipmentService.svc/RTONotifyShipment",
                            headers=headers, data=cancel_body)



    import requests
    from .models import Orders, ProductsCombos, ClientPickups, Products, ProductQuantity, PickupPoints, ReturnPoints
    myfile = request.files['myfile']
    data_xlsx = pd.read_excel(myfile)
    import json, re
    count = 0
    iter_rw = data_xlsx.iterrows()
    for row in iter_rw:
        try:
            warehouse_prefix= "FURTADOS_"+str(row[1].SNo)
            pickup_point = PickupPoints(pickup_location = str(row[1].SellerName),
                                        name = str(row[1].SellerName),
                                        phone =str(row[1].Contact),
                                        address = str(row[1].SellerAddress),
                                        address_two = "",
                                        city = str(row[1].City),
                                        state = str(row[1].State),
                                        country = str(row[1].Contact),
                                        pincode = str(row[1].Pincode),
                                        warehouse_prefix = warehouse_prefix)

            return_point = ReturnPoints(return_location=str(row[1].SellerName),
                                        name=str(row[1].SellerName),
                                        phone=str(row[1].Contact),
                                        address=str(row[1].SellerAddress),
                                        address_two="",
                                        city=str(row[1].City),
                                        state=str(row[1].State),
                                        country=str(row[1].Contact),
                                        pincode=str(row[1].Pincode),
                                        warehouse_prefix=warehouse_prefix)

            client_pickup = ClientPickups(pickup=pickup_point,
                                          return_point=return_point,
                                          client_prefix='FURTADOS')

            db.session.add(client_pickup)

        except Exception as e:
            pass

    db.session.commit()

    db.session.commit()
    from .fetch_orders import lambda_handler
    lambda_handler()
    return 0

    from .update_status import lambda_handler
    lambda_handler()
    myfile = request.files['myfile']

    data_xlsx = pd.read_excel(myfile)
    from .models import Products, OrdersPayments
    import json, re
    count = 0
    iter_rw = data_xlsx.iterrows()
    for row in iter_rw:
        sku = row[1].MasterSKU
        try:
            prod_obj = db.session.query(Products).filter(Products.client_prefix == 'NASHER',
                                                         Products.master_sku == 'sku').first()
            if not prod_obj:
                dimensions = re.findall(r"[-+]?\d*\.\d+|\d+", str(row[1].Dimensions))
                inactive_reason = "Delhivery Surface Standard"
                if float(dimensions[0]) > 41:
                    inactive_reason = "Delhivery Heavy 2"
                elif float(dimensions[0]) > 40:
                    inactive_reason = "Delhivery Heavy"
                elif float(dimensions[0]) > 19:
                    inactive_reason = "Delhivery Bulk"

                dimensions = {"length": float(dimensions[0]), "breadth": float(dimensions[1]),
                              "height": float(dimensions[2])}
                prod_obj_x = Products(name=str(sku),
                                      sku=str(sku),
                                      master_sku=str(sku),
                                      dimensions=dimensions,
                                      weight=float(row[1].Weight),
                                      price=float(float(row[1].Price)),
                                      client_prefix='NASHER',
                                      active=True,
                                      channel_id=4,
                                      inactive_reason=inactive_reason,
                                      date_created=datetime.now()
                                      )
                db.session.add(prod_obj_x)
                if row[0]%50==0:
                    db.session.commit()
        except Exception as e:
            print(str(sku) + "\n" + str(e.args[0]))
            db.session.rollback()
    db.session.commit()
    import requests
    create_fulfillment_url = "https://39690624bee51fde0640bfa0e3832744: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@successcraft.myshopify.com/admin/api/2020-07/orders.json?limit=250"
    qs = requests.get(create_fulfillment_url)


    db.session.commit()

    from .fetch_orders import lambda_handler
    lambda_handler()
    return 0
    return 0
    import requests
    url = "https://clbeta.ecomexpress.in/apiv2/fetch_awb/"
    body = {"username": "wareiq30857_temp", "password": "VRmjC8yGc99TAjuC", "count": 5, "type":"ppd"}
    qs = requests.post(url, data = json.dumps(body))
    return 0
    from requests_oauthlib.oauth1_session import OAuth1Session
    auth_session = OAuth1Session("ck_e6e779af2808ee872ba3fa4c0eab26b5f434af8c",
                                 client_secret="cs_696656b398cc783073b281eca3bf02c3c4de0cd1")
    url = '%s/wp-json/wc/v3/orders?per_page=100&order=asc&consumer_key=ck_e6e779af2808ee872ba3fa4c0eab26b5f434af8c&consumer_secret=cs_696656b398cc783073b281eca3bf02c3c4de0cd1' % (
        "https://silktree.in")
    r = auth_session.get(url)



    return 0
    import requests
    count = 250
    return 0

    import requests
    create_fulfillment_url = "https://9bf9e5f5fb698274d52d0e8a734354d7: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@actifiber.myshopify.com/admin/api/2020-07/orders.json?limit=250&fulfillment_status=unfulfilled"
    qs = requests.get(create_fulfillment_url)
    return 0
    from .models import Orders, ReturnPoints, ClientPickups, Products, ProductQuantity
    from woocommerce import API
    prod_obj_x = db.session.query(Products).filter(Products.client_prefix == 'OMGS').all()
    for prod_obj in prod_obj_x:
        wcapi = API(
            url="https://omgs.in",
            consumer_key="ck_97d4a88accab308268c16ce65011e6f2800c601a",
            consumer_secret="cs_e05e0aeac78b76b623ef6463482cc8ca88ae0636",
            version="wc/v3"
        )
        r = wcapi.get('products/%s'%prod_obj.sku)
        if r.status_code==200:
            print("a")
            prod_obj.master_sku = r.json()['sku']


    from .fetch_orders import lambda_handler
    lambda_handler()
    return 0

    return 0
    from .fetch_orders import lambda_handler
    lambda_handler()

    return 0


    return 0
    from .fetch_orders import lambda_handler
    lambda_handler()
    return 0
    create_fulfillment_url = "https://9bf9e5f5fb698274d52d0e8a734354d7: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@actifiber.myshopify.com/admin/api/2020-07/orders.json?limit=250"
    qs = requests.get(create_fulfillment_url)
    return 0


    from .fetch_orders import lambda_handler
    lambda_handler()

    return 0
    from requests_oauthlib.oauth1_session import OAuth1Session
    auth_session = OAuth1Session("ck_9a540daf59bd7e78268d80ed0db14d03a0e68b57",
                                 client_secret="cs_017c6ac59089de2dfd4e2f99e56513aec093464")
    url = '%s/wp-json/wc/v3/products?per_page=100&consumer_key=ck_9a540daf59bd7e78268d80ed0db14d03a0e68b57&consumer_secret=cs_017c6ac59089de2dfd4e2f99e56513aec093464' % (
        "https://naaginsauce.com")
    r = auth_session.get(url)
    return 0

    import requests

    return 0
    from .models import Orders
    for order_id in order_ids:
        del_order =None
        keep_order = None
        order_qs = db.session.query(Orders).filter(Orders.order_id_channel_unique==order_id, Orders.client_prefix=='KAMAAYURVEDA').all()
        for new_order in order_qs:
            if new_order.channel_order_id.endswith("-A"):
                del_order = new_order
            else:
                keep_order = new_order

        cur.execute("""UPDATE op_association SET order_id=%s where order_id=%s"""%(str(keep_order.id), str(del_order.id)))
        keep_order.payments[0].amount = keep_order.payments[0].amount + del_order.payments[0].amount
        keep_order.payments[0].shipping_charges = keep_order.payments[0].shipping_charges + del_order.payments[0].shipping_charges
        keep_order.payments[0].subtotal = keep_order.payments[0].subtotal + del_order.payments[0].subtotal
        conn.commit()
        db.session

    return 0


    from .models import Products, ProductQuantity


    myfile = request.files['myfile']

    data_xlsx = pd.read_excel(myfile)
    from .models import Products, ReturnPoints, ClientPickups
    import json, re
    count = 0
    iter_rw = data_xlsx.iterrows()
    for row in iter_rw:
        try:
            sku = str(row[1].SKU)
            prod_obj_x = db.session.query(Products).filter(Products.master_sku == sku, Products.client_prefix=='KAMAAYURVEDA').first()
            if not prod_obj_x:
                print("product not found: "+sku)
            else:
                name = row[1].Name
                prod_obj_x.name=name
                """
                box_pack = str(row[1].Boxpack)
                dimensions = None
                if weight:
                    weight = int(weight)/1000
                if box_pack == "Small":
                    dimensions =  {"length": 9.5, "breadth": 12.5, "height": 20}
                elif box_pack == "Medium":
                    dimensions =  {"length": 9.5, "breadth": 19, "height": 26}
                elif box_pack == "Large":
                    dimensions =  {"length": 11, "breadth": 20, "height": 31}
                prod_obj_x.weight = weight
                prod_obj_x.dimensions = dimensions
                cmb_quantity = row[1].CMBQTY
                cmb_quantity = int(cmb_quantity) if cmb_quantity else 0

                prod_quan_obj_b = ProductQuantity(product=prod_obj_x,
                                                  total_quantity=cmb_quantity,
                                                  approved_quantity=cmb_quantity,
                                                  available_quantity=cmb_quantity,
                                                  inline_quantity=0,
                                                  rto_quantity=0,
                                                  current_quantity=cmb_quantity,
                                                  warehouse_prefix="KACMB",
                                                  status="APPROVED",
                                                  date_created=datetime.now()
                                                  )

                db.session.add(prod_quan_obj_b)
                """
                if count%100==0:
                    db.session.commit()

        except Exception as e:
            print(str(row[1].SKU))
            pass
        count += 1

    db.session.commit()
    idx = 0
    for prod in data.json()['items']:
        if 'main' in prod['sku']:
            continue
        try:
            prod_obj = Products(name=prod['name'],
                                sku=str(prod['id']),
                                master_sku=prod['sku'],
                                dimensions=None,
                                weight=None,
                                price=float(prod['price']) if prod.get('price') else None,
                                client_prefix='KAMAAYURVEDA',
                                active=True,
                                channel_id=6,
                                date_created=datetime.now()
                                )
            db.session.add(prod_obj)
            idx += 1
            if idx%100==0:
                db.session.commit()
        except Exception as e:
            print(str(prod['sku']) + "\n" + str(e.args[0]))
    db.session.commit()
    return 0

    return 0


    magento_orders_url = """%s/V1/orders?searchCriteria[filter_groups][0][filters][0][field]=updated_at&searchCriteria[filter_groups][0][filters][0][value]=%s&searchCriteria[filter_groups][0][filters][0][condition_type]=gt""" % (
        "https://demokama2.com/rest/default", "2020-06-23")
    headers = {'Authorization': "Bearer " + "gfl1ilzw8iwe4yf06iuiophjfq1gb49k",
               'Content-Type': 'application/json'}
    data = requests.get(magento_orders_url, headers=headers, verify=False)



    from zeep import Client
    wsdl = "https://netconnect.bluedart.com/Ver1.9/ShippingAPI/Finder/ServiceFinderQuery.svc?wsdl"
    client = Client(wsdl)
    request_data = {
        'pinCode': '560068',
        "profile": {
                    "LoginID": "HOW53544",
                    "LicenceKey": "goqshifiomf4qw01yll5fqgtthjgksmj",
                    "Api_type": "S",
                    "Version": "1.9"
                  }
    }
    response = client.service.GetServicesforPincode(**request_data)

    return 0

    return 0
    from .models import Orders
    all_orders = db.session.query(Orders).filter(Orders.client_prefix == 'SSTELECOM',
                                                 Orders.status.in_(['IN TRANSIT']), Orders.channel_order_id>'2093').all()
    for order in all_orders:
        shopify_url = "https://e156f178d7a211b66ae0870942ff32b1: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@trendy-things-2020.myshopify.com/admin/api/2020-04/orders/%s/fulfillments.json" % order.order_id_channel_unique
        tracking_link = "http://webapp.wareiq.com/tracking/%s" % str(order.shipments[0].awb)
        ful_header = {'Content-Type': 'application/json'}
        fulfil_data = {
            "fulfillment": {
                "tracking_number": str(order.shipments[0].awb),
                "tracking_urls": [
                    tracking_link
                ],
                "tracking_company": "WareIQ",
                "location_id": 40801534089,
                "notify_customer": True
            }
        }
        req_ful = requests.post(shopify_url, data=json.dumps(fulfil_data),
                                headers=ful_header)
    return 0

    return 0
    headers = {"Content-Type": "application/json",
               "XBKey": "NJlG1ISTUa2017XzrCG6OoJng"}
    body = {"ShippingID": "14201720000512"}
    xpress_url = "http://xbclientapi.xbees.in/POSTShipmentService.svc/UpdateNDRDeferredDeliveryDate"
    req = requests.post(xpress_url, headers=headers, data=json.dumps(body))

    return 0
    from .update_status_utils import send_bulk_emails
    from project import create_app
    app = create_app()
    query_to_run = """select aa.id, bb.awb, aa.status, aa.client_prefix, aa.customer_phone, 
                                    aa.order_id_channel_unique, bb.channel_fulfillment_id, cc.api_key, 
                                    cc.api_password, cc.shop_url, bb.id, aa.pickup_data_id, aa.channel_order_id, ee.payment_mode, 
                                    cc.channel_id, gg.location_id, mm.item_list, mm.sku_quan_list , aa.customer_name, aa.customer_email, 
                                    nn.client_name, nn.client_logo, nn.custom_email_subject, bb.courier_id, nn.theme_color, bb.edd
                                    from orders aa
                                    left join shipments bb
                                    on aa.id=bb.order_id
                                    left join (select order_id, array_agg(channel_item_id) as item_list, array_agg(quantity) as sku_quan_list from
                                      		  (select kk.order_id, kk.channel_item_id, kk.quantity
                                              from op_association kk
                                              left join products ll on kk.product_id=ll.id) nn
                                              group by order_id) mm
                                    on aa.id=mm.order_id
                                    left join client_channel cc
                                    on aa.client_channel_id=cc.id
                                    left join client_pickups dd
                                    on aa.pickup_data_id=dd.id
                                    left join orders_payments ee
                                    on aa.id=ee.order_id
                                    left join client_channel_locations gg
                                    on aa.client_channel_id=gg.client_channel_id
                                    and aa.pickup_data_id=gg.pickup_data_id
                                    left join client_mapping nn
                                    on aa.client_prefix=nn.client_prefix
                                    where aa.status in ('IN TRANSIT')
                                    and aa.status_type is distinct from 'RT'
                                    and bb.awb != ''
                                    and aa.order_date>'2020-06-01'
                                    and aa.customer_email is not null
                                    and bb.awb is not null;"""
    cur = conn.cursor()
    cur.execute(query_to_run)
    all_orders = cur.fetchall()
    email_list = list()
    for order in all_orders:
        try:
            edd = order[25].strftime('%-d %b') if order[25] else ""
            email = create_email(order, edd, order[19])
            email_list.append((email, [order[19]]))
        except Exception as e:
            pass

    send_bulk_emails(email_list)

    return 0



    return 0


    return 0
    create_fulfillment_url = "https://17146b742aefb92ed627add9e44538a2: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@yo-aatma.myshopify.com/admin/api/2019-10/orders.json?ids=2240736788557,2240813563981,2241321435213,2243709665357,2245366349901,2245868355661,2246144163917,2247595196493&limit=250"
    import requests
    qs = requests.get(create_fulfillment_url)

    return 0



    return 0

    import requests, json

    create_fulfillment_url = "https://ef2a4941548279dc7ba487e5e39cb6ce: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@behir.myshopify.com/admin/api/2019-10/orders.json?limit=250"

    tracking_link = "http://webapp.wareiq.com/tracking/%s" % str("3992410212881")
    ful_header = {'Content-Type': 'application/json'}
    fulfil_data = {
        "fulfillment": {
            "tracking_number": str("3992410212881"),
            "tracking_urls": [
                tracking_link
            ],
            "tracking_company": "WareIQ",
            "location_id": int(15879471202),
            "notify_customer": False
        }
    }
    req_ful = requests.post(create_fulfillment_url, data=json.dumps(fulfil_data),
                            headers=ful_header)



    return 0






    import requests, json
    customer_phone = "09819368887"
    data = {
        'From': customer_phone,
        'CallerId': '01141182252',
        'Url': 'http://my.exotel.com/wareiq1/exoml/start_voice/262896',
        'CustomField': "21205"
    }
    req = requests.post(
        'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Calls/connect',
        data=data)

    shopify_url = "https://e67230312f67dd92f62bea398a1c7d38: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@the-organic-riot.myshopify.com/admin/api/2020-04/locations.json?limit=100"
    data = requests.get(shopify_url).json()

    return 0
    from .fetch_orders import lambda_handler
    lambda_handler()
    import requests

    from .update_status import lambda_handler
    lambda_handler()

    from .request_pickups import lambda_handler
    lambda_handler()
    myfile = request.files['myfile']

    data_xlsx = pd.read_excel(myfile)
    from .models import Products, ProductQuantity
    count = 0
    iter_rw = data_xlsx.iterrows()

    import requests
    url = 'https://vearth.codolin.com/rest/v1/orders/1258'
    headers = {'Authorization': "Bearer h5e9tmzud0c8p0o82gaobegxpw9tjaqq",
               'Content-Type': 'application/json'}
    apiuser = 'wareiq'
    apipass = 'h5e9tmzud0c8p0o82gaobegxpw9tjaqq'

    return 0
    from .models import Products, ProductQuantity

    for prod in data['products']:
        for prod_obj in prod['variants']:
            prod_name = prod['title']
            if prod_obj['title'] != 'Default Title':
                prod_name += " - "+prod_obj['title']
            prod_obj_x = Products(name=prod_name,
                                  sku=str(prod_obj['id']),
                                  master_sku=prod_obj['sku'],
                                  dimensions=None,
                                  weight=None,
                                  price=float(prod_obj['price']),
                                  client_prefix='HOMEALONE',
                                  active=True,
                                  channel_id=1,
                                  date_created=datetime.datetime.now()
                                  )
            prod_quan_obj = ProductQuantity(product=prod_obj_x,
                                            total_quantity=100,
                                            approved_quantity=100,
                                            available_quantity=100,
                                            inline_quantity=0,
                                            rto_quantity=0,
                                            current_quantity=100,
                                            warehouse_prefix="HOMEALONE",
                                            status="APPROVED",
                                            date_created=datetime.datetime.now()
                                            )

            db.session.add(prod_quan_obj)



    from .models import Orders
    all_ord = db.session.query(Orders).filter(Orders.client_prefix=='ZLADE').filter(Orders.status.in_(['DELIVERED', 'IN TRANSIT', 'READY TO SHIP'])).all()
    for ord in all_ord:
        url = '%s/wp-json/wc/v3/orders/%s' % ("https://www.zladeformen.com", ord.order_id_channel_unique)
        r = auth_session.post(url, data={"status": "completed"})
    from .models import Products, ProductQuantity
    for prod in r.json():
        try:
            if not prod['variations']:
                continue
            if prod['name'] == 'Dummy' or 'Demo' in prod['name']:
                continue
            weight = float(prod['weight']) if prod['weight'] else None
            dimensions = None
            if prod['dimensions']['length']:
                dimensions = {"length":int(prod['dimensions']['length']),
                              "breadth":int(prod['dimensions']['width']),
                              "height": int(prod['dimensions']['height'])
                              }
            """
            if '2kg' in prod['name']:
                weight = 2.1
                dimensions = {"length": 10, "breadth": 30, "height":30}
            elif '1kg' in prod['name'] or '1L' in prod['name']:
                weight = 1.1
                dimensions = {"length": 10, "breadth": 10, "height":30}
            elif '500g' in prod['name'] or '500ml' in prod['name']:
                weight = 0.55
                dimensions = {"length": 10, "breadth": 10, "height":20}
            elif '250g' in prod['name']:
                weight = 0.30
                dimensions = {"length": 10, "breadth": 10, "height":10}
            elif '220g' in prod['name']:
                weight = 0.25
                dimensions = {"length": 10, "breadth": 10, "height":10}
            elif '400g' in prod['name'] or '400 G' in prod['name']:
                weight = 0.45
                dimensions = {"length": 10, "breadth": 10, "height":20}
            elif '1.5kg' in prod['name']:
                weight = 1.6
                dimensions = {"length": 10, "breadth": 20, "height":30}
            elif '2.4kg' in prod['name'] or '2.4 KG' in prod['name']:
                weight = 2.5
                dimensions = {"length": 10, "breadth": 30, "height":30}
            else:
                weight = None
                dimensions = None
                continue
            """
            for idx , value in enumerate(prod['variations']):

                prod_obj_x = Products(name=prod['name'] + " - "+prod['attributes'][0]['options'][idx],
                                      sku=str(value),
                                      master_sku=prod['sku'],
                                      dimensions=dimensions,
                                      weight=weight,
                                      price=float(prod['price']),
                                      client_prefix='ZLADE',
                                      active=True,
                                      channel_id=5,
                                      date_created=datetime.datetime.now()
                                      )
                prod_quan_obj = ProductQuantity(product=prod_obj_x,
                                                total_quantity=0,
                                                approved_quantity=0,
                                                available_quantity=0,
                                                inline_quantity=0,
                                                rto_quantity=0,
                                                current_quantity=0,
                                                warehouse_prefix="ZLADE",
                                                status="APPROVED",
                                                date_created=datetime.datetime.now()
                                                )

                db.session.add(prod_quan_obj)
        except Exception as e:
            pass

    db.session.commit()

    return 0

    since_id='1'
    for i in range(10):
        shopify_url = "https://d243df784237ef6c45aa3a9368ca63da:5888fae7757115f891d0f6774a6c5ed5@gorg-co-in.myshopify.com/admin/api/2019-10/orders.json?limit=100"
        data = requests.get(shopify_url).json()

        since_id = str(prod['id'])
        db.session.commit()

    client = db.session.query(ClientMapping).filter(ClientMapping.client_prefix=='&NOTHINGELSE').first()
    html = client.custom_email
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    # create message object instance
    msg = MIMEMultipart('alternative')

    recipients = ["sahil@andnothingelse.in"]
    msg['From'] = "and nothing else. <noreply@wareiq.com>"
    msg['To'] = ", ".join(recipients)
    msg['Subject'] = client.custom_email_subject

    # write the HTML part

    part2 = MIMEText(html, "html")
    msg.attach(part2)

    email_server.sendmail(msg['From'], recipients, msg.as_string())

    return 0

    import requests, json
    from .models import Orders
    orders = db.session.query(Orders).filter(Orders.client_prefix == 'MUWU').all()
    for order in orders:
        if order.shipments and order.shipments[0].awb:
            create_fulfillment_url = "https://%s:%s@%s/admin/api/2019-10/orders.json?limit=100&since_id=1980049653824" % (
                "a97f8f4744d02183b84b20469af2bc3d", "f9ed6255a50a66a7af6bcdff93b3ce81",
                "unitedbyhope.myshopify.com")
            requests.get(create_fulfillment_url).json()
            tracking_link = "http://webapp.wareiq.com/tracking/%s" % str(order.shipments[0].awb)
            ful_header = {'Content-Type': 'application/json'}
            fulfil_data = {
                "fulfillment": {
                    "tracking_number": str(order.shipments[0].awb),
                    "tracking_urls": [
                        tracking_link
                    ],
                    "tracking_company": "WareIQ",
                    "location_id": 38995263623,
                    "notify_customer": False
                }
            }
            req_ful = requests.post(create_fulfillment_url, data=json.dumps(fulfil_data),
                                    headers=ful_header)
    return 0

    auth_session = OAuth1Session("ck_48b2a03de2cc5906951ff783c6c0cf83d0fa6af4",
                                 client_secret="cs_5d401d8aeaa8a16f5f82c81089290b392420891d")
    url = '%s/wp-json/wc/v3/orders?per_page=100&order=asc' % ("https://andnothingelse.in/stag")
    r = auth_session.get(url)


    from .models import Products, ProductQuantity

    import requests, json
    shopify_url = "https://a97f8f4744d02183b84b20469af2bc3d:f9ed6255a50a66a7af6bcdff93b3ce81@unitedbyhope.myshopify.com/admin/api/2019-10/orders.json?limit=250"
    data = requests.get(shopify_url).json()
    for prod in data['products']:
        for prod_obj in prod['variants']:
            prod_obj_x = Products(name=prod['title'] + " - " + prod_obj['title'],
                                sku=str(prod_obj['id']),
                                master_sku = prod_obj['sku'],
                                dimensions={
                                    "length": 7,
                                    "breadth": 14,
                                    "height": 21
                                },
                                weight=0.35,
                                price=float(prod_obj['price']),
                                client_prefix='UNITEDBYHOPE',
                                active=True,
                                channel_id=1,
                                date_created=datetime.datetime.now()
                                )
            prod_quan_obj = ProductQuantity(product=prod_obj_x,
                                            total_quantity=100,
                                            approved_quantity=100,
                                            available_quantity=100,
                                            inline_quantity=0,
                                            rto_quantity=0,
                                            current_quantity=100,
                                            warehouse_prefix="UNITEDBYHOPE",
                                            status="APPROVED",
                                            date_created=datetime.datetime.now()
                                            )
            db.session.add(prod_quan_obj)

        db.session.commit()
    return 0
    from .models import Products, ProductQuantity


    for prod in r.json():
        try:
            prod_obj = db.session.query(Products).filter(Products.sku == str(prod['id'])).first()
            if prod_obj:
                prod_obj.sku = str(prod['id'])
                prod_obj.master_sku = str(prod['sku'])

            db.session.commit()
        except Exception as e:
            print(str(e.args[0]))


    shopify_url = "https://006fce674dc07b96416afb8d7c075545:0d36560ddaf82721bfbb93f909ab5f47@themuwu.myshopify.com/admin/api/2019-10/products.json?limit=250"
    data = requests.get(shopify_url).json()

    for prod in data['products']:
        for e_sku in prod['variants']:
            try:
                count += 1
                prod_title = prod['title'] + "-" +e_sku['title']
                """
                excel_sku = ""
                if "This is my" in prod['title']:
                    if "Woman" in prod['title']:
                        excel_sku = "FIMD"
                    else:
                        excel_sku = "MIMD"
                if "I just entered" in prod['title']:
                    if "Woman" in prod['title']:
                        excel_sku = "FIJE"
                    else:
                        excel_sku = "MIJE"
                if "Black Hoodie" in prod['title']:
                    if "Man" in prod['title']:
                        excel_sku = "MBHM"
                    else:
                        excel_sku = "FBHM"
    
                if "White Hoodie" in prod['title']:
                    if "Man" in prod['title']:
                        excel_sku = "MWHM"
                    else:
                        excel_sku = "FWHM"
    
                if "To all my" in prod['title']:
                    if "Woman" in prod['title']:
                        excel_sku = "FAMH"
                    else:
                        excel_sku = "MAMH"
                if "Blue Hoodie" in prod['title']:
                    if "Woman" in prod['title']:
                        excel_sku = "FBHD"
                    else:
                        excel_sku = "MBHD"
                if e_sku['title'] == 'XS':
                    excel_sku += "1"
                if e_sku['title'] == 'S':
                    excel_sku += "2"
                if e_sku['title'] == 'M':
                    excel_sku += "3"
                if e_sku['title'] == 'L':
                    excel_sku += "4"
                if e_sku['title'] == 'XL':
                    excel_sku += "5"
                """
                excel_sku =""
                iter_rw = data_xlsx.iterrows()
                for row in iter_rw:
                    if row[1].description == prod_title:
                        excel_sku = row[1].master_sku
                prod_obj = db.session.query(Products).join(ProductQuantity, Products.id == ProductQuantity.product_id) \
                    .filter(Products.sku == str(e_sku['id'])).first()
                prod_obj.master_sku = excel_sku
                """
                if prod_obj:
                    prod_obj.dimensions = {
                        "length": 6.87,
                        "breadth": 22.5,
                        "height": 27.5
                    }
                    prod_obj.weight = 0.5
                else:
                    prod_obj = Products(name=prod['title'] + " - " + e_sku['title'],
                                        sku=str(e_sku['id']),
                                        dimensions={
                                            "length": 6.87,
                                            "breadth": 22.5,
                                            "height": 27.5
                                        },
                                        weight=0.5,
                                        price=float(e_sku['price']),
                                        client_prefix='MUWU',
                                        active=True,
                                        channel_id=1,
                                        date_created=datetime.datetime.now()
                                        )
                prod_quan_obj = ProductQuantity(product=prod_obj,
                                                total_quantity=quan,
                                                approved_quantity=quan,
                                                available_quantity=quan,
                                                inline_quantity=0,
                                                rto_quantity=0,
                                                current_quantity=quan,
                                                warehouse_prefix="HOLISOLBW",
                                                status="APPROVED",
                                                date_created=datetime.datetime.now()
                                                )
                """
                db.session.commit()
            except Exception as e:
                print(str(e))

    import requests, json
    prod_list = requests.get("https://640e8be5fbd672844636885fc3f02d6b:07d941b140370c8c975d8e83ee13e524@clean-canvass.myshopify.com/admin/api/2019-10/products.json?limit=250").json()
    myfile = request.files['myfile']
    data_xlsx = pd.read_excel(myfile)
    from .models import Products, ProductQuantity
    uri = """requests.get("https://www.nyor.in/wp-json/wc/v3/orders?oauth_consumer_key=ck_1e1ab8542c4f22b20f1b9810cd670716bf421ba8&oauth_timestamp=1583243314&oauth_nonce=kYjzVBB8Y0ZFabxSWbWovY3uYSQ2pTgmZeNu2VS4cg&oauth_signature=d07a4be56681016434803eb054cfd8b45a8a2749&oauth_signature_method=HMAC-SHA1")"""
    for row in data_xlsx.iterrows():

        cur_2.execute("select city from city_pin_mapping where pincode='%s'"%str(row[1].destinaton_pincode))
        des_city = cur_2.fetchone()
        if not des_city:
            cur_2.execute("insert into city_pin_mapping (pincode,city) VALUES ('%s','%s');" % (str(row[1].destinaton_pincode),str(row[1].destination_city)))

        cur_2.execute("select zone_value from city_zone_mapping where zone='%s' and city='%s' and courier_id=%s"%(str(row[1].origin_city),str(row[1].destination_city), 1))
        mapped_pin = cur_2.fetchone()
        if not mapped_pin:
            cur_2.execute("insert into city_zone_mapping (zone,city,courier_id) VALUES ('%s','%s', %s);" % (str(row[1].origin_city),str(row[1].destination_city), 1))

        cur_2.execute("select zone_value from city_zone_mapping where zone='%s' and city='%s' and courier_id=%s" % (
        str(row[1].origin_city), str(row[1].destination_city), 2))
        mapped_pin = cur_2.fetchone()
        if not mapped_pin:
            cur_2.execute("insert into city_zone_mapping (zone,city,courier_id) VALUES ('%s','%s', %s);" % (
            str(row[1].origin_city), str(row[1].destination_city), 2))

    """
        row_data = row[1]
        

    db.session.commit()
    try:
        for warehouse in ('NASHER_HYD','NASHER_GUR','NASHER_SDR','NASHER_VADPE','NASHER_BAN','NASHER_MUM'):
            cur.execute("select sku, product_id, sum(quantity) from 
                        (select * from op_association aa
                        left join orders bb on aa.order_id=bb.id
                        left join client_pickups cc on bb.pickup_data_id=cc.id
                        left join pickup_points dd on cc.pickup_id=dd.id
                        left join products ee on aa.product_id=ee.id
                        where status in ('DELIVERED','DISPATCHED','IN TRANSIT','ON HOLD','PENDING')
                        and dd.warehouse_prefix='__WH__'
                        and ee.sku='__SKU__') xx
                        group by sku, product_id".replace('__WH__', warehouse).replace('__SKU__', str(row_data.SKU)))
            count_tup = cur.fetchone()
            row_count = 0
            if warehouse=='NASHER_HYD':
                row_count = row_data.NASHER_HYD
            if warehouse=='NASHER_GUR':
                row_count = row_data.NASHER_GUR
            if warehouse=='NASHER_SDR':
                row_count = row_data.NASHER_SDR
            if warehouse=='NASHER_VADPE':
                row_count = row_data.NASHER_VADPE
            if warehouse=='NASHER_BAN':
                row_count = row_data.NASHER_BAN
            if warehouse=='NASHER_MUM':
                row_count = row_data.NASHER_MUM

            row_count = int(row_count)
            if count_tup:
                quan_to_add = count_tup[2]
                cur.execute("update products_quantity set total_quantity=%s, approved_quantity=%s WHERE product_id=%s "
                            "and warehouse_prefix=%s", (row_count+quan_to_add, row_count+quan_to_add, count_tup[1], warehouse))
            else:
                cur.execute("select id from products where sku=%s", (str(row_data.SKU), ))
                product_id = cur.fetchone()
                if product_id:
                    product_id = product_id[0]
                    cur.execute(
                        "update products_quantity set total_quantity=%s, approved_quantity=%s WHERE product_id=%s "
                        "and warehouse_prefix=%s",
                        (row_count , row_count, product_id, warehouse))
                else:
                    print("SKU not found: "+str(row_data.SKU))
        if row[0]%20==0 and row[0]!=0:
            conn.commit()
    except Exception as e:
        print(row_data.SKU + ": " +str(e.args[0]))
         """
    conn.commit()

    if not myfile:
        prod_obj = db.session.query(Products).join(ProductQuantity, Products.id == ProductQuantity.product_id) \
            .filter(Products.sku == str(row[1]['sku'])).first()


    return 0
    from .update_status import lambda_handler
    lambda_handler()
    import requests, json

    all_orders = db.session.query(Orders).filter(Orders.status.in_(["IN TRANSIT","PENDING","DISPATCHED"]))\
        .filter(Orders.client_prefix=='DAPR').all()

    headers = {"Content-Type": "application/json",
                "Authorization": "Token 1368a2c7e666aeb44068c2cd17d2d2c0e9223d37"}
    for order in all_orders:
        try:
            shipment_body = {"waybill": order.shipments[0].awb,
                             "phone": order.customer_phone}
            req = requests.post("https://track.delhivery.com/api/p/edit", headers=headers, data=json.dumps(shipment_body))

        except Exception as e:
            print(str(e)+str(order.id))

    return 0

    myfile = request.files['myfile']

    data_xlsx = pd.read_excel(myfile)
    from .models import Products, ProductQuantity
    for row in data_xlsx.iterrows():
        prod_obj = db.session.query(Products).join(ProductQuantity, Products.id == ProductQuantity.product_id) \
            .filter(Products.sku == str(row[1]['sku'])).first()

        if not prod_obj:
            prod_obj = Products(name=str(row[1]['sku']),
                            sku=str(row[1]['sku']),
                            dimensions={
                                "length": 3,
                                "breadth": 26,
                                "height": 27
                            },
                            weight=0.5,
                            price=0,
                            client_prefix='LMDOT',
                            active=True,
                            channel_id=4,
                            date_created=datetime.datetime.now()
                            )
            prod_quan_obj = ProductQuantity(product=prod_obj,
                                            total_quantity=100,
                                            approved_quantity=100,
                                            available_quantity=100,
                                            inline_quantity=0,
                                            rto_quantity=0,
                                            current_quantity=100,
                                            warehouse_prefix="LMDOT",
                                            status="APPROVED",
                                            date_created=datetime.datetime.now()
                                            )
            db.session.add(prod_quan_obj)


    db.session.commit()

    return 0
    pick = db.session.query(ClientPickups).filter(ClientPickups.client_prefix == 'NASHER').all()
    for location in pick:
        loc_body = {
            "phone": location.pickup.phone,
            "city": location.pickup.city,
            "name": location.pickup.warehouse_prefix,
            "pin": str(location.pickup.pincode),
            "address": location.pickup.address + " " + str(location.pickup.address_two),
            "country": location.pickup.country,
            "registered_name": location.pickup.name,
            "return_address": location.return_point.address +" "+ str(location.return_point.address_two),
            "return_pin": str(location.return_point.pincode),
            "return_city": location.return_point.city,
            "return_state": location.return_point.state,
            "return_country": location.return_point.country
        }

        headers = {"Authorization": "Token c5fd3514bd4cb65432ce31688b049ca6cf417b28",
                   "Content-Type": "application/json"}

        delhivery_url = "https://track.delhivery.com/api/backend/clientwarehouse/create/"

        req = requests.post(delhivery_url, headers=headers, data=json.dumps(loc_body))
        headers = {"Authorization": "Token 538ee2e5f226a85e4a97ad3aa0ae097b41bdb89c",
                   "Content-Type": "application/json"}
        req = requests.post(delhivery_url, headers=headers, data=json.dumps(loc_body))

    myfile = request.files['myfile']

    data_xlsx = pd.read_excel(myfile)
    failed_ids = dict()
    from .models import Products, ProductQuantity
    import json
    for row in data_xlsx.iterrows():
        try:
            if row[0]<1800:
                continue
            row_data = row[1]
            product = db.session.query(Products).filter(Products.sku == str(row_data.seller_sku)).first()

            product.inactive_reason = str(row_data.courier)

            """
            delhivery_url = "https://track.delhivery.com/api/backend/clientwarehouse/create/"
            headers = {"Content-Type": "application/json",
                       "Authorization": "Token d6ce40e10b52b5ca74805a6e2fb45083f0194185"}
            import json, requests
            req= requests.post(delhivery_url, headers=headers, data=json.dumps(del_body))
            headers = {"Content-Type": "application/json",
                       "Authorization": "Token 5f4c836289121eaabc9484a3a46286290c70e69e"}
            req= requests.post(delhivery_url, headers=headers, data=json.dumps(del_body))
            """
            if row[0] % 100 == 0 and row[0] != 0:
                db.session.commit()
        except Exception as e:
            failed_ids[str(row[1].seller_sku)] = str(e.args[0])
            db.session.rollback()
    db.session.commit()
    return 0

    from .request_pickups import lambda_handler
    lambda_handler()

    return 0
    import requests, json

    fulfie_data = {
        "fulfillment": {
            "tracking_number": "3991610025771",
            "tracking_urls": [
                "https://www.delhivery.com/track/package/3991610025771"
            ],
            "tracking_company": "Delhivery",
            "location_id": 21056061499,
            "notify_customer": False
        }
    }
    ful_header = {'Content-Type': 'application/json'}

    url = "https://e35b2c3b1924d686e817b267b5136fe0:a5e60ec3e34451e215ae92f0877dddd0@daprstore.myshopify.com/admin/api/2019-10/orders/1972315848819/fulfillments.json"

    requests.post(url, data=json.dumps(fulfie_data),
                  headers=ful_header)
    import requests
    request_body = {
                    "CallerId": "01141182252",
                    "CallType": "trans",
                    "Url":"http://my.exotel.in/exoml/start/262896",
                    "CustomField": 7277,
                    "From": "8088671652"
                    }

    url = "https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Calls/connect"
    headers = {"Content-Type": "application/json"}
    requests.post(url, data=request_body, header=headers)
    import requests, json
    from .models import ReturnPoints

    myfile = request.files['myfile']

    data_xlsx = pd.read_excel(myfile)
    headers = {"Authorization": "Token 5f4c836289121eaabc9484a3a46286290c70e69e",
               "Content-Type": "application/json"}

    for row in data_xlsx.iterrows():
        try:
            row_data = row[1]
            delivery_shipments_body = {
                "name": str(row_data.warehouse_prefix),
                "contact_person": row_data.name_new,
                "registered_name": "WAREIQ1 SURFACE",
                "phone": int(row_data.phone)
            }

            delhivery_url = "https://track.delhivery.com/api/backend/clientwarehouse/edit/"

            req = requests.post(delhivery_url, headers=headers, data=json.dumps(delivery_shipments_body))

        except Exception as e:
            print(str(e))

    return 0
    import requests, json
    url = "https://dtdc.vineretail.com/RestWS/api/eretail/v1/order/shipDetail"
    headers = {"Content-Type": "application/x-www-form-urlencoded",
               "ApiKey": "8dcbc7d756d64a04afb21e00f4a053b04a38b62de1d3481dadc8b54",
               "ApiOwner": "UMBAPI"}
    form_data = {"RequestBody":
        {
            "order_no": "9251",
            "statuses": [""],
            "order_location": "DWH",
            "date_from": "",
            "date_to": "",
            "pageNumber": ""
        },
        "OrgId": "DTDC"
    }
    req = requests.post(url, headers=headers, data=json.dumps(form_data))
    print(req.json())

    exotel_call_data = {"From": "09999503623",
                        "CallerId": "01141182252",
                        "CallType": "trans",
                        "Url":"http://my.exotel.in/exoml/start/262896",
                        "CustomField": 7277}
    lad = requests.post(
        'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Calls/connect',
        data=exotel_call_data)

    a = "09634814148"
    import json
    1961667264627
    ful_header = {'Content-Type': 'application/json'}
    url = "http://114.143.206.69:803/StandardForwardStagingService.svc/GetBulkShipmentStatus"
    post_body = {
                  "fulfillment": {
                    "location_id": 21056061499,
                    "tracking_number": "3991610018922",
                    "tracking_company": "Delhivery",
                    "tracking_urls": ["https://www.delhivery.com/track/package/3991610018922"],
                    "notify_customer": False

                  }
                }
    req = requests.post(url, data=json.dumps(post_body), headers=ful_header)

    from .update_status import lambda_handler
    lambda_handler()
    form_data = {"RequestBody": {
        "order_no": "9251",
        "statuses": [""],
        "order_location": "DWH",
        "date_from": "",
        "date_to": "",
        "pageNumber": ""
    },
        "ApiKey": "8dcbc7d756d64a04afb21e00f4a053b04a38b62de1d3481dadc8b54",
        "ApiOwner": "UMBAPI",
    }
    # headers = {"Content-Type": "application/x-www-form-urlencoded"
    #            }
    # req = requests.post("http://dtdc.vineretail.com/RestWS/api/eretail/v1/order/shipDetail",
    #               headers=headers, data=json.dumps(form_data))
    # from .create_shipments import lambda_handler
    # lambda_handler()

    shopify_url = "https://b27f6afd9506d0a07af9a160b1666b74:6c8ca315e01fe612bc997e70c7a21908@mirakki.myshopify.com/admin/api/2019-10/orders.json?since_id=1921601077383&limit=250"
    data = requests.get(shopify_url).json()

    return 0

    # from .create_shipments import lambda_handler
    # lambda_handler()


    order_qs = db.session.query(Orders).filter(Orders.client_prefix=="KYORIGIN").filter(Orders.status.in_(['READY TO SHIP', 'PICKUP REQUESTED', 'NOT PICKED'])).all()
    for order in order_qs:
        try:
            create_fulfillment_url = "https://%s:%s@%s/admin/api/2019-10/orders/%s/fulfillments.json" % (
                order.client_channel.api_key, order.client_channel.api_password,
                order.client_channel.shop_url, order.order_id_channel_unique)
            tracking_link = "https://www.delhivery.com/track/package/%s" % str(order.shipments[0].awb)
            ful_header = {'Content-Type': 'application/json'}
            fulfil_data = {
                "fulfillment": {
                    "tracking_number": str(order.shipments[0].awb),
                    "tracking_urls": [
                        tracking_link
                    ],
                    "tracking_company": "Delhivery",
                    "location_id": 16721477681,
                    "notify_customer": False
                }
            }
            try:
                req_ful = requests.post(create_fulfillment_url, data=json.dumps(fulfil_data),
                                        headers=ful_header)
                fulfillment_id = str(req_ful.json()['fulfillment']['id'])
                order.shipments[0].tracking_link = tracking_link
                order.shipments[0].channel_fulfillment_id = fulfillment_id

            except Exception as e:
                pass
        except Exception as e:
            pass

    db.session.commit()
    # for awb in awb_list:
    #     try:
    #         req = requests.get("https://track.delhivery.com/api/status/packages/json/?waybill=%s&token=d6ce40e10b52b5ca74805a6e2fb45083f0194185"%str(awb)).json()
    #         r = req['ShipmentData']
    #         print(r[0]['Shipment']['Status']['Status'])
    #     except Exception as e:
    #         pass

    exotel_call_data = {"From": "08750108744",
                        "CallType": "trans",
                        "Url": "http://my.exotel.in/exoml/start/257945",
                        "CallerId": "01141182252"}
    res = requests.post(
        'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Calls/connect',
        data=exotel_call_data)

    exotel_sms_data = {
        'From': 'LM-WAREIQ',
        'Messages[0][To]': '08750108744',
        'Messages[0][Body]': 'Dear Customer, your Know Your Origin order with AWB number 123456 is IN-TRANSIT via Delhivery and will be delivered by 23 Dec. Thank you for ordering.'
    }

    lad = requests.post(
        'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend',
        data=exotel_sms_data)

    orders = db.session.query(Orders).join(Shipments, Orders.id==Shipments.order_id).filter(Orders.status == "IN TRANSIT", Orders.status_type=='UD', Orders.client_prefix=="KYORIGIN").all()
    awb_str = ""

    awb_dict = {}
    for order in orders:
        awb_str += order.shipments[0].awb+","
        customer_phone = order.customer_phone
        customer_phone = customer_phone.replace(" ", "")
        customer_phone = "0" + customer_phone[-10:]
        awb_dict[order.shipments[0].awb] = customer_phone

    req = requests.get("https://track.delhivery.com/api/status/packages/json/?waybill=%s&token=1368a2c7e666aeb44068c2cd17d2d2c0e9223d37"%awb_str).json()

    exotel_sms_data = {
      'From': 'LM-WAREIQ'
    }
    exotel_idx = 0
    for shipment in req['ShipmentData']:
        try:
            sms_to_key = "Messages[%s][To]" % str(exotel_idx)
            sms_body_key = "Messages[%s][Body]" % str(exotel_idx)
            expected_date = shipment['Shipment']['expectedDate']
            expected_date = datetime.datetime.strptime(expected_date, '%Y-%m-%dT%H:%M:%S')
            if expected_date < datetime.datetime.today():
                continue
            expected_date = expected_date.strftime('%-d %b')

            exotel_sms_data[sms_to_key] =  awb_dict[shipment["Shipment"]['AWB']]
            exotel_sms_data[
                sms_body_key] = "Dear Customer, your Know Your Origin order with AWB number %s is IN-TRANSIT via Delhivery and will be delivered by %s. Thank you for ordering." % (
                shipment["Shipment"]['AWB'], expected_date)
            exotel_idx += 1
        except Exception:
            pass

    lad = requests.post(
        'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend',
        data=exotel_sms_data)
    return 0

    lad = requests.post('https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend', data=data)



    # for order in data['orders']:
    #     order_qs = db.session.query(Orders).filter(Orders.channel_order_id==str(order['order_number'])).first()
    #     if not order_qs:
    #         continue
    #     order_qs.order_id_channel_unique = str(order['id'])
    #
    # db.session.commit()

    import csv
    with open('dapr_products.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Shopify Product ID", "Title", "Product Type", "Master SKU", "Channel SKU", "Price", "Weight(Kg)", "Quantity", "Image URL"])
        for prod in data['products']:
            for p_sku in prod['variants']:
                try:
                    list_item = list()
                    list_item.append("ID"+str(prod['id']))
                    list_item.append(str(prod['title']))
                    list_item.append(str(prod['product_type']))
                    list_item.append(str(p_sku['sku']))
                    list_item.append("ID"+str(p_sku['id']))
                    list_item.append(str(p_sku['price']))
                    list_item.append(p_sku['weight'])
                    list_item.append(p_sku['inventory_quantity'])
                    list_item.append(prod['image']['src'])
                    writer.writerow(list_item)

                #     sku = str(p_sku['id'])
                #     product = Products(name=prod['title'] + " - " + p_sku['title'],
                #                        sku=sku,
                #                        active=True,
                #                        channel_id=1,
                #                        client_prefix="KYORIGIN",
                #                        date_created=datetime.datetime.now(),
                #                        dimensions = {"length":1.25, "breadth":30, "height":30},
                #                        price=0,
                #                        weight=0.25)
                #
                #     product_quantity = ProductQuantity(product=product,
                #                                        total_quantity=5000,
                #                                        approved_quantity=5000,
                #                                        available_quantity=5000,
                #                                        warehouse_prefix="KYORIGIN",
                #                                        status="APPROVED",
                #                                        date_created=datetime.datetime.now()
                #                                        )
                #     db.session.add(product)
                #     db.session.commit()
                except Exception as e:
                    print("Exception for "+ str(e.args[0]))

    return jsonify({
        'status': 'success',
        'message': 'pong!'
    })

    from .create_shipments import lambda_handler
    lambda_handler()
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter, A4
    from reportlab.lib.units import inch

    c = canvas.Canvas("testing.pdf", pagesize=letter)
    c.translate(inch, inch)
    c.rect(0.2 * inch, 0.2 * inch, 1 * inch, 1.5 * inch, fill=1)
    c.drawString(0.3 * inch, -inch, "Hello World")
    c.showPage()
    c.save()
    return jsonify({
        'status': 'success',
        'message': 'pong!'
    })


    """

    for order in data:


    createBarCodes()

    datetime.strptime("2010-06-04 21:08:12", "%Y-%m-%d %H:%M:%S")
    """
    return jsonify({
        'status': 'success',
        'message': 'pong!'
    })



    shiprocket_token = """Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOjI0NzIyNiwiaXNzIjoiaHR0cHM6Ly9hcGl2Mi5zaGlwcm9ja2V0LmluL3YxL2V4dGVybmFsL2F1dGgvbG9naW4iLCJpYXQiOjE1NzMzNTIzMTYsImV4cCI6MTU3NDIxNjMxNiwibmJmIjoxNTczMzUyMzE2LCJqdGkiOiJmclBCRHZNYnVUZEEwanZOIn0.Gqax7B1zPWoM34yKkUz2Oa7vIvja7D6Z-C8NsyNIIE4"""

    url = "https://apiv2.shiprocket.in/v1/external/orders?per_page=100&page=1"
    headers = {'Authorization': shiprocket_token}
    response = requests.get(url, headers=headers)


    data = response.json()['data']

    for point in data:
        try:
            shipment_dimensions = {}
            try:
                shipment_dimensions['length'] = float(point['shipments'][0]['dimensions'].split('x')[0])
                shipment_dimensions['breadth'] = float(point['shipments'][0]['dimensions'].split('x')[1])
                shipment_dimensions['height'] = float(point['shipments'][0]['dimensions'].split('x')[2])
            except Exception:
                pass
            order_date = datetime.datetime.strptime(point['created_at'], '%d %b %Y, %I:%M %p')
            url_specific_order = "https://apiv2.shiprocket.in/v1/external/orders/show/%s"%(str(point['id']))

            order_spec = requests.get(url_specific_order, headers=headers).json()['data']
            delivery_address = {
                "address": order_spec["customer_address"],
                "address_two": order_spec["customer_address_2"],
                "city": order_spec["customer_city"],
                "state": order_spec["customer_state"],
                "country": order_spec["customer_country"],
                "pincode": int(order_spec["customer_pincode"]),
            }

            new_order = Orders(
                channel_order_id=point['channel_order_id'],
                order_date=order_date,
                customer_name=point['customer_name'],
                customer_email=point['customer_email'],
                customer_phone=point['customer_phone'],
                status=point['status'],
                delivery_address=delivery_address,
                client_prefix='MIRAKKI',
            )
            shipment = Shipments(
                awb=point['shipments'][0]['awb'],
                weight=float(point['shipments'][0]['weight']),
                volumetric_weight=point['shipments'][0]['volumetric_weight'],
                dimensions=shipment_dimensions,
                order=new_order,
            )
            courier = db.session.query(MasterCouriers).filter(MasterCouriers.courier_name==point['shipments'][0]['courier']).first()
            shipment.courier = courier

            payment = OrdersPayments(
                payment_mode=point['payment_method'],
                amount=float(point['total']),
                currency='INR',
                order=new_order
            )
            for prod in point['products']:
                prod_obj = db.session.query(Products).filter(Products.sku==prod['channel_sku']).first()
                op_association = OPAssociation(order=new_order, product=prod_obj, quantity=prod['quantity'])
                new_order.products.append(op_association)

            db.session.add(new_order)
            db.session.commit()

        except Exception as e:
            print(point['id'])
            print(e)
            pass



        """
        dimensions = {}
        try:
            dimensions['length'] = float(point['dimensions'].split(' ')[0])
            dimensions['breadth'] = float(point['dimensions'].split(' ')[2])
            dimensions['height'] = float(point['dimensions'].split(' ')[4])
        except Exception:
            pass
        weight = None
        try:
            weight = float(point['weight'].split(' ')[0])
        except Exception:
            pass

        new_product = Products(
            name=point['name'],
            sku=point['sku'],
            product_image=point['image'],
            price=point['mrp'],
            client_prefix='KYORIGIN',
            active=True,
            dimensions=dimensions,
            weight=weight,
        )
        
        try:
            product_id = Products.query.filter_by(sku=point['sku']).first().id
            new_quantity = ProductQuantity(product_id=product_id,
                                           total_quantity=point['total_quantity'],
                                           approved_quantity=point['total_quantity'],
                                           available_quantity=point['available_quantity'],
                                           warehouse_prefix='MIRAKKI',
                                           status='APPROVED')
            db.session.add(new_quantity)
            db.session.commit()
        except Exception:
            pass
        """

    return jsonify({
        'status': 'success',
        'message': 'pong!'
    })

import smtplib, logging
from datetime import datetime
from flask import render_template
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def create_email(order, edd, email):
    try:
        from project import create_app
        app = create_app()
        background_color = str(order[24]) if order[24] else "#B5D0EC"
        client_logo = str(order[21]) if order[21] else "https://logourls.s3.amazonaws.com/client_logos/logo_ane.png"
        client_name = str(order[20]) if order[20] else "WareIQ"
        email_title = str(order[22]) if order[22] else "Your order has been shipped!"
        order_id = str(order[12]) if order[12] else ""
        customer_name = str(order[18]) if order[18] else "Customer"
        courier_name = "WareIQ"
        if order[23] in (1,2,8,11,12):
            courier_name = "Delhivery"
        elif order[23] in (5,13):
            courier_name = "Xpressbees"
        elif order[23] in (4):
            courier_name = "Shadowfax"

        edd = edd if edd else ""
        awb_number = str(order[1]) if order[1] else ""
        tracking_link = "http://webapp.wareiq.com/tracking/" + str(order[1])
        html = render_template("order_shipped.html", background_color=background_color,
                               client_logo=client_logo,
                               client_name=client_name,
                               email_title=email_title,
                               order_id=order_id,
                               customer_name=customer_name,
                               courier_name=courier_name,
                               edd=edd,
                               awb_number=awb_number,
                               tracking_link=tracking_link)

        # create message object instance
        msg = MIMEMultipart('alternative')

        recipients = [email]
        msg['From'] = "%s <noreply@wareiq.com>"%client_name
        msg['To'] = ", ".join(recipients)
        msg['Subject'] = email_title

        # write the HTML part

        part2 = MIMEText(html, "html")
        msg.attach(part2)
        return msg
    except Exception as e:
        return None


@core_blueprint.route('/core/send', methods=['GET'])
def send_dev():
    return 0
    import requests, json

    return 0
    from project import create_app
    app = create_app()
    from flask import render_template, Flask
    html = render_template("order_shipped.html", background_color="#B5D0EC",
                           client_logo="https://logourls.s3.amazonaws.com/client_logos/logo_zlade.png",
                           client_name="Zlade",
                           email_title="Your order has been shipped!",
                           order_id = "12345",
                           customer_name="Suraj Chaudhari",
                           courier_name = "Delhivery",
                           edd="14 June",
                           awb_number = "3992410231781",
                           tracking_link = "http://webapp.wareiq.com/tracking/3992410231781")
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    # create message object instance
    msg = MIMEMultipart('alternative')

    recipients = ["cravi8750@gmail.com"]
    msg['From'] = "Zlade <noreply@wareiq.com>"
    msg['To'] = ", ".join(recipients)
    msg['Subject'] = "Your order has been shipped!"

    # write the HTML part

    part2 = MIMEText(html, "html")
    msg.attach(part2)

    email_server.sendmail(msg['From'], recipients, msg.as_string())

    return 0
    return jsonify({
        'status': 'success',
        'message': 'pong!'
    })
    # origin fulfilment on shopify
    cur.execute("""select bb.order_id_channel_unique, aa.awb from shipments aa
                        left join orders bb
                        on aa.order_id = bb.id
                        where channel_fulfillment_id is null
                        and client_prefix='KYORIGIN'
                        and order_date>'2020-01-15'
                        and aa.status='Success'
                        order by bb.id DESC""")
    all_orders = cur.fetchall()

    for order in all_orders:
        create_fulfillment_url = "https://%s:%s@%s/admin/api/2019-10/orders/%s/fulfillments.json" % (
            "dc8ae0b7f5c1c6558f551d81e1352bcd", "00dfeaf8f77b199597e360aa4a50a168",
            "origin-clothing-india.myshopify.com", order[0])
        tracking_link = "https://www.delhivery.com/track/package/%s" % str(order[1])
        ful_header = {'Content-Type': 'application/json'}
        fulfil_data = {
            "fulfillment": {
                "tracking_number": str(order[1]),
                "tracking_urls": [
                    tracking_link
                ],
                "tracking_company": "Delhivery",
                "location_id": 16721477681,
                "notify_customer": False
            }
        }
        try:
            req_ful = requests.post(create_fulfillment_url, data=json.dumps(fulfil_data),
                                    headers=ful_header)
            fulfillment_id = str(req_ful.json()['fulfillment']['id'])
            cur.execute("UPDATE shipments SET channel_fulfillment_id=%s WHERE awb=%s", (str(fulfillment_id), order[1]))
        except Exception as e:
            logger.error("Couldn't update shopify for: " + str(order[1])
                         + "\nError: " + str(e.args))

    # end of fulfilment

    # cod_verification call
    cur.execute("""select aa.order_id, bb.customer_phone from cod_verification aa
                        left join orders bb on aa.order_id=bb.id
                        where client_prefix='DAPR'
                        and bb.status='NEW'
                        and cod_verified is null
                        order by bb.id DESC""")
    all_orders = cur.fetchall()

    for order in all_orders:
        data = {
            'From': str(order[1]),
            'CallerId': '01141182252',
            'Url': 'http://my.exotel.com/wareiq1/exoml/start_voice/262896',
            'CustomField': str(order[0])
        }
        req = requests.post(
            'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Calls/connect',
            data=data)

    # end of cod_verification call


    sms_data = {
        'From': 'LM-WAREIQ'
    }
    itt = 0
    for idx, awb in enumerate(awbs):

        sms_to_key = "Messages[%s][To]"%str(itt)
        sms_body_key = "Messages[%s][Body]"%str(itt)
        customer_phone = awb[1].replace(" ","")
        customer_phone = "0"+customer_phone[-10:]
        sms_data[sms_to_key] = customer_phone
        sms_data[sms_body_key] = "Dear Customer, we apologise for the delay. Your order from Know Your Origin has not been shipped due to huge volumes. It will be shipped with in next two days with AWB no. %s by Delhivery."%str(awb[0])
        itt +=1
    lad = requests.post(
        'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend',
        data=sms_data)

    """
    
    from .utils import createBarCodes

    for order in data:


    createBarCodes()

    datetime.strptime("2010-06-04 21:08:12", "%Y-%m-%d %H:%M:%S")
    """
    return jsonify({
        'status': 'success',
        'message': 'pong!'
    })

'''
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
'''