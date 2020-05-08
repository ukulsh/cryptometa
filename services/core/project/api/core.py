# services/core/project/api/core.py

import requests, json, math, datetime, pytz, psycopg2
import boto3, os, csv, io, smtplib
import pandas as pd
import re
from flask_cors import cross_origin
from sqlalchemy import or_, func, not_, and_
from flask import Blueprint, request, jsonify, make_response
from flask_restful import Resource, Api
from reportlab.lib.pagesizes import landscape, A4
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
from psycopg2.extras import RealDictCursor

from project import db
from .queries import product_count_query, available_warehouse_product_quantity, fetch_warehouse_to_pick_from, \
    select_product_list_query, select_orders_list_query, select_wallet_deductions_query
from project.api.models import Products, ProductQuantity, InventoryUpdate, \
    Orders, OrdersPayments, PickupPoints, MasterChannels, ClientPickups, CodVerification, NDRVerification,\
    MasterCouriers, Shipments, OPAssociation, ShippingAddress, Manifests, ClientCouriers, OrderStatus, DeliveryCheck, ClientMapping
from project.api.utils import authenticate_restful, get_products_sort_func, \
    get_orders_sort_func, create_shiplabel_blank_page, fill_shiplabel_data

core_blueprint = Blueprint('core', __name__)
api = Api(core_blueprint)

session = boto3.Session(
    aws_access_key_id='AKIAWRT2R3KC3YZUBFXY',
    aws_secret_access_key='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',
)

conn = psycopg2.connect(host="wareiq-core-prod2.cvqssxsqruyc.us-east-1.rds.amazonaws.com", database="core_prod", user="postgres", password="aSderRFgd23")
conn_2 = psycopg2.connect(host="wareiq-core-prod.cvqssxsqruyc.us-east-1.rds.amazonaws.com", database="core_prod", user="postgres", password="aSderRFgd23")

email_server = smtplib.SMTP_SSL('smtpout.secureserver.net', 465)
email_server.login("noreply@wareiq.com", "Berlin@123")

ORDERS_DOWNLOAD_HEADERS = ["Order ID", "Customer Name", "Customer Email", "Customer Phone", "Order Date",
                            "Courier", "Weight", "awb", "Expected Delivery Date", "Status", "Address_one", "Address_two",
                           "City", "State", "Country", "Pincode", "Pickup Point", "Products", "Quantity", "Order Type", "Amount", "Pickup Date", "Delivered Date"]

PRODUCTS_DOWNLOAD_HEADERS = ["S. No.", "Product Name", "Channel SKU", "Master SKU", "Price", "Total Quantity",
                             "Available Quantity", "Current Quantity", "Inline Quantity", "RTO Quantity", "Dimensions", "Weight"]

DEDUCTIONS_DOWNLOAD_HEADERS = ["Time", "Status", "Courier", "AWB", "order ID", "COD cost", "Forward cost", "Return cost",
                              "Management Fee", "Subtotal", "Total", "Zone", "Weight Charged"]

RECHARGES_DOWNLOAD_HEADERS = ["Payment Time", "Amount", "Transaction ID", "status"]


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
            if auth_data.get('user_group') == 'super-admin' or 'client':
                client_prefix = auth_data.get('client_prefix')
                query_to_execute = select_product_list_query
                if filters:
                    if 'warehouse' in filters:
                        if len(filters['warehouse'])==1:
                            wh_filter = "WHERE warehouse_prefix in ('%s')"%filters['warehouse'][0]
                        else:
                            wh_filter = "WHERE warehouse_prefix in %s"%str(tuple(filters['warehouse']))

                        query_to_execute = query_to_execute.replace('__WAREHOUSE_FILTER__', wh_filter)
                    if 'client' in filters:
                        if len(filters['client'])==1:
                            cl_filter = "AND aa.client_prefix in ('%s')"%filters['client'][0]
                        else:
                            cl_filter = "AND aa.client_prefix in %s"%str(tuple(filters['client']))

                        query_to_execute = query_to_execute.replace('__CLIENT_FILTER__', cl_filter)
                if auth_data['user_group'] != 'super-admin':
                    query_to_execute = query_to_execute.replace('__CLIENT_FILTER__', "AND aa.client_prefix in ('%s')"%client_prefix)

                if type != 'all':
                    return {"success": False, "msg": "Invalid URL"}, 404

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
                    filename = client_prefix+"_EXPORT.csv"
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
    warehouse_qs = db.session.query(ProductQuantity.warehouse_prefix, func.count(ProductQuantity.warehouse_prefix))\
                .join(Products, Products.id == ProductQuantity.product_id)
    if auth_data['user_group'] != 'super-admin':
        warehouse_qs = warehouse_qs.filter(Products.client_prefix == client_prefix)
    if current_tab == 'active':
        warehouse_qs = warehouse_qs.filter(Products.active == True)
    elif current_tab =='inactive':
        warehouse_qs = warehouse_qs.filter(Products.active == False)
    warehouse_qs = warehouse_qs.group_by(ProductQuantity.warehouse_prefix)
    response['filters']['warehouse'] = [{x[0]: x[1]} for x in warehouse_qs]
    if auth_data['user_group'] == 'super-admin':
        client_qs = db.session.query(Products.client_prefix, func.count(Products.client_prefix)).group_by(Products.client_prefix)
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


class OrderList(Resource):

    method_decorators = {'post': [authenticate_restful]}

    def post(self, resp, type):
        try:
            cur = conn.cursor()
            response = {'status':'success', 'data': dict(), "meta": dict()}
            data = json.loads(request.data)
            page = data.get('page', 1)
            per_page = data.get('per_page', 10)
            search_key = data.get('search_key', '')
            since_id = data.get('since_id', None)
            filters = data.get('filters', {})
            search_key = '%{}%'.format(search_key)
            download_flag = request.args.get("download", None)
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404
            if auth_data.get('user_group') == 'super-admin' or 'client':
                client_prefix = auth_data.get('client_prefix')
                query_to_run = select_orders_list_query
                if auth_data['user_group'] != 'super-admin':
                    query_to_run = query_to_run.replace("__CLIENT_FILTER__", "AND aa.client_prefix = '%s'"%client_prefix)
                if since_id:
                    query_to_run = query_to_run.replace("__SINCE_ID_FILTER__", "AND id>%s"%str(since_id))
                query_to_run = query_to_run.replace("__SEARCH_KEY__", search_key)

                if type == 'new':
                    query_to_run = query_to_run.replace("__STATUS_FILTER__", "AND aa.status = 'NEW'")
                elif type == 'ready_to_ship':
                    query_to_run = query_to_run.replace("__STATUS_FILTER__", "AND aa.status in ('PICKUP REQUESTED','READY TO SHIP')")
                elif type == 'shipped':
                    query_to_run = query_to_run.replace("__STATUS_FILTER__", "AND aa.status not in ('NEW', 'READY TO SHIP', 'PICKUP REQUESTED','NOT PICKED','CANCELED','PENDING PAYMENT','NEW - FAILED')")
                elif type == "return":
                    query_to_run = query_to_run.replace("__STATUS_FILTER__", "AND (aa.status_type='RT' or (aa.status_type='DL' and aa.status='RTO'))")
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
                    if 'client' in filters and auth_data['user_group'] == 'super-admin':
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

                    if 'delivered_time' in filters:
                        filter_date_start = filters['delivered_time'][0][0:19].replace('T',' ')
                        filter_date_end = filters['delivered_time'][1][0:19].replace('T',' ')
                        query_to_run = query_to_run.replace("__PICKUP_TIME_FILTER__", "AND delivered_time between '%s' and '%s'" %(filter_date_start, filter_date_end))

                if download_flag:
                    date_month_ago = datetime.datetime.utcnow() + datetime.timedelta(hours=5.5) - datetime.timedelta(days=31)
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
                            new_row = list()
                            new_row.append(str(order[0]))
                            new_row.append(str(order[14]))
                            new_row.append(str(order[16]))
                            new_row.append(str(order[15]))
                            new_row.append(order[2].strftime("%Y-%m-%d") if order[2] else "N/A")
                            new_row.append(str(order[8]))
                            new_row.append(str(order[10]))
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
                            new_row.append(order[28])
                            new_row.append(order[30])
                            new_row.append(str(order[25]))
                            new_row.append(order[26])
                            new_row.append(order[24].strftime("%Y-%m-%d %H:%M:%S") if order[24] else "N/A")
                            new_row.append(order[23].strftime("%Y-%m-%d %H:%M:%S") if order[23] else "N/A")
                            cw.writerow(new_row)
                        except Exception as e:
                            pass

                    output = make_response(si.getvalue())
                    filename = client_prefix+"_EXPORT.csv"
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
                    resp_obj['payment'] = {"mode": order[25],
                                               "amount": order[26]}

                    resp_obj['product_details'] = list()
                    if order[28]:
                        for idx, prod in enumerate(order[28]):
                            resp_obj['product_details'].append(
                                {"name": prod,
                                 "sku": order[29][idx],
                                 "quantity": order[30][idx]}
                            )

                    if order[31]:
                        resp_obj['cod_verification'] = {"confirmed": order[32], "via": order[33]}
                    if order[34]:
                        resp_obj['ndr_verification'] = {"confirmed": order[35], "via": order[36]}

                    resp_obj['shipping_details'] = {"courier": order[8],
                                                    "awb":order[6],
                                                    "tracking_link": order[7]}
                    resp_obj['dimensions'] = order[11]
                    resp_obj['weight'] = order[10]
                    resp_obj['volumetric'] = order[12]
                    if order[9]:
                        resp_obj['edd'] = order[9].strftime('%-d %b')
                    if auth_data['user_group'] == 'super-admin':
                        resp_obj['remark'] = order[13]
                    if type == "shipped":
                        resp_obj['status_detail'] = order[5]

                    resp_obj['status'] = order[3]
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


@core_blueprint.route('/dashboard', methods=['GET'])
@authenticate_restful
def get_dashboard(resp):
    response = dict()
    auth_data = resp.get('data')
    client_prefix = auth_data.get('client_prefix')
    from_date = datetime.datetime.utcnow() + datetime.timedelta(hours=5.5)
    from_date = datetime.datetime(from_date.year, from_date.month, from_date.day)
    from_date = from_date - datetime.timedelta(hours=5.5)
    qs_data = db.session.query(func.date_trunc('day', Orders.order_date).label('date'), func.count(Orders.id), func.sum(OrdersPayments.amount))\
        .join(OrdersPayments, Orders.id==OrdersPayments.order_id)\
        .filter(Orders.order_date >= datetime.datetime.today()- datetime.timedelta(days=30))
    cod_verification = db.session.query(CodVerification).join(Orders, Orders.id==CodVerification.order_id)\
        .filter(or_(CodVerification.date_created >= from_date, CodVerification.verification_time >= from_date))
    ndr_verification = db.session.query(NDRVerification).join(Orders, Orders.id==NDRVerification.order_id)\
        .filter(or_(NDRVerification.date_created >= from_date, NDRVerification.verification_time >= from_date))
    if auth_data['user_group'] != 'super-admin':
        qs_data = qs_data.filter(Orders.client_prefix == client_prefix)
        cod_verification = cod_verification.filter(Orders.client_prefix == client_prefix)
        ndr_verification = ndr_verification.filter(Orders.client_prefix == client_prefix)
    qs_data = qs_data.group_by('date').order_by('date').all()
    cod_verification = cod_verification.all()
    ndr_verification = ndr_verification.all()

    cod_check = {"total_checked": len(cod_verification),
                 "confirmed_via_text": 0,
                 "confirmed_via_call": 0,
                 "total_cancelled": 0,
                 "not_confirmed_yet": 0}
    for cod_data in cod_verification:
        if cod_data.cod_verified is True:
            if cod_data.verified_via == 'text':
                cod_check['confirmed_via_text'] += 1
            elif cod_data.verified_via == 'call':
                cod_check['confirmed_via_call'] += 1
        elif cod_data.cod_verified is False:
            cod_check['total_cancelled'] += 1

        else:
            cod_check['not_confirmed_yet'] += 1

    ndr_check = {"total_checked": len(ndr_verification),
                 "confirmed_via_text": 0,
                 "confirmed_via_call": 0,
                 "reattempt_requested": 0,
                 "not_confirmed_yet": 0}
    for ndr_data in ndr_verification:
        if ndr_data.ndr_verified is True:
            if ndr_data.verified_via == 'text':
                ndr_check['confirmed_via_text'] += 1
            elif ndr_data.verified_via == 'call':
                ndr_check['confirmed_via_call'] += 1
        elif ndr_data.ndr_verified is False:
            ndr_check['reattempt_requested'] += 1

        else:
            ndr_check['not_confirmed_yet'] += 1

    response['cod_verification'] = cod_check
    response['ndr_verification'] = ndr_check

    date_today = datetime.datetime.utcnow()
    date_today = date_today + datetime.timedelta(hours=5.5)
    date_yest = date_today - datetime.timedelta(days=1)

    date_today = datetime.datetime.strftime(date_today, '%d-%m-%Y')
    date_yest = datetime.datetime.strftime(date_yest, '%d-%m-%Y')

    response['today'] = {"orders": 0, "revenue": 0}
    response['yesterday'] = {"orders": 0, "revenue": 0}

    response['graph_data'] = list()

    for dat_obj in qs_data:
        date_str=datetime.datetime.strftime(dat_obj[0], '%d-%m-%Y')
        if date_str==date_today:
            response['today'] = {"orders": dat_obj[1], "revenue": dat_obj[2]}
        if date_str==date_yest:
            response['yesterday'] = {"orders": dat_obj[1], "revenue": dat_obj[2]}
        response['graph_data'].append({"date":datetime.datetime.strftime(dat_obj[0], '%d-%m-%Y'),
                                       "orders":dat_obj[1],
                                       "revenue":dat_obj[2]})

    return jsonify(response), 200


@core_blueprint.route('/orders/get_filters', methods=['GET'])
@authenticate_restful
def get_orders_filters(resp):
    response = {"filters":{}, "success": True}
    auth_data = resp.get('data')
    current_tab = request.args.get('tab')
    client_prefix = auth_data.get('client_prefix')
    client_qs = None
    status_qs = db.session.query(Orders.status, func.count(Orders.status)).group_by(Orders.status)
    courier_qs = db.session.query(MasterCouriers.courier_name, func.count(MasterCouriers.courier_name)) \
        .join(Shipments, MasterCouriers.id == Shipments.courier_id).join(Orders, Orders.id == Shipments.order_id) \
        .group_by(MasterCouriers.courier_name)
    pickup_point_qs = db.session.query(PickupPoints.warehouse_prefix, func.count(PickupPoints.warehouse_prefix)) \
        .join(ClientPickups, PickupPoints.id == ClientPickups.pickup_id).join(Orders, ClientPickups.id == Orders.pickup_data_id) \
        .group_by(PickupPoints.warehouse_prefix)
    if auth_data['user_group'] == 'super-admin':
        client_qs = db.session.query(Orders.client_prefix, func.count(Orders.client_prefix))

    if auth_data['user_group'] != 'super-admin':
        status_qs=status_qs.filter(Orders.client_prefix == client_prefix)
        courier_qs = courier_qs.filter(Orders.client_prefix == client_prefix)
        pickup_point_qs = pickup_point_qs.filter(Orders.client_prefix == client_prefix)
    if current_tab=="shipped":
        status_qs = status_qs.filter(not_(Orders.status.in_(["NEW","READY TO SHIP","PICKUP REQUESTED","NOT PICKED","PENDING PAYMENT"])))
        courier_qs = courier_qs.filter(not_(Orders.status.in_(["NEW","READY TO SHIP","PICKUP REQUESTED","NOT PICKED","PENDING PAYMENT"])))
        pickup_point_qs = pickup_point_qs.filter(not_(Orders.status.in_(["NEW","READY TO SHIP","PICKUP REQUESTED","NOT PICKED","PENDING PAYMENT"])))
        if client_qs:
            client_qs = client_qs.filter(not_(Orders.status.in_(["NEW", "READY TO SHIP", "PICKUP REQUESTED", "NOT PICKED","PENDING PAYMENT"])))
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
            if pickup_filter:
                pickup_data = db.session.query(ClientPickups).join(PickupPoints, ClientPickups.pickup_id==PickupPoints.id).filter(PickupPoints.warehouse_prefix==pickup_filter).first()
            else:
                if auth_data.get('client_prefix')!='NASHER':
                    pickup_data = db.session.query(ClientPickups).filter(ClientPickups.client_prefix==auth_data.get('client_prefix')).first()
                else:
                    pickup_data = None

            new_order = Orders(channel_order_id=str(data.get('order_id')),
                           order_date=datetime.datetime.now()+datetime.timedelta(hours=5.5),
                           customer_name=data.get('full_name'),
                           customer_email=data.get('customer_email'),
                           customer_phone=data.get('customer_phone'),
                           delivery_address=delivery_address,
                           status="NEW",
                           client_prefix=auth_data.get('client_prefix'),
                           pickup_data=pickup_data,
                           )

            if data.get('products'):
                for prod in data.get('products'):
                    prod_obj = db.session.query(Products).filter(Products.sku == prod['sku']).first()
                    if not prod_obj:
                        db.session.query(Products).filter(Products.master_sku == prod['sku']).first()
                    if prod_obj:
                        op_association = OPAssociation(order=new_order, product=prod_obj, quantity=prod['quantity'])
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
            return {'status': 'success', 'msg': "successfully added", "order_id": new_order.channel_order_id}, 200

        except Exception as e:
            return {"status":"Failed", "msg":""}, 400

    def get(self, resp):
        auth_data = resp.get('data')
        search_key = request.args.get('search_key', None)
        if not auth_data:
            return {"success": False, "msg": "Auth Failed"}, 404

        if search_key is not None:
            cur = conn.cursor()
            query_to_execute = """SELECT id, name, sku, master_sku FROM products
                                  WHERE (name ilike '%__SEARCH_KEY__%'
                                  OR sku ilike '%__SEARCH_KEY__%'
                                  OR master_sku ilike '%__SEARCH_KEY__%')
                                  __CLIENT_FILTER__
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

            response = {"data": search_list}

            return response, 200
        payment_modes = ['prepaid','COD']
        warehouses = [r.warehouse_prefix for r in db.session.query(PickupPoints.warehouse_prefix)
            .join(ClientPickups, ClientPickups.pickup_id==PickupPoints.id)
            .filter(ClientPickups.client_prefix==auth_data.get('client_prefix'))
            .order_by(PickupPoints.warehouse_prefix)]

        response = {"payment_modes":payment_modes, "warehouses": warehouses}

        return response, 200


api.add_resource(AddOrder, '/orders/add')


@core_blueprint.route('/orders/v1/upload', methods=['POST'])
@authenticate_restful
def upload_orders(resp):
    auth_data = resp.get('data')
    if not auth_data:
        return {"success": False, "msg": "Auth Failed"}, 404

    myfile = request.files['myfile']

    data_xlsx = pd.read_excel(myfile)
    failed_ids = dict()

    for row in data_xlsx.iterrows():
        try:
            row_data = row[1]
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

            new_order = Orders(channel_order_id=str(row_data.order_id),
                               order_date=datetime.datetime.now()+datetime.timedelta(hours=5.5),
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
                    prod_obj = db.session.query(Products).filter(Products.sku == sku_str.strip()).first()
                    if prod_obj:
                        op_association = OPAssociation(order=new_order, product=prod_obj, quantity=int(sku_quantity[idx].strip()))
                        new_order.products.append(op_association)

            payment = OrdersPayments(
                payment_mode=str(row_data.payment_mode),
                subtotal=float(row_data.subtotal),
                amount=float(row_data.subtotal) + float(row_data.shipping_charges),
                shipping_charges=float(row_data.shipping_charges),
                currency='INR',
                order=new_order
            )

            db.session.add(new_order)
            for idx, sku_str in enumerate(sku):
                prod_obj = db.session.query(ProductQuantity).join(Products, ProductQuantity.product_id==Products.id).filter(Products.sku==sku_str).first()
                if prod_obj:
                    prod_obj.available_quantity=prod_obj.available_quantity-int(sku_quantity[idx])
                    prod_obj.inline_quantity=prod_obj.inline_quantity+int(sku_quantity[idx])

            db.session.commit()

        except Exception as e:
            failed_ids[str(row[1].order_id)] = str(e.args[0])
            db.session.rollback()

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
    orders_qs = db.session.query(Orders).filter(Orders.id.in_(order_ids), Orders.delivery_address!=None,
                                                Orders.shipments!=None)

    if auth_data['user_group'] != 'super-admin':
        orders_qs = orders_qs.filter(Orders.client_prefix==auth_data.get('client_prefix'))
    orders_qs = orders_qs.order_by(Orders.id).all()
    if not orders_qs:
        return jsonify({"success": False, "msg": "No valid order ID"}), 404

    file_name = "shiplabels_"+auth_data['client_prefix']+"_"+str(datetime.datetime.now().strftime("%d_%b_%Y_%H_%M_%S"))+".pdf"
    c = canvas.Canvas(file_name, pagesize=landscape(A4))
    create_shiplabel_blank_page(c)
    failed_ids = dict()
    idx=0
    for ixx, order in enumerate(orders_qs):
        try:
            if not order.shipments or not order.shipments[0].awb:
                continue
            if auth_data['client_prefix'] in ("KYORIGIN", "NASHER"):
                offset = 3.913
                try:
                    fill_shiplabel_data(c, order, offset)
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
                    fill_shiplabel_data(c, order, offset_dict[idx%3])
                except Exception:
                    pass
                if idx%3==2 and ixx!=(len(orders_qs)-1):
                    c.showPage()
                    create_shiplabel_blank_page(c)
            idx += 1
        except Exception as e:
            failed_ids[order.channel_order_id] = str(e.args[0])
            pass
    if auth_data['client_prefix'] not in ("KYORIGIN", "NASHER"):
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
    manifest_qs = db.session.query(Manifests).join(ClientPickups, Manifests.client_pickup_id==ClientPickups.id)
    if auth_data['user_group'] != 'super-admin':
        manifest_qs= manifest_qs.filter(ClientPickups.client_prefix==auth_data['client_prefix'])
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
            if auth_data['user_group'] != 'super-admin':
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
                         "quantity": prod.quantity}
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
                resp_obj['status'] = order.status
                resp_obj['remark'] = None
                if auth_data['user_group'] == 'super-admin' and order.shipments:
                    resp_obj['remark'] = order.shipments[0].remark

                if not order.exotel_data:
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

                if order.status in ('NEW', 'READY TO SHIP', 'PICKUP REQUESTED','NOT PICKED','CANCELED','PENDING PAYMENT'):
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
                    order.exotel_data[0].verification_time = datetime.datetime.utcnow() + datetime.timedelta(hours=5.5)
                if data.get('cod_verification') == False:
                    order.status = 'CANCELED'
                    if order.client_channel.channel_id == 6 and order.order_id_channel_unique: #cancel on magento
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
                elif data.get('cod_verification') == True and not order.shipments:
                    if order.shipments and order.shipments[0].awb:
                        order.status = 'READY TO SHIP'
                    else:
                        order.status = 'NEW'

            if 'ndr_verification' in data and order.ndr_verification:
                order.ndr_verification[0].ndr_verified = data.get('ndr_verification')
                order.ndr_verification[0].verified_via = 'manual'
                order.ndr_verification[0].verification_time = datetime.datetime.utcnow() + datetime.timedelta(hours=5.5)

            db.session.commit()
            return {'status': 'success', 'msg': "successfully updated"}, 200

        except Exception as e:
            return {'status': 'Failed'}, 200


api.add_resource(OrderDetails, '/orders/v1/order/<order_id>')


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
                            status_time = datetime.datetime.strptime(status_time, '%Y-%m-%dT%H:%M:%S')
                        else:
                            status_time = datetime.datetime.strptime(status_time, '%Y-%m-%dT%H:%M:%S.%f')
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
            status_time = datetime.datetime.strptime(req_obj['ShipmentData'][0]['Shipment']['PickUpDate'], '%Y-%m-%dT%H:%M:%S.%f')
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
                    status_time = datetime.datetime.strptime(order_status['ScanDetail']['StatusDateTime'],
                                                             '%Y-%m-%dT%H:%M:%S.%f')
                    status_dict['time'] = status_time.strftime("%d %b %Y, %H:%M:%S")
                    picked_obj = status_dict
                elif order_status['ScanDetail']['Scan'] == 'In Transit':
                    status_dict['status'] = 'In Transit'
                    status_dict['city'] = order_status['ScanDetail']['ScannedLocation']
                    status_time = datetime.datetime.strptime(order_status['ScanDetail']['StatusDateTime'],
                                                             '%Y-%m-%dT%H:%M:%S.%f')
                    status_dict['time'] = status_time.strftime("%d %b %Y, %H:%M:%S")
                    in_transit_obj = status_dict
                elif order_status['ScanDetail']['Scan'] == 'Dispatched':
                    status_dict['status'] = 'Out for delivery'
                    status_dict['city'] = order_status['ScanDetail']['ScannedLocation']
                    status_time = datetime.datetime.strptime(order_status['ScanDetail']['StatusDateTime'],
                                                             '%Y-%m-%dT%H:%M:%S.%f')
                    status_dict['time'] = status_time.strftime("%d %b %Y, %H:%M:%S")
                    ofd_obj = status_dict
                elif 'Delivered' in order_status['ScanDetail']['Instructions']:
                    status_dict['status'] = 'Delivered'
                    status_dict['city'] = order_status['ScanDetail']['ScannedLocation']
                    status_time = datetime.datetime.strptime(order_status['ScanDetail']['StatusDateTime'],
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
                    gather_prompt_text = "Hello %s, You recently placed an order from %s with order ID %s." \
                                     " Press 1 to confirm your order or, 0 to cancel." % (order.customer_name,
                                                                                         client_name,
                                                                                         order.channel_order_id)

                    repeat_prompt_text = "It seems that you have not provided any input, please try again. Order from %s, " \
                                     "Order ID %s. Press 1 to confirm your order or, 0 to cancel." % (
                                     client_name,
                                     order.channel_order_id)
                elif ver_type=="ndr":
                    gather_prompt_text = "Hello %s, You recently cancelled your order from %s with order ID %s." \
                                         " Press 1 to confirm cancellation or, 0 to re-attempt." % (order.customer_name,
                                                                                             client_name,
                                                                                             order.channel_order_id)

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

            try:
                if type=="ndr" and cod_ver.ndr_verified == False:
                    if cod_ver.order.shipments[0].courier_id in (1,2,8,11,12): #Delhivery
                        headers = {"Authorization": "Token " + cod_ver.order.shipments[0].courier.api_key,
                                   "Content-Type": "application/json"}
                        delhivery_url = "https://track.delhivery.com/api/p/update"
                        delivery_shipments_body = json.dumps({"data": [{"waybill": cod_ver.order.shipments[0].awb,
                                                                        "act": "RE-ATTEMPT"}]})

                        req = requests.post(delhivery_url, headers=headers, data=delivery_shipments_body)
                    elif cod_ver.order.shipments[0].courier_id in (5,13): #Xpressbees
                        headers = {"Content-Type": "application/json",
                                   "XBKey":cod_ver.order.shipments[0].courier.api_key}
                        body = {"ShippingID": cod_ver.order.shipments[0].awb}
                        xpress_url = "http://xbclientapi.xbees.in/POSTShipmentService.svc/UpdateNDRDeferredDeliveryDate"
                        req = requests.post(xpress_url, headers=headers, data=json.dumps(body))

            except Exception as e:
                pass

            cod_ver.verified_via = verified_via

            current_time = datetime.datetime.now()
            cod_ver.verification_time = current_time

            db.session.commit()

            return jsonify({"success": True}), 200
        else:
            return jsonify({"success": False, "msg": "No Order"}), 400
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
            if not sku_list:
                return {"success": False, "msg": "SKUs not provided"}, 404

            try:
                cod_req = requests.get(
                    "https://track.delhivery.com/c/api/pin-codes/json/?filter_codes=%s&token=d6ce40e10b52b5ca74805a6e2fb45083f0194185" % str(
                        del_pincode)).json()
                if not cod_req.get('delivery_codes'):
                    return {"success": False, "msg": "Pincode not serviceable"}, 404

                if cod_req['delivery_codes'][0]['postal_code']['cod'].lower() == 'y':
                    cod_available = True
            except Exception:
                pass

            sku_dict = dict()
            for sku in sku_list:
                sku_dict[sku['sku']] = sku['quantity']

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
            courier_id = 2
            courier_id_weight = 0.0
            for prod_wh in prod_wh_tuple:
                if prod_wh[5] > courier_id_weight:
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
                return {"success": False, "msg": "Not serviceable", "cod_available": cod_available,
                        "label_url":"https://logourls.s3.amazonaws.com/wareiq_standard.jpeg"}, 404

            current_time = datetime.datetime.utcnow() + datetime.timedelta(hours=5.5)
            order_before = current_time
            if current_time.hour >= 14:
                order_before = order_before + datetime.timedelta(days=1)
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

            delivered_by = datetime.datetime.utcnow() + datetime.timedelta(hours=5.5) + datetime.timedelta(
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
                           "sku_wise": sku_wise_list}

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
            if cod_req['delivery_codes'][0]['postal_code']['cod'].lower() == 'y':
                cod_available = True

            return {"success": True, "data": {"serviceable": True, "cod_available": cod_available}}, 200

        except Exception as e:
            return {"success": False, "msg": ""}, 404


api.add_resource(PincodeServiceabilty, '/orders/v1/serviceability')


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

                    update_obj = InventoryUpdate(product_id=quan_obj.product_id,
                                                 warehouse_prefix=warehouse,
                                                 user=auth_data['email'],
                                                 remark = data.get('remark', None),
                                                 quantity = int(quantity),
                                                 type = str(type).lower(),
                                                 date_created = datetime.datetime.utcnow()+datetime.timedelta(hours=5.5))

                    if not quan_obj:
                        sku_obj['error'] = "Warehouse sku combination not found."
                        failed_list.append(sku_obj)
                        continue

                    shipped_quantity=0
                    try:
                        cur.execute("""  select COALESCE(sum(quantity), 0) from op_association aa
                                left join orders bb on aa.order_id=bb.id
                                left join client_pickups cc on bb.pickup_data_id=cc.id
                                left join pickup_points dd on cc.pickup_id=dd.id
                                left join products ee on aa.product_id=ee.id
                                where status in ('DELIVERED','DISPATCHED','IN TRANSIT','ON HOLD','PENDING')
                                and dd.warehouse_prefix='__WAREHOUSE__'
                                and ee.sku='__SKU__';""".replace('__WAREHOUSE__', warehouse).replace('__SKU__', sku))
                        shipped_quantity_obj = cur.fetchone()
                        if shipped_quantity_obj is not None:
                            shipped_quantity = shipped_quantity_obj[0]
                    except Exception:
                        conn.rollback()

                    if str(type).lower() == 'add':
                        quan_obj.total_quantity = quan_obj.total_quantity+quantity
                        quan_obj.approved_quantity = quan_obj.approved_quantity+quantity
                    elif str(type).lower() == 'subtract':
                        quan_obj.total_quantity = quan_obj.total_quantity - quantity
                        quan_obj.approved_quantity = quan_obj.approved_quantity - quantity
                    elif str(type).lower() == 'replace':
                        quan_obj.total_quantity = quantity + shipped_quantity
                        quan_obj.approved_quantity = quantity + shipped_quantity
                    else:
                        continue

                    current_quantity.append({"warehouse": warehouse, "sku": sku,
                                             "available_quantity": quan_obj.approved_quantity- shipped_quantity})

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
                                  date_created=datetime.datetime.now()
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
                                                date_created=datetime.datetime.now()
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
            if auth_data.get('user_group') == 'super-admin' or 'client':
                client_prefix = auth_data.get('client_prefix')
                query_to_execute = select_wallet_deductions_query
                query_total_recharge = """select COALESCE(sum(recharge_amount), 0) from client_recharges
                                        WHERE recharge_time>'2020-04-01'
                                        AND lower(status)='successful'
                                        __CLIENT_FILTER__"""
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
                if auth_data['user_group'] != 'super-admin':
                    query_to_execute = query_to_execute.replace('__CLIENT_FILTER__', "AND dd.client_prefix = '%s'"%client_prefix)
                    query_total_recharge = query_total_recharge.replace('__CLIENT_FILTER__', "AND client_prefix = '%s'"%client_prefix)

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
                    filename = client_prefix+"_EXPORT.csv"
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
                return {"sucscess": False, "msg": "Auth Failed"}, 404

            filters = dict()
            query_to_run_courier = """SELECT cc.courier_name, count(*) FROM client_deductions aa
                                        LEFT JOIN shipments bb on aa.shipment_id=bb.id
                                        LEFT JOIN master_couriers cc on bb.courier_id=cc.id
                                        LEFT JOIN orders dd on bb.order_id=dd.id
                                        WHERE aa.deduction_time>'2020-04-01'
                                        __CLIENT_FILTER__
                                        GROUP BY courier_name
                                        ORDER BY courier_name"""

            if auth_data['user_group'] != 'super-admin':
                query_to_run_courier = query_to_run_courier.replace("__CLIENT_FILTER__",
                                                    "AND dd.client_prefix='%s'" % auth_data['client_prefix'])
            else:
                query_to_run_courier = query_to_run_courier.replace("__CLIENT_FILTER__", "")
                query_to_run_client = """SELECT cc.client_prefix, count(*) FROM client_deductions aa
                                        LEFT JOIN shipments bb on aa.shipment_id=bb.id
                                        LEFT JOIN orders cc on bb.order_id=cc.id
                                        WHERE aa.deduction_time>'2020-04-01'
                                        GROUP BY client_prefix
                                        ORDER BY client_prefix"""
                cur.execute(query_to_run_client)
                client_data = cur.fetchall()
                filters['client'] = list()
                for client in client_data:
                    if client[0]:
                        filters['client'].append({client[0]:client[1]})

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
            if auth_data.get('user_group') == 'super-admin' or 'client':
                client_prefix = auth_data.get('client_prefix')
                query_to_execute = """SELECT recharge_time, recharge_amount, transaction_id, status FROM client_recharges aa
                                    WHERE (transaction_id ilike '%__SEARCH_KEY__%' or bank_transaction_id ilike '%__SEARCH_KEY__%')
                                    __CLIENT_FILTER__
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
                if auth_data['user_group'] != 'super-admin':
                    query_to_execute = query_to_execute.replace('__CLIENT_FILTER__', "AND client_prefix = '%s'"%client_prefix)

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
                    filename = client_prefix+"_EXPORT.csv"
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

            filters = dict()
            if auth_data['user_group'] == 'super-admin':
                query_to_run= """SELECT client_prefix, count(*) FROM client_recharges
                                GROUP BY client_prefix
                                ORDER BY client_prefix"""

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


@core_blueprint.route('/core/dev', methods=['POST'])
def ping_dev():
    return 0
    from .fetch_orders import lambda_handler
    lambda_handler()
    import requests
    magento_orders_url = """https://www.vedaearth.com/rest/V1/orders?searchCriteria[filter_groups][0][filters][0][field]=created_at&searchCriteria[filter_groups][0][filters][0][value]=2020-05-03 00:00:00&searchCriteria[filter_groups][0][filters][0][condition_type]=gt&searchCriteria[filter_groups][1][filters][0][field]=status&searchCriteria[filter_groups][1][filters][0][value]=processing&searchCriteria[filter_groups][1][filters][0][condition_type]=eq&searchCriteria[filter_groups][1][filters][1][field]=status&searchCriteria[filter_groups][1][filters][1][value]=cod&searchCriteria[filter_groups][1][filters][1][condition_type]=eq"""
    headers = {'Authorization': "Bearer " + "zg1j7voibdpswz0yugnt3pfjfbvog335",
               'Content-Type': 'application/json'}
    data = requests.get(magento_orders_url, headers=headers).json()
    from requests_oauthlib.oauth1_session import OAuth1Session
    auth_session = OAuth1Session("ck_cd462226a5d5c21c5936c7f75e1afca25b9853a6",
                                 client_secret="cs_c897bf3e770e15f518cba5c619b32671b7cc527c")
    url = '%s/wp-json/wc/v3/orders?per_page=100&include=109160,109110&order=asc&consumer_key=ck_cd462226a5d5c21c5936c7f75e1afca25b9853a6&consumer_secret=cs_c897bf3e770e15f518cba5c619b32671b7cc527c' % (
        "https://www.zladeformen.com")
    r = auth_session.get(url)
    return 0
    from .update_status import lambda_handler
    lambda_handler()
    import requests, json
    shopify_url = "https://720247f946e1cb4b64730dc501fc8f75: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@boltcoldbrew.myshopify.com/admin/api/2020-04/orders.json"
    data = requests.get(shopify_url).json()
    from .request_pickups import lambda_handler
    lambda_handler()
    myfile = request.files['myfile']

    data_xlsx = pd.read_excel(myfile)
    from .models import Products, ProductQuantity
    count = 0
    iter_rw = data_xlsx.iterrows()
    for row in iter_rw:
        try:
            sku_name = row[1].Sku
            prod_obj = Products(name=row[1].Products,
                                sku=sku_name,
                                master_sku=sku_name,
                                dimensions={"length":float(row[1].length),
                                          "breadth":float(row[1].breadth),
                                          "height": float(row[1].height)
                                          },
                                weight=row[1].weight,
                                price=0,
                                client_prefix='VITAMINPLANET',
                                active=True,
                                channel_id=4,
                                date_created=datetime.datetime.now()
                                )
            prod_quan_obj = ProductQuantity(product=prod_obj,
                                            total_quantity=1000,
                                            approved_quantity=1000,
                                            available_quantity=1000,
                                            inline_quantity=0,
                                            rto_quantity=0,
                                            current_quantity=0,
                                            warehouse_prefix='VITAMINPLANET',
                                            status="APPROVED",
                                            date_created=datetime.datetime.now()
                                            )


            db.session.add(prod_quan_obj)
            db.session.commit()
        except Exception as e:
            print(str(row[1].Ordr_id) + "\n" + str(e.args[0]))
            db.session.rollback()
    return 0
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


@core_blueprint.route('/core/send', methods=['GET'])
def send_dev():
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
    shopify_url = "https://b27f6afd9506d0a07af9a160b1666b74:6c8ca315e01fe612bc997e70c7a21908@mirakki.myshopify.com/admin/api/2019-10/orders.json?since_id=1873306419335&limit=150"
    data = requests.get(shopify_url).json()
    from .utils import createBarCodes

    for order in data:


    createBarCodes()

    datetime.strptime("2010-06-04 21:08:12", "%Y-%m-%d %H:%M:%S")
    """
    return jsonify({
        'status': 'success',
        'message': 'pong!'
    })