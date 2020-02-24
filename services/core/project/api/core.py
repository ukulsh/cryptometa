# services/core/project/api/core.py

import requests, json, math, datetime, pytz, psycopg2
import boto3, os, csv, io
import pandas as pd
from flask_cors import cross_origin
from sqlalchemy import or_, func, not_, and_
from flask import Blueprint, request, jsonify, make_response
from flask_restful import Resource, Api
from reportlab.lib.pagesizes import landscape, A4
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas

from project import db
from .queries import product_count_query, available_warehouse_product_quantity, fetch_warehouse_to_pick_from
from project.api.models import Products, ProductQuantity, \
    Orders, OrdersPayments, PickupPoints, MasterChannels, ClientPickups, CodVerification, NDRVerification,\
    MasterCouriers, Shipments, OPAssociation, ShippingAddress, Manifests, ClientCouriers, OrderStatus, DeliveryCheck
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
cur = conn.cursor()
cur_2 = conn_2.cursor()

ORDERS_DOWNLOAD_HEADERS = ["Order ID", "Customer Name", "Customer Email", "Customer Phone", "Order_Date",
                            "Courier", "Weight", "awb", "Delivery Date", "Status", "Address_one", "Address_two",
                           "City", "State", "Country", "Pincode", "Pickup Point", "Products", "Quantity"]

class ProductList(Resource):

    method_decorators = {'post': [authenticate_restful]}

    def post(self, resp, type):
        try:
            response = {'status':'success', 'data': dict(), "meta": dict()}
            data = json.loads(request.data)
            page = data.get('page', 1)
            per_page = data.get('per_page', 10)
            sort = data.get('sort', 'asc')
            sort_by = data.get('sort_by', 'available_quantity')
            search_key = data.get('search_key', '')
            search_key = '%{}%'.format(search_key)
            filters = data.get('filters', {})
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404
            if auth_data.get('user_group') == 'super-admin' or 'client':
                client_prefix = auth_data.get('client_prefix')
                sort_func = get_products_sort_func(Products, ProductQuantity, sort, sort_by)
                products_qs = db.session.query(Products).outerjoin(ProductQuantity, Products.id==ProductQuantity.product_id)\
                    .order_by(sort_func())\
                    .filter(or_(Products.name.ilike(search_key), Products.sku.ilike(search_key)))
                if filters:
                    if 'warehouse' in filters:
                        products_qs = products_qs.filter(ProductQuantity.warehouse_prefix.in_(filters['warehouse']))
                    if 'client' in filters:
                        products_qs = products_qs.filter(Products.client_prefix.in_(filters['client']))
                if auth_data['user_group'] != 'super-admin':
                    products_qs = products_qs.filter(Products.client_prefix==client_prefix)

                if type == 'active':
                    products_qs = products_qs.filter(Products.active == True)
                elif type == 'inactive':
                    products_qs = products_qs.filter(Products.active == False)
                elif type == 'all':
                    pass
                else:
                    return {"success": False, "msg": "Invalid URL"}, 404

                product_ids_list = list()
                products_qs_data = products_qs.limit(per_page).offset((page-1)*per_page).all()
                response_dict_sku = dict()
                for product in products_qs_data:
                    resp_obj=dict()
                    product_ids_list.append(product.id)
                    resp_obj['channel_logo'] = product.channel.logo_url
                    resp_obj['product_name'] = product.name
                    resp_obj['product_image'] = product.product_image
                    resp_obj['price'] = product.price
                    resp_obj['master_sku'] = product.sku
                    resp_obj['channel_sku'] = product.sku
                    resp_obj['inline_quantity'] = 0
                    resp_obj['rto_quantity'] = 0
                    resp_obj['total_quantity'] = 0
                    resp_obj['available_quantity'] = 0
                    resp_obj['current_quantity'] = 0
                    resp_obj['dimensions'] = product.dimensions
                    resp_obj['weight'] = product.weight
                    if type == 'inactive':
                        resp_obj['inactive_reason'] = product.inactive_reason

                    for quan_obj in product.quantity:
                        if 'warehouse' in filters and quan_obj.warehouse_prefix in filters['warehouse']:
                            resp_obj['total_quantity'] += quan_obj.approved_quantity
                            resp_obj['available_quantity'] += quan_obj.approved_quantity
                            resp_obj['current_quantity'] += quan_obj.approved_quantity
                    response_dict_sku[product.id] = resp_obj

                if product_ids_list:
                    if len(product_ids_list) == 1:
                        count_query = product_count_query.replace('__PRODUCT_IDS__', '(%s)'%str(product_ids_list[0]))
                    else:
                        count_query = product_count_query.replace('__PRODUCT_IDS__', str(tuple(product_ids_list)))
                    if filters and 'warehouse' in filters:
                        if len(filters['warehouse'])==1:
                            count_query = count_query.replace('__WAREHOUSE_FILTER__', "and warehouse_prefix in ('%s')"%(str(filters['warehouse'][0])))
                        else:
                            count_query = count_query.replace('__WAREHOUSE_FILTER__', "and warehouse_prefix in %s"%(str(tuple(filters['warehouse']))))
                    else:
                        count_query = count_query.replace('__WAREHOUSE_FILTER__', "")

                    cur.execute(count_query)
                    counts_tuple = cur.fetchall()
                    for count_val in counts_tuple:
                        if count_val[1] in ('DELIVERED','DISPATCHED','IN TRANSIT','ON HOLD','PENDING'):
                            response_dict_sku[count_val[0]]['current_quantity'] -= count_val[2]
                            response_dict_sku[count_val[0]]['available_quantity'] -= count_val[2]
                        elif count_val[1] in ('NEW','NOT PICKED','PICKUP REQUESTED','READY TO SHIP'):
                            response_dict_sku[count_val[0]]['inline_quantity'] += count_val[2]
                            response_dict_sku[count_val[0]]['available_quantity'] -= count_val[2]
                        elif count_val[1] in ('RTO'):
                            response_dict_sku[count_val[0]]['rto_quantity'] += count_val[2]

                response_data = list(response_dict_sku.values())

                response['data'] = sorted(response_data, key = lambda i: i['available_quantity'])
                total_count = products_qs.count()

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


class OrderList(Resource):

    method_decorators = {'post': [authenticate_restful]}

    def post(self, resp, type):
        response = {'status':'success', 'data': dict(), "meta": dict()}
        data = json.loads(request.data)
        page = data.get('page', 1)
        per_page = data.get('per_page', 10)
        sort = data.get('sort', 'desc')
        sort_by = data.get('sort_by', 'order_date')
        search_key = data.get('search_key', '')
        filters = data.get('filters', {})
        search_key = '%{}%'.format(search_key)
        download_flag = request.args.get("download", None)
        auth_data = resp.get('data')
        if not auth_data:
            return {"success": False, "msg": "Auth Failed"}, 404
        if auth_data.get('user_group') == 'super-admin' or 'client':
            client_prefix = auth_data.get('client_prefix')
            sort_func = get_orders_sort_func(Orders, sort, sort_by)
            orders_qs = db.session.query(Orders).join(Shipments, Orders.id==Shipments.order_id, isouter=True).join(
                ClientPickups, ClientPickups.id==Orders.pickup_data_id, isouter=True).join(PickupPoints, PickupPoints.id==ClientPickups.pickup_id, isouter=True).join(
                CodVerification, Orders.id==CodVerification.order_id, isouter=True)
            if auth_data['user_group'] != 'super-admin':
                orders_qs = orders_qs.filter(Orders.client_prefix==client_prefix)
            orders_qs = orders_qs.order_by(sort_func())\
                .filter(or_(Orders.channel_order_id.ilike(search_key), Orders.customer_name.ilike(search_key),
                            Shipments.awb.ilike(search_key)))

            if type == 'new':
                orders_qs = orders_qs.filter(Orders.status == 'NEW')
            elif type == 'ready_to_ship':
                orders_qs = orders_qs.filter(Orders.status.in_(['READY TO SHIP', 'PICKUP REQUESTED']))
            elif type == 'shipped':
                orders_qs = orders_qs.filter(not_(Orders.status.in_(["NEW", "READY TO SHIP", "PICKUP REQUESTED","NOT PICKED","CANCELED"])))
            elif type == "return":
                orders_qs = orders_qs.filter(or_(Orders.status_type == 'RT',
                                                 and_(Orders.status_type == 'DL', Orders.status == "RTO")))
            elif type == 'all':
                pass
            else:
                return {"success": False, "msg": "Invalid URL"}, 404

            if filters:
                if 'status' in filters:
                    orders_qs = orders_qs.filter(Orders.status.in_(filters['status']))
                if 'courier' in filters:
                    orders_qs = orders_qs.join(MasterCouriers, MasterCouriers.id==Shipments.courier_id)\
                        .filter(MasterCouriers.courier_name.in_(filters['courier']))
                if 'client' in filters and auth_data['user_group'] == 'super-admin':
                    orders_qs = orders_qs.filter(Orders.client_prefix.in_(filters['client']))

                if 'pickup_point' in filters:
                    orders_qs = orders_qs.filter(PickupPoints.warehouse_prefix.in_(filters['pickup_point']))

                if 'order_date' in filters:
                    filter_date_start = datetime.datetime.strptime(filters['order_date'][0], "%Y-%m-%dT%H:%M:%S.%fZ")
                    filter_date_end = datetime.datetime.strptime(filters['order_date'][1], "%Y-%m-%dT%H:%M:%S.%fZ")
                    orders_qs = orders_qs.filter(Orders.order_date >= filter_date_start).filter(Orders.order_date <= filter_date_end)

            orders_qs_data = orders_qs.limit(per_page).offset((page-1)*per_page).all()

            if download_flag:
                si = io.StringIO()
                cw = csv.writer(si)
                cw.writerow(ORDERS_DOWNLOAD_HEADERS)
                for order in orders_qs_data:
                    try:
                        new_row = list()
                        new_row.append(str(order.channel_order_id))
                        new_row.append(str(order.customer_name))
                        new_row.append(str(order.customer_email))
                        new_row.append(str(order.customer_phone))
                        new_row.append(order.order_date.strftime("%Y-%m-%d") if order.order_date else "N/A")
                        new_row.append(str(order.shipments[0].courier.courier_name) if order.shipments and order.shipments[0].courier else "N/A")
                        new_row.append(str(order.shipments[0].weight) if order.shipments else "N/A")
                        new_row.append(str(order.shipments[0].awb) if order.shipments else "N/A")
                        new_row.append(order.shipments[0].edd.strftime("%Y-%m-%d") if order.shipments and order.shipments[0].edd else "N/A")
                        new_row.append(str(order.status))
                        new_row.append(str(order.delivery_address.address_one))
                        new_row.append(str(order.delivery_address.address_two))
                        new_row.append(str(order.delivery_address.city))
                        new_row.append(str(order.delivery_address.state))
                        new_row.append(str(order.delivery_address.country))
                        new_row.append(str(order.delivery_address.pincode))
                        pickup_point = order.pickup_data.pickup.warehouse_prefix if order.pickup_data else ""
                        new_row.append(pickup_point)
                        prod_list = list()
                        prod_quan = list()
                        for prod in order.products:
                            prod_list.append(str(prod.product.name))
                            prod_quan.append(prod.quantity)
                        new_row.append(prod_list)
                        new_row.append(prod_quan)
                        cw.writerow(new_row)
                    except Exception as e:
                        pass

                output = make_response(si.getvalue())
                filename = client_prefix+"_EXPORT.csv"
                output.headers["Content-Disposition"] = "attachment; filename="+filename
                output.headers["Content-type"] = "text/csv"
                return output

            response_data = list()
            for order in orders_qs_data:
                resp_obj=dict()
                resp_obj['order_id'] = order.channel_order_id
                resp_obj['customer_details'] = {"name":order.customer_name,
                                                "email":order.customer_email,
                                                "phone":order.customer_phone}
                resp_obj['order_date'] = order.order_date.strftime("%d %b %Y, %I:%M %p")
                if order.payments:
                    resp_obj['payment'] = {"mode": order.payments[0].payment_mode,
                                           "amount": order.payments[0].amount}

                else:
                    resp_obj['payment'] = {"mode": None,
                                           "amount": None}
                resp_obj['product_details'] = list()
                for prod in order.products:
                    resp_obj['product_details'].append(
                        {"name": prod.product.name,
                         "sku": prod.product.sku,
                         "quantity": prod.quantity}
                    )

                resp_obj['shipping_details'] = dict()
                resp_obj['dimensions'] = None
                resp_obj['weight'] = None
                resp_obj['volumetric'] = None

                if not order.exotel_data:
                    pass
                elif order.exotel_data[0].cod_verified == None:
                    resp_obj['cod_verification'] = "Order not confirmed yet"
                elif order.exotel_data[0].cod_verified == False:
                    resp_obj['cod_verification'] = "Customer cancelled (Verified via %s)" % str(
                        order.exotel_data[0].verified_via)
                else:
                    resp_obj['cod_verification'] = "Customer confirmed (Verified via %s)" % str(
                        order.exotel_data[0].verified_via)

                if order.shipments and order.shipments[0].courier:
                    resp_obj['shipping_details'] = {"courier": order.shipments[0].courier.courier_name,
                                                    "awb":order.shipments[0].awb}
                    resp_obj['dimensions'] = order.shipments[0].dimensions
                    resp_obj['weight'] = order.shipments[0].weight
                    resp_obj['volumetric'] = order.shipments[0].volumetric_weight
                    edd = order.shipments[0].edd
                    if edd:
                        edd = edd.strftime('%-d %b')
                    resp_obj['edd'] = edd
                if order.shipments and auth_data['user_group'] == 'super-admin':
                    resp_obj['remark'] = order.shipments[0].remark
                if type == "shipped":
                    resp_obj['status_detail'] = order.status_detail

                resp_obj['status'] = order.status
                response_data.append(resp_obj)

            response['data'] = response_data
            total_count = orders_qs.count()

            total_pages = math.ceil(total_count/per_page)
            response['meta']['pagination'] = {'total': total_count,
                                              'per_page':per_page,
                                              'current_page': page,
                                              'total_pages':total_pages}

            return response, 200


api.add_resource(OrderList, '/orders/<type>')


@core_blueprint.route('/dashboard', methods=['GET'])
@authenticate_restful
def get_dashboard(resp):
    response = dict()
    auth_data = resp.get('data')
    client_prefix = auth_data.get('client_prefix')
    qs_data = db.session.query(func.date_trunc('day', Orders.order_date).label('date'), func.count(Orders.id), func.sum(OrdersPayments.amount))\
        .join(OrdersPayments, Orders.id==OrdersPayments.order_id)\
        .filter(Orders.order_date >= datetime.datetime.today()- datetime.timedelta(days=30))
    if auth_data['user_group'] != 'super-admin':
        qs_data = qs_data.filter(Orders.client_prefix == client_prefix)
    qs_data = qs_data.group_by('date').order_by('date').all()

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
        status_qs = status_qs.filter(not_(Orders.status.in_(["NEW","READY TO SHIP","PICKUP REQUESTED","NOT PICKED"])))
        courier_qs = courier_qs.filter(not_(Orders.status.in_(["NEW","READY TO SHIP","PICKUP REQUESTED","NOT PICKED"])))
        pickup_point_qs = pickup_point_qs.filter(not_(Orders.status.in_(["NEW","READY TO SHIP","PICKUP REQUESTED","NOT PICKED"])))
        if client_qs:
            client_qs = client_qs.filter(not_(Orders.status.in_(["NEW", "READY TO SHIP", "PICKUP REQUESTED", "NOT PICKED"])))
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
        status_qs = status_qs.filter(Orders.status == "READY TO SHIP")
        courier_qs = courier_qs.filter(Orders.status == "READY TO SHIP")
        pickup_point_qs = pickup_point_qs.filter(Orders.status == "READY TO SHIP")
        if client_qs:
            client_qs = client_qs.filter(Orders.status == "READY TO SHIP")
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
                pickup_data = db.session.query(ClientPickups).filter(ClientPickups.client_prefix==auth_data.get('client_prefix')).first()

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
                    if prod_obj:
                        op_association = OPAssociation(order=new_order, product=prod_obj, quantity=prod['quantity'])
                        new_order.products.append(op_association)

                    prod_quan_obj = db.session.query(ProductQuantity).join(Products, ProductQuantity.product_id == Products.id).filter(
                        Products.sku == prod['sku']).first()
                    if prod_quan_obj:
                        prod_quan_obj.available_quantity = prod_quan_obj.available_quantity - int(prod['quantity'])
                        prod_quan_obj.inline_quantity = prod_quan_obj.inline_quantity + int(prod['quantity'])

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
        if not auth_data:
            return {"success": False, "msg": "Auth Failed"}, 404

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
                pickup_data = db.session.query(ClientPickups).filter(
                    ClientPickups.client_prefix == auth_data.get('client_prefix')).first()

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
                    prod_obj = db.session.query(Products).filter(Products.sku == sku_str).first()
                    if prod_obj:
                        op_association = OPAssociation(order=new_order, product=prod_obj, quantity=int(sku_quantity[idx]))
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
    orders_qs = db.session.query(Orders).filter(Orders.channel_order_id.in_(order_ids), Orders.delivery_address!=None,
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
                offset = 3.863
                try:
                    fill_shiplabel_data(c, order, offset)
                except Exception:
                    pass
                c.setFillColorRGB(1, 1, 1)
                c.rect(6.730 * inch, -1.0 * inch, 10 * inch, 10 * inch, fill=1)
                c.rect(-1.0 * inch, -1.0 * inch, 3.857 * inch, 10 * inch, fill=1)
                if idx != len(orders_qs) - 1:
                    c.showPage()
                    create_shiplabel_blank_page(c)
            else:
                offset_dict = {0:0.0, 1:3.863, 2:7.726}
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
            c.rect(2.867 * inch, -1.0 * inch, 10 * inch, 10*inch, fill=1)
        if idx%3==2:
            c.rect(6.730 * inch, -1.0 * inch, 10 * inch, 10*inch, fill=1)

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
    manifest_qs = db.session.query(Manifests).join(ClientPickups, Manifests.pickup_id==ClientPickups.pickup_id)
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

            order = db.session.query(Orders).filter(Orders.channel_order_id == str(order_id))
            if auth_data['user_group'] != 'super-admin':
                order = order.filter(Orders.client_prefix==auth_data['client_prefix'])

            order = order.first()

            if order:
                resp_obj = dict()
                resp_obj['order_id'] = order.channel_order_id
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
                         "sku": prod.product.sku,
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

            if auth_data['user_group'] != 'super-admin':
                return {"success": False, "msg": "User not admin"}, 404

            order = db.session.query(Orders).filter(Orders.channel_order_id==order_id).first()

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

            db.session.commit()
            return {'status': 'success', 'msg': "successfully updated"}, 200

        except Exception as e:
            return {'status': 'Failed'}, 200


api.add_resource(OrderDetails, '/orders/v1/order/<order_id>')


@core_blueprint.route('/orders/v1/ship/<order_id>', methods=['GET'])
@authenticate_restful
def ship_order(resp, order_id):
    try:
        auth_data = resp.get('data')
        if not auth_data:
            return jsonify({"success": False, "msg": "Auth Failed"}), 404
        if auth_data['user_group'] != 'super-admin':
            return {"success": False, "msg": "User not admin"}, 404

        order = db.session.query(Orders).filter(Orders.channel_order_id==order_id).first()

        db.session.query(Shipments).filter(Shipments.order_id==order.id).delete()

        courier = db.session.query(ClientCouriers).filter(ClientCouriers.client_prefix==order.client_prefix).first()

        if not courier:
            return jsonify({"success": False, "msg": "Courier not assigned for client"}), 400

        headers = {"Authorization": "Token " + courier.courier.api_key,
                   "Content-Type": "application/json"}

        check_url = "https://track.delhivery.com/c/api/pin-codes/json/?filter_codes=%s" % str(order.delivery_address.pincode)
        req = requests.get(check_url, headers=headers)
        if not req.json()['delivery_codes']:
            return jsonify({"success": False, "msg": "Pincode not serviceable"}), 400

        order_weight = 0.0
        product_quan = 0
        order_dimensions = None
        package_string = ""
        for prod in order.products:
            order_weight += prod.quantity*prod.product.weight
            product_quan += prod.quantity
            if not order_dimensions:
                order_dimensions = prod.product.dimensions
                order_dimensions['length'] = order_dimensions['length']*prod.quantity
            else:
                order_dimensions['length'] += prod.product.dimensions['length']*prod.quantity

            package_string += prod.product.name + " (" + str(prod.quantity) + ") + "

        package_string += "Shipping"

        order_volumetric = None
        if order_dimensions:
            order_volumetric = (order_dimensions['length']*order_dimensions['breadth']*order_dimensions['height'])/5000

        shipment_data = dict()
        shipment_data['city'] = order.delivery_address.city
        shipment_data['weight'] = order_weight
        shipment_data['add'] = order.delivery_address.address_one
        if order.delivery_address.address_two:
            shipment_data['add'] += '\n' + order.delivery_address.address_two
        shipment_data['phone'] = order.delivery_address.phone
        shipment_data['payment_mode'] = order.payments[0].payment_mode
        shipment_data['name'] = order.delivery_address.first_name
        if order.delivery_address.last_name:
            shipment_data['name'] += " " + order.delivery_address.last_name
        shipment_data['product_quantity'] = product_quan
        shipment_data['pin'] = order.delivery_address.pincode
        shipment_data['state'] = order.delivery_address.state
        shipment_data['order_date'] = str(order.order_date)
        shipment_data['total_amount'] = order.payments[0].amount
        shipment_data['country'] = order.delivery_address.country
        shipment_data['client'] = courier.courier.api_password
        shipment_data['order'] = order_id
        shipment_data['products_desc'] = package_string
        if order.payments[0].payment_mode.lower() == "cod":
            shipment_data['cod_amount'] = order.payments[0].amount

        pickup_point = order.pickup_data
        pick_add = pickup_point.pickup.address
        if pickup_point.pickup.address_two:
            pick_add += "\n" + pickup_point.pickup.address_two
        pickup_location = {"city": pickup_point.pickup.city,
                           "name": pickup_point.pickup.warehouse_prefix,
                           "pin": pickup_point.pickup.pincode,
                           "country": pickup_point.pickup.country,
                           "phone": pickup_point.pickup.phone,
                           "add": pick_add,
                           }

        delivery_shipments_body = {"data": json.dumps({"shipments": [shipment_data], "pickup_location": pickup_location}),
                                   "format": "json"}
        delhivery_url = courier.courier.api_url + "api/cmu/create.json"
        req = requests.post(delhivery_url, headers=headers, data=delivery_shipments_body)
        return_data = req.json()['packages']

        if not return_data:
            return jsonify({"success": False, "msg": "Some error occurred"}), 400

        package = return_data[0]
        tracking_link = None
        fulfillment_id = None
        try:
            exotel_sms_data = {
                'From': 'LM-WAREIQ'
            }
            customer_phone = order.customer_phone.replace(" ", "")
            customer_phone = "0" + customer_phone[-10:]

            sms_to_key = "Messages[0][To]"
            sms_body_key = "Messages[0][Body]"

            exotel_sms_data[sms_to_key] = customer_phone
            exotel_sms_data[
                sms_body_key] = "Dear Customer, thank you for ordering from %s. Your order will be shipped by Delhivery with AWB number %s. " \
                                "You can track your order using this AWB number." % (
                                order.client_prefix, str(package['waybill']))

            lad = requests.post(
                'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend',
                data=exotel_sms_data)
        except Exception as e:
            pass

        try:
            create_fulfillment_url = "https://%s:%s@%s/admin/api/2019-10/orders/%s/fulfillments.json" % (
                order.client_channel.api_key, order.client_channel.api_password,
                order.client_channel.shop_url, order.order_id_channel_unique)
            tracking_link = "https://www.delhivery.com/track/package/%s" % str(package['waybill'])
            ful_header = {'Content-Type': 'application/json'}
            fulfil_data = {
                "fulfillment": {
                    "tracking_number": str(package['waybill']),
                    "tracking_urls": [
                        "https://www.delhivery.com/track/package/"+str(package['waybill'])
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
            except Exception as e:
                pass
        except Exception as e:
            pass

        shipment = Shipments(status=package['status'],
                             weight=order_weight,
                             volumetric_weight=order_volumetric,
                             dimensions=order_dimensions,
                             order=order,
                             pickup=pickup_point.pickup,
                             return_point=pickup_point.return_point,
                             channel_fulfillment_id=fulfillment_id,
                             tracking_link=tracking_link,
                             )

        if not package['waybill']:
            shipment.awb = ""
            shipment.remark = package['remarks'][0]
            success = False
            msg = package['remarks'][0]
            status_code = 400
        else:
            shipment.awb = package['waybill']
            shipment.courier = courier.courier
            shipment.routing_code = package['sort_code']
            order.status = "READY TO SHIP"
            success = True
            msg = "successfully shipped"
            status_code = 200

        db.session.add(shipment)
        db.session.commit()
        return jsonify({"success": success, "msg":msg}), status_code
    except Exception as e:
        return jsonify({"success": False, "msg": str(e.args[0])}), 404


@core_blueprint.route('/orders/v1/track/<awb>', methods=['GET'])
@cross_origin()
def track_order(awb):
    try:
        shipment = db.session.query(Shipments).filter(Shipments.awb==awb).first()
        if not shipment:
            return jsonify({"success": False, "msg": "tracking id not found"}), 400

        details = request.args.get('details')
        if details:
            if shipment.courier_id in (1,2,8): #Delhivery details of status
                try:
                    return_details = list()
                    delhivery_url = "https://track.delhivery.com/api/status/packages/json/?waybill=%s&token=%s" \
                                    % (str(awb), shipment.courier.api_key)
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
                        return_details_obj['time'] = status_time.strftime("%d %b %Y, %I:%M %p")
                        return_details.append(return_details_obj)
                    return jsonify({"success": True, "data": return_details}), 200
                except Exception as e:
                    return jsonify({"success": False, "msg": "Details not available"}), 400
            else:
                return jsonify({"success": False, "msg": "Data not available"}), 400
        order_statuses = db.session.query(OrderStatus).filter(OrderStatus.shipment==shipment)\
            .order_by(OrderStatus.status_time).all()
        if not order_statuses:
            return jsonify({"success": False, "msg": "tracking not available for this id"}), 400

        response = dict()
        last_status = order_statuses[-1].status
        response['tracking_id'] = awb
        response['status'] = last_status
        response['logo_url'] = "https://www.google.com/url?sa=i&url=https%3A%2F%2Fwww.linkedin.com%2Fcompany%2Fwareiq&psig=AOvVaw0YvqAql_oPH2DcoCxxEGGc&ust=1582282802313000&source=images&cd=vfe&ved=0CAIQjRxqFwoTCIDO7vb83-cCFQAAAAAdAAAAABAD"
        response['remark'] = order_statuses[-1].status_text
        response['order_id'] = order_statuses[-1].order.channel_order_id
        response['placed_on'] = order_statuses[-1].order.order_date.strftime("%d %b %Y, %I:%M %p")
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
            addition_statuses =  ["Picked", "In Transit", "Out for delivery", "Delivered"]
        elif last_status == "Picked":
            addition_statuses =  ["In Transit", "Out for delivery", "Delivered"]
        elif last_status == "In Transit":
            addition_statuses =  ["Out for delivery", "Delivered"]
        elif last_status == "Out for delivery":
            addition_statuses =  ["Delivered"]

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

    def get(self):
        try:
            order_id = request.args.get('CustomField')
            if order_id:
                order_id = int(order_id)
                order = db.session.query(Orders).filter(Orders.id == order_id).first()
                gather_prompt_text = "Hello %s, You recently placed an order from %s with order ID %s." \
                                     " Press 1 to confirm your order or 0 to cancel." % (order.customer_name,
                                                                                         order.client_prefix.lower(),
                                                                                         order.channel_order_id)

                repeat_prompt_text = "It seems that you have not provided any input, please try again. Order from %s, " \
                                     "Order ID %s. Press 1 to confirm your order or 0 to cancel." % (
                                     order.client_prefix.lower(),
                                     order.channel_order_id)
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


api.add_resource(CodVerificationGather, '/core/v1/cod_verification_gather')


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
                cod_ver.ndr_verified = cod_verified
            else:
                pass

            if call_sid:
                verified_via = 'call'
            else:
                verified_via = 'text'
                cod_ver.click_browser = request.user_agent.browser
                cod_ver.click_platform = request.user_agent.platform
                cod_ver.click_string = request.user_agent.string
                cod_ver.click_version = request.user_agent.version

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

    method_decorators = {'post': [authenticate_restful]}

    def post(self, resp):
        try:
            auth_data = resp.get('data')
            data = json.loads(request.data)
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404

            del_pincode = data.get("pincode")
            sku_list = data.get("sku_list")
            if not del_pincode:
                return {"success": False, "msg": "Pincode not provided"}, 404
            if not sku_list:
                return {"success": False, "msg": "SKUs not provided"}, 404

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
                return {"success": False, "msg": ""}, 404

            prod_wh_tuple = cur.fetchall()
            wh_dict = dict()
            courier_id = 2
            courier_id_weight = 0.0
            for prod_wh in prod_wh_tuple:
                if prod_wh[5] > courier_id_weight:
                    courier_id = prod_wh[4]
                    courier_id_weight = prod_wh[5]
                if sku_dict[prod_wh[2]] < prod_wh[3]:
                    if prod_wh[0] not in wh_dict:
                        wh_dict[prod_wh[0]] = {"pincode": prod_wh[6], "count": 1}
                    else:
                        wh_dict[prod_wh[0]]['count'] += 1

            warehouse_pincode_str = ""
            for key, value in wh_dict.items():
                if value['count'] == no_sku:
                    warehouse_pincode_str += "('" + key + "','" + str(value['pincode']) + "'),"

            warehouse_pincode_str = warehouse_pincode_str.rstrip(',')
            if not warehouse_pincode_str:
                return {"success": False, "msg": "One or more SKUs not serviceable"}, 400

            if courier_id in (8, 11, 12):
                courier_id = 1

            try:
                cur_2.execute(fetch_warehouse_to_pick_from.replace('__WAREHOUSE_PINCODES__', warehouse_pincode_str).replace(
                    '__COURIER_ID__', str(courier_id)).replace('__DELIVERY_PINCODE__', str(del_pincode)))
            except Exception:
                conn_2.rollback()
                return {"success": False, "msg": ""}, 404

            final_wh = cur_2.fetchone()

            if not final_wh or final_wh[1] is None:
                return {"success": False, "msg": "Not serviceable"}, 404

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

            return_data = {"warehouse": final_wh[0],
                           "delivery_date": delivered_by.strftime('%d-%m-%Y'),
                           "cod_available": False,
                           "order_before": order_before.strftime('%d-%m-%Y %H:%M:%S'),
                           "delivery_zone": delivery_zone,
                           "label_url": None,
                           "sku_wise": sku_wise_list}

            return {"success": True, "data": return_data}, 200

        except Exception as e:
            return {"success": False, "msg": ""}, 404


api.add_resource(PincodeServiceabilty, '/orders/v1/serviceability')


@core_blueprint.route('/core/dev', methods=['POST'])
def ping_dev():
    return 0
    import requests, json
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

    shopify_url = "https://e35b2c3b1924d686e817b267b5136fe0:a5e60ec3e34451e215ae92f0877dddd0@daprstore.myshopify.com/admin/api/2019-10/orders.json?limit=250"
    data = requests.get(shopify_url).json()
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
