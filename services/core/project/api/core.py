# services/core/project/api/core.py

import requests, json, math, datetime, pytz
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
from project.api.models import Products, ProductQuantity, \
    Orders, OrdersPayments, PickupPoints, MasterChannels, ClientPickups, CodVerification,\
    MasterCouriers, Shipments, OPAssociation, ShippingAddress, Manifests, ClientCouriers, OrderStatus
from project.api.utils import authenticate_restful, get_products_sort_func, \
    get_orders_sort_func, create_shiplabel_blank_page, fill_shiplabel_data

core_blueprint = Blueprint('core', __name__)
api = Api(core_blueprint)

session = boto3.Session(
    aws_access_key_id='AKIAWRT2R3KC3YZUBFXY',
    aws_secret_access_key='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',
)

ORDERS_DOWNLOAD_HEADERS = ["Order ID", "Customer Name", "Customer Email", "Customer Phone", "Order_Date",
                            "Courier", "Weight", "awb", "Delivery Date", "Status"]

class ProductList(Resource):

    method_decorators = {'post': [authenticate_restful]}

    def post(self, resp, type):
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
            products_qs = db.session.query(Products, ProductQuantity)\
                .filter(Products.id==ProductQuantity.product_id).order_by(sort_func())\
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

            products_qs_data = products_qs.limit(per_page).offset((page-1)*per_page).all()
            response_dict_sku = dict()
            for product in products_qs_data:
                resp_obj=dict()
                if product[0].sku not in response_dict_sku:
                    resp_obj['channel_logo'] = product[0].channel.logo_url
                    resp_obj['product_name'] = product[0].name
                    resp_obj['product_image'] = product[0].product_image
                    resp_obj['price'] = product[0].price
                    resp_obj['master_sku'] = product[0].sku
                    resp_obj['channel_sku'] = product[0].sku
                    resp_obj['total_quantity'] = product[1].approved_quantity
                    resp_obj['available_quantity'] = product[1].available_quantity
                    resp_obj['dimensions'] = product[0].dimensions
                    resp_obj['weight'] = product[0].weight
                    if type == 'inactive':
                        resp_obj['inactive_reason'] = product[0].inactive_reason
                    response_dict_sku[product[0].sku] = resp_obj
                else:
                    response_dict_sku[product[0].sku]['total_quantity'] += product[1].approved_quantity
                    response_dict_sku[product[0].sku]['available_quantity'] += product[1].available_quantity

            response_data = list(response_dict_sku.values())

            response['data'] = sorted(response_data, key = lambda i: i['available_quantity'])
            total_count = products_qs.count()

            total_pages = math.ceil(total_count/per_page)
            response['meta']['pagination'] = {'total': total_count,
                                              'per_page':per_page,
                                              'current_page': page,
                                              'total_pages':total_pages}

            return response, 200


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
            orders_qs = db.session.query(Orders).join(Shipments, Orders.id==Shipments.order_id, isouter=True)
            if auth_data['user_group'] != 'super-admin':
                orders_qs = orders_qs.filter(Orders.client_prefix==client_prefix)
            orders_qs = orders_qs.order_by(sort_func())\
                .filter(or_(Orders.channel_order_id.ilike(search_key), Orders.customer_name.ilike(search_key),
                            Shipments.awb.ilike(search_key)))

            if type == 'new':
                orders_qs = orders_qs.filter(Orders.status == 'NEW')
            elif type == 'ready_to_ship':
                orders_qs = orders_qs.filter(Orders.status == 'READY TO SHIP')
            elif type == 'shipped':
                orders_qs = orders_qs.filter(not_(Orders.status.in_(["NEW", "READY TO SHIP", "PICKUP REQUESTED","NOT PICKED"])))
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
                    cw.writerow(new_row)

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

    response['today'] = {"orders": qs_data[-1][1] if qs_data else 0, "revenue": qs_data[-1][2] if qs_data else 0}
    response['yesterday'] = {"orders": qs_data[-2][1] if len(qs_data)>1 else 0, "revenue": qs_data[-2][2] if len(qs_data)>1 else 0}

    response['graph_data'] = list()

    for dat_obj in qs_data:
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
    if auth_data['user_group'] == 'super-admin':
        client_qs = db.session.query(Orders.client_prefix, func.count(Orders.client_prefix))

    if auth_data['user_group'] != 'super-admin':
        status_qs=status_qs.filter(Orders.client_prefix == client_prefix)
        courier_qs = courier_qs.filter(Orders.client_prefix == client_prefix)
    if current_tab=="shipped":
        status_qs = status_qs.filter(not_(Orders.status.in_(["NEW","READY TO SHIP","PICKUP REQUESTED","NOT PICKED"])))
        courier_qs = courier_qs.filter(not_(Orders.status.in_(["NEW","READY TO SHIP","PICKUP REQUESTED","NOT PICKED"])))
        if client_qs:
            client_qs = client_qs.filter(not_(Orders.status.in_(["NEW", "READY TO SHIP", "PICKUP REQUESTED", "NOT PICKED"])))
    if current_tab=="return":
        status_qs = status_qs.filter(or_(Orders.status_type == 'RT', and_(Orders.status_type == 'DL', Orders.status == "RTO")))
        courier_qs = courier_qs.filter(or_(Orders.status_type == 'RT', and_(Orders.status_type == 'DL', Orders.status == "RTO")))
        if client_qs:
            client_qs = client_qs.filter(or_(Orders.status_type == 'RT', and_(Orders.status_type == 'DL', Orders.status == "RTO")))
    if current_tab=="new":
        status_qs = status_qs.filter(Orders.status=="NEW")
        courier_qs = courier_qs.filter(Orders.status=="NEW")
        if client_qs:
            client_qs = client_qs.filter(Orders.status=="NEW")
    if current_tab=="ready_to_ship":
        status_qs = status_qs.filter(Orders.status == "READY TO SHIP")
        courier_qs = courier_qs.filter(Orders.status == "READY TO SHIP")
        if client_qs:
            client_qs = client_qs.filter(Orders.status == "READY TO SHIP")
    status_qs = status_qs.order_by(Orders.status).all()
    response['filters']['status'] = [{x[0]:x[1]} for x in status_qs]
    courier_qs = courier_qs.order_by(MasterCouriers.courier_name).all()
    response['filters']['courier'] = [{x[0]:x[1]} for x in courier_qs]
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

            new_order = Orders(channel_order_id=str(data.get('order_id')),
                           order_date=datetime.datetime.now(tz=pytz.timezone('Asia/Calcutta')),
                           customer_name=data.get('full_name'),
                           customer_email=data.get('customer_email'),
                           customer_phone=data.get('customer_phone'),
                           delivery_address=delivery_address,
                           status="NEW",
                           client_prefix=auth_data.get('client_prefix'),
                           )

            if data.get('products'):
                for prod in data.get('products'):
                    prod_obj = db.session.query(Products).filter(Products.sku == prod['sku']).first()
                    if prod_obj:
                        op_association = OPAssociation(order=new_order, product=prod_obj, quantity=prod['quantity'])
                        new_order.products.append(op_association)

            payment = OrdersPayments(
                payment_mode=data['payment_method'],
                subtotal=float(data['total']),
                amount=float(data['total'])+float(data['shipping_charges']),
                shipping_charges=float(data['shipping_charges']),
                currency='INR',
                order=new_order
            )

            db.session.add(new_order)
            db.session.commit()
            return {'status': 'success', 'msg': "successfully added", "order_id": new_order.channel_order_id}, 200

        except Exception as e:
            return {"status":"Failed", "msg":""}, 400

    def get(self, resp):
        auth_data = resp.get('data')
        if not auth_data:
            return {"success": False, "msg": "Auth Failed"}, 404

        payment_modes = ['prepaid','COD']
        warehouses = [r.name for r in db.session.query(PickupPoints.name)
            .filter(PickupPoints.warehouse_prefix==auth_data.get('warehouse_prefix'))
            .order_by(PickupPoints.name)]

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

            new_order = Orders(channel_order_id=str(row_data.order_id),
                               order_date=datetime.datetime.now(tz=pytz.timezone('Asia/Calcutta')),
                               customer_name=str(row_data.customer_name),
                               customer_email=str(row_data.customer_email),
                               customer_phone=str(row_data.customer_phone),
                               delivery_address=delivery_address,
                               status="NEW",
                               client_prefix=auth_data.get('client_prefix'),
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
                                                Orders.shipments!=None).order_by(Orders.id).all()
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
            if auth_data['client_prefix'] == "KYORIGIN":
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
    if auth_data['client_prefix'] != "KYORIGIN":
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
    manifest_qs = db.session.query(Manifests)
    if auth_data['user_group'] != 'super-admin':
        manifest_qs= manifest_qs.filter(Manifests.warehouse_prefix == auth_data['warehouse_prefix'])
    manifest_qs = manifest_qs.order_by(Manifests.pickup_date.desc())

    manifest_qs_data = manifest_qs.limit(per_page).offset((page - 1) * per_page).all()

    for manifest in manifest_qs_data:
        manifest_dict = dict()
        manifest_dict['manifest_id'] = manifest.manifest_id
        manifest_dict['courier'] = manifest.courier.courier_name
        manifest_dict['pickup_point'] = manifest.pickup.name
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

        pickup_point = db.session.query(ClientPickups).filter(ClientPickups.client_prefix==order.client_prefix).first()
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
            if shipment.courier_id in (1,2): #Delhivery details of status
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

"""
@core_blueprint.route('/core/v1/cod_verification_gather', methods=['GET'])
def cod_verification_gather():
    try:
        order_id = request.args.get('CustomField')
        if order_id:
            order_id = int(order_id)
            order = db.session.query(Orders).filter(Orders.id==order_id).first()
            gather_prompt_text = "Hello %s, You recently placed an order from %s with order ID %s." \
                                 " Press 1 to confirm your order or 0 to cancel." %(order.customer_name,
                                                                                   order.client_prefix.lower(),
                                                                                   order.channel_order_id)

            repeat_prompt_text = "It seems that you have not provided any input, please try again. Order from %s, " \
                                 "Order ID %s. Press 1 to confirm your order or 0 to cancel."%(order.client_prefix.lower(),
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
            return jsonify(response), 200
        else:
            return jsonify({
                        "gather_prompt": {
                            "text": "press 1 or 0",
                          },
                        "max_input_digits": 1,
                        "repeat_menu": 2,
                        "repeat_gather_prompt": {
                            "text": "press 1 or 0"
                          }
                        }), 200
    except Exception as e:
        return jsonify({
            "gather_prompt": {
                "text": "press 1 or 0",
            },
            "max_input_digits": 1,
            "repeat_menu": 2,
            "repeat_gather_prompt": {
                "text": "press 1 or 0"
            }
        }), 200
"""


@core_blueprint.route('/core/v1/cod_verification_passthru', methods=['GET'])
def cod_verification_passthru():
    try:
        order_id = request.args.get('CustomField')
        digits = request.args.get('digits')
        recording_url = request.args.get('RecordingUrl')
        call_sid = request.args.get('CallSid')
        if not digits:
            return jsonify({"success": False, "msg": "No Input"}), 400
        digits = digits.replace('"', '')
        if order_id:
            order_id = int(order_id)
            order = db.session.query(Orders).filter(Orders.id==order_id).first()
            cod_verified = None
            if digits=="1":
                cod_verified = True
            elif digits=="0":
                cod_verified = False
            cod_ver = CodVerification(order=order,
                                      call_sid=call_sid,
                                      recording_url=recording_url,
                                      cod_verified=cod_verified)

            db.session.add(cod_ver)
            db.session.commit()

            return jsonify({"success": True}), 200
        else:
            return jsonify({"success": False, "msg": "No Order"}), 400
    except Exception as e:
        return jsonify({"success": False, "msg": str(e.args[0])}), 404



@core_blueprint.route('/core/dev', methods=['GET'])
def ping_dev():
    return 0

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
    ful_header = {'Content-Type': 'application/json'}
    url = "http://114.143.206.69:803/StandardForwardStagingService.svc/GetBulkShipmentStatus"
    post_body = {
                  "fulfillment": {
                    "location_id": 21056061499,
                    "tracking_number": "3991610014066",
                    "tracking_company": "Delhivery",
                    "tracking_urls": ["https://www.delhivery.com/track/package/3991610014066"],
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

    awbs = [('3991610006801', '+917506690669'),
             ('3991610004336','	+918787482176      '),
             ('3991610008293','	+919820230796      '),
             ('3991610006731','	+918138092054      '),
             ('3991610007700','	98209 30321        '),
             ('3991610000943','	+917005627591      '),
             ('3991610001175','	+919717176667      '),
             ('3991610001374','	090960 71512       '),
             ('3991610001481','	+919741674496      '),
             ('3991610008050','	+1918595044988     '),
             ('3991610008595','	+919526986805      '),
             ('3991610004782','	+91 89717 33536    '),
             ('3991610007943','	86553 85670        '),
             ('3991610002321','	98100 45281        '),
             ('3991610007711','	98209 30321        '),
             ('3991610007722','	+917012049592      '),
             ('3991610002796','	+917977617587      '),
             ('3991610008470','	+44919328994129    '),
             ('3991610008385','	+919618260073      '),
             ('3991610003172','	+919930102345      '),
             ('3991610003301','	+917021962754      '),
             ('3991610000755','	+918001067507      '),
             ('3991610003835','	+916909446693      '),
             ('3991610008621','	+918416008617      '),
             ('3991610008632','	+918259050206      '),
             ('3991610008013','	+916363505914      '),
             ('3991610008643','	98629 76373        '),
             ('3991610007302','	81210 72107        '),
             ('3991610007324','	+919449431826      '),
             ('3991610008326','	+916238686207      '),
             ('3991610005681','	70056 74243        '),
             ('3991610005703','	85002 82645        '),
             ('3991610008260','	+91 99141 67766    '),
             ('3991610008315','	+919958487091      '),
             ('3991610008330','	+916238686207      '),
             ('3991610007615','	88992 33470        '),
             ('3991610007313','	+919829056309      '),
             ('3991610007254','	82510 81952        '),
             ('3991610006871','	+919394755677      '),
             ('3991610008304','	+916360510948      '),
             ('3991610007243','	+918828339077      '),
             ('3991610007663','	+919821014568      '),
             ('3991610008072','	+918169956705      '),
             ('3991610007361','	+91 99018 55486    '),
             ('3991610007464','	+919068293393      '),
             ('3991610007696','	+919644720222      '),
             ('3991610008341','	95262 89568        '),
             ('3991610008083','	+919821446978      '),
             ('3991610007685','	98716 61078        '),
             ('3991610006226','	+917000369859      '),
             ('3991610008352','	98301 83388        '),
             ('3991610007534','	98201 17697        '),
             ('3991610008374','	70058 68142        '),
             ('3991610007921','	92165 02343        '),
             ('3991610006436','	+919833016729      '),
             ('3991610007910','	98920 08381        '),
             ('3991610007906','	+919811552594      '),
             ('3991610007851','	+917678129877      '),
             ('3991610007862','	99613 96558        '),
             ('3991610007873','	+1 647-765-9590    '),
             ('3991610007895','	+91 98737 6865     '),
             ('3991610007980','	+917907547041      '),
             ('3991610007991','	+919899001111      '),
             ('3991610008002','	89549 22096        '),
             ('3991610008024','	+919749523194      '),
             ('3991610008035','	62943 69034        '),
             ('3991610008094','	99221 09350        '),
             ('3991610008105','	95662 44655        '),
             ('3991610007733','	+919873245510      '),
             ('3991610008116','	97696 98672        '),
             ('3991610008610','	+919774392766      '),
             ('3991610008396','	9873611567         '),
             ('3991610008120','	773 811 7123       '),
             ('3991610008046','	99203 55018        '),
             ('3991610008131','	94372 26316        '),
             ('3991610007781','	+919871679545      '),
             ('3991610006694','	+918879864512      '),
             ('3991610007766','	09868383418        '),
             ('3991610007770','	89795 58889        '),
             ('3991610008400','	+918800211112      '),
             ('3991610008411','	+918105814941      '),
             ('3991610008363','	+918800809478      '),
             ('3991610007792','	+919945606401      '),
             ('3991610007803','	9560960800         '),
             ('3991610008142','	+918007555572      '),
             ('3991610008422','	63661 06748        '),
             ('3991610008433','	+919901855486      '),
             ('3991610007814','	+917977617587      '),
             ('3991610008153','	+918800809478      '),
             ('3991610008061','	+919833779503      '),
             ('3991610008175','	79922 69795        '),
             ('3991610007825','	96228 40080        '),
             ('3991610008164','	+919871666200      '),
             ('3991610007836','	99304 05052        '),
             ('3991610008455','	91599 22197        '),
             ('3991610008186','	+917005586957      '),
             ('3991610008190','	98846 47422        '),
             ('3991610008444','	+917770008889      '),
             ('3991610007840','	99300 47404        '),
             ('3991610008201','	97248 55955        '),
             ('3991610008606','	+917640937804      '),
             ('3991610008466','	+919350595059      '),
             ('3991610008573','	+917506990571      '),
             ('3991610006086','	+917387856666      '),
             ('3991610008514','	81058 14941        '),
             ('3991610007884','	+917011324658      '),
             ('3991610007954','	87225 70614        '),
             ('3991610003614','	+919526155206      '),
             ('3991610008503','	+919164813738      '),
             ('3991610008223','	+919810188004      '),
             ('3991610007626','	97115 19100        '),
             ('3991610008271','	+918806432161      '),
             ('3991610008282','	+919958366365      '),
             ('3991610008584','	79070 53381        '),
             ('3991610008525','	+919999033337      '),
             ('3991610008245','	+919582523379      '),
             ('3991610008256','	98202 20247        '),
             ('3991610007571','	+919898554996      '),
             ('3991610007545','	99209 49032        '),
             ('3991610007556','	+919930126434      '),
             ('3991610007976','	+918454962678      '),
             ('3991610008234','	+919739753385      '),
             ('3991610008212','	99997 21539]       '),      ]

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
