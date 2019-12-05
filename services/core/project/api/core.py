# services/core/project/api/core.py

import requests, json, math, datetime, pytz
import boto3, os
from sqlalchemy import or_, func
from flask import Blueprint, request, jsonify
from flask_restful import Resource, Api
from reportlab.lib.pagesizes import landscape, A4
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas

from project import db
from project.api.models import Products, ProductQuantity, \
    Orders, OrdersPayments, PickupPoints, MasterChannels, ClientPickups, \
    MasterCouriers, Shipments, OPAssociation, ShippingAddress, Manifests, ClientCouriers
from project.api.utils import authenticate_restful, get_products_sort_func, \
    get_orders_sort_func, create_shiplabel_blank_page, fill_shiplabel_data

core_blueprint = Blueprint('core', __name__)
api = Api(core_blueprint)

session = boto3.Session(
    aws_access_key_id='AKIAWRT2R3KC3YZUBFXY',
    aws_secret_access_key='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',
)


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
        auth_data = resp.get('data')
        if not auth_data:
            return {"success": False, "msg": "Auth Failed"}, 404
        if auth_data.get('user_group') == 'super-admin' or 'client':
            client_prefix = auth_data.get('client_prefix')
            sort_func = get_products_sort_func(Products, ProductQuantity, sort, sort_by)
            products_qs = db.session.query(Products, ProductQuantity)\
                .filter(Products.id==ProductQuantity.product_id).order_by(sort_func())\
                .filter(or_(Products.name.ilike(search_key), Products.sku.ilike(search_key)))
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
            response_data = list()
            for product in products_qs_data:
                resp_obj=dict()
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
                response_data.append(resp_obj)

            response['data'] = response_data
            total_count = products_qs.count()

            total_pages = math.ceil(total_count/per_page)
            response['meta']['pagination'] = {'total': total_count,
                                              'per_page':per_page,
                                              'current_page': page,
                                              'total_pages':total_pages}

            return response, 200


api.add_resource(ProductList, '/products/<type>')


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
        auth_data = resp.get('data')
        if not auth_data:
            return {"success": False, "msg": "Auth Failed"}, 404
        if auth_data.get('user_group') == 'super-admin' or 'client':
            client_prefix = auth_data.get('client_prefix')
            sort_func = get_orders_sort_func(Orders, sort, sort_by)
            orders_qs = db.session.query(Orders)
            if auth_data['user_group'] != 'super-admin':
                orders_qs = orders_qs.filter(Orders.client_prefix==client_prefix)
            orders_qs = orders_qs.order_by(sort_func())\
                .filter(or_(Orders.channel_order_id.ilike(search_key), Orders.customer_name.ilike(search_key)))

            if type == 'new':
                orders_qs = orders_qs.filter(Orders.status == 'NEW')
            elif type == 'ready_to_ship':
                orders_qs = orders_qs.filter(Orders.status == 'READY TO SHIP')
            elif type == 'shipped':
                orders_qs = orders_qs.filter(Orders.status == 'DELIVERED')
            elif type == 'all':
                pass
            else:
                return {"success": False, "msg": "Invalid URL"}, 404

            if filters:
                if 'status' in filters:
                    orders_qs = orders_qs.filter(Orders.status.in_(filters['status']))
                if 'courier' in filters:
                    orders_qs = orders_qs.join(Shipments, Orders.id==Shipments.order_id)\
                        .join(MasterCouriers, MasterCouriers.id==Shipments.courier_id)\
                        .filter(MasterCouriers.courier_name.in_(filters['courier']))
                if 'order_date' in filters:
                    filter_date_start = datetime.datetime.strptime(filters['order_date'][0], "%Y-%m-%dT%H:%M:%S.%fZ")
                    filter_date_end = datetime.datetime.strptime(filters['order_date'][1], "%Y-%m-%dT%H:%M:%S.%fZ")
                    orders_qs = orders_qs.filter(Orders.order_date >= filter_date_start).filter(Orders.order_date <= filter_date_end)

            orders_qs_data = orders_qs.limit(per_page).offset((page-1)*per_page).all()
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
                if order.shipments and auth_data['user_group'] == 'super-admin':
                    resp_obj['remark'] = order.shipments[0].remark

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

    response['today'] = {"orders": qs_data[-1][1], "revenue": qs_data[-1][2]}
    response['yesterday'] = {"orders": qs_data[-2][1], "revenue": qs_data[-2][2]}
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
    client_prefix = auth_data.get('client_prefix')
    status_qs = db.session.query(Orders.status.distinct().label('status'))
    if auth_data['user_group'] != 'super-admin':
        status_qs=status_qs.filter(Orders.client_prefix == client_prefix)
    status_qs = status_qs.order_by(Orders.status).all()
    response['filters']['status'] = [x.status for x in status_qs]
    courier_qs = db.session.query(MasterCouriers.courier_name.distinct().label('courier')) \
        .join(Shipments, MasterCouriers.id == Shipments.courier_id).join(Orders, Orders.id == Shipments.order_id)
    if auth_data['user_group'] != 'super-admin':
        courier_qs = courier_qs.filter(Orders.client_prefix == client_prefix)
    courier_qs = courier_qs.order_by(MasterCouriers.courier_name).all()
    response['filters']['courier'] = [x.courier for x in courier_qs]
    if auth_data['user_group'] == 'super-admin':
        client_qs = db.session.query(Orders.client_prefix.distinct().label('client')).order_by(Orders.client_prefix).all()
        response['filters']['client'] = [x.client for x in client_qs]

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
    for idx, order in enumerate(orders_qs):
        try:
            if auth_data['client_prefix'] == "KYORIGIN":
                offset = 3.863
                fill_shiplabel_data(c, order, offset)
                c.setFillColorRGB(1, 1, 1)
                c.rect(6.730 * inch, -1.0 * inch, 10 * inch, 10 * inch, fill=1)
                c.rect(-1.0 * inch, -1.0 * inch, 3.857 * inch, 10 * inch, fill=1)
                if idx != len(orders_qs) - 1:
                    c.showPage()
                    create_shiplabel_blank_page(c)
            else:
                offset_dict = {0:0.0, 1:3.863, 2:7.726}
                fill_shiplabel_data(c, order, offset_dict[idx%3])
                if idx%3==2 and idx!=(len(orders_qs)-1):
                    c.showPage()
                    create_shiplabel_blank_page(c)
        except Exception as e:
            failed_ids[order.channel_order_id] = str(e.args[0])
            pass
    if auth_data['client_prefix'] != "KYORIGIN":
        c.setFillColorRGB(1, 1, 1)
        if idx%3==0:
            c.rect(2.867 * inch, -1.0 * inch, 10 * inch, 10*inch, fill=1)
        if idx%3==1:
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
        manifest_dict['no_of_orders'] = manifest.no_of_orders
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
                if auth_data['user_group'] == 'super-admin':
                    resp_obj['remark'] = order.shipments[0].remark

                response['data'] = resp_obj
                return response, 200
            else:
                response["status"] = "Failed"
                response["msg"] = "No order with given ID found"
                return response, 404

        except Exception as e:
            return {"status": "Failed", "msg": ""}, 400

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
        for prod in order.products:
            order_weight += prod.quantity*prod.product.weight
            product_quan += prod.quantity
            if not order_dimensions:
                order_dimensions = prod.product.dimensions
                order_dimensions['length'] = order_dimensions['length']*prod.quantity
            else:
                order_dimensions['length'] += prod.product.dimensions['length']*prod.quantity

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

        shipment = Shipments(status=package['status'],
                             weight=order_weight,
                             volumetric_weight=order_volumetric,
                             dimensions=order_dimensions,
                             order=order,
                             pickup=pickup_point.pickup,
                             return_point=pickup_point.return_point,
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


@core_blueprint.route('/core/ping', methods=['POST'])
@authenticate_restful
def ping_pong(resp):

    return jsonify({
        'status': 'success',
        'message': 'pong!'
    })
    from .request_pickups import lambda_handler
    lambda_handler()
    return jsonify({
        'status': 'success',
        'message': 'pong!'
    })
    data = json.loads(request.data)
    auth_data = resp.get('data')
    if not auth_data:
        return {"success": False, "msg": "Auth Failed"}, 404

    order_ids = data['order_ids']
    orders_qs = db.session.query(Shipments).join(Orders, Shipments.order_id==Orders.id).filter(
        Orders.channel_order_id.in_(order_ids), Orders.delivery_address!=None, Shipments.awb!=None)\
        .order_by(Orders.id).all()
    if not orders_qs:
        return {"success": False, "msg": "No valid order ID"}, 404

    manifest_dict = dict()
    for order in orders_qs:
        if order.pickup.name not in manifest_dict:
            manifest_dict[order.pickup.name] = {order.courier.courier_name:[order]}
        elif order.courier.courier_name not in manifest_dict[order.pickup.name]:
            manifest_dict[order.pickup.name][order.courier.courier_name] = [order]
        else:
            manifest_dict[order.pickup.name][order.courier.courier_name].append(order)


    file_name = "MANIFEST_MIRAKKI.pdf"
    c = canvas.Canvas(file_name, pagesize=A4)
    create_manifests_blank_page(c)
    c.save()

    return jsonify({
        'status': 'success',
        'message': 'pong!'
    })


@core_blueprint.route('/core/dev', methods=['GET'])
def ping_dev():
    shopify_url = "https://dc8ae0b7f5c1c6558f551d81e1352bcd:00dfeaf8f77b199597e360aa4a50a168@origin-clothing-india.myshopify.com/admin/api/2019-10/orders.json?limit=250"
    data = requests.get(shopify_url).json()

    for prod in data['products']:
        for p_sku in prod['variants']:
            try:
                sku = str(p_sku['id'])
                product = Products(name=prod['title'] + " - " + p_sku['title'],
                                   sku=sku,
                                   active=True,
                                   channel_id=1,
                                   client_prefix="KYORIGIN",
                                   date_created=datetime.datetime.now(),
                                   dimensions = {"length":1.25, "breadth":30, "height":30},
                                   price=0,
                                   weight=0.25)

                product_quantity = ProductQuantity(product=product,
                                                   total_quantity=5000,
                                                   approved_quantity=5000,
                                                   available_quantity=5000,
                                                   warehouse_prefix="KYORIGIN",
                                                   status="APPROVED",
                                                   date_created=datetime.datetime.now()
                                                   )
                db.session.add(product)
                db.session.commit()
            except Exception as e:
                print("Exception for " + sku+ " "+ str(e.args[0]))



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

    from .fetch_orders import create_pdf
    create_pdf()

    #shopify_url = "https://b27f6afd9506d0a07af9a160b1666b74:6c8ca315e01fe612bc997e70c7a21908@mirakki.myshopify.com/admin/api/2019-10/orders.json?since_id=1873306419335&limit=250"
    #data = requests.get(shopify_url).json()
    """

    for order in data:


    createBarCodes()

    datetime.strptime("2010-06-04 21:08:12", "%Y-%m-%d %H:%M:%S")
    """
    return jsonify({
        'status': 'success',
        'message': 'pong!'
    })

    """
    import requests

    data = {
      'From': 'LM-WAREIQ',
      'Messages[0][Body]': 'Dear Customer, your Origin order has been shipped via Delhivery with AWB number 1904116940032. It is expected to arrive by 21/10/2235. You shall be notified when the order is dispatched for delivery.',
      'Messages[0][To]': '8750108744',
      'Messages[1][Body]': "Dear Customer, your Origin order has been shipped via Delhivery with AWB number 1904116940032. It is expected to arrive by 21/10/2235. You shall be notified when the order is dispatched for delivery.",
      'Messages[1][To]': "9999503623",
      'Messages[2][Body]': "Dear Customer, your Origin order has been shipped via Delhivery with AWB number 1904116940032. It is expected to arrive by 21/10/2235. You shall be notified when the order is dispatched for delivery.",
      'Messages[2][To]': "9650010831"
    }

    lad = requests.post('https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend', data=data)
    """

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

    awbs = [(3991610001002,"	+919969087591  "),
            (3991610001024,"	98200 41554    "),
            (3991610000151,"	+917780902661  "),
            (3991610000162,"	96344 99890    "),
            (3991610000173,"	99096 02605    "),
            (3991610001046,"	99328 55751    "),
            (3991610000184,"	+919717229103  "),
            (3991610000195,"	+919820081682  "),  ]

    sms_data = {
        'From': 'LM-WAREIQ'
    }
    itt = 0
    for idx, awb in enumerate(awbs):
        url = "https://track.delhivery.com/api/status/packages/json/?waybill=%s&token=d6ce40e10b52b5ca74805a6e2fb45083f0194185"%str(awb[0])
        try:
            edd = requests.get(url).json()['ShipmentData'][0]['Shipment']['expectedDate']
            edd = datetime.datetime.strptime(edd, '%Y-%m-%dT%H:%M:%S')
            if edd<datetime.datetime.now():
                continue
            edd = edd.strftime('%-d %b')
        except Exception:
            continue

        sms_to_key = "Messages[%s][To]"%str(itt)
        sms_body_key = "Messages[%s][Body]"%str(itt)
        customer_phone = awb[1].replace(" ","")
        customer_phone = "0"+customer_phone[-10:]
        sms_data[sms_to_key] = customer_phone
        sms_data[sms_body_key] = "Dear Customer, your Origin order has been shipped via Delhivery with AWB number %s. It is expected to arrive by %s. You shall be notified when the order is dispatched for delivery."%(str(awb[0]), edd)
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
