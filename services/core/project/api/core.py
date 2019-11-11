# services/core/project/api/core.py

import requests, json, math, datetime
from sqlalchemy import or_
from flask import Blueprint, request, jsonify
from flask_restful import Resource, Api

from project import db
from project.api.models import Products, ProductQuantity, \
    Orders, OrdersPayments, PickupPoints, MasterChannels, \
    MasterCouriers, Shipments
from project.api.utils import authenticate_restful, get_products_sort_func, get_orders_sort_func


core_blueprint = Blueprint('core', __name__)
api = Api(core_blueprint)


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
                .filter(Products.client_prefix==client_prefix)\
                .filter(Products.id==ProductQuantity.product_id).order_by(sort_func())\
                .filter(or_(Products.name.ilike(search_key), Products.sku.ilike(search_key)))

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
        search_key = '%{}%'.format(search_key)
        auth_data = resp.get('data')
        if not auth_data:
            return {"success": False, "msg": "Auth Failed"}, 404
        if auth_data.get('user_group') == 'super-admin' or 'client':
            client_prefix = auth_data.get('client_prefix')
            sort_func = get_orders_sort_func(Orders, sort, sort_by)
            orders_qs = db.session.query(Orders)\
                .filter(Orders.client_prefix==client_prefix).order_by(sort_func())\
                .filter(or_(Orders.channel_order_id.ilike(search_key), Orders.customer_name.ilike(search_key)))

            if type == 'new':
                orders_qs = orders_qs.filter(Orders.status == 'NEW')
            elif type == 'ready_to_ship':
                orders_qs = orders_qs.filter(Orders.status == 'PICKUP SCHEDULED')
            elif type == 'shipped':
                orders_qs = orders_qs.filter(Orders.status == 'SHIPPED')
            elif type == 'all':
                pass
            else:
                return {"success": False, "msg": "Invalid URL"}, 404

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
                        {"name": prod.name,
                         "sku": prod.sku}
                    )

                resp_obj['shipping_details'] = dict()
                if order.shipments[0].courier:
                    resp_obj['shipping_details'] = {"courier": order.shipments[0].courier.courier_name,
                                                    "awb":order.shipments[0].awb}
                resp_obj['dimensions'] = order.shipments[0].dimensions
                resp_obj['weight'] = order.shipments[0].weight
                resp_obj['volumetric'] = order.shipments[0].volumetric_weight
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


@core_blueprint.route('/core/ping', methods=['GET'])
@authenticate_restful
def ping_pong():
    return jsonify({
        'status': 'success',
        'message': 'pong!'
    })


@core_blueprint.route('/core/dev', methods=['GET'])
def ping_dev():
    shiprocket_token = """Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOjI0NzIyNiwiaXNzIjoiaHR0cHM6Ly9hcGl2Mi5zaGlwcm9ja2V0LmluL3YxL2V4dGVybmFsL2F1dGgvbG9naW4iLCJpYXQiOjE1NzMzNTIzMTYsImV4cCI6MTU3NDIxNjMxNiwibmJmIjoxNTczMzUyMzE2LCJqdGkiOiJmclBCRHZNYnVUZEEwanZOIn0.Gqax7B1zPWoM34yKkUz2Oa7vIvja7D6Z-C8NsyNIIE4"""

    url = "https://apiv2.shiprocket.in/v1/external/orders?per_page=1000&page=2"
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
            shipment = Shipments(
                awb=point['shipments'][0]['awb'],
                weight=float(point['shipments'][0]['weight']),
                volumetric_weight=point['shipments'][0]['volumetric_weight'],
                dimensions=shipment_dimensions,
            )
            courier = db.session.query(MasterCouriers).filter(MasterCouriers.courier_name==point['shipments'][0]['courier']).first()
            shipment.courier = courier

            payment = OrdersPayments(
                payment_mode=point['payment_method'],
                amount=float(point['total']),
                currency='INR',
            )

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
                shipments=[shipment],
                payment=[payment],
                delivery_address=delivery_address,
                               )
            for prod in point['products']:
                prod_obj = db.session.query(Products).filter(Products.sku==prod['channel_sku']).first()
                new_order.products.append(prod_obj)

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
