# services/core/project/api/core.py

import requests, json, math
from sqlalchemy import or_
from flask import Blueprint, request, jsonify
from flask_restful import Resource, Api

from project import db
from project.api.models import Products, ProductQuantity
from project.api.utils import authenticate_restful, get_products_sort_func


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

            products_qs_data = products_qs.limit(per_page).offset(page).all()
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



@core_blueprint.route('/core/ping', methods=['GET'])
@authenticate_restful
def ping_pong():
    return jsonify({
        'status': 'success',
        'message': 'pong!'
    })


@core_blueprint.route('/core/dev', methods=['GET'])
def ping_dev():
    shiprocket_token = """Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOjI0NzIyNiwiaXNzIjoiaHR0cHM6Ly9hcGl2Mi5zaGlwcm9ja2V0LmluL3YxL2V4dGVybmFsL2F1dGgvbG9naW4iLCJpYXQiOjE1NzI0NTA5OTIsImV4cCI6MTU3MzMxNDk5MiwibmJmIjoxNTcyNDUwOTkyLCJqdGkiOiIweFdHMmNYRnNWRFQ3d0pnIn0.AWgRCHQV3yEpv6jtSq7J-byGwZ7HN7zxJPEYUWODPKE"""

    url = "https://apiv2.shiprocket.in/v1/external/inventory?per_page=100&page=1"
    headers = {'Authorization': shiprocket_token}
    response = requests.get(url, headers=headers)

    data = response.json()['data']

    for point in data:
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
        """
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

    return jsonify({
        'status': 'success',
        'message': 'pong!'
    })


api.add_resource(ProductList, '/products/<type>')