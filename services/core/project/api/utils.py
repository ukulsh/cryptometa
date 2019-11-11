# project/api/utils.py


import json
from functools import wraps

import requests
from flask import request, jsonify, current_app


def authenticate(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        response_object = {
            'status': 'error',
            'message': 'Something went wrong. Please contact us.'
        }
        code = 401
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            response_object['message'] = 'Provide a valid auth token.'
            code = 403
            return jsonify(response_object), code
        auth_token = auth_header.split(" ")[1]
        response = ensure_authenticated(auth_token)
        if not response:
            response_object['message'] = 'Invalid token.'
            return jsonify(response_object), code
        return f(response, *args, **kwargs)
    return decorated_function


def authenticate_restful(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        response_object = {
            'status': 'error',
            'message': 'Something went wrong. Please contact us.'
        }
        code = 401
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            response_object['message'] = 'Provide a valid auth token.'
            code = 403
            return response_object, code
        auth_token = auth_header.split(" ")[1]
        response = ensure_authenticated(auth_token)
        if not response:
            response_object['message'] = 'Invalid token.'
            return response_object, code
        return f(response, *args, **kwargs)
    return decorated_function


def ensure_authenticated(token):
    if current_app.config['TESTING']:
        return True
    url = '{0}/auth/status'.format(current_app.config['USERS_SERVICE_URL'])
    bearer = 'Bearer {0}'.format(token)
    headers = {'Authorization': bearer}
    response = requests.get(url, headers=headers)
    data = json.loads(response.text)
    if response.status_code == 200 and \
       data['status'] == 'success' and \
       data['data']['active']:
        return data
    else:
        return False


def get_products_sort_func(Products, ProductsQuantity, sort, sort_by):
    if sort_by == 'product_name':
        x = Products.name
    elif sort_by == 'price':
        x = Products.price
    elif sort_by == 'master_sku':
        x = Products.sku
    elif sort_by == 'total_quantity':
        x = ProductsQuantity.approved_quantity
    elif sort_by == 'weight':
        x = Products.weight
    else:
        x = ProductsQuantity.available_quantity

    if sort.lower() == 'desc':
        x = x.desc
    else:
        x = x.asc
    return x


def get_orders_sort_func(Orders, sort, sort_by):
    if sort_by == 'order_id':
        x = Orders.channel_order_id
    elif sort_by == 'status':
        x = Orders.status
    else:
        x = Orders.order_date

    if sort.lower() == 'asc':
        x = x.asc
    else:
        x = x.desc
    return x