# services/users/project/api/utils.py


from functools import wraps

from flask import request, jsonify

from project.api.models import User


def authenticate(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        response_object = {
            'status': 'fail',
            'message': 'Provide a valid auth token.'
        }
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            return jsonify(response_object), 403
        auth_token = auth_header.split(" ")[1]
        resp = User.decode_auth_token(auth_token)
        if isinstance(resp, str):
            response_object['message'] = resp
            return jsonify(response_object), 401
        user = User.query.filter_by(id=resp).first()
        if not user or not user.active:
            return jsonify(response_object), 401
        return f(resp, *args, **kwargs)
    return decorated_function


def authenticate_token_restful(f):

    @wraps(f)
    def decorated_token_function(*args, **kwargs):
        response_object = {
            'status': 'fail',
            'message': 'Provide a valid auth token.'
        }
        auth_header = request.headers.get('Authorization')
        if auth_header and auth_header.split(" ")[0] == 'Token':
            auth_token = auth_header.split(" ")[1]
            user = User.query.filter_by(token=auth_token).first()
            if user:
                return f(user.id, *args, **kwargs)
        return jsonify(response_object), 403
    return decorated_token_function


def authenticate_restful(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        response_object = {
            'status': 'fail',
            'message': 'Provide a valid auth token.'
        }
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            return response_object, 403
        auth_token = auth_header.split(" ")[1]
        resp = User.decode_auth_token(auth_token)
        if isinstance(resp, str):
            response_object['message'] = resp
            return response_object, 401
        user = User.query.filter_by(id=resp).first()
        if not user or not user.active:
            return response_object, 401
        if user.group_id == 1 and user.login_as:
            resp = user.login_as
        return f(resp, *args, **kwargs)
    return decorated_function


def is_admin(user_id):
    user = User.query.filter_by(id=user_id).first()
    return user.admin


def pagination_validator(page_size, page_number):
    if page_size is None:
        page_size = 10
    else:
        page_size = int(page_size)
    if page_number is None:
        page_number = 1
    else:
        page_number = int(page_number)
    return page_size, page_number