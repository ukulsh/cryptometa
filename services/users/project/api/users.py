# services/users/project/api/users.py


from flask import Blueprint, request, jsonify
from flask_restful import Resource, Api

from project import db
from project.api.models import User, Client
from sqlalchemy import exc, or_
from project.api.utils import authenticate_restful, pagination_validator
from project.api.users_util import user_register
import uuid

users_blueprint = Blueprint('users', __name__)
api = Api(users_blueprint)


class UsersPing(Resource):
    def get(self):
        return {
        'status': 'success',
        'message': 'pong!'
    }


class UsersList(Resource):

    method_decorators = {'post': [authenticate_restful], 'get': [authenticate_restful], 'patch': [authenticate_restful]}

    def post(self, resp):  # new
        post_data = request.get_json()
        response_object = {
            'status': 'fail',
            'message': 'Invalid payload.'
        }
        # new
        if not post_data:
            return response_object, 400
        email = post_data.get('email')
        username = post_data.get('username')
        try:
            user = User.query.filter(
                or_(User.username == username, User.email == email)).first()
            if not user:
                user = user_register(post_data, resp)
                db.session.add(user)
                db.session.commit()
                response_object['status'] = 'success'
                response_object['message'] = f'{email} was added!'
                return response_object, 201
            else:
                response_object['message'] = \
                    'Sorry. That email already exists.'
                return response_object, 400
        except (exc.IntegrityError, ValueError):
            db.session.rollback()
            return response_object, 400

    def patch(self, resp):
        response_object = {'status': 'fail'}
        try:
            patch_data = request.get_json()
            username = patch_data.get('username')
            email = patch_data.get('email')
            first_name = patch_data.get('first_name')
            last_name = patch_data.get('last_name')
            tabs = patch_data.get('tabs')
            calling_active = patch_data.get('calling_active')
            phone_no = patch_data.get('phone_no')
            user = User.query.filter(
                or_(User.username == username, User.email == email)).first()
            user.first_name = first_name
            user.last_name = last_name
            user.tabs = tabs
            user.calling_active = calling_active
            user.phone_no = phone_no
            db.session.commit()
            response_object['status'] = 'success'
            return response_object, 200
        except Exception as e:
            db.session.rollback()
            response_object['message'] = 'failed while updating data'
            return response_object, 400

    def get(self, resp):
        """Get all users"""
        response_object = {'status': 'fail'}
        try:
            user_group_filter = []
            user = User.query.filter_by(id=resp).first()
            if user.group_id != 1:
                user_group_filter.append(User.client_id == user.client_id)
            page_number = request.args.get('page_number')
            page_size = request.args.get('page_size')
            searched_query = request.args.get('search_query')
            searched_query = searched_query if searched_query else ''
            page_size, page_number = pagination_validator(page_size, page_number)
            users_data = User.query.join(Client).filter(or_(
                User.username.ilike(r"%{}%".format(searched_query)),
                User.email.ilike(r"%{}%".format(searched_query)),
                Client.client_prefix.ilike(r"%{}%".format(searched_query))
            )).filter(*user_group_filter).paginate(page=page_number, per_page=page_size, error_out=False)
            total_page = users_data.total // page_size if users_data.total % page_size == 0 else (users_data.total // page_size) + 1
            response_object = {
                'status': 'success',
                'data': {
                    'users': [user.to_json() for user in users_data.items]
                },
                'page_number': page_number,
                'page_size': page_size,
                'total_page': total_page
            }
            return response_object, 200
        except Exception as e:
            print(e)
            response_object['message'] = 'failed while fetching the client'
            return response_object, 400


class Users(Resource):
    def get(self, user_id):
        """Get single user details"""
        response_object = {
            'status': 'fail',
            'message': 'User does not exist'
        }
        try:
            user = User.query.filter_by(id=int(user_id)).first()
            if not user:
                return response_object, 404
            else:
                response_object = {
                    'status': 'success',
                    'data': {
                        'id': user.id,
                        'username': user.username,
                        'email': user.email,
                        'active': user.active
                    }
                }
                return response_object, 200
        except ValueError:
            return response_object, 404


@users_blueprint.route('/users/checkUsername', methods=['GET'])
@authenticate_restful
def check_username(resp):
    response_object = {'status': 'fail'}
    try:
        username = request.args.get('username')
        user = User.query.filter_by(username=username).first()
        response_object['status'] = 'success'
        response_object['exists'] = True if user else False
        return jsonify(response_object), 200
    except Exception as e:
        response_object['message'] = 'failed while checking username'
        return jsonify(response_object), 400


@users_blueprint.route('/users/generateToken', methods=['GET'])
@authenticate_restful
def generate_token(resp):
    response_object = {'status': 'fail'}
    try:
        regenerate = request.args.get('regenerate')
        user = User.query.filter_by(id=resp).first()
        if regenerate:
            user.token = uuid.uuid4().hex
            db.session.commit()
        response_object['token'] = user.token
        response_object['status'] = 'success'
        return jsonify(response_object), 200
    except Exception as e:
        response_object['message'] = 'failed while getting  user token'
        return jsonify(response_object), 400


api.add_resource(UsersPing, '/users/ping')
api.add_resource(UsersList, '/users')
api.add_resource(Users, '/users/<user_id>')


