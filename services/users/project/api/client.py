from flask import Blueprint, request, jsonify
from flask_restful import Resource, Api

from project import db
from project.api.models import Client
from sqlalchemy import exc
from project.api.utils import authenticate_restful, is_admin, pagination_validator
import logging

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
clients_blueprint = Blueprint('clients', __name__)
api = Api(clients_blueprint)


class Clients(Resource):
    method_decorators = {'post': [authenticate_restful], 'get': [authenticate_restful]}

    def post(self, resp):
        response_object = {'status': 'fail'}
        post_data = request.get_json()
        client_name = post_data.get('client_name')
        client_prefix = post_data.get('client_prefix')
        primary_email = post_data.get('primary_email')
        tabs = post_data.get('tabs')
        try:
            client = Client(client_name=client_name, client_prefix=client_prefix, primary_email=primary_email, tabs=tabs)
            db.session.add(client)
            db.session.commit()
            response_object['status'] = 'success'
            logger.info('client created successfully')
            return response_object, 201
        except Exception as e:
            db.session.rollback()
            logger.error('Failed while creating the client', e)
            response_object['message'] = 'failed while creating client'
            return response_object, 400

    def get(self, resp):
        try:
            page_number = request.args.get('page_number')
            page_size = request.args.get('page_size')
            page_size, page_number = pagination_validator(page_size, page_number)
            clients_data = Client.query.paginate(page=page_number, per_page=page_size, error_out=False)
            total_page = clients_data.total // page_size if clients_data.total % page_size == 0 else (clients_data.total // page_size) + 1
            response_object = {
                'status': 'success',
                'data': {
                    'clients': [client.to_json() for client in clients_data.items]
                },
                'page_number': page_number,
                'page_size': page_size,
                'total_page': total_page
            }
            logger.info('fetched the clients...')
            return response_object, 200
        except Exception as e:
            logger.error('Failed while fetching the client', e)
            return {'status': 'fail', 'message': 'failed while fetching the client'}, 400


@clients_blueprint.route('/users/v1/clients/updateStatus', methods=['POST'])
@authenticate_restful
def update_client_status(resp):
    response_object = {'status': 'fail'}
    try:
        request_data = request.get_json()
        active = True if request_data.get('active') else False
        client_prefix = request_data.get('client_prefix')
        client = Client.query.filter_by(client_prefix=client_prefix).first()
        client.active = active
        db.session.commit()
        response_object['status'] = 'success'
        logger.info('client status updated successfully')
        return jsonify(response_object), 200
    except Exception as e:
        db.session.rollback()
        logger.error('Failed while updating client status', e)
        response_object['message'] = 'failed while creating client'
        return jsonify(response_object), 400


@clients_blueprint.route('/users/v1/clients/checkClientPrefix', methods=['GET'])
@authenticate_restful
def check_client_prefix(resp):
    response_object = {'status': 'fail'}
    try:
        client_prefix = request.args.get('client_prefix')
        client = Client.query.filter_by(client_prefix=client_prefix).first()
        response_object['status'] = 'success'
        response_object['exists'] = True if client else False
        return jsonify(response_object), 200
    except Exception as e:
        logger.error('Failed while check client prefix', e)
        response_object['message'] = 'failed while checking client prefix'
        return jsonify(response_object), 400


api.add_resource(Clients, '/users/v1/clients')

