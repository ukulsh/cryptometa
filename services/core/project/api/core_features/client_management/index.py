from project.api.models import ClientMapping
from flask_restful import Api, Resource
from flask import Blueprint, request, jsonify
from project import db
from project.api.utils import authenticate_restful
import logging

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

client_management_blueprint = Blueprint('clientManagement', __name__)
api = Api(client_management_blueprint)


class ClientManagement(Resource):

    def post(self):
        response_object = {'status': 'fail'}
        try:
            posted_data = request.get_json()
            client_prefix = posted_data['client_prefix']
            client_name = posted_data['client_name']
            client_mapping_ref = ClientMapping(client_name, client_prefix)
            db.session.add(client_mapping_ref)
            db.session.commit()
            response_object['status'] = 'success'
            return response_object, 201
        except Exception as e:
            logger.error('Failed while inserting clients info', e)
            response_object['message'] = 'failed while inserting client info'
            return jsonify(response_object), 400

    def patch(self):
        response_object = {'status': 'fail'}
        try:
            posted_data = request.get_json()
            client_prefix = posted_data['client_prefix']
            client_name = posted_data['client_name']
            client_mapping_ref = ClientMapping.query.filter_by(client_prefix=client_prefix).first()
            client_mapping_ref.client_name = client_name
            db.session.commit()
            response_object['status'] = 'success'
            return response_object, 200
        except Exception as e:
            db.session.rollback()
            logger.error('Failed while updating clients info', e)
            response_object['message'] = 'failed while updating client info'
            return jsonify(response_object), 400


api.add_resource(ClientManagement, '/core/v1/clientManagement')


class ClientGeneralInfo(Resource):

    method_decorators = {'post': [authenticate_restful], 'get': [authenticate_restful], 'patch': [authenticate_restful]}

    def post(self, resp):
        pass

    def get(self, resp):
        pass


api.add_resource(ClientGeneralInfo, '/core/v1/clientGeneralSetting')


