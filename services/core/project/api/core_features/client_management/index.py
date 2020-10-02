from project.api.models import ClientMapping
from flask_restful import Api, Resource
from flask import Blueprint, request, jsonify
from project import db
from project.api.utils import authenticate_restful
from project.api.utilities.s3_utils import process_upload_logo_file
import json
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
        response_object = {'status': 'fail'}
        try:
            authz_data = resp.get('data')
            client_prefix = authz_data.get('client_prefix')
            posted_data = json.loads(request.form.get('data'))
            client_mapping_ref = ClientMapping.query.filter_by(client_prefix=client_prefix).first()
            client_mapping_ref.theme_color = posted_data.get('theme_color')
            client_mapping_ref.verify_ndr = posted_data.get('verify_ndr')
            client_mapping_ref.verify_cod = posted_data.get('verify_cod')
            client_mapping_ref.cod_ship_unconfirmed = posted_data.get('cod_ship_unconfirmed')
            client_mapping_ref.verify_cod_manual = posted_data.get('verify_cod_manual')
            client_mapping_ref.hide_products = posted_data.get('hide_products')
            client_mapping_ref.hide_shipper_address = posted_data.get('hide_shipper_address')
            client_mapping_ref.shipping_label = posted_data.get('shipping_label')
            client_mapping_ref.default_warehouse = posted_data.get('default_warehouse')
            client_mapping_ref.order_split = posted_data.get('order_split')
            client_mapping_ref.auto_pur = posted_data.get('auto_pur')
            client_mapping_ref.auto_pur_time = posted_data.get('auto_pur_time')
            logo_file = request.files.get('logo_file')
            if logo_file:
                logo_url = process_upload_logo_file(client_prefix, logo_file)
                client_mapping_ref.client_logo = logo_url
            db.session.commit()
            response_object['status'] = 'success'
            return response_object, 200
        except Exception as e:
            logger.error('Failed while updating client general info', e)
            response_object['message'] = 'Failed while updating client general info'
            return response_object, 400

    def get(self, resp):
        response_object = {'status': 'fail'}
        try:
            authz_data = resp.get('data')
            client_prefix = authz_data.get('client_prefix')
            client_mapping_ref = ClientMapping.query.filter_by(client_prefix=client_prefix).first()
            response_object['data'] = client_mapping_ref.to_json()
            response_object['status'] = 'success'
            return response_object, 200
        except Exceptiona as e:
            logger.error('Failed while getting client general info',  e)
            response_object['message'] = 'Failed while getting the client general info'
            return jsonify(response_object), 400


api.add_resource(ClientGeneralInfo, '/core/v1/clientGeneralSetting')
