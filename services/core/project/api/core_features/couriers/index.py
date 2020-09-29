from project.api.models import MasterCouriers, ClientCouriers
from flask_restful import Api, Resource
from flask import Blueprint, request, jsonify
from project import db
from project.api.utils import authenticate_restful
import logging

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

couriers_blueprint = Blueprint('couriers', __name__)
api = Api(couriers_blueprint)


class CourierIntegration(Resource):
    method_decorators = {'post': [authenticate_restful], 'get': [authenticate_restful]}

    def post(self, resp):
        response_object = {'status': 'fail'}
        try:
            authz_data = resp.get('data')
            client_prefix = authz_data.get('client_prefix')
            post_data = request.get_json()
            couriers_mapping = post_data.get('couriers_mapping')
            couriers = MasterCouriers.query.filter_by(integrated=True)
            couriers_name_to_id_mapping = {}
            for iterator in couriers:
                couriers_name_to_id_mapping[iterator.courier_name] = iterator.id
            for iterator in couriers_mapping:
                client_mapping_existence = ClientCouriers.query.filter_by(client_prefix=client_prefix, courier_id=
                                                                          couriers_name_to_id_mapping[iterator.get('courier_name')]).first()
                if client_mapping_existence:
                    client_mapping_existence.priority = iterator.get('priority')
                else:
                    new_client_mapping = ClientCouriers(client_prefix=client_prefix,
                                                        courier_id=couriers_name_to_id_mapping[
                                                            iterator.get('courier_name')],
                                                        priority=iterator.get('priority'), active=True)
                    db.session.add(new_client_mapping)
                couriers_name_to_id_mapping.pop(iterator.get('courier_name'))

            for iterator in couriers_name_to_id_mapping:
                ClientCouriers.query.filter_by(client_prefix=client_prefix,
                                               courier_id=couriers_name_to_id_mapping[iterator]).delete()
            db.session.commit()
            response_object['status'] = 'success'
            return response_object, 200
        except Exception as e:
            db.session.rollback()
            logger.error('Failed while integrating with couriers', e)
            response_object['message'] = 'Failed while integration with couriers'
            return response_object, 400

    def get(self, resp):
        response_object = {'status': 'fail'}
        try:
            authz_data = resp.get('data')
            client_prefix = authz_data.get('client_prefix')
            client_couriers = ClientCouriers.query.filter_by(client_prefix=client_prefix).order_by(ClientCouriers.priority)
            response_object['data'] = [iterator.to_json() for iterator in client_couriers]
            response_object['status'] = 'success'
            return response_object, 200
        except Exception as e:
            logger.error('Failed while getting couriers..', e)
            response_object['message'] = 'failed while getting couriers mapping'
            return response_object, 400


@couriers_blueprint.route('/core/v1/getCouriers', methods=['GET'])
@authenticate_restful
def get_couriers(resp):
    response_object = {'status': 'fail'}
    try:
        couriers_data = MasterCouriers.query.filter_by(integrated=True)
        response_object['data'] = [courier.to_json() for courier in couriers_data]
        response_object['status'] = 'success'
        return jsonify(response_object), 200
    except Exception as e:
        logger.error('Failed while getting the couriers', e)
        response_object['message'] = 'Failed while getting the couriers'
        return jsonify(response_object), 400


@couriers_blueprint.route('/core/v1/updateCourierStatus', methods=['POST'])
@authenticate_restful
def update_courier_status(resp):
    response_object = {'status': 'fail'}
    try:
        authz_data = resp.get('data')
        client_prefix = authz_data.get('client_prefix')
        post_data = request.get_json()
        activation_value = post_data.get('active')
        ClientCouriers.query.filter_by(client_prefix=client_prefix).update({'active': activation_value})
        db.session.commit()
        response_object['status'] = 'success'
        return jsonify(response_object), 200
    except Exception as e:
        logger.error('Failed while updating courier status', e)
        response_object['message'] = 'Failed while updating courier status'
        return jsonify(response_object), 400


api.add_resource(CourierIntegration, '/core/v1/integrateCourier')