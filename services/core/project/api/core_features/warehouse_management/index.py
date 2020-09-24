from project.api.models import PickupPoints, ReturnPoints, ClientPickups
from flask_restful import Api, Resource
from flask import Blueprint, request, jsonify
from project.api.utils import authenticate_restful, pagination_validator
from project.api.core_features.warehouse_management.utils import parse_client_mapping
from project import db
import logging

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

warehouse_blueprint = Blueprint('warehouse', __name__)
api = Api(warehouse_blueprint)


class ClientPickupsAndReturns(Resource):
    method_decorators = {'post': [authenticate_restful], 'get': [authenticate_restful]}

    def get(self, resp):
        response_object = {'status': 'fail'}
        try:
            authz_data = resp.get('data')
            client_prefix = authz_data.get('client_prefix')
            client_data = ClientPickups.query.filter_by(client_prefix=client_prefix)
            response_object['data'] = [iterator.to_json() for iterator in client_data]
            response_object['status'] = 'success'
            return response_object, 200
        except Exception as e:
            logger.error('Failed while getting with ClientPickups and returns', e)
            response_object['message'] = 'Failed while getting with ClientPickups and returns'
            return response_object, 400

    def post(self, resp):
        response_object = {'status': 'fail'}
        try:
            authz_data = resp.get('data')
            client_prefix = authz_data.get('client_prefix')
            post_data = request.get_json()
            objects_to_create = parse_client_mapping(post_data.get('client_mapping'), client_prefix)
            client_pickups = []
            for iterator in objects_to_create:
                db.session.add_all([iterator[0], iterator[1]])
                pickup_ref = PickupPoints.query.filter_by(warehouse_prefix=iterator[0].warehouse_prefix).first()
                return_ref = ReturnPoints.query.filter_by(warehouse_prefix=iterator[1].warehouse_prefix).first()
                client_pickups_ref = ClientPickups(client_prefix, pickup_ref.id, return_ref.id, iterator[2])
                client_pickups.append(client_pickups_ref)
            db.session.add_all(client_pickups)
            db.session.commit()
            response_object['status'] = 'success'
            return response_object, 201
        except Exception as e:
            logger.error('Failed while adding entry in ClientPickups and returns', e)
            response_object['message'] = 'Failed while adding the entry in ClientPickups and returns'
            return response_object, 400


@warehouse_blueprint.route('/core/v1/updateClientPickupsAndReturnStatus', methods=['POST'])
@authenticate_restful
def update_warehouse_mapping(resp):
    response_object = {'status': 'fail'}
    try:
        authz_data = resp.get('data')
        client_prefix = authz_data.get('client_prefix')
        post_data = request.get_json()
        id = post_data.get('id')
        active = post_data.get('active')
        ClientPickups.query.filter_by(client_prefix=client_prefix, id=id).update({'active': active})
        db.session.commit()
        response_object['status'] = 'success'
        return jsonify(response_object), 200
    except Exception as e:
        logger.error('Failed while updating client pickups', e)
        response_object['message'] = 'Failed while updating client pickups'
        return jsonify(response_object), 400


api.add_resource(ClientPickupsAndReturns, '/core/v1/clientPickupsAndReturns')

