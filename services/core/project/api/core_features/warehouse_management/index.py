from project.api.models import PickupPoints, ReturnPoints, ClientPickups
from flask_restful import Api, Resource
from flask import Blueprint, request, jsonify
from sqlalchemy import and_
from project.api.utils import authenticate_restful, pagination_validator
from project.api.core_features.warehouse_management.utils import parse_client_mapping
from project import db
import logging

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

warehouse_blueprint = Blueprint('warehouse', __name__)
api = Api(warehouse_blueprint)


class ClientPickupsAndReturns(Resource):
    method_decorators = {'post': [authenticate_restful], 'get': [authenticate_restful], 'patch': [authenticate_restful]}

    def get(self, resp):
        response_object = {'status': 'fail'}
        try:
            authz_data = resp.get('data')
            client_prefix = authz_data.get('client_prefix')
            page_number = request.args.get('page_number')
            page_size = request.args.get('page_size')
            page_size, page_number = pagination_validator(page_size, page_number)
            client_data = ClientPickups.query.filter_by(client_prefix=client_prefix).paginate(page=page_number, per_page=page_size, error_out=False)
            total_page = client_data.total // page_size if client_data.total % page_size == 0 else (client_data.total // page_size) + 1
            response_object['data'] = [iterator.to_json() for iterator in client_data.items]
            response_object['status'] = 'success'
            response_object['page_numner'] = page_number
            response_object['page_size'] = page_size
            response_object['total_page'] = total_page
            return response_object, 200
        except Exception as e:
            logger.error('Failed while getting with ClientPickups and returns', e)
            response_object['message'] = 'Failed while getting with ClientPickups and returns'
            return response_object, 400

    def patch(self, resp):
        response_object = {'status': 'fail'}
        try:
            authz_data = resp.get('data')
            client_prefix = authz_data.get('client_prefix')
            post_data = request.get_json()
            id = post_data.get('id')
            pickup_name = post_data.get('pickup_name')
            pickup_phone = post_data.get('pickup_phone')
            return_name = post_data.get('return_name')
            return_phone = post_data.get('return_phone')
            gstin = post_data.get('gstin')
            client_pickup_ref = ClientPickups.query.filter_by(client_prefix=client_prefix, id=id).first()
            client_pickup_ref.gstin = gstin
            client_pickup_ref.pickup.name = pickup_name
            client_pickup_ref.pickup.phone = pickup_phone
            client_pickup_ref.return_point.name = return_name
            client_pickup_ref.return_point.phone = return_phone
            db.session.commit()
            response_object['status'] = 'success'
            return response_object, 200
        except Exception as e:
            logger.error('Failed while updating  entry in ClientPickups and returns', e)
            response_object['message'] = 'Failed while updating the entry in ClientPickups and returns'
            return response_object, 400

    def post(self, resp):
        response_object = {'status': 'fail'}
        try:
            authz_data = resp.get('data')
            client_prefix = authz_data.get('client_prefix')
            post_data = request.get_json()
            objects_to_create = parse_client_mapping(post_data)
            db.session.add_all(objects_to_create[:2])
            pickup_ref = PickupPoints.query.filter_by(warehouse_prefix=objects_to_create[0].warehouse_prefix).first()
            return_ref = ReturnPoints.query.filter_by(warehouse_prefix=objects_to_create[1].warehouse_prefix).first()
            client_pickups_ref = ClientPickups(client_prefix, pickup_ref.id, return_ref.id, objects_to_create[2])
            db.session.add(client_pickups_ref)
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


@warehouse_blueprint.route('/core/v1/checkWarehousePrefix', methods=['POST'])
@authenticate_restful
def check_warehouse_prefix(resp):
    response_object = {'status': 'fail'}
    try:
        post_data = request.get_json()
        warehouse_prefix = post_data.get('warehouse_prefix')
        pickup_ref = PickupPoints.query.filter_by(warehouse_prefix=warehouse_prefix).first()
        return_points_ref = ReturnPoints.query.filter_by(warehouse_prefix=warehouse_prefix).first()
        response_object['exists'] = False
        if pickup_ref or return_points_ref:
            response_object['exists'] = True
            response_object['status'] = 'success'
        return jsonify(response_object), 200
    except Exception as e:
        logger.error('Failed while checking warehouse prefix', e)
        response_object['message'] = 'Failed while checking warehouse prefix'
        return jsonify(response_object), 400


api.add_resource(ClientPickupsAndReturns, '/core/v1/clientPickupsAndReturns')


@warehouse_blueprint.route('/core/v1/getWarehousePickups', methods=['GET'])
@authenticate_restful
def get_warehouse_pickups(resp):
    response_object = {'status': 'fail'}
    try:
        authz_data = resp.get('data')
        page_number = request.args.get('page_number')
        page_size = request.args.get('page_size')
        searched_query = request.args.get('search_query')
        searched_query = searched_query if searched_query else ''
        page_size, page_number = pagination_validator(page_size, page_number)
        client_prefix = authz_data.get('client_prefix')
        pickups_data = ClientPickups.query.join(PickupPoints).filter(and_(ClientPickups.client_prefix == client_prefix,
                                                                          ClientPickups.active == True,
                                                                          PickupPoints.warehouse_prefix.ilike(r"%{}%".format(searched_query))
                                                                          )).paginate(page=page_number, per_page=page_size, error_out=False)
        print(pickups_data.total, pickups_data.items)
        total_page = pickups_data.total // page_size if pickups_data.total % page_size == 0 else (pickups_data.total // page_size) + 1
        response_object = {
            'status': 'success',
            'data': {
                'pickups': [_iterator.pickup.to_json() for _iterator in pickups_data.items]
            },
            'page_number': page_number,
            'page_size': page_size,
            'total_page': total_page
        }
        return jsonify(response_object), 200
    except Exception as e:
        logger.error('Failed while getting warehouse pickups', e)
        response_object['message'] = 'Failed while getting warehouse pickups'
        return jsonify(response_object), 400