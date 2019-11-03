# services/core/project/api/core.py


from sqlalchemy import exc
from flask import Blueprint, request, jsonify
from flask_restful import Resource, Api

from project import db
from project.api.models import Products
from project.api.utils import authenticate_restful


core_blueprint = Blueprint('core', __name__)
api = Api(core_blueprint)


class ProductList(Resource):

    method_decorators = {'get': [authenticate_restful]}

    def get(self, request):
        """Get all exercises"""
        response_object = {
            'status': 'success',
            'data': {
                'products': [
                    product.to_json() for product in Products.query.all()
                ]
            }
        }
        return response_object, 200


@core_blueprint.route('/core/ping', methods=['GET'])
def ping_pong():
    return jsonify({
        'status': 'success',
        'message': 'pong!'
    })


api.add_resource(ProductList, '/products')