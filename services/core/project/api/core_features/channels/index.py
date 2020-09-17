from project.api.models import MasterChannels
from flask_restful import Api, Resource
from flask import Blueprint, request, jsonify
from project.api.utils import authenticate_restful
from project import db
from project.api.core_features.channels.channel_utils import get_channel_integration_object
import logging

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

channels_blueprint = Blueprint('channels', __name__)
api = Api(channels_blueprint)


class ClientChannel(Resource):
    method_decorators = {'post': [authenticate_restful]}

    def post(self, resp):
        response_object = {'status': 'fail'}
        try:
            authz_data = resp.get('data')
            client_prefix = authz_data.get('client_prefix')
            post_data = request.get_json()
            channel_name = post_data.get('channel_name')
            channel = MasterChannels.query.filter_by(channel_name=channel_name).first()
            if not channel:
                raise Exception('Invalid channel...')
            channel_ref = get_channel_integration_object(post_data, client_prefix, channel.id)
            db.session.add(channel_ref)
            db.session.commit()
            response_object['status'] = 'success'
            return response_object, 201
        except Exception as e:
            db.session.rollback()
            logger.error('Failed while integrating with respective channel', e)
            response_object['message'] = 'failed while integrating with channel'
            return response_object, 400


@channels_blueprint.route('/core/v1/getChannel', methods=['GET'])
@authenticate_restful
def get_channel(resp):
    response_object = {'status': 'fail'}
    try:
        channels_data = MasterChannels.query.all()
        response_object['data'] = [channel.to_json() for channel in channels_data]
        response_object['status'] = 'success'
        return jsonify(response_object), 200
    except Exception as e:
        logger.error('Failed while getting the channels', e)
        response_object['message'] = 'Failed while getting the channels'
        return jsonify(response_object), 400


api.add_resource(ClientChannel, '/core/v1/integrateChannel')

