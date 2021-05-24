from project.api.models import MasterChannels, ClientChannel
from flask_restful import Api, Resource
from flask import Blueprint, request, jsonify
from project.api.utils import authenticate_restful, pagination_validator
from project import db
from project.api.core_features.channels.channel_utils import get_channel_integration_object
import logging, requests, json

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

channels_blueprint = Blueprint('channels', __name__)
api = Api(channels_blueprint)


class ClientChannelIntegration(Resource):
    method_decorators = {'post': [authenticate_restful], 'patch': [authenticate_restful], 'get': [authenticate_restful]}

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

    def patch(self, resp):
        response_object = {'status': 'fail'}
        try:
            post_data = request.get_json()
            id = post_data.get('id')
            client_channel = ClientChannel.query.filter_by(id=id).first()
            client_channel.store_name = post_data.get('store_name')
            client_channel.api_key = post_data.get('api_key')
            client_channel.api_password = post_data.get('api_password')
            client_channel.shared_secret = post_data.get('shared_secret')
            client_channel.shop_url = post_data.get('shop_url')
            client_channel.fetch_status = post_data.get('fetch_status')
            client_channel.mark_shipped = post_data.get('mark_shipped')
            client_channel.shipped_status = post_data.get('shipped_status')
            client_channel.mark_canceled = post_data.get('mark_canceled')
            client_channel.canceled_status = post_data.get('canceled_status')
            client_channel.mark_returned = post_data.get('mark_returned')
            client_channel.returned_status = post_data.get('returned_status')
            client_channel.mark_delivered = post_data.get('mark_delivered')
            client_channel.delivered_status = post_data.get('delivered_status')
            client_channel.mark_invoiced = post_data.get('mark_invoiced')
            client_channel.invoiced_status = post_data.get('invoiced_status')
            client_channel.sync_inventory = post_data.get('sync_inventory')
            client_channel.status = post_data.get('status')
            if post_data.get('status'):
                client_channel.connection_status = post_data.get('status')
            if post_data.get('activate_badge') is not None:
                if post_data.get('activate_badge'):
                    shopify_script_url = "https://%s:%s@%s/admin/api/2021-04/script_tags.json" % (
                        client_channel.api_key, client_channel.api_password, client_channel.shop_url)
                    ful_header = {'Content-Type': 'application/json'}
                    shopify_script_body = {"script_tag": {
                                                "event": "onload",
                                                "src": "https://wareiq-shopify.s3.amazonaws.com/wareiq-shopify.js"
                                              }
                                            }

                    req_ful = requests.post(shopify_script_url, data=json.dumps(shopify_script_body),
                                            headers=ful_header)

                    script_id = req_ful.json()['script_tag']['id']
                    client_channel.script_id = str(script_id)
                elif post_data.get('activate_badge')==False and client_channel.script_id:
                    shopify_script_url = "https://%s:%s@%s/admin/api/2021-04/script_tags/%s.json" % (
                        client_channel.api_key, client_channel.api_password, client_channel.shop_url, client_channel.script_id)
                    req_scr = requests.delete(shopify_script_url)
                    client_channel.script_id = None

            db.session.commit()
            response_object['status'] = 'success'
            return response_object, 200
        except Exception as e:
            db.session.rollback()
            logger.error('Failed while updating the record', e)
            response_object['message'] = 'failed while updating channel information'


    def get(self, resp):
        response_object = {'status': 'fail'}
        try:
            page_number = request.args.get('page_number')
            page_size = request.args.get('page_size')
            page_size, page_number = pagination_validator(page_size, page_number)
            authz_data = resp.get('data')
            client_prefix = authz_data.get('client_prefix')
            client_channel_data = ClientChannel.query.filter(ClientChannel.client_prefix == client_prefix).paginate(page=page_number, per_page=page_size, error_out=False)
            total_page = client_channel_data.total // page_size if client_channel_data.total % page_size == 0 else (client_channel_data.total // page_size) + 1
            response_object['data'] = [iterator.to_json() for iterator in client_channel_data.items]
            response_object['status'] = 'success'
            response_object['page_numner'] = page_number
            response_object['page_size'] = page_size
            response_object['total_page'] = total_page
            return response_object, 200
        except Exception as e:
            db.session.rollback()
            logger.error('Failed while getting the records', e)
            response_object['message'] = 'Failed while getting the records'
            return response_object, 200


@channels_blueprint.route('/core/v1/getChannel', methods=['GET'])
@authenticate_restful
def get_channel(resp):
    response_object = {'status': 'fail'}
    try:
        channels_data = MasterChannels.query.order_by(MasterChannels.integrated.desc()).all()
        response_object['data'] = [channel.to_json() for channel in channels_data]
        for channel in response_object['data']:
            if channel['channel_name'] in ('Bikayi','EasyEcom','Manual'):
                response_object['data'].remove(channel)
        response_object['status'] = 'success'
        return jsonify(response_object), 200
    except Exception as e:
        logger.error('Failed while getting the channels', e)
        response_object['message'] = 'Failed while getting the channels'
        return jsonify(response_object), 400


api.add_resource(ClientChannelIntegration, '/core/v1/integrateChannel')

