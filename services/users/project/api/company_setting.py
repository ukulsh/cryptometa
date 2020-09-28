from flask import Blueprint, request, jsonify
from flask_restful import Resource, Api
from project import db
from project.api.models import Client, User
from sqlalchemy import or_
from project.api.utils import authenticate_restful, pagination_validator
from project.api.users_util import based_user_register
from project.api.s3_utils import process_upload_file, get_presigned_url
import urllib
import json
import os
import logging

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
company_setting_blueprint = Blueprint('company_setting', __name__)
api = Api(company_setting_blueprint)


class CompanySetting(Resource):
    method_decorators = {'post': [authenticate_restful], 'get': [authenticate_restful]}

    def get(self, resp):
        response_object = {'status': 'fail'}
        try:
            user = User.query.filter_by(id=resp).first()
            client_ref = user.client
            data = client_ref.to_full_json()
            data['gst_cert_url'] = get_presigned_url(data['gst_cert_url'])
            data['pan_link'] = get_presigned_url(data['pan_link'])
            data['canceled_cheque_link'] = get_presigned_url(data['canceled_cheque_link'])
            data['signed_agreement_link'] = get_presigned_url(data['signed_agreement_link'])
            response_object['data'] = data
            response_object['status'] = 'success'
            return response_object, 200
        except Exception as e:
            logger.error('Failed while getting the company setting', e)
            response_object['message'] = 'Failed while getting the Company Settings'
            return response_object, 400


    def post(self, resp):
        response_object = {'status': 'fail'}
        try:
            user = User.query.filter_by(id=resp).first()
            client_prefix = user.client.client_prefix
            client_ref = user.client
            data = json.loads(request.form.get('data'))
            client_ref.client_name = data.get('client_name')
            client_ref.primary_email = data.get('primary_email')
            client_ref.client_url = data.get('website_url')
            client_ref.address = data.get('address')
            client_ref.city = data.get('city')
            client_ref.state = data.get('state')
            client_ref.country = data.get('country')
            client_ref.pincode = data.get('pincode')
            client_ref.gst_number = data.get('gst_number')
            client_ref.pan_number = data.get('pan_number')
            client_ref.account_name = data.get('account_name')
            client_ref.account_no = data.get('account_no')
            client_ref.bank_name = data.get('bank_name')
            client_ref.bank_branch = data.get('bank_branch')
            client_ref.account_type = data.get('account_type')
            client_ref.ifsc_code = data.get('ifsc_code')
            canceled_cheque_link = data.get('canceled_cheque_link')
            gst_cert_url = data.get('gst_cert_url')
            pan_link = data.get('pan_link')
            signed_agreement_link = data.get('signed_agreement_link')
            gst_file = request.files.get('gst_file')
            canceled_cheque_file = request.files.get('canceled_cheque_file')
            pan_file = request.files.get('pan_file')
            signed_agreement_file = request.files.get('signed_agreement_file')
            if gst_file:
                gst_cert_url = process_upload_file(client_prefix, gst_file, 'gst_file')
            if canceled_cheque_file:
                canceled_cheque_link = process_upload_file(client_prefix, canceled_cheque_file, 'canceled_cheque_file')
            if pan_file:
                pan_link = process_upload_file(client_prefix, pan_file, 'pan_file')
            if signed_agreement_file:
                signed_agreement_link = process_upload_file(client_prefix, signed_agreement_file, 'signed_agreement_file')
            client_ref.canceled_cheque_link = canceled_cheque_link
            client_ref.gst_cert_url = gst_cert_url
            client_ref.pan_link = pan_link
            client_ref.signed_agreement_link = signed_agreement_link
            db.session.commit()
            response_object['status'] = 'success'
            return response_object, 200
        except Exception as e:
            logger.error('Failed while updating company settings', e)
            response_object['message'] = 'Failed while updating the Company Settings'
            return response_object, 400


api.add_resource(CompanySetting, '/users/v1/companySetting')