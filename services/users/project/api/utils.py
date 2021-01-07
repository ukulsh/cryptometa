# services/users/project/api/utils.py

import hmac, hashlib, json, requests, os, base64, jwt
from functools import wraps
from datetime import datetime

from flask import request, jsonify

from project.api.models import User, Client
from project import db
from project.api.users_util import based_user_register

from jwt.contrib.algorithms.pycrypto import RSAAlgorithm
jwt.register_algorithm('RS256', RSAAlgorithm(RSAAlgorithm.SHA256))

CORE_SERVICE_URL = os.environ.get('CORE_SERVICE_URL') or 'http://localhost:5010'


def authenticate(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        response_object = {
            'status': 'fail',
            'message': 'Provide a valid auth token.'
        }
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            return jsonify(response_object), 403
        auth_token = auth_header.split(" ")[1]
        resp = User.decode_auth_token(auth_token)
        if isinstance(resp, str):
            response_object['message'] = resp
            return jsonify(response_object), 401
        user = User.query.filter_by(id=resp).first()
        if not user or not user.active:
            return jsonify(response_object), 401
        return f(resp, *args, **kwargs)
    return decorated_function


def authenticate_token_restful(f):

    @wraps(f)
    def decorated_token_function(*args, **kwargs):
        response_object = {
            'status': 'fail',
            'message': 'Provide a valid auth token.'
        }
        auth_header = request.headers.get('Authorization')
        if auth_header and auth_header.split(" ")[0] == 'Token':
            auth_token = auth_header.split(" ")[1]
            user = User.query.filter_by(token=auth_token).first()
            if user:
                return f(user.id, *args, **kwargs)
        return jsonify(response_object), 403
    return decorated_token_function


def authenticate_restful(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        response_object = {
            'status': 'fail',
            'message': 'Provide a valid auth token.'
        }
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            return response_object, 403
        auth_token = auth_header.split(" ")[1]
        resp = User.decode_auth_token(auth_token)
        if isinstance(resp, str):
            response_object['message'] = resp
            return response_object, 401
        user = User.query.filter_by(id=resp).first()
        if not user or not user.active:
            return response_object, 401
        if user.group_id == 1 and user.login_as:
            resp = user.login_as
        return f(resp, *args, **kwargs)
    return decorated_function


def verify_hamc(signature, merchant_id, secret_key):
    new_signature = hmac.new(bytes(secret_key.encode('utf-8')), merchant_id.encode('utf-8'), hashlib.sha256).hexdigest()
    return signature==new_signature


def create_bikayi_user(merchant_id):
    key = "3f638d4ff80defb82109951b9638fae3fe0ff8a2d6dc20ed8c493783"
    secret = "6e130520777eb175c300aefdfc1270a4f9a57f2309451311ad3fdcfb"
    req_body = {"appId": "WAREIQ",
                "merchantId": merchant_id}
    signature = hmac.new(bytes(secret.encode()),
                         (key.encode() + "|".encode() + base64.b64encode(json.dumps(req_body).replace(" ","").encode())),
                         hashlib.sha256).hexdigest()
    headers = {"Content-Type": "application/json",
               "authorization": signature}
    req = requests.post('https://asia-south1-bikai-d5ee5.cloudfunctions.net/platformPartnerFunctions-fetchMerchant',
                        headers=headers, data=json.dumps(req_body))
    req_data=req.json()
    req_doc = requests.post("https://asia-south1-bikai-d5ee5.cloudfunctions.net/platformPartnerFunctions-fetchIdv",
                        headers=headers, data=json.dumps(req_body))
    req_doc_data = req_doc.json()
    pan_link = None
    signed_agreement_link = None
    gst_cert_url=None
    for doc in req_doc_data['documents']:
        if doc['type'].lower()=='pan_card':
            pan_link=doc['urls'][0] if doc['urls'] else None
        elif doc['type'].lower()=="aadhar_card":
            signed_agreement_link = doc['urls'][0] if doc['urls'] else None
        elif doc['type'].lower()=="gst_certificate":
            gst_cert_url = doc['urls'][0] if doc['urls'] else None

    tabs=["Home","Products","Orders","Pickups","Billing","Serviceability","Settings","Apps"]
    client = Client(client_name=str(req_data['merchant'].get('name')),
                    client_prefix="bky_"+merchant_id,
                    primary_email=str(req_data['merchant'].get('email')),
                    tabs=tabs,
                    signed_agreement_link=signed_agreement_link,
                    pan_link=pan_link,
                    gst_cert_url=gst_cert_url,
                    kyc_verified=True)
    db.session.add(client)
    post_data = bky_default_data
    post_data['client_name'] = str(req_data['merchant'].get('name'))
    post_data['client_prefix'] = "bky_"+merchant_id
    post_data['primary_email'] =str(req_data['merchant'].get('email'))
    post_data['tabs']=tabs
    post_data['first_name']=str(req_data['merchant'].get('name'))
    post_data['username']=str(req_data['merchant'].get('email'))
    post_data['tabs']=tabs
    post_data['theme_color']=str(req_data['merchant'].get('color'))
    post_data['client_logo']=str(req_data['merchant'].get('logourl'))
    post_data['account_type']="prepaid"
    user = based_user_register(post_data)
    db.session.add(user)
    res = requests.post(CORE_SERVICE_URL + '/core/v1/clientManagement', json=post_data)
    if res.status_code != 201:
        raise Exception('Failed to create the record in clientMapping')
    db.session.commit()


def is_admin(user_id):
    user = User.query.filter_by(id=user_id).first()
    return user.admin


def pagination_validator(page_size, page_number):
    if page_size is None:
        page_size = 10
    else:
        page_size = int(page_size)
    if page_number is None:
        page_number = 1
    else:
        page_number = int(page_number)
    return page_size, page_number


def jwt_token(nonce, user_data):
    private_key = """-----BEGIN RSA PRIVATE KEY-----
MIICXAIBAAKBgQCXALD6gGB0tB7r9O1ESZMIRYUHd2+peOBsJfQ6XsWadQIg98kX
sbLK1smQYzfYCSVDaacxb6z3sU1xH/RMBkOwC5J+Ix8heEOuy9JoU7OaMha0vAaw
kK+6Zn8Hj4DyyK++I5xDeZoCQvyhvP0hVHE7jAo0HbhNAg3P6ja73/mTWwIDAQAB
AoGAWtOzoDmvywK8xrjgLn8CzarjRYZ1x75JX0PFD4cJ3Mocqa/haTsdjBx9yTek
03FM1KusQXQm2iXvqufJjiEGfOHTfYPxzfczSmWaXpYsA9QhAtR9rTmcVUDHeyTS
0k/P1NAR+RnACaNiiCmGSwI6WfcPpaeI9s9h5QrGz9/VO4ECQQDoud2W2TWrptK7
RIyXtk6YOs202eS1F4XTRP3uHQaJRP0HNAqHXx+z1jPVUydoyw86qoLvcKbRuGeL
S2oewmRRAkEAphqWDpo6mx55CSv6H553rrZ6Eni3B3WCB8DUvW6uy7qg5ooWogG2
3sR5aYgFc2TxyIStoJYOt3Wq/x5EsoFt6wJARhvQDGSNDZPpAe9Jp16NWMDGPYgy
pPdcImQzVys5T9sPmr7ruRJH+6Y44Tf2tFQP122MmlNGfgFeeBEU/AU1sQJBAJR0
wpT+h07Ip4jpAz5rVbCTavtDZOKHxdXEJN/CIvv3K4Og+6WEPrtPguwtJCIEoIyE
+OHD/BdAVbp6hQ+92k0CQCUuWuBMpLANYiKbyuM/+0RcOGzuMpHZAMUtbwvZJjVp
+r3Ech5Q76Wz1s6UHttyhp4wgAmpcSCXc1LwtSITXI4=
-----END RSA PRIVATE KEY-----"""
    payload = {'sub': user_data['id'],'email':user_data['email'],'iat': datetime.utcnow(),'nonce': nonce,'family_name': user_data['last_name'],'given_name':user_data['first_name'], 'company':user_data['client_prefix']}
    headers = { "alg": "RS256", "typ": "JWT"}
    token = jwt.encode(payload,private_key,algorithm='RS256',headers=headers).decode('utf=-8')
    return token


bky_default_data = {
    "password": "bkypasssome",
    "calling_active": False,
    "courier_data": [
        {
            "a_step": 24,
            "additional_weight_offset": 0.5,
            "b_step": 30,
            "c_step": 37,
            "cod_min": 36,
            "cod_ratio": 1.8,
            "courier_name": "Xpressbees Surface",
            "d_step": 40,
            "e_step": 46,
            "id": 1,
            "management_fee": None,
            "rto_ratio": 1,
            "rvp_ratio": 1.5,
            "weight_offset": 0.5,
            "zone_a": 24,
            "zone_b": 30,
            "zone_c": 37,
            "zone_d": 40,
            "zone_e": 46,
            "management_fee_static": 5
        },
        {
            "a_step": 31,
            "additional_weight_offset": 0.5,
            "b_step": 34,
            "c_step": 38,
            "cod_min": 33,
            "cod_ratio": 1.8,
            "courier_name": "Delhivery Surface Standard",
            "d_step": 44,
            "e_step": 50,
            "id": 3,
            "management_fee": None,
            "rto_ratio": 1,
            "rvp_ratio": 1.5,
            "weight_offset": 0.5,
            "zone_a": 31,
            "zone_b": 34,
            "zone_c": 38,
            "zone_d": 44,
            "zone_e": 50,
            "management_fee_static": 5
        },
        {
            "a_step": 48,
            "additional_weight_offset": 0.5,
            "b_step": 56,
            "c_step": 69,
            "cod_min": 38,
            "cod_ratio": 1.8,
            "courier_name": "Bluedart",
            "d_step": 77,
            "e_step": 86,
            "id": 5,
            "management_fee": None,
            "rto_ratio": 1,
            "rvp_ratio": 1.5,
            "weight_offset": 0.5,
            "zone_a": 48,
            "zone_b": 56,
            "zone_c": 69,
            "zone_d": 77,
            "zone_e": 86,
            "management_fee_static": 5
        },
        {
            "a_step": 24,
            "additional_weight_offset": 0.5,
            "b_step": 30,
            "c_step": 39,
            "cod_min": 38,
            "cod_ratio": 1.8,
            "courier_name": "Xpressbees",
            "d_step": 42,
            "e_step": 46,
            "id": 11,
            "management_fee": None,
            "rto_ratio": 1,
            "rvp_ratio": 1.5,
            "weight_offset": 0.5,
            "zone_a": 24,
            "zone_b": 30,
            "zone_c": 39,
            "zone_d": 42,
            "zone_e": 46,
            "management_fee_static": 5
        },
        {
            "a_step": 31,
            "additional_weight_offset": 0.5,
            "b_step": 34,
            "c_step": 44,
            "cod_min": 33,
            "cod_ratio": 1.8,
            "courier_name": "Delhivery",
            "d_step": 52,
            "e_step": 57,
            "id": 10,
            "management_fee": None,
            "rto_ratio": 1,
            "rvp_ratio": 1.5,
            "weight_offset": 0.5,
            "zone_a": 31,
            "zone_b": 34,
            "zone_c": 44,
            "zone_d": 52,
            "zone_e": 57,
            "management_fee_static": 5
        },
        {
            "a_step": 36,
            "additional_weight_offset": 0.5,
            "b_step": 42,
            "c_step": 49,
            "cod_min": 38,
            "cod_ratio": 1.8,
            "courier_name": "Ecom Express",
            "d_step": 55,
            "e_step": 63,
            "id": 4,
            "management_fee": None,
            "rto_ratio": 1,
            "rvp_ratio": 1.5,
            "weight_offset": 0.5,
            "zone_a": 36,
            "zone_b": 42,
            "zone_c": 49,
            "zone_d": 55,
            "zone_e": 63,
            "management_fee_static": 5
        }
    ]
}