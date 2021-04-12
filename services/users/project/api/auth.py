# services/users/project/api/auth.py

from flask import Blueprint, jsonify, request, current_app, redirect
from sqlalchemy import exc, or_

from project.api.models import User, Client
from project import db, bcrypt
from project.api.utils import authenticate, authenticate_token_restful, create_bikayi_user, verify_hamc, jwt_token, authenticate_restful
import os, requests, hmac, hashlib, base64, json

CORE_SERVICE_URL = os.environ.get('CORE_SERVICE_URL') or 'http://localhost:5010'

auth_blueprint = Blueprint('auth', __name__)


@auth_blueprint.route('/auth/register', methods=['POST'])
def register_user():
    # get post data
    post_data = request.get_json()
    response_object = {
        'status': 'fail',
        'message': 'Invalid payload.'
    }
    if not post_data:
        return jsonify(response_object), 400
    username = post_data.get('username')
    email = post_data.get('email')
    password = post_data.get('password')
    try:
        # check for existing user
        user = User.query.filter(
            or_(User.username == username, User.email == email)).first()
        if not user:
            # add new user to db
            new_user = User(
                username=username,
                email=email,
                password=password
            )
            db.session.add(new_user)
            db.session.commit()
            # generate auth token
            auth_token = new_user.encode_auth_token(new_user.id)
            response_object['status'] = 'success'
            response_object['message'] = 'Successfully registered.'
            response_object['auth_token'] = auth_token.decode()
            return jsonify(response_object), 201
        else:
            response_object['message'] = 'Sorry. That user already exists.'
            return jsonify(response_object), 400
    # handler errors
    except (exc.IntegrityError, ValueError):
        db.session.rollback()
        return jsonify(response_object), 400


@auth_blueprint.route('/auth/login', methods=['POST'])
def login_user():
    # get post data
    post_data = request.get_json()
    response_object = {
        'status': 'fail',
        'message': 'Invalid payload.'
    }
    if not post_data:
        return jsonify(response_object), 400
    email = post_data.get('email')
    password = post_data.get('password')
    try:
        # fetch the user data
        user = User.query.filter_by(email=email).first()
        if user and bcrypt.check_password_hash(user.password, password):
            auth_token = user.encode_auth_token(user.id)
            if auth_token:
                response_object['status'] = 'success'
                response_object['message'] = 'Successfully logged in.'
                response_object['auth_token'] = auth_token.decode()
                return jsonify(response_object), 200
        else:
            response_object['message'] = 'User does not exist.'
            return jsonify(response_object), 404
    except Exception:
        response_object['message'] = 'Try again.'
        return jsonify(response_object), 500


@auth_blueprint.route('/auth/login/bikayi', methods=['GET'])
def login_user_bikayi():
    response_object = {
        'status': 'fail',
        'message': 'signature verification failed.'
    }
    merchant_id = request.args.get('merchantId')
    access_token = request.args.get('accessToken')
    key = "3f638d4ff80defb82109951b9638fae3fe0ff8a2d6dc20ed8c493783"
    secret = "6e130520777eb175c300aefdfc1270a4f9a57f2309451311ad3fdcfb"
    req_body = {"appId": "WAREIQ",
                "merchantId": merchant_id}
    signature = hmac.new(bytes(secret.encode()),
                         (key.encode() + "|".encode() + base64.b64encode(
                             json.dumps(req_body).replace(" ", "").encode())),
                         hashlib.sha256).hexdigest()
    headers = {"Content-Type": "application/json",
               "authorization": signature,
               "accesstoken": access_token}
    req = requests.post('https://asia-south1-bikai-d5ee5.cloudfunctions.net/platformPartnerFunctions-validateAccessToken',
                        headers=headers, data=json.dumps(req_body))
    try:
        if req.json()['isValid']!=True:
            return jsonify(response_object), 403
    except Exception as e:
        return jsonify(response_object), 403

    # auth_token = auth_header.split(" ")[1]
    # resp = User.decode_auth_token(auth_token)
    # if isinstance(resp, str):
    #     response_object['message'] = resp
    #     return response_object, 401
    user = db.session.query(User).join(Client).filter(Client.client_prefix=="bky_" + merchant_id).first()
    if not user:
        create_bikayi_user(merchant_id)

    user = db.session.query(User).join(Client).filter(Client.client_prefix=="bky_" + merchant_id).first()
    if user:
        auth_token = user.encode_auth_token(user.id)
        if auth_token:
            response_object['status'] = 'success'
            response_object['message'] = 'Successfully logged in.'
            response_object['auth_token'] = auth_token.decode()
            # try:
            #     requests.get('{0}/scans/v1/sync/orders'.format(current_app.config['CELERY_SERVICE_URL']),
            #                   headers={"Authorization": "Bearer "+auth_token.decode(),
            #                         "Content-Type": "application/json"})
            # except Exception as e:
            #     pass
            return jsonify(response_object), 200
    else:
        response_object['message'] = "Couldn't login."
        return jsonify(response_object), 404


@auth_blueprint.route('/auth/loginAPI', methods=['POST'])
def login_user_api():
    # get post data
    post_data = request.get_json()
    response_object = {
        'status': 'fail',
        'message': 'Invalid payload.'
    }
    if not post_data:
        return jsonify(response_object), 400
    username = post_data.get('username')
    password = post_data.get('password')
    try:
        # fetch the user data
        user = User.query.filter_by(username=username).first()
        if user and bcrypt.check_password_hash(user.password, password):
            auth_token = user.encode_auth_token(user.id)
            if auth_token:
                response_object['status'] = 'success'
                response_object['data'] = user.to_json()
                response_object['message'] = 'Successfully logged in.'
                response_object['auth_token'] = auth_token.decode()
                return jsonify(response_object), 200
        else:
            response_object['message'] = 'User does not exist.'
            return jsonify(response_object), 404
    except Exception:
        response_object['message'] = 'Try again.'
        return jsonify(response_object), 500


@auth_blueprint.route('/auth/logout', methods=['GET'])
@authenticate
def logout_user(resp):
    response_object = {
        'status': 'success',
        'message': 'Successfully logged out.'
    }
    return jsonify(response_object), 200


@auth_blueprint.route('/auth/status', methods=['GET'])
@authenticate
def get_user_status(resp):
    response_object = {'status': 'fail'}
    try:
        user = User.query.filter_by(id=resp).first()
        if user.group_id != 1:
            response_object = {
                'status': 'success',
                'message': 'success',
                'data': user.to_json(),
            }
        else:
            login_as_user_id = user.login_as if user.login_as else user.id
            login_as_user = User.query.filter_by(id=login_as_user_id).first()
            data = login_as_user.to_json()
            """
            try:
                res = requests.get(CORE_SERVICE_URL+'/core/v1/clientManagement?client_prefix=%s'%data['client_prefix'])
                data['thirdwatch_active'] = res.json()['thirdwatch']
            except Exception as e:
                pass
            """
            response_object['data'] = data
            if login_as_user.id != user.id:
                response_object['parent_username'] = user.username
            response_object['status'] = 'success'
        return jsonify(response_object), 200
    except Exception as e:
        response_object['message'] = 'failed while getting login status'
        return jsonify(response_object), 400


@auth_blueprint.route('/auth/v1/updateUser', methods=['POST'])
@authenticate_restful
def update_user(resp):
    response_object = {'status': 'fail'}
    try:
        post_data = request.get_json()
        if not post_data:
            return jsonify(response_object), 400
        user = User.query.filter_by(id=resp).first()
        if not user:
            return jsonify(response_object), 400
        first_name = post_data.get('first_name')
        last_name = post_data.get('last_name')
        phone = post_data.get('phone')
        password = post_data.get('password')
        if first_name!=None:
            user.first_name=first_name
        if last_name!=None:
            user.last_name=last_name
        if password!=None:
            user.password=bcrypt.generate_password_hash(
                password, current_app.config.get('BCRYPT_LOG_ROUNDS')
            ).decode()
        if phone != None:
            user.phone_no=phone

        db.session.commit()
        response_object['status'] = 'success'
        return jsonify(response_object), 200
    except Exception as e:
        response_object['message'] = 'failed while getting login status'
        return jsonify(response_object), 400


@auth_blueprint.route('/auth/tokenStatus', methods=['GET'])
@authenticate_token_restful
def get_token_status(resp):
    response_object = {'status': 'fail'}
    try:
        user = User.query.filter_by(id=resp).first()
        response_object = {
            'status': 'success',
            'data': user.to_json(),
        }
        return jsonify(response_object), 200
    except Exception as e:
        response_object['message'] = 'failed while getting token status'
        return jsonify(response_object), 400


@auth_blueprint.route('/auth/loginAs', methods=['POST'])
@authenticate
def login_as(resp):
    response_object = {'status': 'fail'}
    try:
        source_user = User.query.filter_by(id=resp).first()
        if source_user.group_id != 1:
            raise Exception("user doesnt have access to login as")
        post_data = request.get_json()
        username = post_data.get('username')
        dest_user = User.query.filter_by(username=username).first()
        source_user.login_as = dest_user.id
        db.session.commit()
        response_object['status'] = 'success'
        return jsonify(response_object), 200
    except Exception as e:
        response_object['message'] = 'failed while logging as user'
        return jsonify(response_object), 400


@auth_blueprint.route('/auth/jwt/freshdesk', methods=['GET'])
@authenticate_restful
def login_freshdesk_sso(resp):
    source_user = User.query.filter_by(id=resp).first()
    if not source_user:
        raise Exception("user doesnt have access to login.")
    if source_user.group_id != 1:
        user_data = {'id': str(source_user.id),
                     'email': source_user.email,
                     'last_name': source_user.last_name,
                     'first_name': source_user.first_name,
                     'client_prefix': source_user.client.client_prefix if source_user.client else None,
                     }
    else:
        user_data = {'id': "9",
                     'email': "ravi@wareiq.com",
                     'last_name': "Chaudhary",
                     'first_name': "Ravi",
                     'client_prefix': "WAREIQ",
                     }
    client_id = request.args.get('client_id')
    state = request.args.get('state')
    redirect_uri = request.args.get('redirect_uri')
    nonce = request.args.get('nonce')
    # return '''{}'''.format(state)
    token = jwt_token(nonce, user_data)
    constructed_url = redirect_uri + '?state=' + state + '&id_token=' + token
    return jsonify({"url": constructed_url}), 200