# services/core/project/api/core.py

import requests, json, math, pytz, psycopg2, logging
import boto3, os, csv, io, smtplib, time, hmac
import pandas as pd
import numpy as np
import re, razorpay, jwt, uuid
from flask_cors import cross_origin
from datetime import datetime, timedelta
from sqlalchemy import or_, func, not_, and_
from flask import Blueprint, request, jsonify, make_response, render_template_string
from flask_restful import Resource, Api
from reportlab.lib.pagesizes import landscape, A4
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
from psycopg2.extras import RealDictCursor
from .generate_manifest import fill_manifest_data
from .worker import celery
#from .tasks import consume_ecom_scan

from project import db
from .queries import product_count_query, available_warehouse_product_quantity, fetch_warehouse_to_pick_from, \
    select_product_list_query, select_orders_list_query, select_wallet_deductions_query, select_wallet_remittance_query, \
    select_wallet_remittance_orders_query
from project.api.models import Products, ProductQuantity, InventoryUpdate, WarehouseMapping, NDRReasons, MultiVendor, \
    Orders, OrdersPayments, PickupPoints, MasterChannels, ClientPickups, CodVerification, NDRVerification, NDRShipments,\
    MasterCouriers, Shipments, OPAssociation, ShippingAddress, Manifests, ClientCouriers, OrderStatus, DeliveryCheck, \
    ClientMapping, IVRHistory, ClientRecharges, CODRemittance, ThirdwatchData, ClientChannel
from project.api.utils import authenticate_restful, fill_shiplabel_data_thermal, create_shiplabel_blank_page, \
    fill_shiplabel_data, create_shiplabel_blank_page_thermal, create_invoice_blank_page, fill_invoice_data, \
    generate_picklist, generate_packlist, cancel_order_on_channels


core_blueprint = Blueprint('core', __name__)
api = Api(core_blueprint)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

session = boto3.Session(
    aws_access_key_id='AKIAWRT2R3KC3YZUBFXY',
    aws_secret_access_key='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',
)

conn = psycopg2.connect(host=os.environ.get('DATABASE_HOST'), database=os.environ.get('DATABASE_NAME'), user=os.environ.get('DATABASE_USER'), password=os.environ.get('DATABASE_PASSWORD'))
conn_2 = psycopg2.connect(host=os.environ.get('DATABASE_HOST_PINCODE'), database=os.environ.get('DATABASE_NAME'), user=os.environ.get('DATABASE_USER'), password=os.environ.get('DATABASE_PASSWORD'))

#email_server = smtplib.SMTP_SSL('smtpout.secureserver.net', 465)
#email_server.login("noreply@wareiq.com", "Berlin@123")
razorpay_client = razorpay.Client(auth=("rzp_live_FGAwxhtumHezAw", "IZ7C97EEef0rvyqZJLy0CYNb"))


ORDERS_DOWNLOAD_HEADERS = ["Order ID", "Customer Name", "Customer Email", "Customer Phone", "Order Date",
                            "Courier", "Weight", "awb", "Expected Delivery Date", "Status", "Address_one", "Address_two",
                           "City", "State", "Country", "Pincode", "Pickup Point", "Product", "SKU", "Quantity", "Order Type",
                           "Amount", "Pickup Date", "Delivered Date", "COD Verfication", "COD Verified Via", "NDR Verfication", "NDR Verified Via"]

PRODUCTS_DOWNLOAD_HEADERS = ["S. No.", "Product Name", "Channel SKU", "Master SKU", "Price", "Total Quantity",
                             "Available Quantity", "Current Quantity", "Inline Quantity", "RTO Quantity", "Dimensions", "Weight"]

DEDUCTIONS_DOWNLOAD_HEADERS = ["Time", "Status", "Courier", "AWB", "order ID", "COD cost", "Forward cost", "Return cost",
                              "Management Fee", "Subtotal", "Total", "Zone", "Weight Charged"]

REMITTANCE_DOWNLOAD_HEADERS = ["Order ID", "Order Date", "Courier", "AWB", "Payment Mode", "Amount", "Delivered Date"]

RECHARGES_DOWNLOAD_HEADERS = ["Payment Time", "Amount", "Transaction ID", "status"]

ORDERS_UPLOAD_HEADERS = ["order_id", "customer_name", "customer_email", "customer_phone", "address_one", "address_two",
                         "city", "state", "country", "pincode", "sku", "sku_quantity", "payment_mode", "subtotal", "shipping_charges", "warehouse", "Error"]


class CodVerificationGather(Resource):

    def get(self, ver_type):
        try:
            order_id = request.args.get('CustomField')
            if order_id:
                order_id = int(order_id)
                order = db.session.query(Orders).filter(Orders.id == order_id).first()
                client_name = db.session.query(ClientMapping).filter(ClientMapping.client_prefix == order.client_prefix).first()
                if client_name:
                    client_name = client_name.client_name
                else:
                    client_name = order.client_prefix.lower()
                if ver_type=="cod":
                    gather_prompt_text = "Hello %s, You recently placed an order from %s with amount %s." \
                                     " Press 1 to confirm your order or, 0 to cancel." % (order.customer_name,
                                                                                         client_name,
                                                                                         str(order.payments[0].amount))

                    repeat_prompt_text = "It seems that you have not provided any input, please try again. Order from %s, " \
                                     "Order ID %s. Press 1 to confirm your order or, 0 to cancel." % (
                                     client_name,
                                     order.channel_order_id)
                elif ver_type=="ndr":
                    gather_prompt_text = "Hello %s, You recently cancelled your order from %s with amount %s." \
                                         " Press 1 to confirm cancellation or, 0 to re-attempt." % (order.customer_name,
                                                                                             client_name,
                                                                                             str(order.payments[0].amount))

                    repeat_prompt_text = "It seems that you have not provided any input, please try again. Order from %s, " \
                                         "Order ID %s. Press 1 to confirm cancellation or, 0 to re-attempt." % (
                                             client_name,
                                             order.channel_order_id)
                else:
                    return {"success": False, "msg": "Order not found"}, 400

                response = {
                    "gather_prompt": {
                        "text": gather_prompt_text,
                    },
                    "max_input_digits": 1,
                    "repeat_menu": 2,
                    "repeat_gather_prompt": {
                        "text": repeat_prompt_text
                    }
                }
                return response, 200
            else:
                return {"success": False, "msg": "Order not found"}, 400
        except Exception as e:
            return {'success': False}, 404


api.add_resource(CodVerificationGather, '/core/v1/verification/gather/<ver_type>')


@core_blueprint.route('/core/v1/passthru/<type>', methods=['GET'])
def verification_passthru(type):
    try:
        order_id = request.args.get('CustomField')
        digits = request.args.get('digits')
        recording_url = request.args.get('RecordingUrl')
        call_sid = request.args.get('CallSid')
        if digits:
            digits = digits.replace('"', '')
        if request.user_agent.browser == 'safari' and request.user_agent.platform=='iphone' and request.user_agent.version=='13.0.1':
            return jsonify({"success": True}), 200
        if order_id:
            order_id = int(order_id)
            if type=='cod':
                cod_ver = db.session.query(CodVerification).filter(CodVerification.order_id==order_id).first()
                if digits=="0" and cod_ver.order.status=='NEW':
                    cod_ver.order.status='CANCELED'
                    cancel_order_on_channels(cod_ver.order)
            elif type=='delivery':
                cod_ver = db.session.query(DeliveryCheck).filter(DeliveryCheck.order_id==order_id).first()
            elif type=='ndr':
                cod_ver = db.session.query(NDRVerification).filter(NDRVerification.order_id==order_id).first()
            else:
                return jsonify({"success": False, "msg": "Not found"}), 400
            cod_verified = None
            if call_sid:
                verified_via = 'call'
            else:
                verified_via = 'text'
                cod_ver.click_browser = request.user_agent.browser
                cod_ver.click_platform = request.user_agent.platform
                cod_ver.click_string = request.user_agent.string
                cod_ver.click_version = request.user_agent.version

            if digits=="1" or digits==None:
                cod_verified = True
            elif digits=="0":
                cod_verified = False
            cod_ver.call_sid = call_sid
            cod_ver.recording_url = recording_url
            if type == 'cod':
                cod_ver.cod_verified = cod_verified
            elif type=='delivery':
                cod_ver.del_verified = cod_verified
            elif type == 'ndr':
                ndr_obj = db.session.query(NDRShipments).filter(NDRShipments.order_id==cod_ver.order_id).order_by(NDRShipments.date_created.desc()).first()
                if ndr_obj:
                    ndr_obj.current_status = "reattempt" if verified_via=='text' or cod_verified==False else "cancelled"
                if verified_via=='text':
                    cod_ver.ndr_verified = False
                else:
                    cod_ver.ndr_verified = cod_verified

            cod_ver.verified_via = verified_via

            current_time = datetime.now()
            cod_ver.verification_time = current_time

            db.session.commit()
            return_template = """<html>
                              <head>
                                <link href="https://fonts.googleapis.com/css?family=Nunito+Sans:400,400i,700,900&display=swap" rel="stylesheet">
                              </head>
                                <style>
                                  body {
                                    text-align: center;
                                    padding: 40px 0;
                                    background: #EBF0F5;
                                  }
                                    h1 {
                                      color: #88B04B;
                                      font-family: "Nunito Sans", "Helvetica Neue", sans-serif;
                                      font-weight: 900;
                                      font-size: 40px;
                                      margin-bottom: 10px;
                                    }
                                    p {
                                      color: #404F5E;
                                      font-family: "Nunito Sans", "Helvetica Neue", sans-serif;
                                      font-size:20px;
                                      margin: 0;
                                    }
                                  i {
                                    color: #9ABC66;
                                    font-size: 100px;
                                    line-height: 200px;
                                    margin-left:-15px;
                                  }
                                  .card {
                                    background: white;
                                    padding: 60px;
                                    border-radius: 4px;
                                    box-shadow: 0 2px 3px #C8D0D8;
                                    display: inline-block;
                                    margin: 0 auto;
                                  }
                                </style>
                                <body>
                                  <div class="card">
                                  <div style="border-radius:200px; height:200px; width:200px; background: #F8FAF5; margin:0 auto;">
                                    <i class="checkmark">✓</i>
                                  </div>
                                    <h1>Success</h1> 
                                    <p>__TEXT__. Thank You!</p>
                                  </div>
                                </body>
                            </html>"""

            if type == 'cod':
                return_template = return_template.replace("__TEXT__","COD order confirmed successfully")
            elif type=='delivery':
                return_template = return_template.replace("__TEXT__","We'll call you soon")
            elif type == 'ndr':
                return_template = return_template.replace("__TEXT__","Delivery will be re-attempted soon")

            return render_template_string(return_template), 200
        else:
            return jsonify({"success": False, "msg": "No Order"}), 400
    except Exception as e:
        return jsonify({"success": False, "msg": str(e.args[0])}), 400


@core_blueprint.route('/core/v1/balance', methods=['GET'])
@authenticate_restful
def check_balance(resp):
    auth_data = resp.get('data')
    if auth_data.get('user_group') not in ('client', 'super-admin'):
        return jsonify({"msg": "Invalid user type"}), 400

    qs = db.session.query(ClientMapping).filter(ClientMapping.client_prefix==auth_data.get('client_prefix')).first()
    if not qs:
        return jsonify({"msg": "Not found"}), 400

    type = str(qs.account_type).lower()
    balance = qs.current_balance

    return jsonify({"type": type, "balance": round(balance, 2) if balance else 0}), 200


@core_blueprint.route('/core/v1/downloads', methods=['POST'])
@authenticate_restful
def get_downloads(resp):
    cur = conn.cursor()
    response = {'status': 'success', 'data': list(), "meta": dict()}
    auth_data = resp.get('data')
    data = json.loads(request.data)
    page = data.get('page', 1)
    per_page = data.get('per_page', 10)

    query_to_run = """SELECT created_by, type, title, download_link, status, date_created, file_size FROM downloads
                      __CLWH_FILTER__
                      __TYPE_FILTER__
                      order by date_created DESC 
                      __PAGINATION__"""

    query_to_run = query_to_run.replace('__CLWH_FILTER__', "WHERE created_by = '%s'"%auth_data.get('username'))
    query_to_run = query_to_run.replace('__TYPE_FILTER__', "AND type in %s" % str(tuple(auth_data.get('tabs'))))

    cur.execute(query_to_run.replace('__PAGINATION__', ""))
    total_count = cur.rowcount
    query_to_run = query_to_run.replace('__PAGINATION__', "OFFSET %s LIMIT %s"%(str((page-1)*per_page), str(per_page)))
    cur.execute(query_to_run)
    dl_qs = cur.fetchall()
    data = list()
    for dl in dl_qs:
        dl_obj = dict()
        dl_obj['created_by'] = dl[0]
        dl_obj['type'] = dl[1]
        dl_obj['title'] = dl[2]
        dl_obj['download_link'] = dl[3]
        dl_obj['status'] = dl[4]
        dl_obj['date_created'] = dl[5].strftime("%d %b %Y, %I:%M %p") if dl[5] else None
        file_size = None
        if dl[6] and dl[6]<100:
            file_size = str(dl[6]) + " KB"
        elif dl[6]:
            file_size = str(round(dl[6]/1024, 2)) + " MB"
        dl_obj['file_size'] = file_size
        data.append(dl_obj)

    response['data'] = data

    total_pages = math.ceil(total_count / per_page)
    response['meta']['pagination'] = {'total': total_count,
                                      'per_page': per_page,
                                      'current_page': page,
                                      'total_pages': total_pages}
    return jsonify(response), 200


@core_blueprint.route('/core/v1/create_payment', methods=['POST'])
@authenticate_restful
def create_payment(resp):
    auth_data = resp.get('data')
    data = json.loads(request.data)
    if auth_data.get('user_group') not in ('client', 'super-admin'):
        return jsonify({"msg": "Invalid user type"}), 400

    amount = data.get('amount')

    recharge_obj = ClientRecharges(client_prefix=auth_data.get('client_prefix'),
                                   recharge_amount=amount/100,
                                   type="credit",
                                   status="pending",
                                   recharge_time=datetime.utcnow()+timedelta(hours=5.5)
                                   )

    db.session.add(recharge_obj)
    db.session.commit()

    receipt = "receipt#"+str(recharge_obj.id)

    notes = {'client': str(auth_data.get('client_prefix'))}

    res = razorpay_client.order.create(dict(amount=amount, currency="INR", receipt=receipt, notes=notes))

    recharge_obj.transaction_id = res.get('id')

    db.session.commit()

    return jsonify({"key": "rzp_live_FGAwxhtumHezAw",
                    "amount": res.get('amount'),
                    "currency":"INR",
                    "order_id": res.get('id'),
                    "prefill": {"name": auth_data.get("first_name"),
                                "email": auth_data.get("email"),
                                "contact": auth_data.get("phone_no")},
                    "notes": notes,
                    "wareiq_id": recharge_obj.id
                    }), 201


@core_blueprint.route('/core/v1/capture_payment', methods=['POST'])
@authenticate_restful
def capture_payment(resp):
    auth_data = resp.get('data')
    data = json.loads(request.data)
    if auth_data.get('user_group') not in ('client', 'super-admin'):
        return jsonify({"msg": "Invalid user type"}), 400

    razorpay_payment_id = data.get('razorpay_payment_id')
    razorpay_order_id = data.get('razorpay_order_id')
    razorpay_signature = data.get('razorpay_signature')
    success = data.get('success')
    code = data.get('code')
    description = data.get('description')
    source = data.get('source')
    step = data.get('step')
    reason = data.get('reason')

    recharge_obj = db.session.query(ClientRecharges).filter(ClientRecharges.transaction_id==razorpay_order_id,
                                                            ClientRecharges.client_prefix==auth_data.get('client_prefix')).first()

    if not recharge_obj:
        return jsonify({"msg": "Transaction not found"}), 400

    if not success:
        recharge_obj.status = "failed"
        db.session.commit()
        return jsonify({"msg": "payment failed"}), 200

    params_dict = {
        'razorpay_order_id': razorpay_order_id,
        'razorpay_payment_id': razorpay_payment_id,
        'razorpay_signature': razorpay_signature
    }
    try:
        razorpay_client.utility.verify_payment_signature(params_dict)
    except Exception as e:
        recharge_obj.status = "failed"
        db.session.commit()
        return jsonify({"msg": "Signature verification failed"}), 400

    mapping_obj = db.session.query(ClientMapping).filter(ClientMapping.client_prefix==auth_data.get('client_prefix')).first()
    if mapping_obj:
        mapping_obj.current_balance = mapping_obj.current_balance + recharge_obj.recharge_amount

    #capture = razorpay_client.payment.capture(razorpay_payment_id, recharge_obj.recharge_amount*100, {"currency": "INR"})
    recharge_obj.bank_transaction_id = razorpay_payment_id
    recharge_obj.status = "successful"
    recharge_obj.recharge_time = datetime.utcnow() + timedelta(hours=5.5)
    recharge_obj.code = code
    recharge_obj.description = description
    recharge_obj.source = source
    recharge_obj.step = step
    recharge_obj.reason = reason
    recharge_obj.signature = razorpay_signature

    db.session.commit()

    return jsonify({"msg": "successfully captured"}), 200


@core_blueprint.route('/core/v1/payout/razorpayx', methods=['POST'])
def consume_x_payout():
    webhook_body = json.loads(request.data)
    webhook_signature = request.headers.get('X-Razorpay-Signature')
    webhook_secret = "OR2PXJ5KzWO2u9an7kw8"
    logger.info("webhook signature: "+str(webhook_signature))
    logger.info(json.dumps(webhook_body))
    try:
        #razorpay_client.utility.verify_webhook_signature(json.dumps(webhook_body), webhook_signature, webhook_secret)
        payout_id = webhook_body['payload']['payout']['entity']['id']
        mode = webhook_body['payload']['payout']['entity']['mode']
        status = webhook_body['payload']['payout']['entity']['status']
        transaction_id = webhook_body['payload']['payout']['entity']['utr']
        amount = float(webhook_body['payload']['payout']['entity']['amount'])/100
        remit_obj = db.session.query(CODRemittance).filter(CODRemittance.payout_id==payout_id).first()
        if not remit_obj:
            logger.error("remit obj not found: "+str(payout_id))
            return jsonify({"success": False}), 200

        remit_obj.status=status
        remit_obj.mode=mode
        remit_obj.transaction_id=transaction_id
        remit_obj.remitted_amount=amount
        db.session.commit()

    except Exception as e:
        logger.error("Exception occured: " + str(e.args))
        return jsonify({"success": False}), 200

    return jsonify({"success":True}), 200


@core_blueprint.route('/core/v1/thirdwatch/postback', methods=['POST'])
def thirdwatch_webhook():
    try:
        webhook_body = json.loads(request.data)
        client = db.session.query(ClientChannel).filter(ClientChannel.unique_parameter==webhook_body['merchant_identifier']).first()
        if not client:
            return jsonify({"success": False, "msg": "Merchant not found"}), 400

        event = webhook_body['event']
        order_data = webhook_body['payload']
        order = db.session.query(Orders).filter(Orders.client_prefix == client.client_prefix,
                                                Orders.client_channel_id == str(order_data['order_id'])).first()

        if event.lower()=='score':
            thirdwatch_obj = ThirdwatchData(order=order,
                                             flag=order_data['flag'],
                                             order_timestamp=order_data['order_timestamp'],
                                             score=order_data['score'],
                                             tags=order_data['tags'],
                                             reasons=order_data['reasons'],
                                             channel_order_id=str(order_data['order_id']),
                                             client_prefix=client.client_prefix,
                                             )

            db.session.add(thirdwatch_obj)
            db.session.commit()
            return jsonify({"success": True}), 200

        if not order:
            return jsonify({"success": False, "msg": "Order not found"}), 400

        if event.lower()=='action':
            if order_data['action_type'] == "declined" and order.status in ('NEW', 'READY TO SHIP', 'PICKUP REQUESTED'):
                order.status='CANCELED'
                db.session.commit()

        return jsonify({"success": True}), 200

    except Exception as e:
        return jsonify({"success": False}), 400


@core_blueprint.route('/core/v1/thirdwatch/postbackMerchant', methods=['POST'])
def thirdwatch_webhook_partner():
    try:
        webhook_body = json.loads(request.data)
        thirdwatch_obj = ThirdwatchData(order_id=int(webhook_body['order_id']),
                                         flag=webhook_body['flag'],
                                         order_timestamp=webhook_body['order_timestamp'],
                                         score=webhook_body['score'],
                                         tags=webhook_body['tags'],
                                         reasons=webhook_body['reasons']
                                         )

        db.session.add(thirdwatch_obj)
        db.session.commit()
        return jsonify({"success": True}), 200

    except Exception as e:
        return jsonify({"success": False}), 400


@core_blueprint.route('/core/v1/getshipcouriers', methods=['GET'])
@authenticate_restful
def getshiporders(resp):
    try:
        auth_data = resp.get('data')
        if auth_data.get('user_group') not in ('client', 'super-admin'):
            return jsonify({"msg": "Invalid user type"}), 400
        cur = conn.cursor()
        courier_list = list()
        cur.execute("""select aa.courier_name from master_couriers aa
                        left join (select * from client_couriers where client_prefix='__CLIENT_PREFIX__') bb on aa.id=bb.courier_id
                        where aa.integrated=true
                        order by bb.priority nulls last""".replace('__CLIENT_PREFIX__', auth_data.get('client_prefix')))
        all_cour = cur.fetchall()
        for cour in all_cour:
            courier_list.append(cour[0])

        return jsonify({"courier_list": courier_list}), 200
    except Exception:
        return jsonify({"success": False}), 400


@core_blueprint.route('/core/case_studies', methods=['GET'])
def website_case_study():
    data = [{"image_link": "https://wareiqfiles.s3.amazonaws.com/kamaayurveda.png",
            "summary": "WareIQ connects Kama Ayurveda’s existing supply chain infra & WareIQ fulfillment hubs to its central platform allowing them to offer premium shipping experience in sync with their brand positioning.",
            "name": "",
            "title": "",
            "case_study_link": ""},
            {"image_link": "https://wareiqfiles.s3.amazonaws.com/organicriot.png",
             "summary": "WareIQ fulfillment network allows Organic Riot to utilize pan-India WareIQ hubs and be quick to customers, and offer custom experiences like branded tracking page.",
             "name": "",
             "title": "",
             "case_study_link": ""},
            {"image_link": "https://wareiqfiles.s3.amazonaws.com/nasher.png",
             "summary": "WareIQ enabled Prime-like shipping on Nashermiles website by onboarding its network of fulfillment centers to its platform, and orchestrate heavy-item shipping.",
             "name": "",
             "title": "",
             "case_study_link": ""},
            {"image_link": "https://wareiqfiles.s3.amazonaws.com/zlade.png",
             "summary": "WareIQ enabled COD and NDR verification through automated SMS and IVR calls to preempt RTOS, and brought Zlade’s inventory closer to demand centers using our fulfillment network in metros.",
             "name": "",
             "title": "",
             "case_study_link": ""},
            {"image_link": "https://wareiqfiles.s3.amazonaws.com/timios.png",
             "summary": "WareIQ enabled fulfillment for Timios across its online channels: own website, WhatsApp, Amazon, Firstcry, Flipkart & BigBasket at competitive cost points required for a FMCG player.",
             "name": "",
             "title": "",
             "case_study_link": ""},
            {"image_link": "https://wareiqfiles.s3.amazonaws.com/sangeetha.png",
             "summary": "WareIQ allows Sangeetha Mobiles to leverage its existing network of retail stores to drive an omnichannel experience and enable ship from store, and warehouse - all centralized in one platform.",
             "name": "",
             "title": "",
             "case_study_link": ""},
            {"image_link": "https://wareiqfiles.s3.amazonaws.com/wingreens.png",
             "summary": "Wingreens Farms is an ethical and innovative farm to retail food and beverage company. WareIQ enables eCommerce fulfillment & shipping for Wingreens on their online-store and various marketplaces through our pan-India fulfillment network.",
             "name": "",
             "title": "",
             "case_study_link": ""},
            ]
    return jsonify({"success": True, "data": data}), 200


@core_blueprint.route('/core/v1/freshdesk_sso', methods=['GET'])
@authenticate_restful
def freshdesk_sso(resp):
    auth_data = resp.get('data')
    freshdesk_url = get_freshdesk_url(auth_data)
    return jsonify({"url": freshdesk_url}), 200


def get_freshdesk_url(auth_data):
    portal = "https://wareiq.freshdesk.com"

    if not auth_data.get('first_name'):
        name = auth_data.get('email')
    else:
        name = auth_data.get('first_name')
        if auth_data.get('last_name'):
            name += " "+auth_data.get('last_name')
    email = auth_data.get('email')
    company = auth_data.get('client_prefix') if auth_data.get('client_prefix') else auth_data.get('warehouse_prefix')
    timestamp = math.floor(time.time())
    hash_str = name + "6c8bed3ac91458e883ac1bd6f17b5614" + email + str(timestamp)
    fd_hash = hmac.new(
        key=bytes("6c8bed3ac91458e883ac1bd6f17b5614", 'utf-8'),
        msg=bytes(hash_str, 'utf-8'),
    ).hexdigest()
    req_url = (
        "%s/login/sso?name=%s&email=%s&timestamp=%s&company=%s&hash=%s&redirect_to=%s"
        % (portal, name, email, timestamp, company, fd_hash, "https://wareiq.freshdesk.com/support/home")
    )
    return req_url


def freshdesk_url_new(auth_data):
    payload = {
        "iat": int(time.time()),
        "jti": str(uuid.uuid1()),
        # populate these values from your data source
        "name": 'Ravi Chaudhary',
        "email": "ravi@wareiq.com"
    }

    jwt_string = jwt.encode(payload, """MIICXgIBAAKBgQDLPqlfK8X7nnTOUzeoqpB/ITv0BBWtGJfPBEHMs4hqj6f4QQWJ
                                        rDe4qyzN1FFUMZAyR2bCi05cDQPO9ZxKtzeoZKEVZLqyYooq4VmvPkY/aqfBNvw/
                                        mhwnwKv7wqiAD+L5jFWTztzQoI9WG5tcoW/GGWFVvJB/VPr17USLOiKGkQIDAQAB
                                        AoGBAJzg3ydAyG7x3kMr5Lhh/2rBol81j+2uH++A37E7mr516DctKTMdJV17cvfl
                                        PGl6YE0O4/iiQne27AuhdWQOi/wkYpLJdEPyu+3DLn2g++mOjLZmz97Pfu2qq7LP
                                        xrXNecE7ZWgfQkZ/9yTsgZ+4JiBWivWfPFykyikhIZuHdlxhAkEA7IIUY1/9vjCK
                                        JLbHSuNiZRVUEm6dmiksDMlwIsViMKIvl/opBPa/ESJEph5vYff2xcs7XC/9r4JD
                                        p2vxwlNWFQJBANv+yBpO7WhhrpZgUtMz2MnntEnRnCHyvALIx2yf9A9Kl8mClEXX
                                        1DvOFlQ/InUH91fSFNv9TBvL4mdQUmkX6Y0CQQCNHhCMOvpsQQosXeMMDCDpr5A6
                                        b0YadfaSqp7+IEg2PI+pXtqa25+l2YyckgxjqFDWNC13FCuKTW+QthfKm28BAkEA
                                        tUA9xwceLHDGcdichKvjwb2n2WXcGFAvvK+FNqa2Yt7XHcv6s2x1zGwoE3MMxZUx
                                        oOZSD0mqqgAZ4XrY2wM7hQJAexfCxK1RjEvKV2F4Wmwwkmn5wX7I89jzhwfTuBuk
                                        S7Kgp6BVj42oPe2vBPozV9UjH8iEKYzxuH/Ly/dnRfIGZA==""")
    sso_url = "https://wareiq-team.freshworks.com/sp/OIDC/250936934228312763/implicit?id_token=" + jwt_string

    return sso_url


@core_blueprint.route('/core/dev', methods=['POST'])
def ping_dev():
    # cur = conn.cursor()
    # myfile = request.files['myfile']
    # data_xlsx = pd.read_excel(myfile)
    # iter_rw = data_xlsx.iterrows()
    # for row in iter_rw:
    #     order_id = str(row[1].OrderID)
    #     try:
    #         cur.execute("""select id from orders where channel_order_id='%s' and client_prefix='HEALTHI'
    #                         and status in ('READY TO SHIP', 'PICKUP REQUESTED', 'NEW', 'NOT SHIPPED') and pickup_data_id=1215"""%order_id)
    #         unique_id = cur.fetchone()[0]
    #         awb = None
    #         cur.execute("SELECT awb from shipments where order_id=%s"%str(unique_id))
    #         try:
    #             awb = cur.fetchone()[0]
    #         except Exception:
    #             pass
    #
    #         if awb:
    #             cur.execute("UPDATE orders set status='READY TO SHIP' where id=%s"%str(unique_id))
    #         else:
    #             cur.execute("UPDATE orders set status='NEW' where id=%s"%str(unique_id))
    #
    #         conn.commit()
    #         cur.execute("""select master_product_id from op_association aa
    #                     left join master_products bb on aa.master_product_id=bb.id
    #                     where aa.order_id=__ORDER_ID__
    #                     and bb.sku ilike 'Hl.Kal%'""".replace('__ORDER_ID__', str(unique_id)))
    #
    #         ava_prod_ids = cur.fetchall()
    #         cur.execute("""select master_product_id from op_association aa
    #                                     left join master_products bb on aa.master_product_id=bb.id
    #                                     where aa.order_id=__ORDER_ID__
    #                                     and bb.sku not ilike 'Hl.Kal%'""".replace('__ORDER_ID__', str(unique_id)))
    #         unava_prod_ids = cur.fetchall()
    #         if ava_prod_ids and unava_prod_ids:
    #             cur.execute("""INSERT INTO orders (channel_order_id, order_date, customer_name, customer_email, customer_phone,
    #                     date_created, status, client_prefix,client_channel_id, delivery_address_id, order_id_channel_unique, status_type, status_detail, pickup_data_id, billing_address_id, chargeable_weight, master_channel_id)
    #                     select channel_order_id, order_date, customer_name, customer_email, customer_phone,
    #                     date_created, 'NOT SHIPPED', client_prefix,client_channel_id, delivery_address_id, order_id_channel_unique, status_type, status_detail, null, billing_address_id, chargeable_weight, master_channel_id
    #                     from orders where id=%s returning id""" % str(unique_id))
    #
    #             new_unique_id = cur.fetchone()[0]
    #
    #             cur.execute("""INSERT INTO orders_payments (payment_mode, amount, currency, order_id, shipping_charges, subtotal)
    #                 SELECT payment_mode, amount, currency, %s, shipping_charges, subtotal FROM orders_payments where order_id=%s"""%(str(new_unique_id), str(unique_id)))
    #
    #             cur.execute("""update op_association set order_id=%s where order_id=%s and master_product_id not in %s"""%(str(new_unique_id), str(unique_id), """(select master_product_id from op_association aa
    #                                             left join master_products bb on aa.master_product_id=bb.id
    #                                             where aa.order_id=__ORDER_ID__
    #                                             and bb.sku ilike 'Hl.Kal%')""".replace('__ORDER_ID__', str(unique_id))))
    #         conn.commit()
    #     except Exception as e:
    #         conn.rollback()
    #         print(str(row[1].OrderID)+": "+str(e.args[0]))
 #    cur = conn.cursor()
 #    cur.execute("""select id from orders where status in ('NEW') and id in
 #  (select order_id from
 #  (select order_id, count(*) as count_quan, sum(quantity) as sum_quan from op_association where order_id in
 #  (select order_id from op_association where master_product_id in (select id from master_products where sku='HL.Mil.SS800'))
 #  group by order_id
 #  order by count_quan DESC, sum_quan) xx
 #    where count_quan>1
 #  )
 # and channel_order_id in
 # ('25543','25540','25525','25512','25505','25464','25441','25427','25421','25420','25415','25413','25387','25384','25379','25378','25353','25337','25334','25297','25272','25236','25192','25178','25168','25167','25148','25142','25135','25109','25097','25046','25002','24946','24944','24940','24936','24935','24932','24907','24892','24888','24850','24835','24810','24783','24760','24751','24744','24743','24739','24734','24709','24685','24666','24633','24593','24575','24572','24570','24569','24560','24556','24543','24537','24533','24525','24508','24502','24480','24479','24477','24453','24448','24428','24427','24425','24377','24327','24324','24296','24292','24277','24251','24171','24110','24086','24072','24049','24024','24009','23985','23954','23938','23930','23929','23901','23900','23893','23891','23885','23870','23865','23864','23852','23849','23839','23836','23832','23813','23806','23796','23792','23783','23775','23746','23705','23694','23674','23662','23661','23659','23657','23655','23626','23577','23559','23513','23484','23431','23426','23351','23332','23282','23274','23272','23271','23269','23250','23204','23199','23190','23179','23155','23152','23150','23146','23101','23098','23068','23063','23050','23040','23035','23027','23019','23008','22964','22956','22950','22945','22937','22920','22912','22886','22833','22829','22827','22808','22806','22742','22728','22721','22709','22678','22658','22652','21968','21944','21941','21928','21919','21917','21906','21903','21901','21886','21881','21869','21868','21856','21836','21812','21803','21781','21777','21757','21753','21748','21744','21707','21685','21683','21668','21656','21644','21643','21624','21612','21611','21574','21573','21563','21558','21545','21542','21535','21490','21458','21402','21398','21391','21379','21373','21360','21337','21336','21334','21315','21282','21269','21243','21223','21214','21208','21203','21174','21120','21111','21103','20993','20973','20943','20941','20939','20910','20904','20898','20867','20860','20843','20819','20817','20816','20812','20804','20782','20766','20750','20708','20636','20594','20579','20577','20551','20532','20512','20467','20454','20451','20436','20417','20392','20329','20295','20293','20290','20288','20281','20279','20273','20234','20223','20215','20188','20177','20176','20166','20128','20090','20073','20061','20031','20026','20007','19992','19955','19946','19943','19939','19932','19930','19929','19896','19873','19850','19847','19844','19832','19824','19814','19812','19785','19776','19765','19731','19728','19719','19711','19675','19665','19649','19637','19629','19617','19614','19612','19611','19593','19584','19545','19539','19510','19483','19467','19446','19435','19421','19367','19360','19354','19353','19352','19345','19344','19343','19336','19329','19304','19296','19292','19283','19255','19198','19190','19184','19165','19158','19146','19143','19098','19093','19085','19058','19009','18994','18966','18962','18955','18952','18917','18889','18876','18872','18851','18828','18818','18783','18780','18767','18763','18757','18747','18733','18722','18710','18692','18691','18669','18648','18635','18627','18614','18575','18551','18544','18531','18526','18513','18505','18484','18465','18411','18295','18292','18276','18267','18258','18200','18189','18182','18173','18151','18147','18055','18040','18024','18006','17955','17942','17931','17893','17879','17877','17866','17859','17839','17831','17826','17823','17816','17802','17768','17747','17730','17728','17727','17725','17722','17704','17703','17676','17645','17633','16995','16967','16944','16915','16909','16900','16884','16878','16840','16839','16827','16826','16815','16777','16760','16743','16722','16713','16653','16636','16611','16586','16549','16534','16531','16482','16469','16461','16441','16426','16398','16393','16387','16367','16330','16316','16295','16291','16287','16242','16239','16223','16210','16176','16171','16164','16148','16128','16083','16052','16040','16039','16022','16004','15994','15979','15949','15930','15916','15903','15890','15883','15868','15852','15849','15840','15836','15829','15823','15795','15779','15762','15748','15745','15739','15706','15669','15641','15609','15583','15563','15523','15498','15495','15440','15413','15370','15341','15326','15321','15293','15269','15239','15238','15209','15201','15171','15146','15116','15089','15073','15072','15056','15048','15029','15010','14995','14971','14951','14946','14914','14906','14904','14898','14879','14863','14852','14844','14841','14831','14820','14792','14784','14762','14761','14752','14743','14720','14711','14688','14680','14650','14639','14611','14609','14574','14565','14538','14532','14493','14489','14467','14456','14442','14439','14431','14411','14406','14389','14376','14369','14367','14346','14331','14321','14292','14285','14281','14272','14266','14226','14209','14204','14192','14179','14171','14092','14087','14064','14028','14023','14015','13994','13976','13961','13956','13951','13943','13939','13901','13889','13873','13865','13824','13806','13762','13761','13727','13702','13692','13687','13674','13655','13646','13644','13598','13585','13522','13512','13497','13459','13448','13393','13380','13366','13334','13303','13296','13165','13164','13161','13157','13147','13122','13114','13112','13111','13088','13085','13081','13064','13061','13031','13029','13022','13009','12997','12981','12979','12955','12932','12926','12898','12848','12798','12781','12771','12672','12666','12667','12663','12661','12660','12643','12618','12590','12577','12531','12506','12505','12500','12492','12442','12418','12349','12315','12304','12269','12243','12121','12069','12017','12007','11975','11958','11929','11911','11903','11888','11828','11814','11780','11779','11766','11703','11695','11682','11679','11678','11669','11635','11611','11609','11597','11593','11582','11577','11567','11561','11538','11537','11533','11524','11519','11451','11435','11418','11377','11346','11342','11319','11318','11298','11287','11252','11251','11237','11236','11233','11185','11170','11142','11136','11101','11062','11034','11033','11015','11004','10943','10852','10818','10817','10802','10675','10655','10624','10543','10502','10462','10446','10419','10409','10382','10372','10276','10231','10228','10187','10159','10155','10149','10145','10140','10137','10125','10103','10102','10058','10055','10050','10043','10031','10021','10000','9988','9987','9973','9970','9963','9957','9954','9939','9929','9923','9922','9916','9903','9884','9882','9848','9841','9826','9804','9802','9791','9790','9784','9763','9699','9670','9660','9658','9654','9641','9638','9633','9630','9596','9595','9566','9545','9541','9528','9517','9498','9477','9393','9327','9313','9273','9261','9232','9198','9189','9177','9172','9144','9126','9111','9073','9018','9012','9007','8971','8963','8947','8945','8943','8937','8896','8889','8888','8842','8810','8788','8775','8766','8764','8750','8708','8580','8571','8558','8534','8525','8456','8450','8448','8409','8407','8376','8361','8356','8340','8337','8329','8320','8317','8311','8299','8261','8202','8194','8185','8136','8098','8090','8083','7999','7979','7958','7936','7888','7883','7851','7850','7841','7781','7779','7771','7747','7707','7632','7630','7617','7614','7595','7557','7548','7528','7523','7518','7517','7515','7512','7509','7495','7485','7472','7453','7447','7437','7408','7356','7329','7316','7309','7300','7280','7276','7243','7213','7150','7145','7126','7123','7119','7108','7102','7100','7039','6967','6965','6964','6953','6925','6903','6890','6879','6875','6828','6703','6652','6631','6630','6601','6526','6504','6500','6499','6489','6476','6456','6452','6442','6382','6380','6366','6348','6330','6305','6279','6277','6268','6210','6143','6130','6065','5863','5805','5796','5777','5711','5530','5490','5472','5457','5449','5390','5356','5350','5344','5336','5332','5309','5281','5271','5256','5255','5241','5231','5225','5178','5149','5148','3098','3005','2872','2843','2814','2269','2220','2150','2121','2060','2009','1853','1848','1829','25461','25440','25412','25250','25248','25226','25208','25207','25202','25161','25158','25021','24999','24965','24915','24886','24832','24805','24731','24697','24650','24648','24634','24523','24491','24461','24413','24387','24309','24259','24244','24179','24170','24130','24116','24067','24027','24015','24001','23995','23932','23920','23906','23882','23825','23755','23754','23743','23651','23638','23525','23465','23438','23403','23342','23335','23319','23263','23258','23255','23210','23195','23172','23122','23116','23082','23034','23021','22978','22947','22929','22884','22878','22845','22832','22794','22791','22788','22769','22765','22730','22690','22687','22680','22668','22651','22616','21915','21871','21851','21823','21738','21733','21705','21698','21645','21557','21527','21506','21475','21473','21468','21454','21446','21439','21404','21386','21378','21350','21331','21326','21309','21302','21292','21291','21289','21280','21216','21186','21181','21178','21172','21136','21131','21119','21117','21091','21089','21041','20936','20884','20866','20834','20796','20786','20716','20713','20710','20637','20635','20617','20564','20557','20538','20502','20497','20496','20484','20446','20422','20403','20350','20296','20294','20265','20248','20237','20222','20173','20168','20143','20139','20099','20065','20049','20041','19991','19941','19905','19871','19855','19845','19827','19794','19793','19742','19670','19663','19660','19573','19556','19543','19482','19448','19400','19384','19377','19371','19347','19316','19313','19277','19249','19196','19191','19189','19174','19161','19144','19139','19140','19131','19128','19123','19099','19004','18895','18854','18812','18788','18653','18571','18563','18522','18497','18478','18467','18402','18399','18372','18357','18278','18269','18237','18218','18209','18206','18204','18161','18160','18144','18112','18077','18068','17992','17981','17978','17976','17938','17919','17907','17835','17784','17732','17708','17675','17613','17601','16973','16928','16821','16788','16736','16683','16652','16646','16590','16581','16572','16530','16529','16395','16384','16378','16342','16322','16269','16261','16254','16246','16225','16174','16172','16152','16108','16102','16099','16085','16059','15997','15975','15967','15862','15855','15788','15750','15733','15732','15687','15654','15644','15635','15469','15442','15396','15395','15391','15379','15352','15311','15282','15273','15247','15218','15184','15183','15181','15166','15138','15120','15097','15062','15050','15032','14997','14965','14956','14891','14855','14854','14835','14779','14755','14745','14719','14673','14580','14567','14413','14381','14344','14335','14194','14172','14167','14132','14072','14060','14057','14027','13957','13855','13837','13745','13739','13730','13718','13630','13608','13518','13511','13419','13402','13273','13258','13228','13224','13169','13156','13152','13053','13040','13036','12950','12931','12902','12796','12782','12740','12633','12543','12538','12534','12372','12367','12309','12283','12270','12244','12209','12202','12200','12188','12187','12148','12134','12055','11943','11922','11893','11890','11807','11752','11723','11718','11711','11628','11581','11549','11532','11502','11485','11465','11419','11386','11381','11355','11266','11196','11174','11122','11084','11080','11055','10978','10927','10904','10687','10666','10631','10605','10549','10503','10488','10475','10465','10463','10455','10437','10427','10413','10334','10327','10325','10321','10294','10284','10211','10204','10169','10151','10098','10033','9849','9801','9776','9770','9726','9681','9594','9583','9569','9564','9559','9554','9543','9404','9376','9353','9336','9328','9321','9245','9227','9187','9185','9164','9084','9044','9041','9009','8972','8969','8951','8936','8894','8854','8837','8807','8742','8738','8670','8646','8626','8546','8476','8452','8379','8375','8334','8330','8296','8224','8189','8168','7985','7977','7960','7935','7924','7881','7875','7869','7856','7838','7827','7764','7749','7715','7705','7693','7667','7651','7583','7570','7482','7461','7423','7412','7384','7301','7279','7238','7222','7153','7140','7069','7029','6988','6981','6901','6831','6786','6777','6775','6717','6654','6640','6554','6514','6458','6448','6446','6423','6242','6144','6046','6009','5943','5577','5554','5489','5474','5293','5279')
 #
 # """)
 #    all_orders = cur.fetchall()
 #    for order in all_orders:
 #        cur.execute("""INSERT INTO orders (channel_order_id, order_date, customer_name, customer_email, customer_phone,
 #        date_created, status, client_prefix,client_channel_id, delivery_address_id, order_id_channel_unique, status_type, status_detail, pickup_data_id, billing_address_id, chargeable_weight, master_channel_id)
 #        select channel_order_id, order_date, customer_name, customer_email, customer_phone,
 #        date_created, 'NEW', client_prefix,client_channel_id, delivery_address_id, order_id_channel_unique, status_type, status_detail, pickup_data_id, billing_address_id, chargeable_weight, master_channel_id
 #        from orders where id=%s returning id"""%str(order[0]))
 #
 #        order_id = cur.fetchone()[0]
 #
 #        cur.execute("""INSERT INTO orders_payments (payment_mode, amount, currency, order_id, shipping_charges, subtotal)
 #        SELECT payment_mode, amount, currency, %s, shipping_charges, subtotal FROM orders_payments where order_id=%s"""%(str(order_id), str(order[0])))
 #
 #        cur.execute("""update op_association set order_id=%s where order_id=%s and master_product_id!=58054"""%(str(order_id), str(order[0])))
 #        conn.commit()
    return 0
    # url = "https://stage.lotus-organics.com/api/v1/order/wareiq/update"
    # headers = {"Content-Type": "application/json",
    #            "x-api-key": "901192e41675e1b908d26a7e95c77ddc"}
    # data  = {
    #     "id": 3724,
    #     "ware_iq_id": 349098,
    #     "awb_number": "3992413193326",
    #     "status_information": "IN TRANSIT"
    # }
    #
    # import requests
    # requests.put(url, headers, data=data)
    #
    # return 0
    # myfile = request.files['myfile']
    # data_xlsx = pd.read_excel(myfile)
    # from .models import Products, OrdersPayments
    # import json, re
    # count = 0
    # iter_rw = data_xlsx.iterrows()
    # for row in iter_rw:
    #     sku = row[1].SKU
    #     try:
    #         prod_obj = db.session.query(Products).filter(Products.client_prefix == 'NASHER',
    #                                                      Products.master_sku == sku).first()
    #         if not prod_obj:
    #             dimensions = re.findall(r"[-+]?\d*\.\d+|\d+", str(row[1].Dimensions))
    #
    #             dimensions = {"length": float(dimensions[0]), "breadth": float(dimensions[1]),
    #                           "height": float(dimensions[2])}
    #             prod_obj_x = Products(name=str(row[1].Name),
    #                                   sku=str(sku),
    #                                   master_sku=str(sku),
    #                                   dimensions=dimensions,
    #                                   weight=float(row[1].Weight),
    #                                   price=float(float(row[1].Price)),
    #                                   client_prefix='NASHER',
    #                                   active=True,
    #                                   channel_id=4,
    #                                   date_created=datetime.now()
    #                                   )
    #             db.session.add(prod_obj_x)
    #             if row[0]%50==0:
    #                 db.session.commit()
    #     except Exception as e:
    #         print(str(sku) + "\n" + str(e.args[0]))
    #         db.session.rollback()
    # db.session.commit()
#     import requests
#     cur = conn.cursor()
#     cur.execute("""select aa.order_id_channel_unique, ee.courier_name, bb.awb, cc.unique_parameter from orders aa
# left join shipments bb on aa.id=bb.order_id
# left join client_couriers cc on aa.client_prefix=cc.client_prefix and bb.courier_id=cc.courier_id
# left join (select * from order_status where status='Received') dd on dd.order_id=aa.id
# left join master_couriers ee on ee.id=bb.courier_id
# where aa.client_prefix='ACTIFIBER'
# and dd.status_time>'2021-01-06'
# and aa.status in ('PICKUP REQUESTED', 'READY TO SHIP')""")
#     all_orders = cur.fetchall()
#     for order in all_orders:
#         api_token="66520512ca5f160015a23617b8ecaa546cc20a6af846b958b2b9b9c31fb6b12f"
#         post_url = "https://api.easyecom.io/orders/updateQC?api_token=%s" % api_token
#         post_body = {
#             "invoiceId": order[0],
#             "api_token": api_token,
#             "courier": order[1],
#             "awbNum": order[2],
#             "companyCarrierId": int(order[3])
#         }
#         req = requests.post(post_url, data=post_body)
#     return 0
    # cur_2 = conn_2.cursor()
    # myfile = request.files['myfile']
    # data_xlsx = pd.read_excel(myfile)
    # from .models import Products, ProductQuantity
    # uri = """requests.get("https://www.nyor.in/wp-json/wc/v3/orders?oauth_consumer_key=ck_1e1ab8542c4f22b20f1b9810cd670716bf421ba8&oauth_timestamp=1583243314&oauth_nonce=kYjzVBB8Y0ZFabxSWbWovY3uYSQ2pTgmZeNu2VS4cg&oauth_signature=d07a4be56681016434803eb054cfd8b45a8a2749&oauth_signature_method=HMAC-SHA1")"""
    # for row in data_xlsx.iterrows():
    #     """
    #     cur_2.execute("select city from city_pin_mapping where pincode='%s'" % str(row[1].delivery_pincode))
    #     des_city = cur_2.fetchone()
    #     if not des_city:
    #         cur_2.execute("select city from city_pin_mapping where city ilike '%s'" % str(row[1].city))
    #         des_city = cur_2.fetchone()
    #         if not des_city:
    #             cur_2.execute("insert into city_pin_mapping (pincode,city) VALUES ('%s','%s');" % (
    #             str(row[1].pincode), str(row[1].city)))
    #         else:
    #             cur_2.execute("insert into city_pin_mapping (pincode,city) VALUES ('%s','%s');" % (
    #                 str(row[1].pincode), str(des_city[0])))
    #
    #     """
    #     cur_2.execute("select city from city_pin_mapping where pincode='%s'" % str(row[1].delivery_pincode))
    #     des_city = cur_2.fetchone()
    #     cur_2.execute("select city from city_pin_mapping where pincode='%s'" % str(row[1].pickup_pincode))
    #     pick_city = cur_2.fetchone()
    #     if not pick_city or not des_city:
    #         print(str(row[1].delivery_pincode)+ "  "+ str(row[1].pickup_pincode))
    #
    #     cur_2.execute("select zone_value from city_zone_mapping where zone='%s' and city='%s';" % (
    #     str(pick_city[0]), str(des_city[0])))
    #     mapped_pin = cur_2.fetchone()
    #     if not mapped_pin:
    #         cur_2.execute("insert into city_zone_mapping (zone,city,zone_value,courier_id) VALUES ('%s','%s','%s',%s);" % (
    #         str(pick_city[0]), str(des_city[0]), str(row[1].zone), 1))
    #     else:
    #         print("Zone found for this: "+str(row[1].delivery_pincode)+ "  "+ str(row[1].pickup_pincode))
    #
    #     """
    #
    #     cur_2.execute("select zone_value from city_zone_mapping where zone='%s' and city='%s' and courier_id=%s" % (
    #         str(row[1].origin_city), str(row[1].destination_city), 2))
    #     mapped_pin = cur_2.fetchone()
    #     if not mapped_pin:
    #         cur_2.execute("insert into city_zone_mapping (zone,city,courier_id) VALUES ('%s','%s', %s);" % (
    #             str(row[1].origin_city), str(row[1].destination_city), 2))
    #     """
    # return 0
    # from .models import CostToClients
    # myfile = request.files['myfile']
    # data_xlsx = pd.read_excel(myfile)
    #
    # iter_rw = data_xlsx.iterrows()
    # for row in iter_rw:
    #     if int(int(row[1].Courier))==16:
    #         for courier_id in (1,13,15,9,4):
    #             cost_obj = CostToClients(client_prefix=str(row[1].Client),
    #                                      courier_id=courier_id,
    #                                      zone_a=float(row[1].A),
    #                                      zone_b=float(row[1].B),
    #                                      zone_c=float(row[1].C),
    #                                      zone_d=float(row[1].D),
    #                                      zone_e=float(row[1].E),
    #                                      a_step=float(row[1].AS),
    #                                      b_step=float(row[1].BS),
    #                                      c_step=float(row[1].CS),
    #                                      d_step=float(row[1].DS),
    #                                      e_step=float(row[1].ES),
    #                                      cod_min=float(row[1].COD),
    #                                      cod_ratio=float(row[1].CODRATIO),
    #                                      rto_ratio=1,
    #                                      rvp_ratio=1.5,
    #                                      management_fee_static=5,
    #                                      management_fee=None,
    #                                      )
    #             db.session.add(cost_obj)
    #             db.session.commit()
    #     else:
    #         cost_obj = CostToClients(client_prefix=str(row[1].Client),
    #                                  courier_id=int(row[1].Courier),
    #                                  zone_a=float(row[1].A),
    #                                  zone_b=float(row[1].B),
    #                                  zone_c=float(row[1].C),
    #                                  zone_d=float(row[1].D),
    #                                  zone_e=float(row[1].E),
    #                                  a_step=float(row[1].AS),
    #                                  b_step=float(row[1].BS),
    #                                  c_step=float(row[1].CS),
    #                                  d_step=float(row[1].DS),
    #                                  e_step=float(row[1].ES),
    #                                  cod_min=float(row[1].COD),
    #                                  cod_ratio=float(row[1].CODRATIO),
    #                                  rto_ratio=1,
    #                                  rvp_ratio=1.5,
    #                                  management_fee_static=5,
    #                                  management_fee=None,
    #                                  )
    #         db.session.add(cost_obj)
    #         db.session.commit()
    #
    # return 0
    # from .models import Orders, ReturnPoints, ClientPickups, Products, ProductQuantity
    # myfile = request.files['myfile']
    # data_xlsx = pd.read_excel(myfile)
    # import json, re
    # count = 0
    # iter_rw = data_xlsx.iterrows()
    # for row in iter_rw:
    #     try:
    #         sku = str(row[1].SKU)
    #         qs = db.session.query(Products).filter(Products.master_sku==sku, Products.client_prefix=='VENUS').first()
    #         if not qs:
    #             dimensions = {"length": 5, "breadth": 5,
    #                           "height": 5}
    #             weight = 0.01
    #             prod_obj = Products(name=str(row[1].Name),
    #                                 sku=str(sku),
    #                                 master_sku=str(sku),
    #                                 dimensions=dimensions,
    #                                 weight=weight,
    #                                 price=None,
    #                                 client_prefix='VENUS',
    #                                 active=True,
    #                                 channel_id=4,
    #                                 inactive_reason=None,
    #                                 date_created=datetime.now()
    #                                 )
    #             db.session.add(prod_obj)
    #         else:
    #             qs.name = str(row[1].Name)
    #
    #
    #         db.session.commit()
    #
    #
    #     except Exception as e:
    #         pass
    #
    #
    # return 0
    """
    import requests
    create_fulfillment_url = "https://app.easyecom.io/orders/getAllOrders?api_token=8ad11f5f608737f85bc0a5d04aa954d75ad378202c0800fa195d9738efd94a44&start_date=2020-10-15&end_date=2020-10-21"
    req = requests.get(create_fulfillment_url)
    create_fulfillment_url = "https://dc948a1330721a0116d84fb76ab168c4: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@daily-veggies-india.myshopify.com/admin/api/2020-07/orders/2728800518305.json?"
    return 0
    """
    myfile = request.files['myfile']
    import json, requests
    data_xlsx = pd.read_excel(myfile)

    iter_rw = data_xlsx.iterrows()
    source_items = list()
    sku_list = list()
    """
    cur = conn_2.cursor()
    for row in iter_rw:
        pickup_city = str(row[1].pickup_city)
        cur.execute("SELECT city FROM city_pin_mapping where pincode='%s'" % str(row[1].pickup_pincode))
        try:
            pickup_city = cur.fetchone()[0]
        except Exception:
            cur.execute("INSERT INTO city_pin_mapping (pincode, city, district) VALUES (%s,%s,%s)", (str(row[1].pickup_pincode), pickup_city, pickup_city))

        del_city = str(row[1].des_city)
        cur.execute("SELECT city FROM city_pin_mapping where pincode='%s'" % str(row[1].delivery_pincode))
        try:
            del_city = cur.fetchone()[0]
        except Exception:
            cur.execute("INSERT INTO city_pin_mapping (pincode, city, district) VALUES (%s,%s,%s)", (str(row[1].delivery_pincode), del_city, del_city))

        for courier_id in (1,2):
            cur.execute("SELECT zone_value from city_zone_mapping where zone='%s' and city='%s' and courier_id=%s" %(pickup_city, del_city, str(courier_id)))
            ent = None
            try:
                ent = cur.fetchone()[0]
            except Exception:
                pass
            if not ent:
                cur.execute("INSERT INTO city_zone_mapping (zone, city, zone_value, courier_id) VALUES (%s,%s,%s,%s)", (pickup_city, del_city, str(row[1].zone), courier_id))
                
    """
    for row in iter_rw:
        try:
            sku = str(row[1].SKU)
            del_qty = int(row[1].Qty)
            # cb_qty = int(row[1].CBQT)
            # mh_qty = int(row[1].MHQT)
            """
            source_items.append({
                "sku": sku,
                "source_code": "default",
                "quantity": del_qty + cb_qty + mh_qty,
                "status": 1
            })
            """

            """
            data = {"sku_list": [{"sku": sku,
                                  "warehouse": "DLWHEC",
                                  "quantity": del_qty,
                                  "type": "replace",
                                  "remark": "30th aug resync"}]}
            req = requests.post("http://track.wareiq.com/products/v1/update_inventory", headers=headers,
                                data=json.dumps(data))

            """
            sku_list.append({"sku": sku,
                             "warehouse": "TNPMRO",
                             "quantity": del_qty,
                             "type": "subtract",
                             "remark": "21 feb sale"})

            """

            data = {"sku_list": [{"sku": sku,
                                  "warehouse": "MHWHECB2C",
                                  "quantity": mh_qty,
                                  "type": "replace",
                                  "remark": "30th aug resync"}]}
            req = requests.post("http://track.wareiq.com/products/v1/update_inventory", headers=headers,
                                data=json.dumps(data))

            combo = str(row[1].SKU)
            combo_prod = str(row[1].childsku)
            combo = db.session.query(Products).filter(Products.master_sku == combo,
                                                      Products.client_prefix == 'URBANGABRU').first()
            combo_prod = db.session.query(Products).filter(Products.master_sku == combo_prod,
                                                           Products.client_prefix == 'URBANGABRU').first()
            if combo and combo_prod:
                combo_obj = ProductsCombos(combo=combo,
                                           combo_prod=combo_prod,
                                           quantity=int(row[1].qty))
                db.session.add(combo_obj)
            else:
                pass
            """
            if row[0]%100==0:
                headers = {
                    'Authorization': "Bearer " + "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE2MTU1NzI1MzUsImlhdCI6MTYxMjk4MDUzNSwic3ViIjo5fQ.H4MjEuE2zhPqSufcyC9WIr27coPALTn7IIlmgbtCnxI",
                    'Content-Type': 'application/json'}

                data = {"sku_list": sku_list}
                req = requests.post("https://track.wareiq.com/products/v1/update_inventory", headers=headers,
                                    data=json.dumps(data))

                sku_list = list()



        except Exception as e:
            pass

    headers = {
        'Authorization': "Bearer " + "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE2MTU1NzI1MzUsImlhdCI6MTYxMjk4MDUzNSwic3ViIjo5fQ.H4MjEuE2zhPqSufcyC9WIr27coPALTn7IIlmgbtCnxI",
        'Content-Type': 'application/json'}

    data = {"sku_list": sku_list}
    req = requests.post("https://track.wareiq.com/products/v1/update_inventory", headers=headers,
                        data=json.dumps(data))

    return 0
    return 0
    import boto3
    return 0
    from woocommerce import API
    wcapi = API(
        url="https://nchantstore.com",
        consumer_key="ck_9b1c9a4774b6453e99cacceb15d99da56843a54d",
        consumer_secret="cs_ba1766d2e5f3a070e419039308a1c59f18ad57bf",
        version="wc/v3"
    )
    r = wcapi.get('products?per_page=100&page=1')
    from botocore.exceptions import ClientError

    # Replace sender@example.com with your "From" address.
    # This address must be verified with Amazon SES.
    SENDER = "WareIQ <noreply@wareiq.com>"

    # Replace recipient@example.com with a "To" address. If your account
    # is still in the sandbox, this address must be verified.
    RECIPIENT = "ravi@wareiq.com"

    # Specify a configuration set. If you do not want to use a configuration
    # set, comment the following variable, and the
    # ConfigurationSetName=CONFIGURATION_SET argument below.
    CONFIGURATION_SET = "ConfigSet"

    # If necessary, replace us-west-2 with the AWS Region you're using for Amazon SES.
    AWS_REGION = "us-east-1"

    # The subject line for the email.
    SUBJECT = "Amazon SES Test (SDK for Python)"

    # The email body for recipients with non-HTML email clients.
    BODY_TEXT = ("Amazon SES Test (Python)\r\n"
                 "This email was sent with Amazon SES using the "
                 "AWS SDK for Python (Boto)."
                 )

    # The HTML body of the email.
    BODY_HTML = """<html>
    <head></head>
    <body>
      <h1>Amazon SES Test (SDK for Python)</h1>
      <p>This email was sent with
        <a href='https://aws.amazon.com/ses/'>Amazon SES</a> using the
        <a href='https://aws.amazon.com/sdk-for-python/'>
          AWS SDK for Python (Boto)</a>.</p>
    </body>
    </html>
                """

    # The character encoding for the email.
    CHARSET = "UTF-8"

    # Create a new SES resource and specify a region.
    client = boto3.client('ses', region_name=AWS_REGION, aws_access_key_id='AKIAWRT2R3KC3YZUBFXY',
    aws_secret_access_key='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')

    # Try to send the email.
    try:
        # Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
        )
    # Display an error if something goes wrong.
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])
    return 0


    exotel_idx = 0
    exotel_sms_data = {
        'From': '01141182252'
    }

    myfile = request.files['myfile']
    data_xlsx = pd.read_excel(myfile)

    iter_rw = data_xlsx.iterrows()
    for row in iter_rw:
        sms_to_key = "Messages[%s][To]" % str(exotel_idx)
        sms_body_key = "Messages[%s][Body]" % str(exotel_idx)
        sms_From_key = "Messages[%s][From]" % str(exotel_idx)

        exotel_sms_data[sms_to_key] = "0"+str(row[1].Number)
        exotel_sms_data[sms_From_key] = "01141182252"
        exotel_sms_data[
            sms_body_key] = str(row[1].SMS) + ". Thanks!"
        exotel_idx += 1

    lad = requests.post(
        'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend',
        data=exotel_sms_data)

    return 0

    return 0

    return 0
    from woocommerce import API
    wcapi = API(
        url="https://lmdot.com",
        consumer_key="ck_c5b8db7f9451efc310dd4506a1eed5e8aecd6ffe",
        consumer_secret="cs_71b436acd31c9c7f6d354330cdf84f26d05b7d94",
        version="wc/v3"
    )
    r = wcapi.get('orders')
    return 0

    return 0
    import json, requests
    req = requests.post("https://www.sangeethamobiles.com/get-sku-list", data={"request_from":"warelq"})
    count = 1
    for prod in req.json()['data']:
        prod_obj_x = Products(name=prod['product_name'],
                              sku=prod['product_sku'],
                              master_sku=prod['product_sku'],
                              dimensions=None,
                              weight=None,
                              price=None,
                              client_prefix='SANGEETHA',
                              active=True,
                              channel_id=4,
                              date_created=datetime.now()
                              )

        db.session.add(prod_obj_x)
        count += 1
        if count%100==0:
            db.session.commit()
        # prod_quan_obj = ProductQuantity(product=prod_obj_x,
        #                                 total_quantity=0,
        #                                 approved_quantity=0,
        #                                 available_quantity=0,
        #                                 inline_quantity=0,
        #                                 rto_quantity=0,
        #                                 current_quantity=0,
        #                                 warehouse_prefix="QSDWARKA",
        #                                 status="APPROVED",
        #                                 date_created=datetime.now()
        #                                 )
        #
        # db.session.add(prod_quan_obj)


    return 0
    since_id = "1"
    count = 250
    while count == 250:
        create_fulfillment_url = "https://f2e810c7035e1653f0191cb8f5da58f6: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@rattleandco.myshopify.com/admin/api/2020-07/products.json?limit=250&since_id=%s"%since_id
        qs = requests.get(create_fulfillment_url)
        for prod in qs.json()['products']:
            for prod_obj in prod['variants']:
                prod_obj_x = db.session.query(Products).filter(Products.sku == str(prod_obj['id'])).first()
                if prod_obj_x:
                    prod_obj_x.master_sku = prod_obj['sku']
                else:
                    prod_name = prod['title']
                    if prod_obj['title'] != 'Default Title':
                        prod_name += " - " + prod_obj['title']
                    prod_obj_x = Products(name=prod_name,
                                          sku=str(prod_obj['id']),
                                          master_sku=str(prod_obj['sku']),
                                          dimensions=None,
                                          weight=None,
                                          price=float(prod_obj['price']),
                                          client_prefix='RATTLEANDCO',
                                          active=True,
                                          channel_id=1,
                                          date_created=datetime.now()
                                          )

                    db.session.add(prod_obj_x)
                    # prod_quan_obj = ProductQuantity(product=prod_obj_x,
                    #                                 total_quantity=0,
                    #                                 approved_quantity=0,
                    #                                 available_quantity=0,
                    #                                 inline_quantity=0,
                    #                                 rto_quantity=0,
                    #                                 current_quantity=0,
                    #                                 warehouse_prefix="QSDWARKA",
                    #                                 status="APPROVED",
                    #                                 date_created=datetime.now()
                    #                                 )
                    #
                    # db.session.add(prod_quan_obj)

        count = len(qs.json()['products'])
        since_id = str(qs.json()['products'][-1]['id'])


    import requests

    since_id = "1"
    count = 250
    myfile = request.files['myfile']
    while count==250:
        create_fulfillment_url = "https://f2e810c7035e1653f0191cb8f5da58f6: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@rattleandco.myshopify.com/admin/api/2020-07/products.json?limit=250&since_id=%s"%since_id
        qs = requests.get(create_fulfillment_url)
        for prod in qs.json()['products']:
            for prod_obj in prod['variants']:
                prod_name = prod['title']
                quan = 0
                if not prod_obj['sku']:
                    continue
                master_sku = str(prod_obj['sku'])
                data_xlsx = pd.read_excel(myfile)
                iter_rw = data_xlsx.iterrows()
                for row in iter_rw:
                    try:
                        if master_sku == str(row[1].SKU):
                            quan = str(row[1].quan)
                            break
                    except Exception as e:
                        pass
                if prod_obj['title'] != 'Default Title':
                    prod_name += " - " + prod_obj['title']
                if not quan:
                    continue
                prod_obj_x = db.session.query(Products).filter(Products.sku == str(prod_obj['id'])).first()
                if not prod_obj_x:
                    prod_obj_x = Products(name=prod_name,
                                          sku=str(prod_obj['id']),
                                          master_sku=master_sku,
                                          dimensions=None,
                                          weight=None,
                                          price=float(prod_obj['price']),
                                          client_prefix='SPORTSQVEST',
                                          active=True,
                                          channel_id=1,
                                          date_created=datetime.now()
                                          )

                    db.session.add(prod_obj_x)

                if quan:
                    prod_quan_obj = ProductQuantity(product=prod_obj_x,
                                                    total_quantity=quan,
                                                    approved_quantity=quan,
                                                    available_quantity=quan,
                                                    inline_quantity=0,
                                                    rto_quantity=0,
                                                    current_quantity=quan,
                                                    warehouse_prefix="QSBHIWANDI",
                                                    status="APPROVED",
                                                    date_created=datetime.now()
                                                    )

                    db.session.add(prod_quan_obj)
        count = len(qs.json()['products'])
        since_id = str(qs.json()['products'][-1]['id'])
        db.session.commit()

    db.session.commit()
    import requests, json

    return 0

    lambda_handler()
    return 0
    from requests_oauthlib.oauth1_session import OAuth1Session
    auth_session = OAuth1Session("ck_43f358286bc3a3a30ffd00e22d2282db07ed7f5d",
                                 client_secret="cs_970ec6a2707c17fc2d04cc70e87972faf3c98918")
    url = '%s/wp-json/wc/v3/orders/%s' % ("https://bleucares.com", str(6613))
    status_mark = "completed"
    if not status_mark:
        status_mark = "completed"
    r = auth_session.post(url, data={"status": status_mark})

    from .fetch_orders import lambda_handler
    lambda_handler()

    #push magento inventory
    cur = conn.cursor()

    cur.execute("""select master_sku, GREATEST(available_quantity, 0) as available_quantity from
                    (select master_sku, sum(available_quantity) as available_quantity from products_quantity aa
                    left join products bb on aa.product_id=bb.id
                    where bb.client_prefix='KAMAAYURVEDA'
                    group by master_sku
                    order by available_quantity) xx""")

    all_quan = cur.fetchall()
    source_items = list()
    for quan in all_quan:
        source_items.append({
            "sku": quan[0],
            "source_code": "default",
            "quantity": quan[1],
            "status": 1
        })

    magento_url = "https://www.kamaayurveda.com/rest/default/V1/inventory/source-items"
    body = {
        "sourceItems": source_items}
    headers = {'Authorization': "Bearer q4ldi2wasczvm7l8caeyozgkxf4qanfr",
               'Content-Type': 'application/json'}
    r = requests.post(magento_url, headers=headers, data=json.dumps(body))




    return 0
    import requests, json
    magento_orders_url = """%s/V1/orders/12326""" % (
        "https://magento.feelmighty.com/rest")
    headers = {'Authorization': "Bearer " + "f3ekur9ci3gc0cb63y743dvcy3ptyxe5",
               'Content-Type': 'application/json'}
    data = requests.get(magento_orders_url, headers=headers)

    return 0




    return 0



    from .fetch_orders import lambda_handler
    lambda_handler()
    from .models import PickupPoints, ReturnPoints, ClientPickups, Products, ProductQuantity, ProductsCombos, Orders


    db.session.commit()
    import requests
    myfile = request.files['myfile']

    db.session.commit()
    import requests
    req = requests.get("https://api.ecomexpress.in/apiv2/fetch_awb/?username=warelqlogisticspvtltd144004_pro&password=LdGvdcTFv6n4jGMT&count=10000&type=PPD")

    from requests_oauthlib import OAuth1Session
    auth_session = OAuth1Session("ck_cd462226a5d5c21c5936c7f75e1afca25b9853a6",
                                 client_secret="cs_c897bf3e770e15f518cba5c619b32671b7cc527c")

    order_qs = db.session.query(Orders).filter(Orders.client_prefix=='ZLADE', Orders.status.in_(['DELIVERED'])).all()
    for order in order_qs:
        if order.order_id_channel_unique:
            url = '%s/wp-json/wc/v3/orders/%s' % ("https://www.zladeformen.com", order.order_id_channel_unique)
            r = auth_session.post(url, data={"status": "completed"})

    import requests, json



    return 0


    count = 0
    return 0

    import requests
    create_fulfillment_url = "https://87c506a89d76c5815d7f1c4f782a4bef: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@perfour.myshopify.com/admin/api/2020-07/orders.json"
    qs = requests.get(create_fulfillment_url)
    from .fetch_orders import lambda_handler
    lambda_handler()


    return 0

    from .models import PickupPoints, ReturnPoints, ClientPickups, Products, ProductQuantity
    import requests, json, xmltodict
    req = requests.get("https://plapi.ecomexpress.in/track_me/api/mawbd/?awb=8636140444,8636140446,8636140435&username=warelqlogisticspvtltd144004_pro&password=LdGvdcTFv6n4jGMT")
    req = json.loads(json.dumps(xmltodict.parse(req.content)))
    cancel_body = json.dumps({"AWBNumber": "14201720011569", "XBkey": "NJlG1ISTUa2017XzrCG6OoJng",
                              "RTOReason": "Cancelled by seller"})
    headers = {"Authorization": "Basic " + "NJlG1ISTUa2017XzrCG6OoJng",
               "Content-Type": "application/json"}
    req_can = requests.post("http://xbclientapi.xbees.in/POSTShipmentService.svc/RTONotifyShipment",
                            headers=headers, data=cancel_body)



    import requests
    from .models import Orders, ProductsCombos, ClientPickups, Products, ProductQuantity, PickupPoints, ReturnPoints
    myfile = request.files['myfile']
    data_xlsx = pd.read_excel(myfile)
    import json, re
    count = 0
    iter_rw = data_xlsx.iterrows()
    for row in iter_rw:
        try:
            warehouse_prefix= "FURTADOS_"+str(row[1].SNo)
            pickup_point = PickupPoints(pickup_location = str(row[1].SellerName),
                                        name = str(row[1].SellerName),
                                        phone =str(row[1].Contact),
                                        address = str(row[1].SellerAddress),
                                        address_two = "",
                                        city = str(row[1].City),
                                        state = str(row[1].State),
                                        country = str(row[1].Contact),
                                        pincode = str(row[1].Pincode),
                                        warehouse_prefix = warehouse_prefix)

            return_point = ReturnPoints(return_location=str(row[1].SellerName),
                                        name=str(row[1].SellerName),
                                        phone=str(row[1].Contact),
                                        address=str(row[1].SellerAddress),
                                        address_two="",
                                        city=str(row[1].City),
                                        state=str(row[1].State),
                                        country=str(row[1].Contact),
                                        pincode=str(row[1].Pincode),
                                        warehouse_prefix=warehouse_prefix)

            client_pickup = ClientPickups(pickup=pickup_point,
                                          return_point=return_point,
                                          client_prefix='FURTADOS')

            db.session.add(client_pickup)

        except Exception as e:
            pass

    db.session.commit()

    db.session.commit()
    from .fetch_orders import lambda_handler
    lambda_handler()
    return 0

    lambda_handler()

    import requests
    create_fulfillment_url = "https://39690624bee51fde0640bfa0e3832744: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@successcraft.myshopify.com/admin/api/2020-07/orders.json?limit=250"
    qs = requests.get(create_fulfillment_url)


    db.session.commit()

    from .fetch_orders import lambda_handler
    lambda_handler()
    return 0
    return 0
    import requests
    url = "https://clbeta.ecomexpress.in/apiv2/fetch_awb/"
    body = {"username": "wareiq30857_temp", "password": "VRmjC8yGc99TAjuC", "count": 5, "type":"ppd"}
    qs = requests.post(url, data = json.dumps(body))
    return 0
    from requests_oauthlib.oauth1_session import OAuth1Session
    auth_session = OAuth1Session("ck_e6e779af2808ee872ba3fa4c0eab26b5f434af8c",
                                 client_secret="cs_696656b398cc783073b281eca3bf02c3c4de0cd1")
    url = '%s/wp-json/wc/v3/orders?per_page=100&order=asc&consumer_key=ck_e6e779af2808ee872ba3fa4c0eab26b5f434af8c&consumer_secret=cs_696656b398cc783073b281eca3bf02c3c4de0cd1' % (
        "https://silktree.in")
    r = auth_session.get(url)



    return 0
    import requests
    count = 250
    return 0

    import requests
    create_fulfillment_url = "https://9bf9e5f5fb698274d52d0e8a734354d7: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@actifiber.myshopify.com/admin/api/2020-07/orders.json?limit=250&fulfillment_status=unfulfilled"
    qs = requests.get(create_fulfillment_url)
    return 0
    from .models import Orders, ReturnPoints, ClientPickups, Products, ProductQuantity
    from woocommerce import API
    prod_obj_x = db.session.query(Products).filter(Products.client_prefix == 'OMGS').all()
    for prod_obj in prod_obj_x:
        wcapi = API(
            url="https://omgs.in",
            consumer_key="ck_97d4a88accab308268c16ce65011e6f2800c601a",
            consumer_secret="cs_e05e0aeac78b76b623ef6463482cc8ca88ae0636",
            version="wc/v3"
        )
        r = wcapi.get('products/%s'%prod_obj.sku)
        if r.status_code==200:
            print("a")
            prod_obj.master_sku = r.json()['sku']


    from .fetch_orders import lambda_handler
    lambda_handler()
    return 0

    return 0
    from .fetch_orders import lambda_handler
    lambda_handler()

    return 0


    return 0
    from .fetch_orders import lambda_handler
    lambda_handler()
    return 0
    create_fulfillment_url = "https://9bf9e5f5fb698274d52d0e8a734354d7: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@actifiber.myshopify.com/admin/api/2020-07/orders.json?limit=250"
    qs = requests.get(create_fulfillment_url)
    return 0


    from .fetch_orders import lambda_handler
    lambda_handler()

    return 0
    from requests_oauthlib.oauth1_session import OAuth1Session
    auth_session = OAuth1Session("ck_9a540daf59bd7e78268d80ed0db14d03a0e68b57",
                                 client_secret="cs_017c6ac59089de2dfd4e2f99e56513aec093464")
    url = '%s/wp-json/wc/v3/products?per_page=100&consumer_key=ck_9a540daf59bd7e78268d80ed0db14d03a0e68b57&consumer_secret=cs_017c6ac59089de2dfd4e2f99e56513aec093464' % (
        "https://naaginsauce.com")
    r = auth_session.get(url)
    return 0

    import requests

    return 0
    from .models import Orders
    for order_id in order_ids:
        del_order =None
        keep_order = None
        order_qs = db.session.query(Orders).filter(Orders.order_id_channel_unique==order_id, Orders.client_prefix=='KAMAAYURVEDA').all()
        for new_order in order_qs:
            if new_order.channel_order_id.endswith("-A"):
                del_order = new_order
            else:
                keep_order = new_order

        cur.execute("""UPDATE op_association SET order_id=%s where order_id=%s"""%(str(keep_order.id), str(del_order.id)))
        keep_order.payments[0].amount = keep_order.payments[0].amount + del_order.payments[0].amount
        keep_order.payments[0].shipping_charges = keep_order.payments[0].shipping_charges + del_order.payments[0].shipping_charges
        keep_order.payments[0].subtotal = keep_order.payments[0].subtotal + del_order.payments[0].subtotal
        conn.commit()
        db.session

    return 0


    from .models import Products, ProductQuantity


    myfile = request.files['myfile']

    data_xlsx = pd.read_excel(myfile)
    from .models import Products, ReturnPoints, ClientPickups
    import json, re
    count = 0
    iter_rw = data_xlsx.iterrows()
    for row in iter_rw:
        try:
            sku = str(row[1].SKU)
            prod_obj_x = db.session.query(Products).filter(Products.master_sku == sku, Products.client_prefix=='KAMAAYURVEDA').first()
            if not prod_obj_x:
                print("product not found: "+sku)
            else:
                name = row[1].Name
                prod_obj_x.name=name
                """
                box_pack = str(row[1].Boxpack)
                dimensions = None
                if weight:
                    weight = int(weight)/1000
                if box_pack == "Small":
                    dimensions =  {"length": 9.5, "breadth": 12.5, "height": 20}
                elif box_pack == "Medium":
                    dimensions =  {"length": 9.5, "breadth": 19, "height": 26}
                elif box_pack == "Large":
                    dimensions =  {"length": 11, "breadth": 20, "height": 31}
                prod_obj_x.weight = weight
                prod_obj_x.dimensions = dimensions
                cmb_quantity = row[1].CMBQTY
                cmb_quantity = int(cmb_quantity) if cmb_quantity else 0

                prod_quan_obj_b = ProductQuantity(product=prod_obj_x,
                                                  total_quantity=cmb_quantity,
                                                  approved_quantity=cmb_quantity,
                                                  available_quantity=cmb_quantity,
                                                  inline_quantity=0,
                                                  rto_quantity=0,
                                                  current_quantity=cmb_quantity,
                                                  warehouse_prefix="KACMB",
                                                  status="APPROVED",
                                                  date_created=datetime.now()
                                                  )

                db.session.add(prod_quan_obj_b)
                """
                if count%100==0:
                    db.session.commit()

        except Exception as e:
            print(str(row[1].SKU))
            pass
        count += 1

    db.session.commit()
    idx = 0
    for prod in data.json()['items']:
        if 'main' in prod['sku']:
            continue
        try:
            prod_obj = Products(name=prod['name'],
                                sku=str(prod['id']),
                                master_sku=prod['sku'],
                                dimensions=None,
                                weight=None,
                                price=float(prod['price']) if prod.get('price') else None,
                                client_prefix='KAMAAYURVEDA',
                                active=True,
                                channel_id=6,
                                date_created=datetime.now()
                                )
            db.session.add(prod_obj)
            idx += 1
            if idx%100==0:
                db.session.commit()
        except Exception as e:
            print(str(prod['sku']) + "\n" + str(e.args[0]))
    db.session.commit()
    return 0

    return 0


    magento_orders_url = """%s/V1/orders?searchCriteria[filter_groups][0][filters][0][field]=updated_at&searchCriteria[filter_groups][0][filters][0][value]=%s&searchCriteria[filter_groups][0][filters][0][condition_type]=gt""" % (
        "https://demokama2.com/rest/default", "2020-06-23")
    headers = {'Authorization': "Bearer " + "gfl1ilzw8iwe4yf06iuiophjfq1gb49k",
               'Content-Type': 'application/json'}
    data = requests.get(magento_orders_url, headers=headers, verify=False)



    from zeep import Client
    wsdl = "https://netconnect.bluedart.com/Ver1.9/ShippingAPI/Finder/ServiceFinderQuery.svc?wsdl"
    client = Client(wsdl)
    request_data = {
        'pinCode': '560068',
        "profile": {
                    "LoginID": "HOW53544",
                    "LicenceKey": "goqshifiomf4qw01yll5fqgtthjgksmj",
                    "Api_type": "S",
                    "Version": "1.9"
                  }
    }
    response = client.service.GetServicesforPincode(**request_data)

    return 0

    return 0
    from .models import Orders
    all_orders = db.session.query(Orders).filter(Orders.client_prefix == 'SSTELECOM',
                                                 Orders.status.in_(['IN TRANSIT']), Orders.channel_order_id>'2093').all()
    for order in all_orders:
        shopify_url = "https://e156f178d7a211b66ae0870942ff32b1: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@trendy-things-2020.myshopify.com/admin/api/2020-04/orders/%s/fulfillments.json" % order.order_id_channel_unique
        tracking_link = "http://webapp.wareiq.com/tracking/%s" % str(order.shipments[0].awb)
        ful_header = {'Content-Type': 'application/json'}
        fulfil_data = {
            "fulfillment": {
                "tracking_number": str(order.shipments[0].awb),
                "tracking_urls": [
                    tracking_link
                ],
                "tracking_company": "WareIQ",
                "location_id": 40801534089,
                "notify_customer": True
            }
        }
        req_ful = requests.post(shopify_url, data=json.dumps(fulfil_data),
                                headers=ful_header)
    return 0

    return 0
    headers = {"Content-Type": "application/json",
               "XBKey": "NJlG1ISTUa2017XzrCG6OoJng"}
    body = {"ShippingID": "14201720000512"}
    xpress_url = "http://xbclientapi.xbees.in/POSTShipmentService.svc/UpdateNDRDeferredDeliveryDate"
    req = requests.post(xpress_url, headers=headers, data=json.dumps(body))

    return 0
    from project import create_app
    app = create_app()
    query_to_run = """select aa.id, bb.awb, aa.status, aa.client_prefix, aa.customer_phone, 
                                    aa.order_id_channel_unique, bb.channel_fulfillment_id, cc.api_key, 
                                    cc.api_password, cc.shop_url, bb.id, aa.pickup_data_id, aa.channel_order_id, ee.payment_mode, 
                                    cc.channel_id, gg.location_id, mm.item_list, mm.sku_quan_list , aa.customer_name, aa.customer_email, 
                                    nn.client_name, nn.client_logo, nn.custom_email_subject, bb.courier_id, nn.theme_color, bb.edd
                                    from orders aa
                                    left join shipments bb
                                    on aa.id=bb.order_id
                                    left join (select order_id, array_agg(channel_item_id) as item_list, array_agg(quantity) as sku_quan_list from
                                      		  (select kk.order_id, kk.channel_item_id, kk.quantity
                                              from op_association kk
                                              left join products ll on kk.product_id=ll.id) nn
                                              group by order_id) mm
                                    on aa.id=mm.order_id
                                    left join client_channel cc
                                    on aa.client_channel_id=cc.id
                                    left join client_pickups dd
                                    on aa.pickup_data_id=dd.id
                                    left join orders_payments ee
                                    on aa.id=ee.order_id
                                    left join client_channel_locations gg
                                    on aa.client_channel_id=gg.client_channel_id
                                    and aa.pickup_data_id=gg.pickup_data_id
                                    left join client_mapping nn
                                    on aa.client_prefix=nn.client_prefix
                                    where aa.status in ('IN TRANSIT')
                                    and aa.status_type is distinct from 'RT'
                                    and bb.awb != ''
                                    and aa.order_date>'2020-06-01'
                                    and aa.customer_email is not null
                                    and bb.awb is not null;"""
    cur = conn.cursor()
    cur.execute(query_to_run)
    all_orders = cur.fetchall()
    email_list = list()
    for order in all_orders:
        try:
            edd = order[25].strftime('%-d %b') if order[25] else ""
            email = create_email(order, edd, order[19])
            email_list.append((email, [order[19]]))
        except Exception as e:
            pass

    send_bulk_emails(email_list)

    return 0



    return 0


    return 0
    create_fulfillment_url = "https://17146b742aefb92ed627add9e44538a2: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@yo-aatma.myshopify.com/admin/api/2019-10/orders.json?ids=2240736788557,2240813563981,2241321435213,2243709665357,2245366349901,2245868355661,2246144163917,2247595196493&limit=250"
    import requests
    qs = requests.get(create_fulfillment_url)

    return 0



    return 0

    import requests, json

    create_fulfillment_url = "https://ef2a4941548279dc7ba487e5e39cb6ce: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@behir.myshopify.com/admin/api/2019-10/orders.json?limit=250"

    tracking_link = "http://webapp.wareiq.com/tracking/%s" % str("3992410212881")
    ful_header = {'Content-Type': 'application/json'}
    fulfil_data = {
        "fulfillment": {
            "tracking_number": str("3992410212881"),
            "tracking_urls": [
                tracking_link
            ],
            "tracking_company": "WareIQ",
            "location_id": int(15879471202),
            "notify_customer": False
        }
    }
    req_ful = requests.post(create_fulfillment_url, data=json.dumps(fulfil_data),
                            headers=ful_header)



    return 0






    import requests, json
    customer_phone = "09819368887"
    data = {
        'From': customer_phone,
        'CallerId': '01141182252',
        'Url': 'http://my.exotel.com/wareiq1/exoml/start_voice/262896',
        'CustomField': "21205"
    }
    req = requests.post(
        'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Calls/connect',
        data=data)

    shopify_url = "https://e67230312f67dd92f62bea398a1c7d38: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@the-organic-riot.myshopify.com/admin/api/2020-04/locations.json?limit=100"
    data = requests.get(shopify_url).json()

    return 0
    import requests


    from .request_pickups import lambda_handler
    lambda_handler()
    myfile = request.files['myfile']

    data_xlsx = pd.read_excel(myfile)
    from .models import Products, ProductQuantity
    count = 0
    iter_rw = data_xlsx.iterrows()

    import requests
    url = 'https://vearth.codolin.com/rest/v1/orders/1258'
    headers = {'Authorization': "Bearer h5e9tmzud0c8p0o82gaobegxpw9tjaqq",
               'Content-Type': 'application/json'}
    apiuser = 'wareiq'
    apipass = 'h5e9tmzud0c8p0o82gaobegxpw9tjaqq'

    return 0
    from .models import Products, ProductQuantity

    for prod in data['products']:
        for prod_obj in prod['variants']:
            prod_name = prod['title']
            if prod_obj['title'] != 'Default Title':
                prod_name += " - "+prod_obj['title']
            prod_obj_x = Products(name=prod_name,
                                  sku=str(prod_obj['id']),
                                  master_sku=prod_obj['sku'],
                                  dimensions=None,
                                  weight=None,
                                  price=float(prod_obj['price']),
                                  client_prefix='HOMEALONE',
                                  active=True,
                                  channel_id=1,
                                  date_created=datetime.datetime.now()
                                  )
            prod_quan_obj = ProductQuantity(product=prod_obj_x,
                                            total_quantity=100,
                                            approved_quantity=100,
                                            available_quantity=100,
                                            inline_quantity=0,
                                            rto_quantity=0,
                                            current_quantity=100,
                                            warehouse_prefix="HOMEALONE",
                                            status="APPROVED",
                                            date_created=datetime.datetime.now()
                                            )

            db.session.add(prod_quan_obj)



    from .models import Orders
    all_ord = db.session.query(Orders).filter(Orders.client_prefix=='ZLADE').filter(Orders.status.in_(['DELIVERED', 'IN TRANSIT', 'READY TO SHIP'])).all()
    for ord in all_ord:
        url = '%s/wp-json/wc/v3/orders/%s' % ("https://www.zladeformen.com", ord.order_id_channel_unique)
        r = auth_session.post(url, data={"status": "completed"})
    from .models import Products, ProductQuantity
    for prod in r.json():
        try:
            if not prod['variations']:
                continue
            if prod['name'] == 'Dummy' or 'Demo' in prod['name']:
                continue
            weight = float(prod['weight']) if prod['weight'] else None
            dimensions = None
            if prod['dimensions']['length']:
                dimensions = {"length":int(prod['dimensions']['length']),
                              "breadth":int(prod['dimensions']['width']),
                              "height": int(prod['dimensions']['height'])
                              }
            """
            if '2kg' in prod['name']:
                weight = 2.1
                dimensions = {"length": 10, "breadth": 30, "height":30}
            elif '1kg' in prod['name'] or '1L' in prod['name']:
                weight = 1.1
                dimensions = {"length": 10, "breadth": 10, "height":30}
            elif '500g' in prod['name'] or '500ml' in prod['name']:
                weight = 0.55
                dimensions = {"length": 10, "breadth": 10, "height":20}
            elif '250g' in prod['name']:
                weight = 0.30
                dimensions = {"length": 10, "breadth": 10, "height":10}
            elif '220g' in prod['name']:
                weight = 0.25
                dimensions = {"length": 10, "breadth": 10, "height":10}
            elif '400g' in prod['name'] or '400 G' in prod['name']:
                weight = 0.45
                dimensions = {"length": 10, "breadth": 10, "height":20}
            elif '1.5kg' in prod['name']:
                weight = 1.6
                dimensions = {"length": 10, "breadth": 20, "height":30}
            elif '2.4kg' in prod['name'] or '2.4 KG' in prod['name']:
                weight = 2.5
                dimensions = {"length": 10, "breadth": 30, "height":30}
            else:
                weight = None
                dimensions = None
                continue
            """
            for idx , value in enumerate(prod['variations']):

                prod_obj_x = Products(name=prod['name'] + " - "+prod['attributes'][0]['options'][idx],
                                      sku=str(value),
                                      master_sku=prod['sku'],
                                      dimensions=dimensions,
                                      weight=weight,
                                      price=float(prod['price']),
                                      client_prefix='ZLADE',
                                      active=True,
                                      channel_id=5,
                                      date_created=datetime.datetime.now()
                                      )
                prod_quan_obj = ProductQuantity(product=prod_obj_x,
                                                total_quantity=0,
                                                approved_quantity=0,
                                                available_quantity=0,
                                                inline_quantity=0,
                                                rto_quantity=0,
                                                current_quantity=0,
                                                warehouse_prefix="ZLADE",
                                                status="APPROVED",
                                                date_created=datetime.datetime.now()
                                                )

                db.session.add(prod_quan_obj)
        except Exception as e:
            pass

    db.session.commit()

    return 0

    since_id='1'
    for i in range(10):
        shopify_url = "https://d243df784237ef6c45aa3a9368ca63da:5888fae7757115f891d0f6774a6c5ed5@gorg-co-in.myshopify.com/admin/api/2019-10/orders.json?limit=100"
        data = requests.get(shopify_url).json()

        since_id = str(prod['id'])
        db.session.commit()

    client = db.session.query(ClientMapping).filter(ClientMapping.client_prefix=='&NOTHINGELSE').first()
    html = client.custom_email
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    # create message object instance
    msg = MIMEMultipart('alternative')

    recipients = ["sahil@andnothingelse.in"]
    msg['From'] = "and nothing else. <noreply@wareiq.com>"
    msg['To'] = ", ".join(recipients)
    msg['Subject'] = client.custom_email_subject

    # write the HTML part

    part2 = MIMEText(html, "html")
    msg.attach(part2)

    email_server.sendmail(msg['From'], recipients, msg.as_string())

    return 0

    import requests, json
    from .models import Orders
    orders = db.session.query(Orders).filter(Orders.client_prefix == 'MUWU').all()
    for order in orders:
        if order.shipments and order.shipments[0].awb:
            create_fulfillment_url = "https://%s:%s@%s/admin/api/2019-10/orders.json?limit=100&since_id=1980049653824" % (
                "a97f8f4744d02183b84b20469af2bc3d", "f9ed6255a50a66a7af6bcdff93b3ce81",
                "unitedbyhope.myshopify.com")
            requests.get(create_fulfillment_url).json()
            tracking_link = "http://webapp.wareiq.com/tracking/%s" % str(order.shipments[0].awb)
            ful_header = {'Content-Type': 'application/json'}
            fulfil_data = {
                "fulfillment": {
                    "tracking_number": str(order.shipments[0].awb),
                    "tracking_urls": [
                        tracking_link
                    ],
                    "tracking_company": "WareIQ",
                    "location_id": 38995263623,
                    "notify_customer": False
                }
            }
            req_ful = requests.post(create_fulfillment_url, data=json.dumps(fulfil_data),
                                    headers=ful_header)
    return 0

    auth_session = OAuth1Session("ck_48b2a03de2cc5906951ff783c6c0cf83d0fa6af4",
                                 client_secret="cs_5d401d8aeaa8a16f5f82c81089290b392420891d")
    url = '%s/wp-json/wc/v3/orders?per_page=100&order=asc' % ("https://andnothingelse.in/stag")
    r = auth_session.get(url)


    from .models import Products, ProductQuantity

    import requests, json
    shopify_url = "https://a97f8f4744d02183b84b20469af2bc3d:f9ed6255a50a66a7af6bcdff93b3ce81@unitedbyhope.myshopify.com/admin/api/2019-10/orders.json?limit=250"
    data = requests.get(shopify_url).json()
    for prod in data['products']:
        for prod_obj in prod['variants']:
            prod_obj_x = Products(name=prod['title'] + " - " + prod_obj['title'],
                                sku=str(prod_obj['id']),
                                master_sku = prod_obj['sku'],
                                dimensions={
                                    "length": 7,
                                    "breadth": 14,
                                    "height": 21
                                },
                                weight=0.35,
                                price=float(prod_obj['price']),
                                client_prefix='UNITEDBYHOPE',
                                active=True,
                                channel_id=1,
                                date_created=datetime.datetime.now()
                                )
            prod_quan_obj = ProductQuantity(product=prod_obj_x,
                                            total_quantity=100,
                                            approved_quantity=100,
                                            available_quantity=100,
                                            inline_quantity=0,
                                            rto_quantity=0,
                                            current_quantity=100,
                                            warehouse_prefix="UNITEDBYHOPE",
                                            status="APPROVED",
                                            date_created=datetime.datetime.now()
                                            )
            db.session.add(prod_quan_obj)

        db.session.commit()
    return 0
    from .models import Products, ProductQuantity


    for prod in r.json():
        try:
            prod_obj = db.session.query(Products).filter(Products.sku == str(prod['id'])).first()
            if prod_obj:
                prod_obj.sku = str(prod['id'])
                prod_obj.master_sku = str(prod['sku'])

            db.session.commit()
        except Exception as e:
            print(str(e.args[0]))


    shopify_url = "https://006fce674dc07b96416afb8d7c075545:0d36560ddaf82721bfbb93f909ab5f47@themuwu.myshopify.com/admin/api/2019-10/products.json?limit=250"
    data = requests.get(shopify_url).json()

    for prod in data['products']:
        for e_sku in prod['variants']:
            try:
                count += 1
                prod_title = prod['title'] + "-" +e_sku['title']
                """
                excel_sku = ""
                if "This is my" in prod['title']:
                    if "Woman" in prod['title']:
                        excel_sku = "FIMD"
                    else:
                        excel_sku = "MIMD"
                if "I just entered" in prod['title']:
                    if "Woman" in prod['title']:
                        excel_sku = "FIJE"
                    else:
                        excel_sku = "MIJE"
                if "Black Hoodie" in prod['title']:
                    if "Man" in prod['title']:
                        excel_sku = "MBHM"
                    else:
                        excel_sku = "FBHM"
    
                if "White Hoodie" in prod['title']:
                    if "Man" in prod['title']:
                        excel_sku = "MWHM"
                    else:
                        excel_sku = "FWHM"
    
                if "To all my" in prod['title']:
                    if "Woman" in prod['title']:
                        excel_sku = "FAMH"
                    else:
                        excel_sku = "MAMH"
                if "Blue Hoodie" in prod['title']:
                    if "Woman" in prod['title']:
                        excel_sku = "FBHD"
                    else:
                        excel_sku = "MBHD"
                if e_sku['title'] == 'XS':
                    excel_sku += "1"
                if e_sku['title'] == 'S':
                    excel_sku += "2"
                if e_sku['title'] == 'M':
                    excel_sku += "3"
                if e_sku['title'] == 'L':
                    excel_sku += "4"
                if e_sku['title'] == 'XL':
                    excel_sku += "5"
                """
                excel_sku =""
                iter_rw = data_xlsx.iterrows()
                for row in iter_rw:
                    if row[1].description == prod_title:
                        excel_sku = row[1].master_sku
                prod_obj = db.session.query(Products).join(ProductQuantity, Products.id == ProductQuantity.product_id) \
                    .filter(Products.sku == str(e_sku['id'])).first()
                prod_obj.master_sku = excel_sku
                """
                if prod_obj:
                    prod_obj.dimensions = {
                        "length": 6.87,
                        "breadth": 22.5,
                        "height": 27.5
                    }
                    prod_obj.weight = 0.5
                else:
                    prod_obj = Products(name=prod['title'] + " - " + e_sku['title'],
                                        sku=str(e_sku['id']),
                                        dimensions={
                                            "length": 6.87,
                                            "breadth": 22.5,
                                            "height": 27.5
                                        },
                                        weight=0.5,
                                        price=float(e_sku['price']),
                                        client_prefix='MUWU',
                                        active=True,
                                        channel_id=1,
                                        date_created=datetime.datetime.now()
                                        )
                prod_quan_obj = ProductQuantity(product=prod_obj,
                                                total_quantity=quan,
                                                approved_quantity=quan,
                                                available_quantity=quan,
                                                inline_quantity=0,
                                                rto_quantity=0,
                                                current_quantity=quan,
                                                warehouse_prefix="HOLISOLBW",
                                                status="APPROVED",
                                                date_created=datetime.datetime.now()
                                                )
                """
                db.session.commit()
            except Exception as e:
                print(str(e))

    import requests, json
    prod_list = requests.get("https://640e8be5fbd672844636885fc3f02d6b:07d941b140370c8c975d8e83ee13e524@clean-canvass.myshopify.com/admin/api/2019-10/products.json?limit=250").json()

    """
        row_data = row[1]
        

    db.session.commit()
    try:
        for warehouse in ('NASHER_HYD','NASHER_GUR','NASHER_SDR','NASHER_VADPE','NASHER_BAN','NASHER_MUM'):
            cur.execute("select sku, product_id, sum(quantity) from 
                        (select * from op_association aa
                        left join orders bb on aa.order_id=bb.id
                        left join client_pickups cc on bb.pickup_data_id=cc.id
                        left join pickup_points dd on cc.pickup_id=dd.id
                        left join products ee on aa.product_id=ee.id
                        where status in ('DELIVERED','DISPATCHED','IN TRANSIT','ON HOLD','PENDING')
                        and dd.warehouse_prefix='__WH__'
                        and ee.sku='__SKU__') xx
                        group by sku, product_id".replace('__WH__', warehouse).replace('__SKU__', str(row_data.SKU)))
            count_tup = cur.fetchone()
            row_count = 0
            if warehouse=='NASHER_HYD':
                row_count = row_data.NASHER_HYD
            if warehouse=='NASHER_GUR':
                row_count = row_data.NASHER_GUR
            if warehouse=='NASHER_SDR':
                row_count = row_data.NASHER_SDR
            if warehouse=='NASHER_VADPE':
                row_count = row_data.NASHER_VADPE
            if warehouse=='NASHER_BAN':
                row_count = row_data.NASHER_BAN
            if warehouse=='NASHER_MUM':
                row_count = row_data.NASHER_MUM

            row_count = int(row_count)
            if count_tup:
                quan_to_add = count_tup[2]
                cur.execute("update products_quantity set total_quantity=%s, approved_quantity=%s WHERE product_id=%s "
                            "and warehouse_prefix=%s", (row_count+quan_to_add, row_count+quan_to_add, count_tup[1], warehouse))
            else:
                cur.execute("select id from products where sku=%s", (str(row_data.SKU), ))
                product_id = cur.fetchone()
                if product_id:
                    product_id = product_id[0]
                    cur.execute(
                        "update products_quantity set total_quantity=%s, approved_quantity=%s WHERE product_id=%s "
                        "and warehouse_prefix=%s",
                        (row_count , row_count, product_id, warehouse))
                else:
                    print("SKU not found: "+str(row_data.SKU))
        if row[0]%20==0 and row[0]!=0:
            conn.commit()
    except Exception as e:
        print(row_data.SKU + ": " +str(e.args[0]))
         """
    conn.commit()

    if not myfile:
        prod_obj = db.session.query(Products).join(ProductQuantity, Products.id == ProductQuantity.product_id) \
            .filter(Products.sku == str(row[1]['sku'])).first()


    return 0
    import requests, json

    all_orders = db.session.query(Orders).filter(Orders.status.in_(["IN TRANSIT","PENDING","DISPATCHED"]))\
        .filter(Orders.client_prefix=='DAPR').all()

    headers = {"Content-Type": "application/json",
                "Authorization": "Token 1368a2c7e666aeb44068c2cd17d2d2c0e9223d37"}
    for order in all_orders:
        try:
            shipment_body = {"waybill": order.shipments[0].awb,
                             "phone": order.customer_phone}
            req = requests.post("https://track.delhivery.com/api/p/edit", headers=headers, data=json.dumps(shipment_body))

        except Exception as e:
            print(str(e)+str(order.id))

    return 0

    myfile = request.files['myfile']

    data_xlsx = pd.read_excel(myfile)
    from .models import Products, ProductQuantity
    for row in data_xlsx.iterrows():
        prod_obj = db.session.query(Products).join(ProductQuantity, Products.id == ProductQuantity.product_id) \
            .filter(Products.sku == str(row[1]['sku'])).first()

        if not prod_obj:
            prod_obj = Products(name=str(row[1]['sku']),
                            sku=str(row[1]['sku']),
                            dimensions={
                                "length": 3,
                                "breadth": 26,
                                "height": 27
                            },
                            weight=0.5,
                            price=0,
                            client_prefix='LMDOT',
                            active=True,
                            channel_id=4,
                            date_created=datetime.datetime.now()
                            )
            prod_quan_obj = ProductQuantity(product=prod_obj,
                                            total_quantity=100,
                                            approved_quantity=100,
                                            available_quantity=100,
                                            inline_quantity=0,
                                            rto_quantity=0,
                                            current_quantity=100,
                                            warehouse_prefix="LMDOT",
                                            status="APPROVED",
                                            date_created=datetime.datetime.now()
                                            )
            db.session.add(prod_quan_obj)


    db.session.commit()

    return 0
    pick = db.session.query(ClientPickups).filter(ClientPickups.client_prefix == 'NASHER').all()
    for location in pick:
        loc_body = {
            "phone": location.pickup.phone,
            "city": location.pickup.city,
            "name": location.pickup.warehouse_prefix,
            "pin": str(location.pickup.pincode),
            "address": location.pickup.address + " " + str(location.pickup.address_two),
            "country": location.pickup.country,
            "registered_name": location.pickup.name,
            "return_address": location.return_point.address +" "+ str(location.return_point.address_two),
            "return_pin": str(location.return_point.pincode),
            "return_city": location.return_point.city,
            "return_state": location.return_point.state,
            "return_country": location.return_point.country
        }

        headers = {"Authorization": "Token c5fd3514bd4cb65432ce31688b049ca6cf417b28",
                   "Content-Type": "application/json"}

        delhivery_url = "https://track.delhivery.com/api/backend/clientwarehouse/create/"

        req = requests.post(delhivery_url, headers=headers, data=json.dumps(loc_body))
        headers = {"Authorization": "Token 538ee2e5f226a85e4a97ad3aa0ae097b41bdb89c",
                   "Content-Type": "application/json"}
        req = requests.post(delhivery_url, headers=headers, data=json.dumps(loc_body))

    myfile = request.files['myfile']

    data_xlsx = pd.read_excel(myfile)
    failed_ids = dict()
    from .models import Products, ProductQuantity
    import json
    for row in data_xlsx.iterrows():
        try:
            if row[0]<1800:
                continue
            row_data = row[1]
            product = db.session.query(Products).filter(Products.sku == str(row_data.seller_sku)).first()

            product.inactive_reason = str(row_data.courier)

            """
            delhivery_url = "https://track.delhivery.com/api/backend/clientwarehouse/create/"
            headers = {"Content-Type": "application/json",
                       "Authorization": "Token d6ce40e10b52b5ca74805a6e2fb45083f0194185"}
            import json, requests
            req= requests.post(delhivery_url, headers=headers, data=json.dumps(del_body))
            headers = {"Content-Type": "application/json",
                       "Authorization": "Token 5f4c836289121eaabc9484a3a46286290c70e69e"}
            req= requests.post(delhivery_url, headers=headers, data=json.dumps(del_body))
            """
            if row[0] % 100 == 0 and row[0] != 0:
                db.session.commit()
        except Exception as e:
            failed_ids[str(row[1].seller_sku)] = str(e.args[0])
            db.session.rollback()
    db.session.commit()
    return 0

    from .request_pickups import lambda_handler
    lambda_handler()

    return 0
    import requests, json

    fulfie_data = {
        "fulfillment": {
            "tracking_number": "3991610025771",
            "tracking_urls": [
                "https://www.delhivery.com/track/package/3991610025771"
            ],
            "tracking_company": "Delhivery",
            "location_id": 21056061499,
            "notify_customer": False
        }
    }
    ful_header = {'Content-Type': 'application/json'}

    url = "https://e35b2c3b1924d686e817b267b5136fe0:a5e60ec3e34451e215ae92f0877dddd0@daprstore.myshopify.com/admin/api/2019-10/orders/1972315848819/fulfillments.json"

    requests.post(url, data=json.dumps(fulfie_data),
                  headers=ful_header)
    import requests
    request_body = {
                    "CallerId": "01141182252",
                    "CallType": "trans",
                    "Url":"http://my.exotel.in/exoml/start/262896",
                    "CustomField": 7277,
                    "From": "8088671652"
                    }

    url = "https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Calls/connect"
    headers = {"Content-Type": "application/json"}
    requests.post(url, data=request_body, header=headers)
    import requests, json
    from .models import ReturnPoints

    myfile = request.files['myfile']

    data_xlsx = pd.read_excel(myfile)
    headers = {"Authorization": "Token 5f4c836289121eaabc9484a3a46286290c70e69e",
               "Content-Type": "application/json"}

    for row in data_xlsx.iterrows():
        try:
            row_data = row[1]
            delivery_shipments_body = {
                "name": str(row_data.warehouse_prefix),
                "contact_person": row_data.name_new,
                "registered_name": "WAREIQ1 SURFACE",
                "phone": int(row_data.phone)
            }

            delhivery_url = "https://track.delhivery.com/api/backend/clientwarehouse/edit/"

            req = requests.post(delhivery_url, headers=headers, data=json.dumps(delivery_shipments_body))

        except Exception as e:
            print(str(e))

    return 0
    import requests, json
    url = "https://dtdc.vineretail.com/RestWS/api/eretail/v1/order/shipDetail"
    headers = {"Content-Type": "application/x-www-form-urlencoded",
               "ApiKey": "8dcbc7d756d64a04afb21e00f4a053b04a38b62de1d3481dadc8b54",
               "ApiOwner": "UMBAPI"}
    form_data = {"RequestBody":
        {
            "order_no": "9251",
            "statuses": [""],
            "order_location": "DWH",
            "date_from": "",
            "date_to": "",
            "pageNumber": ""
        },
        "OrgId": "DTDC"
    }
    req = requests.post(url, headers=headers, data=json.dumps(form_data))
    print(req.json())

    exotel_call_data = {"From": "09999503623",
                        "CallerId": "01141182252",
                        "CallType": "trans",
                        "Url":"http://my.exotel.in/exoml/start/262896",
                        "CustomField": 7277}
    lad = requests.post(
        'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Calls/connect',
        data=exotel_call_data)

    a = "09634814148"
    import json
    1961667264627
    ful_header = {'Content-Type': 'application/json'}
    url = "http://114.143.206.69:803/StandardForwardStagingService.svc/GetBulkShipmentStatus"
    post_body = {
                  "fulfillment": {
                    "location_id": 21056061499,
                    "tracking_number": "3991610018922",
                    "tracking_company": "Delhivery",
                    "tracking_urls": ["https://www.delhivery.com/track/package/3991610018922"],
                    "notify_customer": False

                  }
                }
    req = requests.post(url, data=json.dumps(post_body), headers=ful_header)

    form_data = {"RequestBody": {
        "order_no": "9251",
        "statuses": [""],
        "order_location": "DWH",
        "date_from": "",
        "date_to": "",
        "pageNumber": ""
    },
        "ApiKey": "8dcbc7d756d64a04afb21e00f4a053b04a38b62de1d3481dadc8b54",
        "ApiOwner": "UMBAPI",
    }
    # headers = {"Content-Type": "application/x-www-form-urlencoded"
    #            }
    # req = requests.post("http://dtdc.vineretail.com/RestWS/api/eretail/v1/order/shipDetail",
    #               headers=headers, data=json.dumps(form_data))
    # from .create_shipments import lambda_handler
    # lambda_handler()

    shopify_url = "https://b27f6afd9506d0a07af9a160b1666b74:6c8ca315e01fe612bc997e70c7a21908@mirakki.myshopify.com/admin/api/2019-10/orders.json?since_id=1921601077383&limit=250"
    data = requests.get(shopify_url).json()

    return 0

    # from .create_shipments import lambda_handler
    # lambda_handler()


    order_qs = db.session.query(Orders).filter(Orders.client_prefix=="KYORIGIN").filter(Orders.status.in_(['READY TO SHIP', 'PICKUP REQUESTED', 'NOT PICKED'])).all()
    for order in order_qs:
        try:
            create_fulfillment_url = "https://%s:%s@%s/admin/api/2019-10/orders/%s/fulfillments.json" % (
                order.client_channel.api_key, order.client_channel.api_password,
                order.client_channel.shop_url, order.order_id_channel_unique)
            tracking_link = "https://www.delhivery.com/track/package/%s" % str(order.shipments[0].awb)
            ful_header = {'Content-Type': 'application/json'}
            fulfil_data = {
                "fulfillment": {
                    "tracking_number": str(order.shipments[0].awb),
                    "tracking_urls": [
                        tracking_link
                    ],
                    "tracking_company": "Delhivery",
                    "location_id": 16721477681,
                    "notify_customer": False
                }
            }
            try:
                req_ful = requests.post(create_fulfillment_url, data=json.dumps(fulfil_data),
                                        headers=ful_header)
                fulfillment_id = str(req_ful.json()['fulfillment']['id'])
                order.shipments[0].tracking_link = tracking_link
                order.shipments[0].channel_fulfillment_id = fulfillment_id

            except Exception as e:
                pass
        except Exception as e:
            pass

    db.session.commit()
    # for awb in awb_list:
    #     try:
    #         req = requests.get("https://track.delhivery.com/api/status/packages/json/?waybill=%s&token=d6ce40e10b52b5ca74805a6e2fb45083f0194185"%str(awb)).json()
    #         r = req['ShipmentData']
    #         print(r[0]['Shipment']['Status']['Status'])
    #     except Exception as e:
    #         pass

    exotel_call_data = {"From": "08750108744",
                        "CallType": "trans",
                        "Url": "http://my.exotel.in/exoml/start/257945",
                        "CallerId": "01141182252"}
    res = requests.post(
        'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Calls/connect',
        data=exotel_call_data)

    exotel_sms_data = {
        'From': 'LM-WAREIQ',
        'Messages[0][To]': '08750108744',
        'Messages[0][Body]': 'Dear Customer, your Know Your Origin order with AWB number 123456 is IN-TRANSIT via Delhivery and will be delivered by 23 Dec. Thank you for ordering.'
    }

    lad = requests.post(
        'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend',
        data=exotel_sms_data)

    orders = db.session.query(Orders).join(Shipments, Orders.id==Shipments.order_id).filter(Orders.status == "IN TRANSIT", Orders.status_type=='UD', Orders.client_prefix=="KYORIGIN").all()
    awb_str = ""

    awb_dict = {}
    for order in orders:
        awb_str += order.shipments[0].awb+","
        customer_phone = order.customer_phone
        customer_phone = customer_phone.replace(" ", "")
        customer_phone = "0" + customer_phone[-10:]
        awb_dict[order.shipments[0].awb] = customer_phone

    req = requests.get("https://track.delhivery.com/api/status/packages/json/?waybill=%s&token=1368a2c7e666aeb44068c2cd17d2d2c0e9223d37"%awb_str).json()

    exotel_sms_data = {
      'From': 'LM-WAREIQ'
    }
    exotel_idx = 0
    for shipment in req['ShipmentData']:
        try:
            sms_to_key = "Messages[%s][To]" % str(exotel_idx)
            sms_body_key = "Messages[%s][Body]" % str(exotel_idx)
            expected_date = shipment['Shipment']['expectedDate']
            expected_date = datetime.datetime.strptime(expected_date, '%Y-%m-%dT%H:%M:%S')
            if expected_date < datetime.datetime.today():
                continue
            expected_date = expected_date.strftime('%-d %b')

            exotel_sms_data[sms_to_key] =  awb_dict[shipment["Shipment"]['AWB']]
            exotel_sms_data[
                sms_body_key] = "Dear Customer, your Know Your Origin order with AWB number %s is IN-TRANSIT via Delhivery and will be delivered by %s. Thank you for ordering." % (
                shipment["Shipment"]['AWB'], expected_date)
            exotel_idx += 1
        except Exception:
            pass

    lad = requests.post(
        'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend',
        data=exotel_sms_data)
    return 0

    lad = requests.post('https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend', data=data)



    # for order in data['orders']:
    #     order_qs = db.session.query(Orders).filter(Orders.channel_order_id==str(order['order_number'])).first()
    #     if not order_qs:
    #         continue
    #     order_qs.order_id_channel_unique = str(order['id'])
    #
    # db.session.commit()

    import csv
    with open('dapr_products.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Shopify Product ID", "Title", "Product Type", "Master SKU", "Channel SKU", "Price", "Weight(Kg)", "Quantity", "Image URL"])
        for prod in data['products']:
            for p_sku in prod['variants']:
                try:
                    list_item = list()
                    list_item.append("ID"+str(prod['id']))
                    list_item.append(str(prod['title']))
                    list_item.append(str(prod['product_type']))
                    list_item.append(str(p_sku['sku']))
                    list_item.append("ID"+str(p_sku['id']))
                    list_item.append(str(p_sku['price']))
                    list_item.append(p_sku['weight'])
                    list_item.append(p_sku['inventory_quantity'])
                    list_item.append(prod['image']['src'])
                    writer.writerow(list_item)

                #     sku = str(p_sku['id'])
                #     product = Products(name=prod['title'] + " - " + p_sku['title'],
                #                        sku=sku,
                #                        active=True,
                #                        channel_id=1,
                #                        client_prefix="KYORIGIN",
                #                        date_created=datetime.datetime.now(),
                #                        dimensions = {"length":1.25, "breadth":30, "height":30},
                #                        price=0,
                #                        weight=0.25)
                #
                #     product_quantity = ProductQuantity(product=product,
                #                                        total_quantity=5000,
                #                                        approved_quantity=5000,
                #                                        available_quantity=5000,
                #                                        warehouse_prefix="KYORIGIN",
                #                                        status="APPROVED",
                #                                        date_created=datetime.datetime.now()
                #                                        )
                #     db.session.add(product)
                #     db.session.commit()
                except Exception as e:
                    print("Exception for "+ str(e.args[0]))

    return jsonify({
        'status': 'success',
        'message': 'pong!'
    })

    lambda_handler()
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter, A4
    from reportlab.lib.units import inch

    c = canvas.Canvas("testing.pdf", pagesize=letter)
    c.translate(inch, inch)
    c.rect(0.2 * inch, 0.2 * inch, 1 * inch, 1.5 * inch, fill=1)
    c.drawString(0.3 * inch, -inch, "Hello World")
    c.showPage()
    c.save()
    return jsonify({
        'status': 'success',
        'message': 'pong!'
    })


    """

    for order in data:


    createBarCodes()

    datetime.strptime("2010-06-04 21:08:12", "%Y-%m-%d %H:%M:%S")
    """
    return jsonify({
        'status': 'success',
        'message': 'pong!'
    })



    shiprocket_token = """Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOjI0NzIyNiwiaXNzIjoiaHR0cHM6Ly9hcGl2Mi5zaGlwcm9ja2V0LmluL3YxL2V4dGVybmFsL2F1dGgvbG9naW4iLCJpYXQiOjE1NzMzNTIzMTYsImV4cCI6MTU3NDIxNjMxNiwibmJmIjoxNTczMzUyMzE2LCJqdGkiOiJmclBCRHZNYnVUZEEwanZOIn0.Gqax7B1zPWoM34yKkUz2Oa7vIvja7D6Z-C8NsyNIIE4"""

    url = "https://apiv2.shiprocket.in/v1/external/orders?per_page=100&page=1"
    headers = {'Authorization': shiprocket_token}
    response = requests.get(url, headers=headers)


    data = response.json()['data']

    for point in data:
        try:
            shipment_dimensions = {}
            try:
                shipment_dimensions['length'] = float(point['shipments'][0]['dimensions'].split('x')[0])
                shipment_dimensions['breadth'] = float(point['shipments'][0]['dimensions'].split('x')[1])
                shipment_dimensions['height'] = float(point['shipments'][0]['dimensions'].split('x')[2])
            except Exception:
                pass
            order_date = datetime.datetime.strptime(point['created_at'], '%d %b %Y, %I:%M %p')
            url_specific_order = "https://apiv2.shiprocket.in/v1/external/orders/show/%s"%(str(point['id']))

            order_spec = requests.get(url_specific_order, headers=headers).json()['data']
            delivery_address = {
                "address": order_spec["customer_address"],
                "address_two": order_spec["customer_address_2"],
                "city": order_spec["customer_city"],
                "state": order_spec["customer_state"],
                "country": order_spec["customer_country"],
                "pincode": int(order_spec["customer_pincode"]),
            }

            new_order = Orders(
                channel_order_id=point['channel_order_id'],
                order_date=order_date,
                customer_name=point['customer_name'],
                customer_email=point['customer_email'],
                customer_phone=point['customer_phone'],
                status=point['status'],
                delivery_address=delivery_address,
                client_prefix='MIRAKKI',
            )
            shipment = Shipments(
                awb=point['shipments'][0]['awb'],
                weight=float(point['shipments'][0]['weight']),
                volumetric_weight=point['shipments'][0]['volumetric_weight'],
                dimensions=shipment_dimensions,
                order=new_order,
            )
            courier = db.session.query(MasterCouriers).filter(MasterCouriers.courier_name==point['shipments'][0]['courier']).first()
            shipment.courier = courier

            payment = OrdersPayments(
                payment_mode=point['payment_method'],
                amount=float(point['total']),
                currency='INR',
                order=new_order
            )
            for prod in point['products']:
                prod_obj = db.session.query(Products).filter(Products.sku==prod['channel_sku']).first()
                op_association = OPAssociation(order=new_order, product=prod_obj, quantity=prod['quantity'])
                new_order.products.append(op_association)

            db.session.add(new_order)
            db.session.commit()

        except Exception as e:
            print(point['id'])
            print(e)
            pass



        """
        dimensions = {}
        try:
            dimensions['length'] = float(point['dimensions'].split(' ')[0])
            dimensions['breadth'] = float(point['dimensions'].split(' ')[2])
            dimensions['height'] = float(point['dimensions'].split(' ')[4])
        except Exception:
            pass
        weight = None
        try:
            weight = float(point['weight'].split(' ')[0])
        except Exception:
            pass

        new_product = Products(
            name=point['name'],
            sku=point['sku'],
            product_image=point['image'],
            price=point['mrp'],
            client_prefix='KYORIGIN',
            active=True,
            dimensions=dimensions,
            weight=weight,
        )
        
        try:
            product_id = Products.query.filter_by(sku=point['sku']).first().id
            new_quantity = ProductQuantity(product_id=product_id,
                                           total_quantity=point['total_quantity'],
                                           approved_quantity=point['total_quantity'],
                                           available_quantity=point['available_quantity'],
                                           warehouse_prefix='MIRAKKI',
                                           status='APPROVED')
            db.session.add(new_quantity)
            db.session.commit()
        except Exception:
            pass
        """

    return jsonify({
        'status': 'success',
        'message': 'pong!'
    })

import smtplib, logging
from datetime import datetime
from flask import render_template
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def create_email(order, edd, email):
    try:
        from project import create_app
        app = create_app()
        background_color = str(order[24]) if order[24] else "#B5D0EC"
        client_logo = str(order[21]) if order[21] else "https://logourls.s3.amazonaws.com/client_logos/logo_ane.png"
        client_name = str(order[20]) if order[20] else "WareIQ"
        email_title = str(order[22]) if order[22] else "Your order has been shipped!"
        order_id = str(order[12]) if order[12] else ""
        customer_name = str(order[18]) if order[18] else "Customer"
        courier_name = "WareIQ"
        if order[23] in (1,2,8,11,12):
            courier_name = "Delhivery"
        elif order[23] in (5,13):
            courier_name = "Xpressbees"
        elif order[23] in (4):
            courier_name = "Shadowfax"

        edd = edd if edd else ""
        awb_number = str(order[1]) if order[1] else ""
        tracking_link = "http://webapp.wareiq.com/tracking/" + str(order[1])
        html = render_template("order_shipped.html", background_color=background_color,
                               client_logo=client_logo,
                               client_name=client_name,
                               email_title=email_title,
                               order_id=order_id,
                               customer_name=customer_name,
                               courier_name=courier_name,
                               edd=edd,
                               awb_number=awb_number,
                               tracking_link=tracking_link)

        # create message object instance
        msg = MIMEMultipart('alternative')

        recipients = [email]
        msg['From'] = "%s <noreply@wareiq.com>"%client_name
        msg['To'] = ", ".join(recipients)
        msg['Subject'] = email_title

        # write the HTML part

        part2 = MIMEText(html, "html")
        msg.attach(part2)
        return msg
    except Exception as e:
        return None


@core_blueprint.route('/core/send', methods=['GET'])
def send_dev():
    return 0
    import requests, json

    return 0
    from project import create_app
    app = create_app()
    from flask import render_template, Flask
    html = render_template("order_shipped.html", background_color="#B5D0EC",
                           client_logo="https://logourls.s3.amazonaws.com/client_logos/logo_zlade.png",
                           client_name="Zlade",
                           email_title="Your order has been shipped!",
                           order_id = "12345",
                           customer_name="Suraj Chaudhari",
                           courier_name = "Delhivery",
                           edd="14 June",
                           awb_number = "3992410231781",
                           tracking_link = "http://webapp.wareiq.com/tracking/3992410231781")
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    # create message object instance
    msg = MIMEMultipart('alternative')

    recipients = ["cravi8750@gmail.com"]
    msg['From'] = "Zlade <noreply@wareiq.com>"
    msg['To'] = ", ".join(recipients)
    msg['Subject'] = "Your order has been shipped!"

    # write the HTML part

    part2 = MIMEText(html, "html")
    msg.attach(part2)

    email_server.sendmail(msg['From'], recipients, msg.as_string())

    return 0
    return jsonify({
        'status': 'success',
        'message': 'pong!'
    })
    # origin fulfilment on shopify
    cur.execute("""select bb.order_id_channel_unique, aa.awb from shipments aa
                        left join orders bb
                        on aa.order_id = bb.id
                        where channel_fulfillment_id is null
                        and client_prefix='KYORIGIN'
                        and order_date>'2020-01-15'
                        and aa.status='Success'
                        order by bb.id DESC""")
    all_orders = cur.fetchall()

    for order in all_orders:
        create_fulfillment_url = "https://%s:%s@%s/admin/api/2019-10/orders/%s/fulfillments.json" % (
            "dc8ae0b7f5c1c6558f551d81e1352bcd", "00dfeaf8f77b199597e360aa4a50a168",
            "origin-clothing-india.myshopify.com", order[0])
        tracking_link = "https://www.delhivery.com/track/package/%s" % str(order[1])
        ful_header = {'Content-Type': 'application/json'}
        fulfil_data = {
            "fulfillment": {
                "tracking_number": str(order[1]),
                "tracking_urls": [
                    tracking_link
                ],
                "tracking_company": "Delhivery",
                "location_id": 16721477681,
                "notify_customer": False
            }
        }
        try:
            req_ful = requests.post(create_fulfillment_url, data=json.dumps(fulfil_data),
                                    headers=ful_header)
            fulfillment_id = str(req_ful.json()['fulfillment']['id'])
            cur.execute("UPDATE shipments SET channel_fulfillment_id=%s WHERE awb=%s", (str(fulfillment_id), order[1]))
        except Exception as e:
            logger.error("Couldn't update shopify for: " + str(order[1])
                         + "\nError: " + str(e.args))

    # end of fulfilment

    # cod_verification call
    cur.execute("""select aa.order_id, bb.customer_phone from cod_verification aa
                        left join orders bb on aa.order_id=bb.id
                        where client_prefix='DAPR'
                        and bb.status='NEW'
                        and cod_verified is null
                        order by bb.id DESC""")
    all_orders = cur.fetchall()

    for order in all_orders:
        data = {
            'From': str(order[1]),
            'CallerId': '01141182252',
            'Url': 'http://my.exotel.com/wareiq1/exoml/start_voice/262896',
            'CustomField': str(order[0])
        }
        req = requests.post(
            'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Calls/connect',
            data=data)

    # end of cod_verification call


    sms_data = {
        'From': 'LM-WAREIQ'
    }
    itt = 0
    for idx, awb in enumerate(awbs):

        sms_to_key = "Messages[%s][To]"%str(itt)
        sms_body_key = "Messages[%s][Body]"%str(itt)
        customer_phone = awb[1].replace(" ","")
        customer_phone = "0"+customer_phone[-10:]
        sms_data[sms_to_key] = customer_phone
        sms_data[sms_body_key] = "Dear Customer, we apologise for the delay. Your order from Know Your Origin has not been shipped due to huge volumes. It will be shipped with in next two days with AWB no. %s by Delhivery."%str(awb[0])
        itt +=1
    lad = requests.post(
        'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend',
        data=sms_data)

    """
    
    from .utils import createBarCodes

    for order in data:


    createBarCodes()

    datetime.strptime("2010-06-04 21:08:12", "%Y-%m-%d %H:%M:%S")
    """
    return jsonify({
        'status': 'success',
        'message': 'pong!'
    })
