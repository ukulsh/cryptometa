from flask import Flask, request, jsonify
from celery import Celery
from flask_cors import cross_origin, CORS
import json, re, base64
import io
import csv
from datetime import timedelta
from celery.schedules import crontab
from .update_status.function import update_status
from .fetch_orders.function import fetch_orders, assign_pickup_points_for_unassigned
from .fetch_orders.sync_channel_status import sync_channel_status
from .ship_orders.function import ship_orders
from .core_app_jobs.tasks import *
from .download_queues.tasks import *
from .core_app_jobs.utils import authenticate_username_password, authenticate_restful
from .order_price_reconciliation.index import process_order_price_reconciliation

cors = CORS()

session = boto3.Session(
    aws_access_key_id='AKIAWRT2R3KC3YZUBFXY',
    aws_secret_access_key='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',
)


def make_celery(app):
    celery = Celery(app.import_name, backend=app.config['CELERY_BACKEND'],
                    broker=app.config['CELERY_BROKER_URL'])
    celery.conf.update(app.config)
    TaskBase = celery.Task
    class ContextTask(TaskBase):
        abstract = True
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery


app = Flask(__name__)
app.config['CELERY_BACKEND'] = "amqp://ravi:Kad97711@rabbitmq:5672"
app.config['CELERY_BROKER_URL'] = "amqp://ravi:Kad97711@rabbitmq:5672"
app.config['USERS_SERVICE_URL'] = os.environ.get('USERS_SERVICE_URL')
cors.init_app(app)

app.config['CELERYBEAT_SCHEDULE'] = {
    'run-status-update': {
            'task': 'status_update',
            'schedule': crontab(minute='50', hour='*'),
            'options': {'queue': 'update_status'}
        },
    'run-status-update-2': {
                'task': 'status_update',
                'schedule': crontab(minute='50', hour='*/2'),
                'options': {'queue': 'update_status_2'},
                'args': (1, )
            },
    'run-fetch-orders': {
                'task': 'fetch_orders',
                'schedule': crontab(minute='15,45', hour='*'),
                'options': {'queue': 'fetch_orders'}
            },
    'run-ship-orders': {
                'task': 'orders_ship',
                'schedule': crontab(minute='*/30'),
                'options': {'queue': 'ship_orders'}
            },
    'run-cod-queue': {
                    'task': 'cod_remittance_queue',
                    'schedule': crontab(hour=19, minute=20, day_of_week='thu'),
                    'options': {'queue': 'mark_channel_delivered'}
                },
    'run-cod-entry': {
                    'task': 'cod_remittance_entry',
                    'schedule': crontab(hour=19, minute=55, day_of_week='wed'),
                    'options': {'queue': 'mark_channel_delivered'}
                },
    'run-calculate-costs': {
                'task': 'calculate_costs',
                'schedule': crontab(minute='*/30'),
                'options': {'queue': 'calculate_costs'}
            },
    'run-sync-all-inventory': {
                    'task': 'sync_all_inventory',
                    'schedule': crontab(minute='*/60'),
                    'options': {'queue': 'sync_all_inventory'}
                },
    'run-ndr-reattempt': {
                        'task': 'ndr_push_reattempts',
                        'schedule': crontab(hour=18, minute=00),
                        'options': {'queue': 'mark_channel_delivered'}
                    },
    'create-pickups-entry': {
                            'task': 'create_pickups_entry',
                            'schedule': crontab(hour=2, minute=45, day_of_week='mon,tue,wed,thu,fri,sat'),
                            'options': {'queue': 'mark_channel_delivered'}
                        },
    'update-pincode-serviceability': {
                        'task': 'update_pincode_serviceability',
                        'schedule': crontab(hour=20, minute=30),
                        'options': {'queue': 'sync_all_inventory'}
                    },
    'sync-channel-status': {
                'task': 'sync_channel_status',
                'schedule': crontab(minute='40', hour='*'),
                'options': {'queue': 'sync_channel_status'}
            },
}

app.config['CELERY_TIMEZONE'] = 'UTC'

celery_app = make_celery(app)


@celery_app.task(name='cod_remittance_entry')
def cod_remittance_entry():
    create_cod_remittance_entry()
    return 'successfully completed cod remittance entry'


@celery_app.task(name='cod_remittance_queue')
def cod_remittance_queue():
    queue_cod_remittance_razorpay()
    return 'successfully completed cod remittance queue'


@celery_app.task(name='ndr_push_reattempts')
def ndr_push_reattempts():
    ndr_push_reattempts_util()
    return 'successfully completed ndr_push_reattempts'


@celery_app.task(name='push_awbs_easyecom')
def push_awbs_easyecom():
    push_awbs_easyecom_util()
    return 'successfully completed push_awbs_easyecom'


@celery_app.task(name='update_pincode_serviceability')
def update_pincode_serviceability():
    update_pincode_serviceability_table()
    return 'successfully completed update_pincode_serviceability'


@celery_app.task(name='calculate_costs')
def calculate_costs():
    calculate_costs_util()
    return 'successfully completed calculate costs'


@celery_app.task(name='sync_all_inventory')
def sync_all_inventory():
    update_available_quantity()
    update_available_quantity_from_easyecom()
    update_available_quantity_on_channel()
    return 'successfully completed sync_all_inventory'


@celery_app.task(name='create_pickups_entry')
def create_pickups_entry():
    create_pickups_entry_util()
    return 'successfully completed create_pickups_entry'


@celery_app.task(name='status_update')
def status_update(sync_ext=None):
    update_status(sync_ext)
    return 'successfully completed status_update'


@celery_app.task(name='orders_ship')
def orders_ship(client_prefix=None):
    ship_orders(client_prefix=client_prefix)
    return 'successfully completed ship_orders'


@celery_app.task(name='ship_bulk_orders_job')
def ship_bulk_orders_job(order_ids, auth_data, courier):
    ship_bulk_orders(order_ids, auth_data, courier)
    return 'successfully completed ship_bulk_orders'


@app.route('/scans/v1/orders/ship', methods = ['GET'])
def ship_orders_api():
    client_prefix = request.args.get('client_prefix')
    if client_prefix:
        orders_ship.apply_async(queue='ship_orders_2', args=(request.args.get('client_prefix'),))
    else:
        orders_ship.apply_async(queue='ship_orders')
    return jsonify({"msg": "ship order task received"}), 200


@app.route('/scans/v1/orders/bulkship', methods = ['POST'])
@authenticate_restful
def bulkship_orders(resp):
    auth_data = resp.get('data')
    if not auth_data:
        return jsonify({"success": False, "msg": "Auth Failed"}), 404
    data = json.loads(request.data)
    order_ids = data['order_ids']
    courier = data['courier']
    if not order_ids:
        return jsonify({"success": False, "msg": "order ids not found"}), 400
    if len(order_ids)==1:
        return_data, code = ship_bulk_orders(order_ids, auth_data, courier)
        return jsonify(return_data), code
    else:
        ship_bulk_orders_job.apply_async(queue='ship_orders', args=(order_ids, auth_data, courier))
        return jsonify({"success": True, "msg": "shipping request created"}), 202


@app.route('/scans/v1/dev', methods = ['GET'])
def celery_dev():
    push_awbs_easyecom.apply_async(queue='mark_channel_delivered')
    return jsonify({"msg": "Task received"}), 200


@celery_app.task(name='fetch_orders')
def orders_fetch(client_prefix=None, sync_all=None):
    fetch_orders(client_prefix, sync_all)
    assign_pickup_points.apply_async(queue='assign_pickup_points')
    return 'successfully completed fetch_orders'


@celery_app.task(name='assign_pickup_points')
def assign_pickup_points():
    assign_pickup_points_for_unassigned()
    return 'successfully completed assign_pickup_points'


@celery_app.task(name='sync_channel_status')
def sync_channel_status_task(client_prefix=None):
    sync_channel_status(client_prefix)
    return 'successfully completed sync_channel_status'


@celery_app.task(name='consume_ecom_scan')
def consume_ecom_scan(payload):
    msg = consume_ecom_scan_util(payload)
    return msg


@celery_app.task(name='consume_sfxsdd_scan')
def consume_sfxsdd_scan(payload):
    msg = consume_sfxsdd_scan_util(payload)
    return msg


@celery_app.task(name='consume_pidge_scan')
def consume_pidge_scan(payload):
    msg = consume_pidge_scan_util(payload)
    return msg


@celery_app.task(name='consume_delhivery_scan')
def consume_delhivery_scan(payload):
    msg = consume_delhivery_scan_util(payload)
    return msg


@celery_app.task(name='consume_xpressbees_scan')
def consume_xpressbees_scan(payload):
    msg = consume_xpressbees_scan_util(payload)
    return msg


@celery_app.task(name='consume_delhivery_pod')
def consume_delhivery_pod(payload):
    cur=conn.cursor()
    base64_img = payload.get('Image')
    if not base64_img:
        return "Invalid image data"
    base64_img = base64.b64decode(base64_img)
    image_name = str(payload.get('Waybill'))+''.join(random.choices(string.ascii_uppercase, k=8))+'_pod.png'
    with open(image_name, "wb") as fh:
        fh.write(base64_img)

    s3 = session.resource('s3')
    bucket = s3.Bucket("wareiqpods")
    bucket.upload_file(image_name, image_name, ExtraArgs={'ACL': 'public-read'})
    pod_url = "https://wareiqpods.s3.amazonaws.com/" + image_name
    os.remove(image_name)
    cur.execute("UPDATE shipments SET tracking_link=%s WHERE awb=%s;", (pod_url, payload.get('Waybill')))
    conn.commit()
    return "Succesfully saved POD for " + str(payload.get('Waybill'))


@app.route('/scans/v1/consume/ecom', methods = ['POST'])
@authenticate_username_password
def ecom_scan(resp):
    auth_data = resp.get('data')
    if not auth_data:
        return jsonify({"success": False, "msg": "Auth Failed"}), 404
    if auth_data.get("username")!="ecomexpress" or auth_data.get("user_group")!="courier":
        return jsonify({"success": False, "msg": "Not allowed"}), 404
    data = json.loads(request.data)
    consume_ecom_scan.apply_async(queue='consume_scans', args=(data, ))
    return jsonify({"awb": data['awb'], "status": True, "status_update_number": data['status_update_number'] }), 200


@app.route('/scans/v1/consume/sfxsdd', methods = ['POST'])
@authenticate_username_password
def sfxsdd_scan(resp):
    auth_data = resp.get('data')
    if not auth_data:
        return jsonify({"success": False, "msg": "Auth Failed"}), 404
    if auth_data.get("username")!="sfxsdd" or auth_data.get("user_group")!="courier":
        return jsonify({"success": False, "msg": "Not allowed"}), 404
    data = json.loads(request.data)
    consume_sfxsdd_scan.apply_async(queue='consume_scans', args=(data, ))
    return jsonify({"awb": data['sfx_order_id'], "status": True, "order_status": data['order_status'] }), 200


@app.route('/scans/v1/consume/pidge', methods = ['POST'])
def pidge_scan():
    if request.headers.get('Authorization')!= "Token 44f9f1bd1894444g568a908520b5dda0":
        return jsonify({"success": False, "msg": "Auth Failed"}), 404
    data = json.loads(request.data)
    consume_pidge_scan.apply_async(queue='consume_scans', args=(data, ))
    return jsonify({"awb": data['PBID'], "success": True, "status_update_number": data['trip_status'] }), 200


@app.route('/scans/v1/consume/pod_delhivery', methods = ['POST'])
def delhivery_pod():
    if request.headers.get('Authorization')!= "Token a9bf5xZ1768e5ff511ab9d6fg8g8090221ghYdtR":
        return jsonify({"success": False, "msg": "Auth Failed"}), 404
    data = json.loads(request.data)
    consume_delhivery_pod.apply_async(queue='mark_channel_delivered', args=(data, ))
    return jsonify({"awb": data['Waybill'], "success": True}), 200


@app.route('/scans/v1/consume/delhivery', methods = ['POST'])
def delhivery_scan():
    if request.headers.get('Authorization')!= "Token a9bf5xZ1768e5ff511ab9d6fg8g8090221ghYdtR":
        return jsonify({"success": False, "msg": "Auth Failed"}), 404
    data = json.loads(request.data)
    consume_delhivery_scan.apply_async(queue='consume_scans', args=(data, ))
    return jsonify({"awb": data['Shipment']['AWB'], "success": True}), 200


@app.route('/scans/v1/consume/xpressbees', methods = ['POST'])
def xpressbees_scan():
    if request.headers.get('Authorization')!= "Token xWDN7TB7yZ9wdp1CQrCxpiN94BqIApf1O72FrDsW":
        return jsonify({"success": False, "msg": "Auth Failed"}), 404
    data = json.loads(request.data)
    consume_xpressbees_scan.apply_async(queue='consume_scans', args=(data, ))
    return jsonify({"awb": data['AWBNO'], "success": True}), 200


@app.route('/scans/v1/mark_delivered_channel', methods = ['POST'])
def mark_delivered_channel_api():
    data = json.loads(request.data)
    token = data.get("token")
    if token!="b4r74rn3r84rn4ru84hr":
        jsonify({"status": "Unauthorized"}), 302
    mark_delivered_channel.apply_async(queue='mark_channel_delivered', args=(data, ))
    return jsonify({"status":"success"}), 200


@app.route('/scans/v1/downloadQueue/orders', methods = ['POST'])
def download_queue_orders_api():
    data = json.loads(request.data)
    token = data.get("token")
    if token!="b4r74rn3r84rn4ru84hr":
        jsonify({"status": "Unauthorized"}), 302
    generate_orders_report.apply_async(queue='mark_channel_delivered', args=(data, ))
    return jsonify({"status":"success"}), 200


@app.route('/scans/v1/downloadQueue/shiplabels', methods = ['POST'])
def download_queue_labels_api():
    data = json.loads(request.data)
    token = data.get("token")
    if token!="b4r74rn3r84rn4ru84hr":
        jsonify({"status": "Unauthorized"}), 302
    generate_orders_labels.apply_async(queue='mark_channel_delivered', args=(data, ))
    return jsonify({"status":"success"}), 200


@celery_app.task(name='mark_delivered_channel')
def mark_delivered_channel(payload):
    msg = mark_order_delivered_channels(payload)
    return msg


@celery_app.task(name='generate_orders_report')
def generate_orders_report(data):
    msg = download_flag_func_orders(data['query_to_run'], data['get_selected_product_details'],
                                    data['auth_data'], data['ORDERS_DOWNLOAD_HEADERS'], data['hide_weights'], data['report_id'])
    return msg


@celery_app.task(name='generate_orders_labels')
def generate_orders_labels(data):
    msg = shiplabel_download_util(data['orders_qs'], data['auth_data'], data['report_id'])
    return msg


@celery_app.task(name='sync_channel_products')
def sync_channel_prods(client_prefix):
    msg = sync_all_products_with_channel(client_prefix)
    return msg


@celery_app.task(name='sync_channel_orders')
def sync_channel_ords(client_prefix):
    sync_channel_status(client_prefix=client_prefix)
    fetch_orders(client_prefix=client_prefix)
    return "received fetch orders"


@app.route('/scans/v1/sync/products', methods = ['GET'])
@authenticate_restful
def sync_channel_products(resp):
    auth_data = resp.get('data')
    client_prefix=auth_data['client_prefix']
    sync_channel_prods.apply_async(queue='consume_scans', args=(client_prefix, ))
    return jsonify({"msg": "Task received"}), 200


@app.route('/scans/v1/sync/orders', methods = ['GET'])
@authenticate_restful
def sync_channel_orders(resp):
    auth_data = resp.get('data')
    client_prefix=auth_data['client_prefix']
    sync_channel_ords.apply_async(queue='fetch_orders', args=(client_prefix, ))
    return jsonify({"msg": "Task received"}), 200


@celery_app.task(name='order_price_reconciliation')
def process_reconciliation(order_data):
    msg = process_order_price_reconciliation(order_data)
    return msg


@app.route('/scans/v1/orderPriceReconciliation', methods=['POST'])
def order_price_reconciliation():
    try:
        recon_file = request.files.get('recon_file')
        awb_dict = get_file_data(recon_file)
        process_reconciliation.apply_async(queue='calculate_costs', args=(awb_dict,))
        return jsonify({"msg": "Task received"}), 200
    except Exception as e:
        return jsonify({"msg": "failed while creating the task"}), 400


def get_file_data(file_ref):
    import pandas as pd
    data_xlsx = pd.read_excel(file_ref)
    iter_rw = data_xlsx.iterrows()
    awb_dict = dict()
    for row in iter_rw:
        awb_dict[str(row[1].AWB).split(".")[0]]=(float(row[1].Weight), None)
    return awb_dict


@celery_app.task(name='prod_upload_job')
def upload_prod_job(order_data):
    upload_products_util(order_data)
    return "completed upload prods"


@app.route('/scans/v1/uploadProducts', methods=['POST'])
def upload_products():
    try:
        recon_file = request.files.get('prod_file')
        order_data = get_prod_file_data(recon_file)
        upload_prod_job.apply_async(queue='update_status', args=(order_data,))
        return jsonify({"msg": "Task received"}), 200
    except Exception as e:
        return jsonify({"msg": "failed while creating the task"}), 400


def get_prod_file_data(file_ref):
    import pandas as pd
    data_xlsx = pd.read_csv(file_ref)
    iter_rw = data_xlsx.iterrows()
    prod_list = list()
    for row in iter_rw:
        try:
            dimensions = re.findall(r"[-+]?\d*\.\d+|\d+", str(row[1].Dimensions))
            dimensions = {"length": float(dimensions[0]), "breadth": float(dimensions[1]),
                          "height": float(dimensions[2])}
            prod_list.append((str(row[1].SKU), float(row[1].Price), float(row[1].Weight), dimensions))
        except Exception:
            pass
    return prod_list