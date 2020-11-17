from flask import Flask, request, jsonify
from celery import Celery
import json, re
import io
import csv
from datetime import timedelta
from celery.schedules import crontab
from .update_status.function import update_status
from .fetch_orders.function import fetch_orders
from .ship_orders.function import ship_orders
from .core_app_jobs.tasks import *
from .core_app_jobs.utils import authenticate_username_password
from app.order_price_reconciliation.index import process_order_price_reconciliation


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


app.config['CELERYBEAT_SCHEDULE'] = {
    'run-status-update': {
            'task': 'status_update',
            'schedule': crontab(minute='50', hour='*/2'),
            'options': {'queue': 'update_status'}
        },
    'run-fetch-orders': {
                'task': 'fetch_orders',
                'schedule': crontab(minute='*/20'),
                'options': {'queue': 'fetch_orders'}
            },
    'run-ship-orders': {
                'task': 'orders_ship',
                'schedule': crontab(minute='*/30'),
                'options': {'queue': 'ship_orders'}
            },
    # 'run-cod-queue': {
    #                 'task': 'cod_remittance_queue',
    #                 'schedule': crontab(hour=12, minute=30, day_of_week='fri'),
    #                 'options': {'queue': 'consume_scans'}
    #             },
    'run-cod-entry': {
                    'task': 'cod_remittance_entry',
                    'schedule': crontab(hour=18, minute=35, day_of_week='wed'),
                    'options': {'queue': 'consume_scans'}
                },
    'run-calculate-costs': {
                'task': 'calculate_costs',
                'schedule': crontab(minute='*/30'),
                'options': {'queue': 'calculate_costs'}
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


@celery_app.task(name='calculate_costs')
def calculate_costs():
    calculate_costs_util()
    return 'successfully completed calculate costs'


@celery_app.task(name='status_update')
def status_update():
    update_status()
    return 'successfully completed status_update'


@celery_app.task(name='orders_ship')
def orders_ship():
    ship_orders()
    return 'successfully completed ship_orders'


@app.route('/scans/v1/orders/ship', methods = ['GET'])
def ship_orders_api():
    orders_ship.apply_async(queue='ship_orders')
    return jsonify({"msg": "ship order task received"}), 200


@app.route('/scans/v1/dev', methods = ['GET'])
def celery_dev():
    calculate_costs.apply_async(queue='calculate_costs')
    return jsonify({"msg": "Task received"}), 200


@celery_app.task(name='fetch_orders')
def orders_fetch():
    fetch_orders()
    return 'successfully completed fetch_orders'


@celery_app.task(name='consume_ecom_scan')
def consume_ecom_scan(payload):
    msg = consume_ecom_scan_util(payload)
    return msg


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


@celery_app.task(name='sync_channel_products')
def sync_channel_prods(client_prefix):
    msg = sync_all_products_with_channel(client_prefix)
    return msg


@app.route('/scans/v1/sync/products', methods = ['GET'])
def sync_channel_products():
    client_prefix=request.args.get('tab')
    sync_channel_prods.apply_async(queue='consume_scans', args=(client_prefix, ))
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