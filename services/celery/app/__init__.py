from flask import Flask, request, jsonify
from celery import Celery
import json
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
}

app.config['CELERY_TIMEZONE'] = 'UTC'

celery_app = make_celery(app)


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
    orders_fetch.apply_async(queue='fetch_orders')
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
        order_data = get_file_data(recon_file)
        process_reconciliation.apply_async(queue='consume_scans', args=(order_data,))
        return jsonify({"msg": "Task received"}), 200
    except Exception as e:
        return jsonify({"msg": "failed while creating the task"}), 400


def get_file_data(file_ref):
    stream = io.StringIO(file_ref.stream.read().decode("UTF8"), newline=None)
    reader = csv.DictReader(stream)
    order_data = {}
    for row in reader:
        awb = row['awb']
        charged_weight = float(row['charged_weight'])
        courier_id = row['courier_id']
        order_data[awb] = [charged_weight, courier_id]
    return order_data