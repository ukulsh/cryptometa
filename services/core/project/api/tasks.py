from project import make_celery
from celery import Celery, shared_task
from flask import current_app

celery_app = make_celery(current_app)


@celery_app.task(name='consume_ecom_scan')
def consume_ecom_scan(payload):
    return 0