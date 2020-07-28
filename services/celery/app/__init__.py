from flask import Flask
from celery import Celery
from datetime import timedelta
from celery.schedules import crontab
from .create_shipments import lambda_handler as create_shipments
from .update_status import lambda_handler as update_status


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
app.config['CELERY_BACKEND'] = "redis://redis:6379/0"
app.config['CELERY_BROKER_URL'] = "redis://redis:6379/0"


app.config['CELERYBEAT_SCHEDULE'] = {
    'run-status-update': {
            'task': 'status_update',
            'schedule': crontab(minute='10', hour='*')
        },
}

app.config['CELERY_TIMEZONE'] = 'UTC'

celery_app = make_celery(app)


@celery_app.task(name='ship_orders')
def ship_orders():
    create_shipments()
    return 'ship orders job run'


@app.route('/ship_orders')
def ship_order_url():
    result = ship_orders.delay()
    return result.wait()


@celery_app.task(name='status_update')
def status_update():
    update_status()
    return 'ship orders job run'


@app.route('/update_status')
def status_update_url():
    result = status_update.delay()
    return result.wait()