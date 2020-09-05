from flask import Flask
from celery import Celery
from datetime import timedelta
from celery.schedules import crontab
from .update_status.function import update_status
from .fetch_orders.function import fetch_orders


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
app.config['CELERY_BACKEND'] = "amqp://guest:guest@rabbitmq:5672"
app.config['CELERY_BROKER_URL'] = "amqp://guest:guest@rabbitmq:5672"


app.config['CELERYBEAT_SCHEDULE'] = {
    'run-status-update': {
            'task': 'status_update',
            'schedule': crontab(minute='31', hour='*/2')
        },
    'run-fetch-orders': {
                'task': 'fetch_orders',
                'schedule': crontab(minute='*/20')
            },
}

app.config['CELERY_TIMEZONE'] = 'UTC'

celery_app = make_celery(app)


"""
@app.route('/ship_orders')
def ship_order_url():
    result = ship_orders.delay()
    return result.wait()
"""

@celery_app.task(name='status_update')
def status_update():
    update_status()
    return 'successfully completed status_update'


@celery_app.task(name='fetch_orders')
def orders_fetch():
    fetch_orders()
    return 'successfully completed fetch_orders'

"""
@app.route('/update_status')
def status_update_url():
    result = status_update.delay()
    return result.wait()
"""