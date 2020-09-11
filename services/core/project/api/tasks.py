from project import make_celery
from celery import Celery, shared_task
from flask import current_app

celery_app = make_celery(current_app)


@celery_app.task(name='add_nos')
def add(a,b):
    return 0