# services/core/project/__init__.py
import os
from flask import Flask
from flask_cors import CORS
from flask_debugtoolbar import DebugToolbarExtension
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from celery import Celery


# instantiate the extensions
db = SQLAlchemy()
migrate = Migrate()
toolbar = DebugToolbarExtension()
cors = CORS()


def create_app(script_info=None):

    # instantiate the app
    app = Flask(__name__)

    # enable CORS
    CORS(app)

    # set config
    app_settings = os.getenv('APP_SETTINGS')
    app.config.from_object(app_settings)
    app.config['CELERY_BACKEND'] = "amqp://ravi:Kad97711@rabbitmq:5672"
    app.config['CELERY_BROKER_URL'] = "amqp://ravi:Kad97711@rabbitmq:5672"
    # set up extensions
    toolbar.init_app(app)
    cors.init_app(app)
    db.init_app(app)
    migrate.init_app(app, db)

    # register blueprints
    from project.api.base import base_blueprint
    from project.api.core import core_blueprint
    from project.api.dashboard.index import dashboard_blueprint
    from project.api.products.index import products_blueprint
    from project.api.orders.index import orders_blueprint
    from project.api.billing.index import billing_blueprint
    from project.api.webhooks.index import webhooks_blueprint
    from project.api.core_features.channels.index import channels_blueprint
    from project.api.core_features.couriers.index import couriers_blueprint
    app.register_blueprint(base_blueprint)
    app.register_blueprint(core_blueprint)
    app.register_blueprint(dashboard_blueprint)
    app.register_blueprint(products_blueprint)
    app.register_blueprint(orders_blueprint)
    app.register_blueprint(billing_blueprint)
    app.register_blueprint(webhooks_blueprint)
    app.register_blueprint(channels_blueprint)
    app.register_blueprint(couriers_blueprint)
    # shell context for flask cli
    @app.shell_context_processor
    def ctx():
        return {'app': app, 'db': db}

    return app


def make_celery(app):
    celery = Celery(
        app.import_name,
        backend=app.config['CELERY_BACKEND'],
        broker=app.config['CELERY_BROKER_URL']
    )
    celery.conf.update(app.config)

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask
    return celery
