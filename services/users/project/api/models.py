# services/users/project/api/models.py

import datetime
import jwt

from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import ARRAY
from flask import current_app

from project import db, bcrypt


class User(db.Model):

    __tablename__ = 'users'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    username = db.Column(db.String(128), unique=True, nullable=False)  # new
    email = db.Column(db.String(128), unique=True, nullable=False)
    password = db.Column(db.String(255), nullable=False)   # new
    first_name = db.Column(db.String(255), nullable=True)   # new
    last_name = db.Column(db.String(255), nullable=True)   # new
    active = db.Column(db.Boolean(), default=True, nullable=False)
    created_date = db.Column(db.DateTime, default=func.now(), nullable=False)
    client_id = db.Column(db.Integer, db.ForeignKey('clients.id'))
    client = db.relationship("Client", backref=db.backref("clients", uselist=True))
    warehouse_id = db.Column(db.Integer, db.ForeignKey('warehouses.id'))
    warehouse = db.relationship("Warehouse", backref=db.backref("warehouses", uselist=True))
    group_id = db.Column(db.Integer, db.ForeignKey('usergroups.id'))
    group = db.relationship("UserGroups", backref=db.backref("usergroups", uselist=True))
    tabs = db.Column(ARRAY(db.String(20)))
    phone_no = db.Column(db.String(13), nullable=True)
    calling_active = db.Column(db.Boolean(), default=False, nullable=True)
    admin = db.Column(db.Boolean, default=False, nullable=False)
    login_as = db.Column(db.Integer, db.ForeignKey('users.id'))

    def __init__(self, username, email, password, first_name=None, last_name=None,
                 tabs=None, calling_active=False, client_id=None, group_id=None, phone_number=None):
        self.username = username
        self.email = email
        self.password = bcrypt.generate_password_hash(
            password, current_app.config.get('BCRYPT_LOG_ROUNDS')
        ).decode()
        self.first_name = first_name
        self.last_name = last_name
        self.tabs = tabs
        self.calling_active = calling_active
        self.client_id = client_id
        self.group_id = group_id
        self.phone_no = phone_number

    def to_json(self):
        return {
            'id': self.id,
            'username': self.username,
            'first_name': self.first_name,
            'last_name': self.last_name,
            'email': self.email,
            'active': self.active,
            'admin': self.admin,
            'user_group': self.group.group,
            'client_prefix': self.client.client_prefix if self.client else None,
            'warehouse_prefix': self.warehouse.warehouse_prefix if self.warehouse else None,
            'tabs': self.tabs,
            'phone_no': self.phone_no,
            'calling_active': self.calling_active,
        }

    def encode_auth_token(self, user_id):
        """Generates the auth token"""
        try:
            # new
            payload = {
                'exp': datetime.datetime.utcnow() + datetime.timedelta(
                    days=current_app.config.get('TOKEN_EXPIRATION_DAYS'),
                    seconds=current_app.config.get('TOKEN_EXPIRATION_SECONDS')
                ),
                'iat': datetime.datetime.utcnow(),
                'sub': user_id
            }
            return jwt.encode(
                payload,
                current_app.config.get('SECRET_KEY'),
                algorithm='HS256'
            )
        except Exception as e:
            return e

    @staticmethod
    def decode_auth_token(auth_token):
        """
        Decodes the auth token - :param auth_token: - :return: integer|string
        """
        try:
            payload = jwt.decode(
                auth_token, current_app.config.get('SECRET_KEY'))
            return payload['sub']
        except jwt.ExpiredSignatureError:
            return 'Signature expired. Please log in again.'
        except jwt.InvalidTokenError:
            return 'Invalid token. Please log in again.'


class Client(db.Model):
    __tablename__ = 'clients'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    client_name = db.Column(db.String(128), nullable=False)
    client_prefix = db.Column(db.String(32), unique=True, nullable=False)
    primary_email = db.Column(db.String(128), nullable=False)
    tabs = db.Column(ARRAY(db.String(20)))
    active = db.Column(db.Boolean(), default=True, server_default="true", nullable=False)
    created_date = db.Column(db.DateTime, default=func.now(), server_default=func.now(), nullable=False)

    def __init__(self, client_name, primary_email, client_prefix, tabs):
        self.client_name = client_name
        self.primary_email = primary_email
        self.client_prefix = client_prefix
        self.tabs = tabs

    def to_json(self):
        return {
            'id': self.id,
            'client_name': self.client_name,
            'client_prefix': self.client_prefix,
            'primary_email': self.primary_email,
            'active': self.active,
            'tabs': self.tabs if isinstance(self.tabs, list) else []
        }


class Warehouse(db.Model):
    __tablename__ = 'warehouses'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    warehouse_name = db.Column(db.String(128), nullable=False)
    warehouse_prefix = db.Column(db.String(32), unique=True, nullable=False)
    primary_email = db.Column(db.String(128), nullable=False)

    def __init__(self, warehouse_name, primary_email):
        self.warehouse_name = warehouse_name
        self.primary_email = primary_email

    def to_json(self):
        return {
            'id': self.id,
            'warehouse_name': self.warehouse_name,
            'primary_email': self.primary_email
        }


class UserGroups(db.Model):
    __tablename__ = 'usergroups'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    group = db.Column(db.String(128), unique=True, nullable=False)

    def __init__(self, group):
        self.group = group