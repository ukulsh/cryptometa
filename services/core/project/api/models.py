# services/exercises/project/api/models.py


from project import db
from sqlalchemy.dialects.postgresql import JSON
from datetime import datetime


class Products(db.Model):
    __tablename__ = "products"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String, nullable=False)
    sku = db.Column(db.String, nullable=False, unique=True)
    dimensions = db.Column(JSON)
    weight = db.Column(db.FLOAT, nullable=True)
    product_image = db.Column(db.String, nullable=True)
    price = db.Column(db.FLOAT, nullable=True)
    client_prefix = db.Column(db.String, nullable=True)
    active = db.Column(db.BOOLEAN, nullable=True, default=True)
    inactive_reason = db.Column(db.String, nullable=True, default="")
    channel_id = db.Column(db.Integer, db.ForeignKey('master_channels.id'))
    channel = db.relationship("MasterChannels", backref=db.backref("products", uselist=True))
    date_created = db.Column(db.DateTime, default=datetime.now)
    date_updated = db.Column(db.DateTime, onupdate=datetime.now)

    def to_json(self):
        return {
            'id': self.id,
            'name': self.name,
            'sku': self.sku
        }


class ProductQuantity(db.Model):
    __tablename__ = "products_quantity"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    product_id = db.Column(db.Integer, db.ForeignKey('products.id'))
    product = db.relationship("Products", backref=db.backref("quantity", uselist=True))
    total_quantity = db.Column(db.Integer, nullable=False)
    approved_quantity = db.Column(db.Integer, nullable=True)
    available_quantity = db.Column(db.Integer, nullable=True)
    warehouse_prefix = db.Column(db.String, nullable=False)
    status = db.Column(db.String, nullable=False)
    date_created = db.Column(db.DateTime, default=datetime.now)
    date_updated = db.Column(db.DateTime, onupdate=datetime.now)


class MasterChannels(db.Model):
    __tablename__ = "master_channels"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    channel_name = db.Column(db.String, nullable=False)
    logo_url = db.Column(db.String, nullable=True)
    date_created = db.Column(db.DateTime, default=datetime.now)
    date_updated = db.Column(db.DateTime, onupdate=datetime.now)


class MasterCouriers(db.Model):
    __tablename__ = "master_couriers"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    courier_name = db.Column(db.String, nullable=False)
    logo_url = db.Column(db.String, nullable=True)
    date_created = db.Column(db.DateTime, default=datetime.now)
    date_updated = db.Column(db.DateTime, onupdate=datetime.now)


class PickupPoints(db.Model):
    __tablename__ = "pickup_points"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    pickup_location = db.Column(db.Text, nullable=False)
    name = db.Column(db.String, nullable=False)
    phone = db.Column(db.String, nullable=False)
    address = db.Column(db.String, nullable=True)
    address_two = db.Column(db.String, nullable=True)
    city = db.Column(db.String, nullable=False)
    state = db.Column(db.String, nullable=False)
    country = db.Column(db.String, nullable=False)
    pincode = db.Column(db.Integer, nullable=False)
    warehouse_prefix = db.Column(db.String, nullable=True)


op_association = db.Table('op_association', db.Model.metadata,
    db.Column('order_id', db.Integer, db.ForeignKey('orders.id')),
    db.Column('product_id', db.Integer, db.ForeignKey('products.id')),
    db.Column('quantity', db.Integer)
)


class Orders(db.Model):
    __tablename__ = "orders"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    channel_order_id = db.Column(db.String, nullable=True)
    order_date = db.Column(db.DateTime, nullable=False)
    customer_name = db.Column(db.String, nullable=False)
    customer_email = db.Column(db.String, nullable=True)
    customer_phone = db.Column(db.String, nullable=False)
    status = db.Column(db.String, nullable=False)
    products = db.relationship("Products", secondary=op_association)
    delivery_address = db.Column(JSON)
    client_prefix = db.Column(db.String, nullable=True)
    date_created = db.Column(db.DateTime, default=datetime.now)
    date_updated = db.Column(db.DateTime, onupdate=datetime.now)


class OrdersPayments(db.Model):
    __tablename__ = "orders_payments"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    payment_mode = db.Column(db.String, nullable=False)
    amount = db.Column(db.FLOAT, nullable=False)
    currency = db.Column(db.String, nullable=False, default='INR')
    order_id = db.Column(db.Integer, db.ForeignKey('orders.id'))
    order = db.relationship("Orders", backref=db.backref("payments", uselist=True))


class Shipments(db.Model):
    __tablename__ = "shipments"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    awb = db.Column(db.String, nullable=False)
    status = db.Column(db.String, nullable=True)
    weight = db.Column(db.FLOAT, nullable=True)
    volumetric_weight = db.Column(db.FLOAT, nullable=True)
    dimensions = db.Column(JSON)
    order_id = db.Column(db.Integer, db.ForeignKey('orders.id'))
    order = db.relationship("Orders", backref=db.backref("shipments", uselist=True))
    pickup_id = db.Column(db.Integer, db.ForeignKey('pickup_points.id'))
    pickup = db.relationship("PickupPoints", backref=db.backref("shipments", uselist=True))
    courier_id = db.Column(db.Integer, db.ForeignKey('master_couriers.id'))
    courier = db.relationship("MasterCouriers", backref=db.backref("shipments", uselist=True))




