# services/exercises/project/api/models.py


from project import db
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy import UniqueConstraint
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
    api_key = db.Column(db.String, nullable=True)
    api_password = db.Column(db.String, nullable=True)
    api_url = db.Column(db.String, nullable=True)
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


class ReturnPoints(db.Model):
    __tablename__ = "return_points"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    return_location = db.Column(db.Text, nullable=True)
    name = db.Column(db.String, nullable=False)
    phone = db.Column(db.String, nullable=False)
    address = db.Column(db.String, nullable=True)
    address_two = db.Column(db.String, nullable=True)
    city = db.Column(db.String, nullable=False)
    state = db.Column(db.String, nullable=False)
    country = db.Column(db.String, nullable=False)
    pincode = db.Column(db.Integer, nullable=False)
    warehouse_prefix = db.Column(db.String, nullable=True)


class OPAssociation(db.Model):
    __tablename__ = 'op_association'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    order_id = db.Column('order_id', db.Integer, db.ForeignKey('orders.id'))
    product_id = db.Column('product_id', db.Integer, db.ForeignKey('products.id'))
    quantity = db.Column(db.Integer)
    order = db.relationship("Orders")
    product = db.relationship("Products")


class Orders(db.Model):
    __tablename__ = "orders"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    channel_order_id = db.Column(db.String, nullable=True)
    order_date = db.Column(db.DateTime, nullable=False)
    customer_name = db.Column(db.String, nullable=False)
    customer_email = db.Column(db.String, nullable=True)
    customer_phone = db.Column(db.String, nullable=False)
    status = db.Column(db.String, nullable=False)
    status_type = db.Column(db.String, nullable=True)
    status_detail = db.Column(db.String, nullable=True)
    products = db.relationship("OPAssociation", backref="orders", primaryjoin=id == OPAssociation.order_id)
    delivery_address_id = db.Column(db.Integer, db.ForeignKey('shipping_address.id'))
    delivery_address = db.relationship("ShippingAddress", backref=db.backref("orders", uselist=True))
    client_prefix = db.Column(db.String, nullable=True)
    client_channel_id = db.Column(db.Integer, db.ForeignKey('client_channel.id'))
    client_channel = db.relationship("ClientChannel", backref=db.backref("orders", uselist=True))
    order_id_channel_unique = db.Column(db.String, nullable=True)
    date_created = db.Column(db.DateTime, default=datetime.now)
    date_updated = db.Column(db.DateTime, onupdate=datetime.now)
    __table_args__ = (UniqueConstraint('channel_order_id', 'client_prefix', name='id_client_unique'),
                      )


class OrdersPayments(db.Model):
    __tablename__ = "orders_payments"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    payment_mode = db.Column(db.String, nullable=False)
    amount = db.Column(db.FLOAT, nullable=False)
    subtotal = db.Column(db.FLOAT, nullable=True)
    shipping_charges = db.Column(db.FLOAT, nullable=True)
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
    return_point_id = db.Column(db.Integer, db.ForeignKey('return_points.id'))
    return_point = db.relationship("ReturnPoints", backref=db.backref("shipments", uselist=True))
    courier_id = db.Column(db.Integer, db.ForeignKey('master_couriers.id'))
    courier = db.relationship("MasterCouriers", backref=db.backref("shipments", uselist=True))
    routing_code = db.Column(db.String, nullable=True)
    edd = db.Column(db.DateTime)
    channel_fulfillment_id = db.Column(db.String, nullable=True)
    tracking_link = db.Column(db.TEXT, nullable=True)
    remark = db.Column(db.Text, nullable=True)


class ClientChannel(db.Model):
    __tablename__ = "client_channel"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    client_prefix = db.Column(db.String, nullable=True)
    channel_id = db.Column(db.Integer, db.ForeignKey('master_channels.id'))
    channel = db.relationship("MasterChannels", backref=db.backref("client_channel", uselist=True))
    api_key = db.Column(db.String, nullable=True)
    api_password = db.Column(db.String, nullable=True)
    shop_url = db.Column(db.String, nullable=True)
    last_synced_order = db.Column(db.String, nullable=True)
    last_synced_time = db.Column(db.DateTime, nullable=True)
    date_created = db.Column(db.DateTime, default=datetime.now)
    date_updated = db.Column(db.DateTime, onupdate=datetime.now)


class ShippingAddress(db.Model):
    __tablename__ = "shipping_address"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    first_name = db.Column(db.String, nullable=True)
    last_name = db.Column(db.String, nullable=True)
    address_one = db.Column(db.Text, nullable=True)
    address_two = db.Column(db.Text, nullable=True)
    city = db.Column(db.String, nullable=True)
    pincode = db.Column(db.String, nullable=True)
    state = db.Column(db.String, nullable=True)
    country = db.Column(db.String, nullable=True)
    phone = db.Column(db.String, nullable=True)
    latitude = db.Column(db.FLOAT, nullable=True)
    longitude = db.Column(db.FLOAT, nullable=True)
    country_code = db.Column(db.String, nullable=True)


class ClientCouriers(db.Model):
    __tablename__ = "client_couriers"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    client_prefix = db.Column(db.String, nullable=True)
    courier_id = db.Column(db.Integer, db.ForeignKey('master_couriers.id'))
    courier = db.relationship("MasterCouriers", backref=db.backref("client_couriers", uselist=True))
    priority = db.Column(db.Integer, nullable=False, default=1)
    last_shipped_order_id = db.Column(db.Integer, db.ForeignKey('orders.id'))
    last_shipped_order = db.relationship("Orders", backref=db.backref("client_couriers", uselist=True))
    last_shipped_time = db.Column(db.DateTime, nullable=True)
    unique_parameter = db.Column(db.String, nullable=True)
    date_created = db.Column(db.DateTime, default=datetime.now)
    date_updated = db.Column(db.DateTime, onupdate=datetime.now)


class ClientPickups(db.Model):
    __tablename__ = "client_pickups"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    client_prefix = db.Column(db.String, nullable=True)
    pickup_id = db.Column(db.Integer, db.ForeignKey('pickup_points.id'))
    pickup = db.relationship("PickupPoints", backref=db.backref("client_pickups", uselist=True))
    return_point_id = db.Column(db.Integer, db.ForeignKey('return_points.id'))
    return_point = db.relationship("ReturnPoints", backref=db.backref("client_returns", uselist=True))
    date_created = db.Column(db.DateTime, default=datetime.now)
    date_updated = db.Column(db.DateTime, onupdate=datetime.now)


class PickupRequests(db.Model):
    __tablename__ = "pickup_requests"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    client_prefix = db.Column(db.String, nullable=False)
    warehouse_prefix = db.Column(db.String, nullable=False)
    last_picked_order_id = db.Column(db.Integer, db.ForeignKey('orders.id'))
    last_picked_order = db.relationship("Orders", backref=db.backref("pickup_requests", uselist=True))
    pickup_after_hours = db.Column(db.Integer, nullable=False)
    last_pickup_request_date = db.Column(db.DateTime, nullable=True)
    pickup_id = db.Column(db.Integer, db.ForeignKey('pickup_points.id'))
    pickup = db.relationship("PickupPoints", backref=db.backref("pickup_requests", uselist=True))
    date_created = db.Column(db.DateTime, default=datetime.now)
    date_updated = db.Column(db.DateTime, onupdate=datetime.now)


class Manifests(db.Model):
    __tablename__ = "manifests"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    manifest_id = db.Column(db.String, nullable=False)
    warehouse_prefix = db.Column(db.String, nullable=False)
    courier_id = db.Column(db.Integer, db.ForeignKey('master_couriers.id'))
    courier = db.relationship("MasterCouriers", backref=db.backref("manifests", uselist=True))
    pickup_id = db.Column(db.Integer, db.ForeignKey('pickup_points.id'))
    pickup = db.relationship("PickupPoints", backref=db.backref("manifests", uselist=True))
    total_scheduled = db.Column(db.Integer, nullable=True)
    total_picked = db.Column(db.Integer, nullable=True)
    pickup_date = db.Column(db.DateTime, nullable=True)
    manifest_url = db.Column(db.TEXT, nullable=False)
    date_created = db.Column(db.DateTime, default=datetime.now)
    date_updated = db.Column(db.DateTime, onupdate=datetime.now)


class OrderStatus(db.Model):
    __tablename__ = "order_status"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    order_id = db.Column(db.Integer, db.ForeignKey('orders.id'))
    order = db.relationship("Orders", backref=db.backref("order_status", uselist=True))
    courier_id = db.Column(db.Integer, db.ForeignKey('master_couriers.id'))
    courier = db.relationship("MasterCouriers", backref=db.backref("order_status", uselist=True))
    status_code = db.Column(db.String, nullable=True)
    status = db.Column(db.String, nullable=True)
    status_text = db.Column(db.String, nullable=True)
    location = db.Column(db.String, nullable=True)
    status_time = db.Column(db.DateTime, default=datetime.now)









