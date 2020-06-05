# services/exercises/project/api/models.py


from project import db
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy import UniqueConstraint
from datetime import datetime
from sqlalchemy.dialects.postgresql import ARRAY


class Products(db.Model):
    __tablename__ = "products"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String, nullable=False)
    sku = db.Column(db.String, nullable=False, unique=True)
    master_sku = db.Column(db.String, nullable=True)
    dimensions = db.Column(JSON)
    weight = db.Column(db.FLOAT, nullable=True)
    product_image = db.Column(db.String, nullable=True)
    price = db.Column(db.FLOAT, nullable=True)
    client_prefix = db.Column(db.String, nullable=True)
    active = db.Column(db.BOOLEAN, nullable=True, default=True)
    inactive_reason = db.Column(db.String, nullable=True, default="")
    channel_id = db.Column(db.Integer, db.ForeignKey('master_channels.id'))
    channel = db.relationship("MasterChannels", backref=db.backref("products", uselist=True))
    subcategory_id = db.Column(db.Integer, db.ForeignKey('products_subcategories.id'))
    subcategory = db.relationship("ProductsSubCategories", backref=db.backref("products"))
    hsn_code = db.Column(db.String, nullable=True)
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
    inline_quantity = db.Column(db.Integer, nullable=True)
    rto_quantity = db.Column(db.Integer, nullable=True)
    current_quantity = db.Column(db.Integer, nullable=True)
    warehouse_prefix = db.Column(db.String, nullable=False)
    status = db.Column(db.String, nullable=False)
    date_created = db.Column(db.DateTime, default=datetime.now)
    date_updated = db.Column(db.DateTime, onupdate=datetime.now)


class KeywordWeights(db.Model):
    __tablename__ = "keyword_weights"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    client_prefix = db.Column(db.String, nullable=True)
    keywords = db.Column(ARRAY(db.String(20)))
    warehouse_prefix = db.Column(db.String, nullable=False)
    dimensions = db.Column(JSON)
    weight = db.Column(db.FLOAT, nullable=True)
    subcategory_id = db.Column(db.Integer, db.ForeignKey('products_subcategories.id'))
    subcategory = db.relationship("ProductsSubCategories", backref=db.backref("keyword_weights"))
    date_created = db.Column(db.DateTime, default=datetime.now)
    date_updated = db.Column(db.DateTime, onupdate=datetime.now)


class ProductsCombos(db.Model):
    __tablename__ = "products_combos"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    combo_id = db.Column(db.Integer, db.ForeignKey('products.id'))
    combo = db.relationship("Products", backref=db.backref("combo", uselist=True), foreign_keys=[combo_id])
    combo_prod_id = db.Column(db.Integer, db.ForeignKey('products.id'))
    combo_prod = db.relationship("Products", backref=db.backref("combo_prod", uselist=True), foreign_keys=[combo_prod_id])
    quantity = db.Column(db.Integer, nullable=False, default=1)
    date_created = db.Column(db.DateTime, default=datetime.now)
    date_updated = db.Column(db.DateTime, onupdate=datetime.now)


class ProductsCategories(db.Model):
    __tablename__ = "products_categories"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String, nullable=False)
    date_created = db.Column(db.DateTime, default=datetime.now)
    date_updated = db.Column(db.DateTime, onupdate=datetime.now)


class ProductsSubCategories(db.Model):
    __tablename__ = "products_subcategories"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String, nullable=False)
    category_id = db.Column(db.Integer, db.ForeignKey('products_categories.id'))
    category = db.relationship("ProductsCategories", backref=db.backref("subcategory"))
    date_created = db.Column(db.DateTime, default=datetime.now)
    date_updated = db.Column(db.DateTime, onupdate=datetime.now)


class InventoryUpdate(db.Model):
    __tablename__ = "inventory_update"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    product_id = db.Column(db.Integer, db.ForeignKey('products.id'))
    product = db.relationship("Products", backref=db.backref("inventory_update", uselist=True))
    warehouse_prefix = db.Column(db.String, nullable=False)
    user = db.Column(db.String, nullable=False)
    remark = db.Column(db.String, nullable=True)
    quantity = db.Column(db.Integer, nullable=False)
    type = db.Column(db.String, nullable=False)
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
    integrated = db.Column(db.BOOLEAN, nullable=True, default=None)
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
    amount = db.Column(db.FLOAT, nullable=True)
    channel_item_id = db.Column(db.String, nullable=True)
    tax_lines = db.Column(JSON)
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
    billing_address_id = db.Column(db.Integer, db.ForeignKey('billing_address.id'))
    billing_address = db.relationship("BillingAddress", backref=db.backref("orders", uselist=True))
    client_prefix = db.Column(db.String, nullable=True)
    client_channel_id = db.Column(db.Integer, db.ForeignKey('client_channel.id'))
    client_channel = db.relationship("ClientChannel", backref=db.backref("orders", uselist=True))
    order_id_channel_unique = db.Column(db.String, nullable=True)
    pickup_data_id = db.Column(db.Integer, db.ForeignKey('client_pickups.id'))
    pickup_data = db.relationship("ClientPickups", backref=db.backref("orders", uselist=True))
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


class CODRemittance(db.Model):
    __tablename__ = "cod_remittance"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    client_prefix = db.Column(db.String, nullable=True)
    remittance_id = db.Column(db.String, nullable=False)
    remittance_date = db.Column(db.DateTime)
    status = db.Column(db.String)
    transaction_id = db.Column(db.String)
    date_created = db.Column(db.DateTime, default=datetime.now)
    date_updated = db.Column(db.DateTime, onupdate=datetime.now)


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
    __table_args__ = (UniqueConstraint('order_id', name='order_id_unique'),
                      )


class NDRReasons(db.Model):
    __tablename__ = "ndr_reasons"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    reason = db.Column(db.String, nullable=False, unique=True)


class NDRShipments(db.Model):
    __tablename__ = "ndr_shipments"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    order_id = db.Column(db.Integer, db.ForeignKey('orders.id'))
    order = db.relationship("Orders", backref=db.backref("ndr_shipments"))
    shipment_id = db.Column(db.Integer, db.ForeignKey('shipments.id'))
    shipment = db.relationship("Shipments", backref=db.backref("ndr_shipments"))
    reason_id = db.Column(db.Integer, db.ForeignKey('ndr_reasons.id'))
    reason = db.relationship("NDRReasons", backref=db.backref("ndr_shipments"))
    current_status = db.Column(db.String, nullable=True)
    ndr_remark = db.Column(db.String, nullable=True)
    request_time = db.Column(db.DateTime, default=datetime.now)
    date_created = db.Column(db.DateTime, default=datetime.now)
    date_updated = db.Column(db.DateTime, onupdate=datetime.now)


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
    fetch_status = db.Column(ARRAY(db.String(20)))
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


class BillingAddress(db.Model):
    __tablename__ = "billing_address"
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
    priority = db.Column(db.Integer, nullable=False, default=1) #column to define product by which be ship order
    last_shipped_order_id = db.Column(db.Integer, db.ForeignKey('orders.id'))
    last_shipped_order = db.relationship("Orders", backref=db.backref("client_couriers", uselist=True))
    last_shipped_time = db.Column(db.DateTime, nullable=True)
    unique_parameter = db.Column(db.String, nullable=True)
    active = db.Column(db.BOOLEAN, nullable=True, default=None)
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
    gstin = db.Column(db.String, nullable=True)
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
    client_pickup_id = db.Column(db.Integer, db.ForeignKey('client_pickups.id'))
    client_pickup = db.relationship("ClientPickups", backref=db.backref("manifests", uselist=True))
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
    shipment_id = db.Column(db.Integer, db.ForeignKey('shipments.id'))
    shipment = db.relationship("Shipments", backref=db.backref("order_status", uselist=True))
    status_code = db.Column(db.String, nullable=True)
    status = db.Column(db.String, nullable=True)
    status_text = db.Column(db.String, nullable=True)
    location = db.Column(db.String, nullable=True)
    location_city = db.Column(db.String, nullable=True)
    status_time = db.Column(db.DateTime, default=datetime.now)


class CodVerification(db.Model):
    __tablename__ = "cod_verification"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    order_id = db.Column(db.Integer, db.ForeignKey('orders.id'))
    order = db.relationship("Orders", backref=db.backref("exotel_data", uselist=True))
    call_sid = db.Column(db.String, nullable=True)
    recording_url = db.Column(db.String, nullable=True)
    cod_verified = db.Column(db.BOOLEAN, nullable=True, default=None)
    verified_via = db.Column(db.String, nullable=True)
    verification_link = db.Column(db.String, nullable=True)
    verification_time = db.Column(db.DateTime, default=datetime.now)
    date_created = db.Column(db.DateTime, default=datetime.now)
    click_browser =  db.Column(db.String, nullable=True)
    click_platform =  db.Column(db.String, nullable=True)
    click_string =  db.Column(db.String, nullable=True)
    click_version =  db.Column(db.String, nullable=True)


class NDRVerification(db.Model):
    __tablename__ = "ndr_verification"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    order_id = db.Column(db.Integer, db.ForeignKey('orders.id'))
    order = db.relationship("Orders", backref=db.backref("ndr_verification", uselist=True))
    call_sid = db.Column(db.String, nullable=True)
    recording_url = db.Column(db.String, nullable=True)
    ndr_verified = db.Column(db.BOOLEAN, nullable=True, default=None)
    verified_via = db.Column(db.String, nullable=True)
    verification_link = db.Column(db.String, nullable=True)
    verification_time = db.Column(db.DateTime, default=datetime.now)
    date_created = db.Column(db.DateTime, default=datetime.now)
    click_browser = db.Column(db.String, nullable=True)
    click_platform = db.Column(db.String, nullable=True)
    click_string = db.Column(db.String, nullable=True)
    click_version = db.Column(db.String, nullable=True)


class DeliveryCheck(db.Model):
    __tablename__ = "delivery_check"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    order_id = db.Column(db.Integer, db.ForeignKey('orders.id'))
    order = db.relationship("Orders", backref=db.backref("delivery_check", uselist=True))
    call_sid = db.Column(db.String, nullable=True)
    recording_url = db.Column(db.String, nullable=True)
    del_verified = db.Column(db.BOOLEAN, nullable=True, default=None)
    verified_via = db.Column(db.String, nullable=True)
    verification_link = db.Column(db.String, nullable=True)
    verification_time = db.Column(db.DateTime, default=datetime.now)
    date_created = db.Column(db.DateTime, default=datetime.now)
    click_browser = db.Column(db.String, nullable=True)
    click_platform = db.Column(db.String, nullable=True)
    click_string = db.Column(db.String, nullable=True)
    click_version = db.Column(db.String, nullable=True)


class CouriersCosts(db.Model):
    __tablename__ = "courier_costs"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    courier_id = db.Column(db.Integer, db.ForeignKey('master_couriers.id'))
    courier = db.relationship("MasterCouriers", backref=db.backref("courier_costs", uselist=True))
    zone_a = db.Column(db.FLOAT, nullable=True)
    zone_b = db.Column(db.FLOAT, nullable=True)
    zone_c1 = db.Column(db.FLOAT, nullable=True)
    zone_c2 = db.Column(db.FLOAT, nullable=True)
    zone_d1 = db.Column(db.FLOAT, nullable=True)
    zone_d2 = db.Column(db.FLOAT, nullable=True)
    zone_e = db.Column(db.FLOAT, nullable=True)
    zone_a_add = db.Column(db.FLOAT, nullable=True)
    zone_b_add = db.Column(db.FLOAT, nullable=True)
    zone_c1_add = db.Column(db.FLOAT, nullable=True)
    zone_c2_add = db.Column(db.FLOAT, nullable=True)
    zone_d1_add = db.Column(db.FLOAT, nullable=True)
    zone_d2_add = db.Column(db.FLOAT, nullable=True)
    zone_e_add = db.Column(db.FLOAT, nullable=True)
    cod_min = db.Column(db.FLOAT, nullable=True)
    cod_ratio = db.Column(db.FLOAT, nullable=True)
    rto_ratio = db.Column(db.FLOAT, nullable=True)
    first_step = db.Column(db.FLOAT, nullable=True)
    next_step = db.Column(db.FLOAT, nullable=True)
    date_created = db.Column(db.DateTime, default=datetime.now)
    date_updated = db.Column(db.DateTime, onupdate=datetime.now)


class CostToClients(db.Model):
    __tablename__ = "cost_to_clients"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    client_prefix = db.Column(db.String, nullable=True)
    courier_id = db.Column(db.Integer, db.ForeignKey('master_couriers.id'))
    courier = db.relationship("MasterCouriers", backref=db.backref("cost_to_clients", uselist=True))
    zone_a = db.Column(db.FLOAT, nullable=True)
    zone_b = db.Column(db.FLOAT, nullable=True)
    zone_c = db.Column(db.FLOAT, nullable=True)
    zone_d = db.Column(db.FLOAT, nullable=True)
    zone_e = db.Column(db.FLOAT, nullable=True)
    cod_min = db.Column(db.FLOAT, nullable=True)
    cod_ratio = db.Column(db.FLOAT, nullable=True)
    rto_ratio = db.Column(db.FLOAT, nullable=True)
    management_fee = db.Column(db.FLOAT, nullable=True)
    date_created = db.Column(db.DateTime, default=datetime.now)
    date_updated = db.Column(db.DateTime, onupdate=datetime.now)


class ClientRecharges(db.Model):
    __tablename__ = "client_recharges"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    client_prefix = db.Column(db.String, nullable=True)
    recharge_amount = db.Column(db.FLOAT, nullable=True)
    transaction_id = db.Column(db.String, nullable=True)
    bank_transaction_id = db.Column(db.String, nullable=True)
    type = db.Column(db.String, nullable=True)
    status = db.Column(db.String, nullable=True)
    recharge_time = db.Column(db.DateTime, default=datetime.now)
    date_created = db.Column(db.DateTime, default=datetime.now)
    date_updated = db.Column(db.DateTime, onupdate=datetime.now)


class ClientDeductions(db.Model):
    __tablename__ = "client_deductions"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    forward_charge = db.Column(db.FLOAT, nullable=True)
    forward_charge_gst = db.Column(db.FLOAT, nullable=True)
    rto_charge = db.Column(db.FLOAT, nullable=True)
    rto_charge_gst = db.Column(db.FLOAT, nullable=True)
    cod_charge = db.Column(db.FLOAT, nullable=True)
    cod_charged_gst = db.Column(db.FLOAT, nullable=True)
    total_charge = db.Column(db.FLOAT, nullable=True)
    total_charged_gst = db.Column(db.FLOAT, nullable=True)
    shipment_id = db.Column(db.Integer, db.ForeignKey('shipments.id'))
    shipment = db.relationship("Shipments", backref=db.backref("client_deductions", uselist=True))
    weight_charged = db.Column(db.FLOAT, nullable=True)
    zone = db.Column(db.String, nullable=True)
    deduction_time = db.Column(db.DateTime, default=datetime.now)
    date_created = db.Column(db.DateTime, default=datetime.now)
    date_updated = db.Column(db.DateTime, onupdate=datetime.now)


class CourierCharges(db.Model):
    __tablename__ = "courier_charges"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    forward_charge = db.Column(db.FLOAT, nullable=True)
    rto_charge = db.Column(db.FLOAT, nullable=True)
    cod_charge = db.Column(db.FLOAT, nullable=True)
    total_charge = db.Column(db.FLOAT, nullable=True)
    shipment_id = db.Column(db.Integer, db.ForeignKey('shipments.id'))
    shipment = db.relationship("Shipments", backref=db.backref("courier_charges", uselist=True))
    weight_charged = db.Column(db.FLOAT, nullable=True)
    zone = db.Column(db.String, nullable=True)
    deduction_time = db.Column(db.DateTime, default=datetime.now)
    date_created = db.Column(db.DateTime, default=datetime.now)
    date_updated = db.Column(db.DateTime, onupdate=datetime.now)


class ClientMapping(db.Model):
    __tablename__ = "client_mapping"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    client_prefix = db.Column(db.String, nullable=False)
    client_name = db.Column(db.String, nullable=False)
    client_logo = db.Column(db.String, nullable=True)
    theme_color = db.Column(db.String, nullable=True)
    api_token = db.Column(db.String, nullable=True)
    verify_cod = db.Column(db.BOOLEAN, nullable=True, default=True)
    essential = db.Column(db.BOOLEAN, nullable=True, default=True)
    custom_email = db.Column(db.Text, nullable=True)
    custom_email_subject = db.Column(db.String, nullable=True)


class MultiVendor(db.Model):
    __tablename__ = "multi_vendor"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    client_prefix = db.Column(db.String, nullable=False)
    vendor_list = db.Column(ARRAY(db.String(50)))


class WarehouseMapping(db.Model):
    __tablename__ = "warehouse_mapping"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    warehouse_prefix = db.Column(db.String, nullable=False)
    shiplabel_type = db.Column(db.String, nullable=True)


class ClientChannelLocations(db.Model):
    __tablename__ = "client_channel_locations"
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    pickup_data_id = db.Column(db.Integer, db.ForeignKey('client_pickups.id'))
    pickup_data = db.relationship("ClientPickups", backref=db.backref("client_channel_locations"))
    client_channel_id = db.Column(db.Integer, db.ForeignKey('client_channel.id'))
    client_channel = db.relationship("ClientChannel", backref=db.backref("client_channel_locations"))
    location_id = db.Column(db.String, nullable=False)