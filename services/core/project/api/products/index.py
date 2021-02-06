import csv
import io
import json
import math
import re
from datetime import datetime, timedelta
import pandas as pd
from flask import Blueprint, request, jsonify, make_response
from flask_restful import Api, Resource
from psycopg2.extras import RealDictCursor
from sqlalchemy import func, or_
from project import db
from project.api.models import Products, ProductQuantity, MultiVendor, InventoryUpdate, MasterProducts, MasterChannels, ProductsCombos
from project.api.queries import select_product_list_query, select_product_list_channel_query, select_combo_list_query
from project.api.utils import authenticate_restful
from project.api.utilities.db_utils import DbConnection

products_blueprint = Blueprint('products', __name__)
api = Api(products_blueprint)

conn = DbConnection.get_db_connection_instance()
conn_2 = DbConnection.get_pincode_db_connection_instance()
PRODUCTS_DOWNLOAD_HEADERS = ["S. No.", "Product Name", "Channel SKU", "Master SKU", "Price", "Total Quantity",
                             "Available Quantity", "Current Quantity", "Inline Quantity", "RTO Quantity", "Dimensions", "Weight"]

CHANNEL_PRODUCTS_DOWNLOAD_HEADERS = ["S. No.", "Product Name", "Channel product id", "Channel SKU", "Master SKU", "Price", "Channel Name", "Status"]
COMBO_DOWNLOAD_HEADERS = ["S. No.", "ParentName", "ParentSKU", "ChildName", "ChildSKU", "Quantity"]

PRODUCT_UPLOAD_HEADERS = ["Name", "SKU", "Price", "WeightKG", "LengthCM", "BreadthCM", "HeightCM", "HSN", "TaxRate"]
PRODUCT_UPLOAD_HEADERS_CHANNEL = ["Name", "ChannelProductId", "SKU", "Price", "MasterSKU", "ChannelName", "ImageURL"]
BULKMAP_SKU_HEADERS = ["ChannelName", "ChannelProdID", "ChannelSKU", "MasterSKU"]
BULK_COMBO_HEADERS = ["ParentSKU", "ChildSKU", "Quantity"]


@products_blueprint.route('/products/v1/details', methods=['GET'])
@authenticate_restful
def get_products_details(resp):
    try:
        cur = conn.cursor()
        auth_data = resp.get('data')
        client_prefix = auth_data.get('client_prefix')
        prod_id = request.args.get('sku_id')
        if not prod_id:
            return jsonify({"success": False, "msg": "Prod ID not provided"}), 400

        query_to_run = """SELECT name, null, sku as master_sku, weight, dimensions, price, bb.warehouse_prefix as warehouse, 
                            bb.approved_quantity as total_quantity, bb.current_quantity, bb.available_quantity, bb.inline_quantity, bb.rto_quantity, aa.id
                            from master_products aa
                            left join products_quantity bb on aa.id=bb.product_id
                            WHERE aa.id=%s
                            __CLIENT_FILTER__
                            __WAREHOUSE_FILTER__"""%(str(prod_id))

        if auth_data['user_group'] == 'client':
            query_to_run = query_to_run.replace('__CLIENT_FILTER__',
                                                        "AND aa.client_prefix in ('%s')" % client_prefix)
        elif auth_data['user_group'] == 'multi-vendor':
            cur.execute("SELECT vendor_list FROM multi_vendor WHERE client_prefix='%s';" % client_prefix)
            vendor_list = cur.fetchone()['vendor_list']
            query_to_run = query_to_run.replace('__CLIENT_FILTER__',
                                                        "AND aa.client_prefix in %s" % str(tuple(vendor_list)))
        else:
            query_to_run = query_to_run.replace('__CLIENT_FILTER__', "")

        warehouse = request.args.get('warehouse')
        if warehouse:
            query_to_run = query_to_run.replace('__WAREHOUSE_FILTER__', "and warehouse_prefix='%s'"%warehouse)
            cur.execute(query_to_run)
            ret_tuple = cur.fetchone()
            if not ret_tuple:
                return jsonify({"success": False, "msg": "SKU, warehouse combination not found"}), 400

            ret_obj = {"name":ret_tuple[0],
                       "master_sku": ret_tuple[2],
                       "weight": ret_tuple[3],
                       "dimensions": ret_tuple[4],
                       "price": ret_tuple[5],
                       "warehouse": ret_tuple[6],
                       "total_quantity": ret_tuple[7],
                       "current_quantity": ret_tuple[8],
                       "available_quantity": ret_tuple[9],
                       "inline_quantity": ret_tuple[10],
                       "rto_quantity": ret_tuple[11],
                       }
            return jsonify({"success": True, "data": ret_obj}), 200

        query_to_run = query_to_run.replace('__WAREHOUSE_FILTER__', "")
        cur.execute(query_to_run)
        ret_tuple_all = cur.fetchall()
        if not ret_tuple_all:
            return jsonify({"success": False, "msg": "SKU, warehouse combination not found"}), 400

        wh_list = list()

        data = {"id": ret_tuple_all[0][12], "name": ret_tuple_all[0][0], "master_sku": ret_tuple_all[0][2],
                "weight": ret_tuple_all[0][3], "dimensions": ret_tuple_all[0][4], "price": ret_tuple_all[0][5]}

        for ret_tuple in ret_tuple_all:
            ret_obj = {"warehouse": ret_tuple[6],
                       "total_quantity": ret_tuple[7],
                       "current_quantity": ret_tuple[8],
                       "available_quantity": ret_tuple[9],
                       "inline_quantity": ret_tuple[10],
                       "rto_quantity": ret_tuple[11],
                       }

            wh_list.append(ret_obj)

        data["inventory"] = wh_list

        return jsonify({"success": True, "data": data}), 200

    except Exception as e:
        conn.rollback()
        return jsonify({"success": False}), 400


@products_blueprint.route('/products/v1/upload_clients', methods=['GET'])
@authenticate_restful
def get_upload_clients(resp):
    try:
        cur = conn.cursor()
        auth_data = resp.get('data')
        if auth_data['user_group'] not in ('warehouse', 'super-admin', 'multi-vendor'):
            return jsonify({"success": False, "msg": "Invalid user type"}), 400

        client_list = list()
        if auth_data['user_group']=='warehouse':
            query_to_execute = """select distinct(client_prefix) from client_pickups aa
                                     left join pickup_points bb on aa.pickup_id=bb.id
                                     WHERE bb.warehouse_prefix='%s'
                                     order by client_prefix;"""%auth_data['warehouse_prefix']

            cur.execute(query_to_execute)
            all_clients = cur.fetchall()
            for client in all_clients:
                client_list.append(client[0])
        elif auth_data['user_group'] == 'multi-vendor':
            all_vendors = None
            if auth_data['user_group'] == 'multi-vendor':
                all_vendors = db.session.query(MultiVendor).filter(
                    MultiVendor.client_prefix == auth_data['client_prefix']).first()
                all_vendors = all_vendors.vendor_list
            client_list = all_vendors
        else:
            query_to_execute = """select distinct(client_prefix) from client_mapping
                                     order by client_prefix"""
            cur.execute(query_to_execute)
            all_clients = cur.fetchall()
            for client in all_clients:
                client_list.append(client[0])

        return jsonify({"success": True, "client_list": client_list}), 200

    except Exception as e:
        return jsonify({"success": False, "Error": str(e.args[0])}), 400


@products_blueprint.route('/products/v1/upload', methods=['POST'])
@authenticate_restful
def upload_master_products(resp):
    auth_data = resp.get('data')
    if not auth_data:
        return {"success": False, "msg": "Auth Failed"}, 404

    myfile = request.files['myfile']

    if auth_data['user_group'] == 'client':
        client_prefix=auth_data['client_prefix']
    else:
        client_prefix=request.args.get('client_prefix')

    data_xlsx = pd.read_csv(myfile)
    failed_skus = list()

    si = io.StringIO()
    cw = csv.writer(si)
    cw.writerow(PRODUCT_UPLOAD_HEADERS)

    def process_row(row, failed_skus):
        row_data = row[1]
        try:
            order_exists = db.session.query(MasterProducts).filter(MasterProducts.sku==str(row_data.SKU).rstrip(), MasterProducts.client_prefix==client_prefix).first()
            if order_exists:
                failed_skus.append(str(row_data.SKU).rstrip())
                cw.writerow(list(row_data.values)+["SKU already exists."])
                return

            dimensions = None
            if row_data.LengthCM==row_data.LengthCM and row_data.BreadthCM==row_data.BreadthCM and row_data.HeightCM==row_data.HeightCM:
                dimensions  = {"length": float(row_data.LengthCM), "breadth": float(row_data.BreadthCM), "height": float(row_data.HeightCM)}
            prod_obj = MasterProducts(name=str(row_data.Name),
                                               sku=str(row_data.SKU),
                                               product_image=str(row_data.ImageURL) if row_data.ImageURL == row_data.ImageURL else None,
                                               client_prefix=client_prefix,
                                               price=float(row_data.Price),
                                               weight=float(row_data.WeightKG) if row_data.WeightKG==row_data.WeightKG else None,
                                               dimensions=dimensions,
                                               active=True,
                                               hsn_code=str(row_data.HSN) if row_data.HSN==row_data.HSN else None,
                                               tax_rate=float(row_data.TaxRate) if row_data.TaxRate==row_data.TaxRate else None,
                                               date_created=datetime.utcnow()+timedelta(hours=5.5))

            db.session.add(prod_obj)
            db.session.commit()

        except Exception as e:
            failed_skus.append(str(row_data.SKU).rstrip())
            cw.writerow(list(row_data.values) + [str(e.args[0])])
            db.session.rollback()

    for row in data_xlsx.iterrows():
        process_row(row, failed_skus)

    if failed_skus:
        output = make_response(si.getvalue())
        filename = "failed_uploads.csv"
        output.headers["Content-Disposition"] = "attachment; filename=" + filename
        output.headers["Content-type"] = "text/csv"
        return output

    return jsonify({
        'status': 'success',
        "failed_skus": failed_skus
    }), 200


@products_blueprint.route('/products/v1/channel_product_upload', methods=['POST'])
@authenticate_restful
def upload_channel_products(resp):
    auth_data = resp.get('data')
    if not auth_data:
        return {"success": False, "msg": "Auth Failed"}, 404

    myfile = request.files['myfile']

    if auth_data['user_group'] == 'client':
        client_prefix=auth_data['client_prefix']
    else:
        client_prefix=request.args.get('client_prefix')

    data_xlsx = pd.read_csv(myfile)
    failed_skus = list()

    si = io.StringIO()
    cw = csv.writer(si)
    cw.writerow(PRODUCT_UPLOAD_HEADERS_CHANNEL)

    def process_row(row, failed_skus):
        row_data = row[1]
        try:
            channel = db.session.query(MasterChannels).filter(MasterChannels.channel_name.ilike(row_data.ChannelName)).first()
            if not channel:
                failed_skus.append(str(row_data.SKU).rstrip())
                cw.writerow(list(row_data.values)+["Channel not found"])
                return
            master_prod = db.session.query(MasterProducts).filter(MasterProducts.sku==str(row_data.MasterSKU).rstrip(), MasterProducts.client_prefix==client_prefix).first()
            if row_data.MasterSKU==row_data.MasterSKU and not master_prod:
                failed_skus.append(str(row_data.SKU).rstrip())
                cw.writerow(list(row_data.values) + ["Master SKU not found"])
                return

            prod_obj = Products(name=str(row_data.Name),
                                               sku=str(row_data.SKU),
                                               product_image=str(row_data.ImageURL) if row_data.ImageURL == row_data.ImageURL else None,
                                               client_prefix=client_prefix,
                                               price=float(row_data.Price),
                                               master_product=master_prod,
                                               date_created=datetime.utcnow()+timedelta(hours=5.5))

            db.session.add(prod_obj)
            db.session.commit()

        except Exception as e:
            failed_skus.append(str(row_data.SKU).rstrip())
            cw.writerow(list(row_data.values) + [str(e.args[0])])
            db.session.rollback()

    for row in data_xlsx.iterrows():
        process_row(row, failed_skus)

    if failed_skus:
        output = make_response(si.getvalue())
        filename = "failed_uploads.csv"
        output.headers["Content-Disposition"] = "attachment; filename=" + filename
        output.headers["Content-type"] = "text/csv"
        return output

    return jsonify({
        'status': 'success',
        "failed_skus": failed_skus
    }), 200


@products_blueprint.route('/products/v1/bulk_map_sku', methods=['POST'])
@authenticate_restful
def bulk_map_sku(resp):
    auth_data = resp.get('data')
    if not auth_data:
        return {"success": False, "msg": "Auth Failed"}, 404

    myfile = request.files['myfile']

    if auth_data['user_group'] == 'client':
        client_prefix=auth_data['client_prefix']
    else:
        client_prefix=request.args.get('client_prefix')

    data_xlsx = pd.read_csv(myfile)
    failed_skus = list()

    si = io.StringIO()
    cw = csv.writer(si)
    cw.writerow(BULKMAP_SKU_HEADERS)

    def process_row(row, failed_skus):
        row_data = row[1]
        try:
            channel = db.session.query(MasterChannels).filter(MasterChannels.channel_name.ilike(row_data.ChannelName)).first()
            if not channel:
                failed_skus.append(str(row_data.SKU).rstrip())
                cw.writerow(list(row_data.values)+["Channel not found"])
                return
            master_prod = db.session.query(MasterProducts).filter(MasterProducts.sku==str(row_data.MasterSKU).rstrip(), MasterProducts.client_prefix==client_prefix).first()
            if not master_prod:
                failed_skus.append(str(row_data.SKU).rstrip())
                cw.writerow(list(row_data.values) + ["MasterSKU not found"])
                return

            channel_prod = db.session.query(Products).filter(Products.sku==str(row_data.ChannelProdID).rstrip(), Products.master_sku==str(row_data.ChannelSKU), Products.client_prefix==client_prefix).first()
            if not channel_prod:
                failed_skus.append(str(row_data.SKU).rstrip())
                cw.writerow(list(row_data.values)+["Channel prod not found"])
                return

            channel_prod.master_product=master_prod
            db.session.commit()

        except Exception as e:
            failed_skus.append(str(row_data.SKU).rstrip())
            cw.writerow(list(row_data.values) + [str(e.args[0])])
            db.session.rollback()

    for row in data_xlsx.iterrows():
        process_row(row, failed_skus)

    if failed_skus:
        output = make_response(si.getvalue())
        filename = "failed_uploads.csv"
        output.headers["Content-Disposition"] = "attachment; filename=" + filename
        output.headers["Content-type"] = "text/csv"
        return output

    return jsonify({
        'status': 'success',
        "failed_skus": failed_skus
    }), 200


@products_blueprint.route('/products/v1/bulk_add_combos', methods=['POST'])
@authenticate_restful
def bulk_add_combos(resp):
    auth_data = resp.get('data')
    if not auth_data:
        return {"success": False, "msg": "Auth Failed"}, 404

    myfile = request.files['myfile']

    if auth_data['user_group'] == 'client':
        client_prefix=auth_data['client_prefix']
    else:
        client_prefix=request.args.get('client_prefix')

    data_xlsx = pd.read_csv(myfile)
    failed_skus = list()

    si = io.StringIO()
    cw = csv.writer(si)
    cw.writerow(BULK_COMBO_HEADERS)

    def process_row(row, failed_skus):
        row_data = row[1]
        try:
            parent_prod = db.session.query(MasterProducts).filter(MasterProducts.sku==str(row_data.ParentSKU).rstrip(), MasterProducts.client_prefix==client_prefix).first()
            if not parent_prod:
                failed_skus.append(str(row_data.SKU).rstrip())
                cw.writerow(list(row_data.values) + ["ParentSKU not found"])
                return

            child_prod = db.session.query(MasterProducts).filter(
                MasterProducts.sku == str(row_data.ChildSKU).rstrip(),
                MasterProducts.client_prefix == client_prefix).first()
            if not child_prod:
                failed_skus.append(str(row_data.SKU).rstrip())
                cw.writerow(list(row_data.values) + ["ChildSKU not found"])
                return

            combo_obj = ProductsCombos(combo=parent_prod,
                                       combo_prod=child_prod,
                                       quantity = int(row_data.Quantity),
                                       date_created = datetime.utcnow()+timedelta(hours=5.5))

            db.session.add(combo_obj)
            db.session.commit()

        except Exception as e:
            failed_skus.append(str(row_data.SKU).rstrip())
            cw.writerow(list(row_data.values) + [str(e.args[0])])
            db.session.rollback()

    for row in data_xlsx.iterrows():
        process_row(row, failed_skus)

    if failed_skus:
        output = make_response(si.getvalue())
        filename = "failed_uploads.csv"
        output.headers["Content-Disposition"] = "attachment; filename=" + filename
        output.headers["Content-type"] = "text/csv"
        return output

    return jsonify({
        'status': 'success',
        "failed_skus": failed_skus
    }), 200


@products_blueprint.route('/products/v1/get_master_products', methods=['GET'])
@authenticate_restful
def get_master_products(resp):
    response = {"success": True}
    try:
        auth_data = resp.get('data')
        search_key = request.args.get('search', "")
        client_prefix = auth_data.get('client_prefix')
        all_vendors = None
        if auth_data['user_group'] == 'multi-vendor':
            all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
            all_vendors = all_vendors.vendor_list
        product_qs = db.session.query(MasterProducts).filter(or_(MasterProducts.name.ilike(r"%{}%".format(search_key)), MasterProducts.sku.ilike(r"%{}%".format(search_key))))
        if auth_data['user_group'] == 'client':
            product_qs = product_qs.filter(MasterProducts.client_prefix == client_prefix)

        if all_vendors:
            product_qs = product_qs.filter(Products.client_prefix.in_(all_vendors))

        product_qs = product_qs.limit(10).all()
        response['data'] = [{"name": x.name, "sku": x.sku, "id":x.id} for x in product_qs]
        return jsonify(response), 200
    except Exception:
        response['success'] = False
        return jsonify(response), 400


@products_blueprint.route('/products/v1/map_sku', methods=['GET'])
@authenticate_restful
def map_products(resp):
    response = {"success": True}
    try:
        auth_data = resp.get('data')
        channel_id = request.args.get('channel_id', None)
        master_id = request.args.get('master_id', None)
        map = request.args.get('map', 1)
        client_prefix = auth_data.get('client_prefix')

        if not channel_id:
            return jsonify({"success": False}), 400
        if master_id:
            master_id = int(master_id)
        channel_prod = db.session.query(Products).filter(Products.id == int(channel_id))
        master_prod = db.session.query(MasterProducts).filter(MasterProducts.id == master_id)
        all_vendors = None
        if auth_data['user_group'] == 'multi-vendor':
            all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
            all_vendors = all_vendors.vendor_list

        if auth_data['user_group'] == 'client':
            channel_prod = channel_prod.filter(Products.client_prefix == client_prefix)
            master_prod = master_prod.filter(MasterProducts.client_prefix == client_prefix)

        if all_vendors:
            channel_prod = channel_prod.filter(Products.client_prefix.in_(all_vendors))
            master_prod = master_prod.filter(MasterProducts.client_prefix.in_(all_vendors))

        channel_prod = channel_prod.first()
        master_prod = master_prod.first()
        if not channel_prod:
            return jsonify({"success": False}), 400

        master_prod = master_prod.first()
        channel_prod.master_product = master_prod
        db.session.commit()

        return jsonify({"success": True}), 200

    except Exception as e:
        response['success'] = False
        response['error'] = str(e.args[0])
        return jsonify(response), 400


@products_blueprint.route('/products/v1/add_combo', methods=['POST'])
@authenticate_restful
def add_combo(resp):
    response = {"success": True}
    try:
        auth_data = resp.get('data')
        data = json.loads(request.data)
        parent_id = data.get('parent_id')
        parent_id = int(parent_id)
        client_prefix = auth_data.get('client_prefix')
        parent_prod = db.session.query(MasterProducts).filter(MasterProducts.id == parent_id)
        all_vendors=None
        if auth_data['user_group'] == 'multi-vendor':
            all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
            all_vendors = all_vendors.vendor_list

        if auth_data['user_group'] == 'client':
            parent_prod = parent_prod.filter(MasterProducts.client_prefix == client_prefix)

        if all_vendors:
            parent_prod = parent_prod.filter(MasterProducts.client_prefix.in_(all_vendors))

        parent_prod = parent_prod.first()

        if not parent_prod:
            return jsonify({"success": False}), 400

        for child in data.get('child_skus'):
            child_prod = db.session.query(MasterProducts).filter(MasterProducts.id == child['id']).first()
            if not child_prod:
                return jsonify({"success": False}), 400
            combo_obj = ProductsCombos(combo=parent_prod,
                                       combo_prod=child_prod,
                                       quantity=child['quantity'],
                                       date_created = datetime.utcnow()+timedelta(hours=5.5))

            db.session.add(combo_obj)

        db.session.commit()
        return jsonify({"success": True}), 200

    except Exception as e:
        response['success'] = False
        response['error'] = str(e.args[0])
        return jsonify(response), 400


@products_blueprint.route('/products/v1/get_filters', methods=['GET'])
@authenticate_restful
def get_products_filters(resp):
    response = {"filters": {}, "success": True}
    auth_data = resp.get('data')
    current_tab = request.args.get('tab')
    client_prefix = auth_data.get('client_prefix')
    all_vendors = None
    if auth_data['user_group'] == 'multi-vendor':
        all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
        all_vendors = all_vendors.vendor_list
    warehouse_qs = db.session.query(ProductQuantity.warehouse_prefix, func.count(ProductQuantity.warehouse_prefix)) \
        .join(Products, Products.id == ProductQuantity.product_id)
    if auth_data['user_group'] == 'client':
        warehouse_qs = warehouse_qs.filter(Products.client_prefix == client_prefix)
    if auth_data['user_group'] == 'warehouse':
        warehouse_qs = warehouse_qs.filter(ProductQuantity.warehouse_prefix == auth_data.get('warehouse_prefix'))
    if all_vendors:
        warehouse_qs = warehouse_qs.filter(Products.client_prefix.in_(all_vendors))
    if current_tab == 'active':
        warehouse_qs = warehouse_qs.filter(Products.active == True)
    elif current_tab == 'inactive':
        warehouse_qs = warehouse_qs.filter(Products.active == False)
    warehouse_qs = warehouse_qs.group_by(ProductQuantity.warehouse_prefix)
    response['filters']['warehouse'] = [{x[0]: x[1]} for x in warehouse_qs]
    if auth_data['user_group'] in ('super-admin', 'warehouse'):
        client_qs = db.session.query(Products.client_prefix, func.count(Products.client_prefix)).join(ProductQuantity,
                                                                                                      ProductQuantity.product_id == Products.id).group_by(
            Products.client_prefix)
        if auth_data['user_group'] == 'warehouse':
            client_qs = client_qs.filter(ProductQuantity.warehouse_prefix == auth_data.get('warehouse_prefix'))
        response['filters']['client'] = [{x[0]: x[1]} for x in client_qs]
    if all_vendors:
        client_qs = db.session.query(Products.client_prefix, func.count(Products.client_prefix)).join(ProductQuantity,
                                                                                                      ProductQuantity.product_id == Products.id).filter(
            Products.client_prefix.in_(all_vendors)).group_by(Products.client_prefix)
        response['filters']['client'] = [{x[0]: x[1]} for x in client_qs]

    return jsonify(response), 200


class ProductUpdate(Resource):

    method_decorators = [authenticate_restful]

    def patch(self, resp, product_id):
        try:
            data = json.loads(request.data)
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404

            product = db.session.query(Products).filter(Products.id==int(product_id)).first()

            if not product:
                return {"success": False, "msg": "No product found for given id"}, 400

            if data.get('product_name'):
                product.name =data.get('product_name')
            if data.get('master_sku'):
                product.master_sku =data.get('master_sku')
            if data.get('price'):
                product.price = float(data.get('price'))
            if data.get('dimensions'):
                product.dimensions = data.get('dimensions')
            if data.get('weight'):
                product.weight = data.get('weight')

            db.session.commit()
            return {'status': 'success', 'msg': "successfully updated"}, 200

        except Exception as e:
            return {'status': 'Failed'}, 200


class ProductList(Resource):

    method_decorators = {'post': [authenticate_restful]}

    def post(self, resp):
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            response = {'status':'success', 'data': dict(), "meta": dict()}
            data = json.loads(request.data)
            page = data.get('page', 1)
            per_page = data.get('per_page', 10)
            if int(per_page) > 250:
                return {"success": False, "error": "upto 250 results allowed per page"}, 401
            sort = data.get('sort', "desc")
            sort_by = data.get('sort_by', 'available_quantity')
            search_key = data.get('search_key', '')
            filters = data.get('filters', {})
            download_flag = request.args.get("download", None)
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404
            client_prefix = auth_data.get('client_prefix')
            query_to_execute = select_product_list_query
            if auth_data['user_group'] == 'client':
                query_to_execute = query_to_execute.replace('__CLIENT_FILTER__', "AND aa.client_prefix in ('%s')"%client_prefix)
            if auth_data['user_group'] == 'warehouse':
                query_to_execute = query_to_execute.replace('__WAREHOUSE_FILTER__', "WHERE warehouse_prefix in ('%s')"%auth_data.get('warehouse_prefix'))
                query_to_execute = query_to_execute.replace('__JOIN_TYPE__', "JOIN")
            if auth_data['user_group'] == 'multi-vendor':
                cur.execute("SELECT vendor_list FROM multi_vendor WHERE client_prefix='%s';"%client_prefix)
                vendor_list = cur.fetchone()['vendor_list']
                query_to_execute = query_to_execute.replace('__MV_CLIENT_FILTER__', "AND aa.client_prefix in %s"%str(tuple(vendor_list)))
            else:
                query_to_execute = query_to_execute.replace('__MV_CLIENT_FILTER__', "")

            if filters:
                if 'warehouse' in filters:
                    if len(filters['warehouse'])==1:
                        wh_filter = "WHERE warehouse_prefix in ('%s')"%filters['warehouse'][0]
                    else:
                        wh_filter = "WHERE warehouse_prefix in %s"%str(tuple(filters['warehouse']))

                    query_to_execute = query_to_execute.replace('__WAREHOUSE_FILTER__', wh_filter)
                    query_to_execute = query_to_execute.replace('__JOIN_TYPE__', "JOIN")

                if 'client' in filters:
                    if len(filters['client'])==1:
                        cl_filter = "AND aa.client_prefix in ('%s')"%filters['client'][0]
                    else:
                        cl_filter = "AND aa.client_prefix in %s"%str(tuple(filters['client']))

                    query_to_execute = query_to_execute.replace('__CLIENT_FILTER__', cl_filter)

            query_to_execute = query_to_execute.replace('__JOIN_TYPE__', "LEFT JOIN")
            query_to_execute = query_to_execute.replace('__CLIENT_FILTER__',"").replace('__WAREHOUSE_FILTER__', "")
            if sort.lower() == 'desc':
                sort = "DESC NULLS LAST"
            query_to_execute = query_to_execute.replace('__ORDER_BY__', sort_by).replace('__ORDER_TYPE__', sort)
            query_to_execute = query_to_execute.replace('__SEARCH_KEY__', search_key)
            if download_flag:
                s_no = 1
                query_to_run = query_to_execute.replace('__PAGINATION__', "")
                query_to_run = re.sub(r"""__.+?__""", "", query_to_run)
                cur.execute(query_to_run)
                products_qs_data = cur.fetchall()
                si = io.StringIO()
                cw = csv.writer(si)
                cw.writerow(PRODUCTS_DOWNLOAD_HEADERS)
                for product in products_qs_data:
                    try:
                        new_row = list()
                        new_row.append(str(s_no))
                        new_row.append(str(product['product_name']))
                        new_row.append(str(product['channel_sku']))
                        new_row.append(str(product['master_sku']))
                        new_row.append(str(product['price']))
                        new_row.append(str(product['total_quantity']))
                        new_row.append(str(product['available_quantity']))
                        new_row.append(str(product['current_quantity']))
                        new_row.append(str(product['inline_quantity']))
                        new_row.append(str(product['rto_quantity']))
                        new_row.append(str(product['dimensions']))
                        new_row.append(str(product['weight']))
                        cw.writerow(new_row)
                        s_no += 1
                    except Exception as e:
                        pass

                output = make_response(si.getvalue())
                filename = str(client_prefix)+"_EXPORT.csv"
                output.headers["Content-Disposition"] = "attachment; filename="+filename
                output.headers["Content-type"] = "text/csv"
                return output

            cur.execute(query_to_execute.replace('__PAGINATION__', ""))
            total_count = cur.rowcount

            query_to_execute = query_to_execute.replace('__PAGINATION__', "OFFSET %s LIMIT %s"%(str((page-1)*per_page), str(per_page)))

            cur.execute(query_to_execute)
            response['data'] = cur.fetchall()

            total_pages = math.ceil(total_count/per_page)
            response['meta']['pagination'] = {'total': total_count,
                                              'per_page':per_page,
                                              'current_page': page,
                                              'total_pages':total_pages}

            return response, 200
        except Exception as e:
            conn.rollback()
            return {"success": False, "error":str(e.args[0])}, 404


class ProductListChannel(Resource):

    method_decorators = [authenticate_restful]

    def post(self, resp):
        try:
            cur = conn.cursor()
            response = {'status':'success', 'data': dict(), "meta": dict()}
            data = json.loads(request.data)
            page = data.get('page', 1)
            per_page = data.get('per_page', 10)
            if int(per_page) > 250:
                return {"success": False, "error": "upto 250 results allowed per page"}, 401
            sort = data.get('sort', "desc")
            sort_by = data.get('sort_by', 'cc.sku')
            search_key = data.get('search_key', '')
            filters = data.get('filters', {})
            download_flag = request.args.get("download", None)
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404
            client_prefix = auth_data.get('client_prefix')
            query_to_execute = select_product_list_channel_query
            if auth_data['user_group'] == 'client':
                query_to_execute = query_to_execute.replace('__CLIENT_FILTER__', "AND aa.client_prefix in ('%s')"%client_prefix)
            if auth_data['user_group'] == 'multi-vendor':
                cur.execute("SELECT vendor_list FROM multi_vendor WHERE client_prefix='%s';"%client_prefix)
                vendor_list = cur.fetchone()['vendor_list']
                query_to_execute = query_to_execute.replace('__MV_CLIENT_FILTER__', "AND aa.client_prefix in %s"%str(tuple(vendor_list)))
            else:
                query_to_execute = query_to_execute.replace('__MV_CLIENT_FILTER__', "")

            if filters:
                if 'client' in filters:
                    if len(filters['client'])==1:
                        cl_filter = "AND aa.client_prefix in ('%s')"%filters['client'][0]
                    else:
                        cl_filter = "AND aa.client_prefix in %s"%str(tuple(filters['client']))

                    query_to_execute = query_to_execute.replace('__CLIENT_FILTER__', cl_filter)

                if 'channel' in filters:
                    if len(filters['channel'])==1:
                        ch_filter = "AND dd.channel_name in ('%s')"%filters['channel'][0]
                    else:
                        ch_filter = "AND dd.channel_name in %s"%str(tuple(filters['channel']))

                    query_to_execute = query_to_execute.replace('__CHANNEL_FILTER__', ch_filter)

                if 'status' in filters:
                    ch_filter = ""
                    if "mapped" in filters['status']:
                        ch_filter += "AND cc.id is not null "
                    elif "unmapped" in filters['status']:
                        ch_filter += "AND cc.id is null "

                    query_to_execute = query_to_execute.replace('__STATUS_FILTER__', ch_filter)

            query_to_execute = query_to_execute.replace('__CLIENT_FILTER__',"").replace('__WAREHOUSE_FILTER__', "").replace('__CHANNEL_FILTER__', "").replace('__STATUS_FILTER__', "")
            if sort.lower() == 'desc':
                sort = "DESC NULLS LAST"
            query_to_execute = query_to_execute.replace('__ORDER_BY__', sort_by).replace('__ORDER_TYPE__', sort)
            query_to_execute = query_to_execute.replace('__SEARCH_KEY__', search_key)
            if download_flag:
                s_no = 1
                query_to_run = query_to_execute.replace('__PAGINATION__', "")
                query_to_run = re.sub(r"""__.+?__""", "", query_to_run)
                cur.execute(query_to_run)
                products_qs_data = cur.fetchall()
                si = io.StringIO()
                cw = csv.writer(si)
                cw.writerow(CHANNEL_PRODUCTS_DOWNLOAD_HEADERS)
                for product in products_qs_data:
                    try:
                        new_row = list()
                        new_row.append(str(s_no))
                        new_row.append(str(product[1]))
                        new_row.append(str(product[2]))
                        new_row.append(str(product[4]))
                        new_row.append(str(product[5]))
                        new_row.append(str(product[6]))
                        new_row.append(str(product[8]))
                        status = "mapped" if product[9] else "unmapped"
                        new_row.append(status)
                        cw.writerow(new_row)
                        s_no += 1
                    except Exception as e:
                        pass

                output = make_response(si.getvalue())
                filename = str(client_prefix)+"_EXPORT.csv"
                output.headers["Content-Disposition"] = "attachment; filename="+filename
                output.headers["Content-type"] = "text/csv"
                return output

            cur.execute(query_to_execute.replace('__PAGINATION__', ""))
            total_count = cur.rowcount

            query_to_execute = query_to_execute.replace('__PAGINATION__', "OFFSET %s LIMIT %s"%(str((page-1)*per_page), str(per_page)))
            data = list()
            cur.execute(query_to_execute)
            all_products = cur.fetchall()
            for product in all_products:
                status = "mapped" if product[9] else "unmapped"
                prod_obj = {"product_name": product[1],
                            "channel_product_id": product[2],
                            "channel_sku": product[4],
                            "master_sku": product[5],
                            "price": product[6],
                            "channel_logo": product[7],
                            "product_image": product[3],
                            "channel_name": product[8],
                            "status":status,
                            "id":product[0],
                            }

                data.append(prod_obj)
            response['data'] = data

            total_pages = math.ceil(total_count/per_page)
            response['meta']['pagination'] = {'total': total_count,
                                              'per_page':per_page,
                                              'current_page': page,
                                              'total_pages':total_pages}

            return response, 200
        except Exception as e:
            conn.rollback()
            return {"success": False, "error":str(e.args[0])}, 404

    def get(self, resp):
        try:
            response = {"filters": {}, "success": True}
            auth_data = resp.get('data')
            client_prefix = auth_data.get('client_prefix')
            all_vendors = None
            if auth_data['user_group'] == 'multi-vendor':
                all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
                all_vendors = all_vendors.vendor_list
            channel_qs = db.session.query(MasterChannels.channel_name,
                                            func.count(MasterChannels.channel_name)) \
                .join(Products, Products.channel_id == MasterChannels.id)
            if auth_data['user_group'] == 'client':
                channel_qs = channel_qs.filter(Products.client_prefix == client_prefix)
            if all_vendors:
                channel_qs = channel_qs.filter(Products.client_prefix.in_(all_vendors))

            channel_qs = channel_qs.group_by(MasterChannels.channel_name)
            response['filters']['channel'] = [{x[0]: x[1]} for x in channel_qs]
            if auth_data['user_group'] == 'super-admin':
                client_qs = db.session.query(Products.client_prefix, func.count(Products.client_prefix)).join(
                    ProductQuantity,
                    ProductQuantity.product_id == Products.id).group_by(
                    Products.client_prefix)
                if auth_data['user_group'] == 'warehouse':
                    client_qs = client_qs.filter(ProductQuantity.warehouse_prefix == auth_data.get('warehouse_prefix'))
                response['filters']['client'] = [{x[0]: x[1]} for x in client_qs]
            if all_vendors:
                client_qs = db.session.query(Products.client_prefix, func.count(Products.client_prefix)).join(
                    ProductQuantity,
                    ProductQuantity.product_id == Products.id).filter(
                    Products.client_prefix.in_(all_vendors)).group_by(Products.client_prefix)
                response['filters']['client'] = [{x[0]: x[1]} for x in client_qs]

            return response, 200
        except Exception as e:
            conn.rollback()
            return {"success": False, "error":str(e.args[0])}, 404


class ComboList(Resource):

    method_decorators = [authenticate_restful]

    def post(self, resp):
        try:
            cur = conn.cursor()
            response = {'status':'success', 'data': dict(), "meta": dict()}
            data = json.loads(request.data)
            page = data.get('page', 1)
            per_page = data.get('per_page', 10)
            if int(per_page) > 250:
                return {"success": False, "error": "upto 250 results allowed per page"}, 401
            sort = data.get('sort', "desc")
            sort_by = data.get('sort_by', 'aa.date_created')
            search_key = data.get('search_key', '')
            filters = data.get('filters', {})
            download_flag = request.args.get("download", None)
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404
            client_prefix = auth_data.get('client_prefix')
            query_to_execute = select_combo_list_query
            if auth_data['user_group'] == 'client':
                query_to_execute = query_to_execute.replace('__CLIENT_FILTER__', "AND bb.client_prefix in ('%s')"%client_prefix)
            if auth_data['user_group'] == 'warehouse':
                query_to_execute = query_to_execute.replace('__WAREHOUSE_FILTER__', "AND dd.warehouse_prefix='%s'"%auth_data['warehouse_prefix'])
            if auth_data['user_group'] == 'multi-vendor':
                cur.execute("SELECT vendor_list FROM multi_vendor WHERE client_prefix='%s';"%client_prefix)
                vendor_list = cur.fetchone()['vendor_list']
                query_to_execute = query_to_execute.replace('__MV_CLIENT_FILTER__', "AND aa.client_prefix in %s"%str(tuple(vendor_list)))
            else:
                query_to_execute = query_to_execute.replace('__MV_CLIENT_FILTER__', "")

            if filters:
                if 'client' in filters:
                    if len(filters['client'])==1:
                        cl_filter = "AND bb.client_prefix in ('%s')"%filters['client'][0]
                    else:
                        cl_filter = "AND bb.client_prefix in %s"%str(tuple(filters['client']))

                    query_to_execute = query_to_execute.replace('__CLIENT_FILTER__', cl_filter)

            query_to_execute = query_to_execute.replace('__CLIENT_FILTER__',"").replace('__WAREHOUSE_FILTER__', "")
            if sort.lower() == 'desc':
                sort = "DESC NULLS LAST"
            query_to_execute = query_to_execute.replace('__ORDER_BY__', sort_by).replace('__ORDER_TYPE__', sort)
            query_to_execute = query_to_execute.replace('__SEARCH_KEY__', search_key)
            if download_flag:
                s_no = 1
                query_to_run = query_to_execute.replace('__PAGINATION__', "")
                query_to_run = re.sub(r"""__.+?__""", "", query_to_run)
                cur.execute(query_to_run)
                products_qs_data = cur.fetchall()
                si = io.StringIO()
                cw = csv.writer(si)
                cw.writerow(COMBO_DOWNLOAD_HEADERS)
                for product in products_qs_data:
                    try:
                        new_row = list()
                        new_row.append(str(s_no))
                        new_row.append(str(product[3]))
                        new_row.append(str(product[4]))
                        new_row.append(str(product[7]))
                        new_row.append(str(product[6]))
                        new_row.append(str(product[8]))
                        cw.writerow(new_row)
                        s_no += 1
                    except Exception as e:
                        pass

                output = make_response(si.getvalue())
                filename = str(client_prefix)+"_EXPORT.csv"
                output.headers["Content-Disposition"] = "attachment; filename="+filename
                output.headers["Content-type"] = "text/csv"
                return output

            cur.execute(query_to_execute.replace('__PAGINATION__', ""))
            total_count = cur.rowcount

            query_to_execute = query_to_execute.replace('__PAGINATION__', "OFFSET %s LIMIT %s"%(str((page-1)*per_page), str(per_page)))
            cur.execute(query_to_execute)
            all_products = cur.fetchall()
            combo_dict = dict()
            for product in all_products:
                if product[1] not in combo_dict:
                    combo_dict[product[1]] = {"child_skus":[{"id": product[2], "name": product[7], "sku":product[6], "quantity": product[8]}],
                                              "name": product[3], "sku": product[4], "id":product[1]}
                else:
                    combo_dict[product[1]]['child_skus'].append({"id": product[2], "name": product[7], "sku":product[6], "quantity": product[8]})

            response['data'] = combo_dict

            total_pages = math.ceil(total_count/per_page)
            response['meta']['pagination'] = {'total': total_count,
                                              'per_page':per_page,
                                              'current_page': page,
                                              'total_pages':total_pages}

            return response, 200
        except Exception as e:
            conn.rollback()
            return {"success": False, "error":str(e.args[0])}, 404

    def get(self, resp):
        try:
            response = {"filters": {}, "success": True}
            auth_data = resp.get('data')
            client_prefix = auth_data.get('client_prefix')
            all_vendors = None
            if auth_data['user_group'] == 'multi-vendor':
                all_vendors = db.sessi11on.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
                all_vendors = all_vendors.vendor_list

            if auth_data['user_group'] == 'super-admin':
                client_qs = db.session.query(MasterProducts.client_prefix, func.count(MasterProducts.client_prefix)).join(
                    ProductsCombos,
                    ProductsCombos.combo_id == MasterProducts.id).join(ProductQuantity, ProductQuantity.product_id==MasterProducts.id).group_by(
                    MasterProducts.client_prefix)
                if auth_data['user_group'] == 'warehouse':
                    client_qs = client_qs.filter(ProductQuantity.warehouse_prefix == auth_data.get('warehouse_prefix'))
                response['filters']['client'] = [{x[0]: x[1]} for x in client_qs]
            if all_vendors:
                client_qs = db.session.query(MasterProducts.client_prefix, func.count(MasterProducts.client_prefix)).join(
                    ProductsCombos,
                    ProductsCombos.combo_id == MasterProducts.id).filter(
                    MasterProducts.client_prefix.in_(all_vendors)).group_by(MasterProducts.client_prefix)
                response['filters']['client'] = [{x[0]: x[1]} for x in client_qs]

            return response, 200
        except Exception as e:
            return {"success": False, "error":str(e.args[0])}, 404

    def patch(self, resp):
        try:
            auth_data = resp.get('data')
            data = json.loads(request.data)
            parent_id = data.get('parent_id')
            parent_id = int(parent_id)
            client_prefix = auth_data.get('client_prefix')
            parent_prod = db.session.query(MasterProducts).filter(MasterProducts.id == parent_id)
            all_vendors = None
            if auth_data['user_group'] == 'multi-vendor':
                all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
                all_vendors = all_vendors.vendor_list

            if auth_data['user_group'] == 'client':
                parent_prod = parent_prod.filter(MasterProducts.client_prefix == client_prefix)

            if all_vendors:
                parent_prod = parent_prod.filter(MasterProducts.client_prefix.in_(all_vendors))

            parent_prod = parent_prod.first()

            if not parent_prod:
                return jsonify({"success": False}), 400

            db.session.query(ProductsCombos).filter(ProductsCombos.combo_id==parent_id).delete()

            for child in data.get('child_skus'):
                child_prod = db.session.query(MasterProducts).filter(MasterProducts.id == child['id']).first()
                if not child_prod:
                    return jsonify({"success": False}), 400
                combo_obj = ProductsCombos(combo=parent_prod,
                                           combo_prod=child_prod,
                                           quantity=child['quantity'],
                                           date_created=datetime.utcnow() + timedelta(hours=5.5))

                db.session.add(combo_obj)

            db.session.commit()
            return jsonify({"success": True}), 200
        except Exception as e:
            conn.rollback()
            return {"success": False, "error":str(e.args[0])}, 404


class UpdateInventory(Resource):

    method_decorators = [authenticate_restful]

    def post(self, resp):
        try:
            cur = conn.cursor()
            auth_data = resp.get('data')
            data = json.loads(request.data)
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404

            sku_list = data.get("sku_list")
            failed_list = list()
            current_quantity = list()
            for sku_obj in sku_list:
                try:
                    warehouse = sku_obj.get('warehouse')
                    if not warehouse:
                        sku_obj['error'] = "Warehouse not provided."
                        failed_list.append(sku_obj)
                        continue
                    sku = sku_obj.get('sku')
                    if not sku:
                        sku_obj['error'] = "SKU not provided."
                        failed_list.append(sku_obj)
                        continue

                    sku = str(sku)

                    type = sku_obj.get('type')
                    if not type or str(type).lower() not in ('add', 'subtract', 'replace'):
                        sku_obj['error'] = "Invalid type"
                        failed_list.append(sku_obj)
                        continue

                    quantity = sku_obj.get('quantity')
                    if quantity is None:
                        sku_obj['error'] = "Invalid Quantity"
                        failed_list.append(sku_obj)
                        continue

                    quantity = int(quantity)

                    quan_obj = db.session.query(ProductQuantity).join(Products, ProductQuantity.product_id==Products.id)\
                        .filter(ProductQuantity.warehouse_prefix==warehouse).filter(
                        or_(Products.sku==sku, Products.master_sku==sku))

                    if auth_data.get('user_group') != 'super-admin':
                        quan_obj = quan_obj.filter(Products.client_prefix==auth_data['client_prefix'])

                    quan_obj = quan_obj.first()

                    if not quan_obj:
                        prod_obj = db.session.query(Products).filter(or_(Products.sku==sku, Products.master_sku==sku))
                        if auth_data.get('user_group') != 'super-admin':
                            prod_obj = prod_obj.filter(Products.client_prefix == auth_data['client_prefix'])

                        prod_obj = prod_obj.first()
                        if not prod_obj:
                            sku_obj['error'] = "Warehouse sku combination not found."
                            failed_list.append(sku_obj)
                            continue
                        else:
                            quan_obj = ProductQuantity(product=prod_obj,
                                                       total_quantity=0,
                                                       approved_quantity=0,
                                                       available_quantity=0,
                                                       inline_quantity=0,
                                                       rto_quantity=0,
                                                       current_quantity=0,
                                                       warehouse_prefix=warehouse,
                                                       status="APPROVED",
                                                       date_created=datetime.now())
                            db.session.add(quan_obj)

                    update_obj = InventoryUpdate(product=quan_obj.product,
                                                 warehouse_prefix=warehouse,
                                                 user=auth_data['email'] if auth_data.get('email') else auth_data[
                                                     'client_prefix'],
                                                 remark=sku_obj.get('remark', None),
                                                 quantity=int(quantity),
                                                 type=str(type).lower(),
                                                 date_created=datetime.utcnow() + timedelta(hours=5.5))

                    shipped_quantity=0
                    dto_quantity=0
                    try:
                        cur.execute("""  select COALESCE(sum(quantity), 0) from op_association aa
                                left join orders bb on aa.order_id=bb.id
                                left join client_pickups cc on bb.pickup_data_id=cc.id
                                left join pickup_points dd on cc.pickup_id=dd.id
                                left join products ee on aa.product_id=ee.id
                                where status in ('DELIVERED','DISPATCHED','IN TRANSIT','ON HOLD','PENDING','LOST')
                                and dd.warehouse_prefix='__WAREHOUSE__'
                                and ee.master_sku='__SKU__';""".replace('__WAREHOUSE__', warehouse).replace('__SKU__', sku))
                        shipped_quantity_obj = cur.fetchone()
                        if shipped_quantity_obj is not None:
                            shipped_quantity = shipped_quantity_obj[0]
                    except Exception:
                        conn.rollback()

                    try:
                        cur.execute("""select COALESCE(sum(quantity), 0) from op_association aa
                                left join orders bb on aa.order_id=bb.id
                                left join client_pickups cc on bb.pickup_data_id=cc.id
                                left join pickup_points dd on cc.pickup_id=dd.id
                                left join products ee on aa.product_id=ee.id
                                where status in ('DTO')
                                and dd.warehouse_prefix='__WAREHOUSE__'
                                and ee.master_sku='__SKU__';""".replace('__WAREHOUSE__', warehouse).replace('__SKU__', sku))
                        dto_quantity_obj = cur.fetchone()
                        if dto_quantity_obj is not None:
                            dto_quantity = dto_quantity_obj[0]
                    except Exception:
                        conn.rollback()

                    if str(type).lower() == 'add':
                        quan_obj.total_quantity = quan_obj.total_quantity+quantity
                        quan_obj.approved_quantity = quan_obj.approved_quantity+quantity
                    elif str(type).lower() == 'subtract':
                        quan_obj.total_quantity = quan_obj.total_quantity - quantity
                        quan_obj.approved_quantity = quan_obj.approved_quantity - quantity
                    elif str(type).lower() == 'replace':
                        quan_obj.total_quantity = quantity + shipped_quantity - dto_quantity
                        quan_obj.approved_quantity = quantity + shipped_quantity - dto_quantity
                    else:
                        continue

                    current_quantity.append({"warehouse": warehouse, "sku": sku,
                                             "current_quantity": quan_obj.approved_quantity- shipped_quantity+dto_quantity})

                except Exception:
                    failed_list.append(sku_obj)
                    continue

                db.session.add(update_obj)
                db.session.commit()

            return {"success": True if not failed_list else False, "failed_list": failed_list, "current_quantity": current_quantity}, 200

        except Exception as e:
            return {"success": False, "msg": str(e.args[0])}, 400

    def get(self, resp):
        try:
            cur = conn.cursor()
            auth_data = resp.get('data')
            sku = request.args.get('sku')
            search_key = request.args.get('search', '')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404

            if not sku:
                query_to_run = """select array_agg(master_sku) from 
                                (SELECT master_sku from products WHERE master_sku ilike '%__SEARCH_KEY__%' __CLIENT_FILTER__ ORDER BY master_sku LIMIT 10) ss""".replace('__SEARCH_KEY__', search_key)
                if auth_data['user_group'] != 'super-admin':
                    query_to_run = query_to_run.replace("__CLIENT_FILTER__", "AND client_prefix='%s'"%auth_data['client_prefix'])
                else:
                    query_to_run = query_to_run.replace("__CLIENT_FILTER__", "")

                cur.execute(query_to_run)

                return {"success": True, "sku_list": cur.fetchone()[0]}, 200

            else:
                if auth_data['user_group']=='super-admin':
                    cur.execute("SELECT client_prefix from products where master_sku='%s'"%sku)
                    client_prefix = cur.fetchone()
                    client_prefix = client_prefix[0]
                elif auth_data['user_group']=='client':
                    client_prefix = auth_data['client_prefix']
                else:
                    return {"success": True, "warehouse_list": None, "sku": sku}, 200

                query_to_run = """select array_agg(warehouse_prefix) from client_pickups aa
                left join pickup_points bb on aa.pickup_id=bb.id
                where client_prefix='%s'"""%(client_prefix)

                cur.execute(query_to_run)

                return {"success": True, "warehouse_list": cur.fetchone()[0], "sku":sku}, 200

        except Exception as e:
            return {"success": False, "msg": ""}, 404


class AddSKU(Resource):

    method_decorators = [authenticate_restful]

    def post(self, resp):
        try:
            auth_data = resp.get('data')
            data = json.loads(request.data)
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404

            product_name = data.get('name')
            sku = data.get('sku')
            dimensions = data.get('dimensions')
            if dimensions:
                dimensions = {"length": float(dimensions['length']), "breadth": float(dimensions['breadth']), "height":  float(dimensions['height'])}
            weight = data.get('weight')
            price = float(data.get('price', 0))
            client = data.get('client')
            warehouse_list= data.get('warehouse_list', [])
            if auth_data['user_group'] != 'super-admin':
                client = auth_data['client_prefix']

            prod_obj_x = db.session.query(Products).filter(Products.client_prefix==client, Products.master_sku==sku).first()
            if prod_obj_x:
                return {"success": False, "msg": "SKU already exists"}, 400

            prod_obj_x = Products(name=product_name,
                                  sku=sku,
                                  master_sku=sku,
                                  dimensions=dimensions,
                                  weight=weight,
                                  price=price,
                                  client_prefix=client,
                                  active=True,
                                  channel_id=4,
                                  date_created=datetime.now()
                                  )

            for wh_obj in warehouse_list:
                prod_quan_obj = ProductQuantity(product=prod_obj_x,
                                                total_quantity=int(wh_obj['quantity']),
                                                approved_quantity=int(wh_obj['quantity']),
                                                available_quantity=int(wh_obj['quantity']),
                                                inline_quantity=0,
                                                rto_quantity=0,
                                                current_quantity=int(wh_obj['quantity']),
                                                warehouse_prefix=wh_obj['warehouse'],
                                                status="APPROVED",
                                                date_created=datetime.now()
                                                )
                db.session.add(prod_quan_obj)

            db.session.commit()
            return {"success": True, "msg": "Successfully added"}, 201

        except Exception as e:
            conn.rollback()
            return {"success": False, "msg": ""}, 404

    def get(self, resp):
        try:
            cur = conn.cursor()
            auth_data = resp.get('data')
            if not auth_data:
                return {"success": False, "msg": "Auth Failed"}, 404

            query_to_run = """select array_agg(warehouse_prefix) from 
                            (select bb.warehouse_prefix from client_pickups aa
                            left join pickup_points bb
                            on aa.pickup_id=bb.id
                            __CLIENT_FILTER__
                            order by warehouse_prefix) ss"""
            if auth_data['user_group'] != 'super-admin':
                query_to_run = query_to_run.replace("__CLIENT_FILTER__", "WHERE aa.client_prefix='%s'"%auth_data['client_prefix'])
            else:
                query_to_run = query_to_run.replace("__CLIENT_FILTER__", "")

            cur.execute(query_to_run)

            return {"success": True, "warehouses": cur.fetchone()[0]}, 200

        except Exception as e:
            return {"success": False, "msg": ""}, 404


api.add_resource(ProductUpdate, '/products/v1/product/<product_id>')
api.add_resource(ProductList, '/products/v1/master')
api.add_resource(ProductListChannel, '/products/v1/channel')
api.add_resource(ComboList, '/products/v1/combos')
api.add_resource(UpdateInventory, '/products/v1/update_inventory')
api.add_resource(AddSKU, '/products/v1/add_sku')