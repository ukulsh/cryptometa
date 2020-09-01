import csv
import io
import json
import math
import re
from datetime import datetime, timedelta
from flask import Blueprint, request, jsonify, make_response
from flask_restful import Api, Resource
from psycopg2.extras import RealDictCursor
from sqlalchemy import func, or_
from project import db
from project.api.models import Products, ProductQuantity, MultiVendor, InventoryUpdate
from project.api.queries import select_product_list_query
from project.api.utils import authenticate_restful
from project.api.utilities.db_utils import DbConnection

products_blueprint = Blueprint('products', __name__)
api = Api(products_blueprint)

conn = DbConnection.get_db_connection_instance()
conn_2 = DbConnection.get_pincode_db_connection_instance()
PRODUCTS_DOWNLOAD_HEADERS = ["S. No.", "Product Name", "Channel SKU", "Master SKU", "Price", "Total Quantity",
                             "Available Quantity", "Current Quantity", "Inline Quantity", "RTO Quantity", "Dimensions", "Weight"]


@products_blueprint.route('/products/v1/details', methods=['GET'])
@authenticate_restful
def get_products_details(resp):
    try:
        cur = conn.cursor()
        auth_data = resp.get('data')
        client_prefix = auth_data.get('client_prefix')
        sku = request.args.get('sku')
        if not sku:
            return jsonify({"success": False, "msg": "SKU not provided"}), 400

        query_to_run = """SELECT name, sku as channel_sku, master_sku, weight, dimensions, price, bb.warehouse_prefix as warehouse, 
                            bb.approved_quantity as total_quantity, bb.current_quantity, bb.available_quantity, bb.inline_quantity, bb.rto_quantity
                            from products aa
                            left join products_quantity bb on aa.id=bb.product_id
                            WHERE client_prefix='%s'
                            and (sku='%s' or master_sku='%s')
                            __WAREHOUSE_FILTER__"""%(client_prefix, sku, sku)
        warehouse = request.args.get('warehouse')
        if warehouse:
            query_to_run = query_to_run.replace('__WAREHOUSE_FILTER__', "and warehouse_prefix='%s'"%warehouse)
            cur.execute(query_to_run)
            ret_tuple = cur.fetchone()
            if not ret_tuple:
                return jsonify({"success": False, "msg": "SKU, warehouse combination not found"}), 400

            ret_obj = {"name":ret_tuple[0],
                       "channel_sku": ret_tuple[1],
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

        ret_list = list()

        for ret_tuple in ret_tuple_all:
            ret_obj = {"name": ret_tuple[0],
                       "channel_sku": ret_tuple[1],
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

            ret_list.append(ret_obj)

        return jsonify({"success": True, "data": ret_list}), 200

    except Exception as e:
        return jsonify({"success": False}), 400


@products_blueprint.route('/products/v1/get_filters', methods=['GET'])
@authenticate_restful
def get_products_filters(resp):
    response = {"filters":{}, "success": True}
    auth_data = resp.get('data')
    current_tab = request.args.get('tab')
    client_prefix = auth_data.get(',')
    all_vendors = None
    if auth_data['user_group'] == 'multi-vendor':
        all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
        all_vendors = all_vendors.vendor_list
    warehouse_qs = db.session.query(ProductQuantity.warehouse_prefix, func.count(ProductQuantity.warehouse_prefix))\
                .join(Products, Products.id == ProductQuantity.product_id)
    if auth_data['user_group'] == 'client':
        warehouse_qs = warehouse_qs.filter(Products.client_prefix == client_prefix)
    if auth_data['user_group'] == 'warehouse':
        warehouse_qs = warehouse_qs.filter(ProductQuantity.warehouse_prefix == auth_data.get('warehouse_prefix'))
    if all_vendors:
        warehouse_qs = warehouse_qs.filter(Products.client_prefix.in_(all_vendors))
    if current_tab == 'active':
        warehouse_qs = warehouse_qs.filter(Products.active == True)
    elif current_tab =='inactive':
        warehouse_qs = warehouse_qs.filter(Products.active == False)
    warehouse_qs = warehouse_qs.group_by(ProductQuantity.warehouse_prefix)
    response['filters']['warehouse'] = [{x[0]: x[1]} for x in warehouse_qs]
    if auth_data['user_group'] in ('super-admin','warehouse'):
        client_qs = db.session.query(Products.client_prefix, func.count(Products.client_prefix)).join(ProductQuantity, ProductQuantity.product_id == Products.id).group_by(Products.client_prefix)
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

    def post(self, resp, type):
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

            if type != 'all':
                return {"success": False, "msg": "Invalid URL"}, 404

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
                        cur.execute("""  select COALESCE(sum(quantity), 0) from op_association aa
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
                query_to_run = """select array_agg(warehouse_prefix) from
                                    (select distinct(warehouse_prefix) from products_quantity WHERE product_id in
                                    (select id from products where master_sku='%s') 
                                    ORDER BY warehouse_prefix) ss"""%(str(sku))

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
api.add_resource(ProductList, '/products/<type>')
api.add_resource(UpdateInventory, '/products/v1/update_inventory')
api.add_resource(AddSKU, '/products/v1/add_sku')