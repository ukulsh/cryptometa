# project/api/utils.py


import json
from functools import wraps

import requests
from reportlab.lib.units import inch
from reportlab.graphics.barcode import code39, code128, code93
from reportlab.graphics.shapes import Drawing
from flask import request, jsonify, current_app

from reportlab.graphics.barcode import code39, code128, code93
from reportlab.graphics.barcode import eanbc, qr, usps
from reportlab.graphics.shapes import Drawing
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas
from reportlab.graphics import renderPDF

from .models import ClientMapping


def authenticate(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        response_object = {
            'status': 'error',
            'message': 'Something went wrong. Please contact us.'
        }
        code = 401
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            response_object['message'] = 'Provide a valid auth token.'
            code = 403
            return jsonify(response_object), code
        auth_token = auth_header.split(" ")[1]
        response = ensure_authenticated(auth_token)
        if not response:
            response_object['message'] = 'Invalid token.'
            return jsonify(response_object), code
        return f(response, *args, **kwargs)
    return decorated_function


def authenticate_restful(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        response_object = {
            'status': 'error',
            'message': 'Something went wrong. Please contact us.'
        }
        code = 401
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            response_object['message'] = 'Provide a valid auth token.'
            code = 403
            return response_object, code
        auth_token = auth_header.split(" ")[1]
        if auth_header.split(" ")[0]=='Token':
            client = ClientMapping.query.filter_by(api_token=auth_token).first()
            if not client:
                response_object['message'] = 'Provide a valid auth token.'
                code = 403
                return response_object, code
            response = {
                "data": {"user_group": "client", "client_prefix": client.client_prefix}}
            return f(response, *args, **kwargs)
        response = ensure_authenticated(auth_token)
        if not response:
            response_object['message'] = 'Invalid token.'
            return response_object, code
        return f(response, *args, **kwargs)
    return decorated_function


def ensure_authenticated(token):
    if current_app.config['TESTING']:
        return True
    url = '{0}/auth/status'.format(current_app.config['USERS_SERVICE_URL'])
    bearer = 'Bearer {0}'.format(token)
    headers = {'Authorization': bearer}
    response = requests.get(url, headers=headers)
    data = json.loads(response.text)
    if response.status_code == 200 and \
       data['status'] == 'success' and \
       data['data']['active']:
        return data
    else:
        return False


def get_products_sort_func(Products, ProductsQuantity, sort, sort_by):
    if sort_by == 'product_name':
        x = Products.name
    elif sort_by == 'price':
        x = Products.price
    elif sort_by == 'master_sku':
        x = Products.master_sku
    elif sort_by == 'total_quantity':
        x = ProductsQuantity.approved_quantity
    elif sort_by == 'weight':
        x = Products.weight
    else:
        x = ProductsQuantity.available_quantity

    if sort.lower() == 'desc':
        x = x.desc().nullslast
    else:
        x = x.asc
    return x


def get_orders_sort_func(Orders, sort, sort_by):
    if sort_by == 'order_id':
        x = Orders.channel_order_id
    elif sort_by == 'status':
        x = Orders.status
    else:
        x = Orders.order_date

    if sort.lower() == 'asc':
        x = x.asc
    else:
        x = x.desc
    return x


def create_shiplabel_blank_page(canvas):
    canvas.setLineWidth(.8)
    canvas.setFont('Helvetica', 12)
    canvas.translate(inch, inch)
    canvas.rect(-0.80 * inch, -0.80 * inch, 11.29 * inch, 7.87 * inch, fill=0)
    canvas.setLineWidth(.05 * inch)
    canvas.line(2.913 * inch, -0.80 * inch, 2.913 * inch, 7.07 * inch)
    canvas.line(6.676 * inch, -0.80 * inch, 6.676 * inch, 7.07 * inch)
    canvas.setLineWidth(0.8)
    for i in (5.42, 3.62, 2.02):
        canvas.line(-0.80 * inch, i * inch, 10.49 * inch, i * inch)
    for i in (1.72, 0.35, 0.05):
        canvas.line(-0.80 * inch, i * inch, 10.49 * inch, i * inch)
    for i in (1.73, 5.47, 9.21):
        canvas.line(i * inch, 3.62 * inch, i * inch, 5.42 * inch)  # upper vertcal
        canvas.line(i * inch, 2.02 * inch, i * inch, 0.05 * inch)  # lower vertcal
    for i in (1.33, 5.07, 8.81):
        canvas.line(i * inch, 3.62 * inch, i * inch, 2.02 * inch)  # middle vertcal
    for i in (-0.70, 3.013, 6.776):
        canvas.drawString(i * inch, 1.80 * inch, "Product(s)")
        canvas.drawString(i * inch, 6.90 * inch, "COURIER: ")
    for i in (1.82, 5.543, 9.266):
        canvas.drawString(i * inch, 1.80 * inch, "Price")
    for i in (1.40, 5.14, 8.88):
        canvas.drawString(i * inch, 3.45 * inch, "Dimensions:")
        canvas.drawString(i * inch, 2.65 * inch, "Weight:")

    canvas.setFont('Helvetica-Bold', 12)
    for i in (-0.70, 3.013, 6.776):
        canvas.drawString(i * inch, 0.13 * inch, "Total")
        canvas.drawString(i * inch, 5.25 * inch, "Deliver To:")
    canvas.setFont('Helvetica-Bold', 9)
    for i in (-0.70, 3.013, 6.776):
        canvas.drawString(i * inch, 3.45 * inch, "Shipped By (Return Address):")
    canvas.setFont('Helvetica', 10)


def fill_shiplabel_data(c, order, offset):
    c.drawString(offset * inch, 6.90 * inch, order.shipments[0].courier.courier_name)
    c.setFont('Helvetica-Bold', 14)
    c.drawString((offset + 1.8) * inch, 4.90 * inch, order.payments[0].payment_mode)
    if order.payments[0].payment_mode.lower()=="cod":
        c.drawString((offset + 1.8) * inch, 4.40 * inch, str(order.payments[0].amount))
    full_name = order.delivery_address.first_name
    c.setFont('Helvetica-Bold', 12)
    if order.delivery_address.last_name:
        full_name += " " + order.delivery_address.last_name
    c.drawString((offset - 0.85) * inch, 5.05 * inch, full_name)
    c.drawString((offset - 0.85) * inch, 3.70 * inch, "PHONE: " + order.delivery_address.phone)
    c.drawString((offset + 1.75) * inch, 0.13 * inch, str(order.payments[0].amount))

    awb_string = order.shipments[0].awb
    awb_barcode = code128.Code128(awb_string,barHeight=0.8*inch, barWidth=0.5*mm)
    temp_param = float((awb_barcode.width/165)-0.7)

    awb_barcode.drawOn(c, (offset-temp_param)*inch, 5.90*inch)

    try:
        order_id_string = order.channel_order_id
        order_id_barcode = code128.Code128(order_id_string, barHeight=0.6*inch, barWidth=0.3*mm)
        order_id_barcode.drawOn(c, (offset+0.2)*inch, -0.6*inch)
        c.drawString((offset+0.65) * inch, -0.75*inch, order_id_string)
    except Exception:
        pass

    c.drawString((offset+0.3) * inch, 5.75*inch, awb_string)
    routing_code = "N/A"
    if order.shipments[0].routing_code:
        routing_code = str(order.shipments[0].routing_code)
    c.drawString((offset+1.8) * inch, 5.50*inch, routing_code)

    c.setFont('Helvetica', 10)
    full_address = order.delivery_address.address_one
    if order.delivery_address.address_two:
        full_address += " "+order.delivery_address.address_two
    full_address = split_string(full_address, 35)
    y_axis = 4.85
    for addr in full_address:
        c.drawString((offset - 0.85) * inch, y_axis * inch, addr)
        y_axis -= 0.15

    try:
        c.drawString((offset - 0.85) * inch, 4.10 * inch, order.delivery_address.city+", "+order.delivery_address.state)
        c.drawString((offset - 0.85) * inch, 3.90 * inch, order.delivery_address.country+", PIN: "+order.delivery_address.pincode)
    except Exception:
        pass

    try:
        if order.pickup_data:
            return_point = order.pickup_data.return_point
        else:
            return_point = order.shipments[0].return_point
        return_address = return_point.address
        if return_point.address_two:
            return_address += " "+ return_point.address_two

        return_address = split_string(return_address, 30)

        c.drawString((offset - 0.85) * inch, 3.25 * inch, return_point.name)
        y_axis = 3.05
        for retn in return_address:
            c.drawString((offset - 0.85) * inch, y_axis * inch, retn)
            y_axis -= 0.15

        c.drawString((offset - 0.85) * inch, 2.40 * inch, return_point.city + ", " + return_point.state)
        c.drawString((offset - 0.85) * inch, 2.25 * inch, return_point.country + ", PIN: " + str(return_point.pincode))
    except Exception:
        pass

    try:
        products_string = ""
        for prod in order.products:
            products_string += prod.product.name + " (" + str(prod.quantity) + ") + "
        products_string = products_string.rstrip(" + ")
        if order.payments[0].shipping_charges:
            products_string += " + Shipping"
        products_string = split_string(products_string, 35)
        if len(products_string) > 7:
            products_string = products_string[:7]
            products_string[6] += "..."

        y_axis = 1.42
        for prod in products_string:
            c.drawString((offset - 0.85) * inch, y_axis * inch, prod)
            y_axis -= 0.15
    except Exception:
        pass

    c.setFont('Helvetica', 12)

    c.drawString((offset + 1.75) * inch, 1.32 * inch, str(order.payments[0].amount))

    try:
        dimension_str = str(order.shipments[0].dimensions['length']) + \
                        " x " + str(order.shipments[0].dimensions['breadth']) + \
                        " x " + str(order.shipments[0].dimensions['height'])

        weight_str = str(order.shipments[0].weight) + " kg"

        c.drawString((offset + 1.35) * inch, 3.15 * inch, dimension_str)
        c.drawString((offset + 1.35) * inch, 2.35 * inch, weight_str)
    except Exception:
        pass

    c.setFont('Helvetica', 10)


def split_string(str, limit, sep=" "):
    words = str.split()
    if max(map(len, words)) > limit:
        str = str.replace(',', ' ')
        str = str.replace(';', ' ')
        words = str.split()
    res, part, others = [], words[0], words[1:]
    for word in others:
        if len(sep)+len(word) > limit-len(part):
            res.append(part)
            part = word
        else:
            part += sep+word
    if part:
        res.append(part)
    return res
