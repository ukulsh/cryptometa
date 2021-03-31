# project/api/utils.py


import json, xmltodict
from functools import wraps
from datetime import datetime, timedelta

import requests, random, string
from reportlab.lib.units import inch
from reportlab.graphics.barcode import code39, code128, code93
from reportlab.graphics.shapes import Drawing
from flask import request, jsonify, current_app

from reportlab.graphics.barcode import code39, code128, code93, qr
from reportlab.graphics.barcode import eanbc, qr, usps
from reportlab.graphics.shapes import Drawing
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas
from reportlab.graphics import renderPDF

from .models import ClientMapping, OrdersInvoice
from project import db
from woocommerce import API


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
            return jsonify(response_object), code
        auth_token = auth_header.split(" ")[1]

        if auth_header.split(" ")[0] == 'Token':
            client = ClientMapping.query.filter_by(api_token=auth_token).first()
            if client:
                response = {"data": {"user_group": "client", "client_prefix": client.client_prefix}}
            else:
                response = ensure_token_authenticated(auth_header)
        else:
            response = ensure_authenticated(auth_token)
        if not response:
            response_object['message'] = 'Invalid token.'
            return jsonify(response_object), code
        return f(response, *args, **kwargs)
    return decorated_function


def ensure_token_authenticated(token):
    user_service_url = current_app.config['USERS_SERVICE_URL']
    url = '{0}/auth/tokenStatus'.format(user_service_url)
    headers = {'Authorization': token}
    response = requests.get(url, headers=headers)
    data = json.loads(response.text)
    if response.status_code == 200 and data['status'] == 'success' and data['data']['active']:
        return data
    else:
        return None


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


def fill_shiplabel_data(c, order, offset, client_name=None):
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

    awb_string = order.shipments[0].awb
    awb_barcode = code128.Code128(awb_string,barHeight=0.8*inch, barWidth=0.5*mm)
    temp_param = float((awb_barcode.width/165)-0.7)

    awb_barcode.drawOn(c, (offset-temp_param)*inch, 5.90*inch)

    try:
        if order.orders_invoice:
            qr_url = order.orders_invoice[-1].qr_url
            qr_code = qr.QrCodeWidget(qr_url)
            bounds = qr_code.getBounds()
            width = bounds[2] - bounds[0]
            height = bounds[3] - bounds[1]
            d = Drawing(60, 60, transform=[60. / width, 0, 0, 60. / height, 0, 0])
            d.add(qr_code)
            d.drawOn(c, (offset+1.2) * inch, -0.80*inch)
            order_id_string = order.channel_order_id
            c.drawString((offset - 0.5) * inch, -0.60 * inch, order_id_string)
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

    if not client_name.hide_address or str(order.shipments[0].courier.courier_name).startswith("Bluedart"):
        try:
            if order.pickup_data:
                return_point = order.pickup_data.return_point
            else:
                return_point = order.shipments[0].return_point
            return_address = return_point.address
            if return_point.address_two:
                return_address += " "+ return_point.address_two

            return_address = split_string(return_address, 30)

            return_point_name = client_name.client_name if client_name else str(return_point.name)
            c.drawString((offset - 0.85) * inch, 3.25 * inch, return_point_name)
            y_axis = 3.05
            for retn in return_address:
                c.drawString((offset - 0.85) * inch, y_axis * inch, retn)
                y_axis -= 0.15

            c.drawString((offset - 0.85) * inch, 2.40 * inch, return_point.city + ", " + return_point.state)
            c.drawString((offset - 0.85) * inch, 2.25 * inch, return_point.country + ", PIN: " + str(return_point.pincode))
        except Exception:
            pass

    if not client_name.hide_products:
        c.setFont('Helvetica', 8)
        try:
            products_string = ""
            for prod in order.products:
                products_string += prod.master_product.name + " (" + str(prod.quantity) + ") + "
            products_string = products_string.rstrip(" + ")
            if order.payments[0].shipping_charges:
                products_string += " + Shipping"
            products_string = split_string(products_string, 35)
            if len(products_string) > 9:
                products_string = products_string[:9]
                products_string[8] += "..."

            y_axis = 1.42
            for prod in products_string:
                c.drawString((offset - 0.85) * inch, y_axis * inch, prod)
                y_axis -= 0.12

            c.setFont('Helvetica', 12)
            c.drawString((offset + 1.75) * inch, 0.13 * inch, str(order.payments[0].amount))
            c.drawString((offset + 1.75) * inch, 1.32 * inch, str(order.payments[0].amount))
        except Exception:
            pass

    c.setFont('Helvetica', 12)

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


def create_shiplabel_blank_page_thermal(canvas):
    canvas.setLineWidth(.8)
    canvas.setFont('Helvetica', 9)
    canvas.translate(inch, inch)
    canvas.rect(-0.9 * inch, -0.9 * inch, 3.8 * inch, 5.8 * inch, fill=0)
    canvas.setLineWidth(0.8)
    for i in (3.2, 2.7, 0.9):
        canvas.line(-0.90 * inch, i * inch, 2.9 * inch, i * inch)

    canvas.drawString(-0.8 * inch, 4.75 * inch, "COURIER:")
    canvas.drawString(-0.8 * inch, 3.05 * inch, "Dimensions:")
    canvas.drawString(-0.8 * inch, 2.77 * inch, "Weight:")
    canvas.drawString(0.85 * inch, 3.05 * inch, "Payment:")
    canvas.setFont('Helvetica-Bold', 9)
    canvas.drawString(1.5 * inch, 0.75 * inch,  "Total")
    canvas.drawString(-0.8 * inch, 2.55 * inch, "Deliver To:")
    canvas.drawString(-0.8 * inch, 0.75 * inch, "Product(s)")
    canvas.drawString(-0.8 * inch, -0.5 * inch, "Shipped By (Return Address):")
    canvas.setFont('Helvetica', 0)


def fill_shiplabel_data_thermal(c, order, client_name=None):
    c.setFont('Helvetica-Bold', 10)
    c.drawString(1.45* inch, 3.05 * inch, order.payments[0].payment_mode)
    if order.payments[0].payment_mode.lower()=="cod":
        c.drawString(1.45 * inch, 2.80 * inch, str(order.payments[0].amount))
    full_name = order.delivery_address.first_name
    if order.delivery_address.last_name:
        full_name += " " + order.delivery_address.last_name
    c.drawString(-0.75 * inch, 2.37 * inch, full_name)

    awb_string = order.shipments[0].awb
    awb_barcode = code128.Code128(awb_string,barHeight=0.8*inch, barWidth=0.5*mm)
    temp_param = float((awb_barcode.width/165)-0.7)

    awb_barcode.drawOn(c, (0.15-temp_param)*inch, 3.75*inch)

    try:
        if order.orders_invoice:
            qr_url = order.orders_invoice[-1].qr_url
            qr_code = qr.QrCodeWidget(qr_url)
            bounds = qr_code.getBounds()
            width = bounds[2] - bounds[0]
            height = bounds[3] - bounds[1]
            d = Drawing(60, 60, transform=[60. / width, 0, 0, 60. / height, 0, 0])
            d.add(qr_code)
            d.drawOn(c, 1.8 * inch, 1.4 * inch)
            order_id_string = order.channel_order_id
            c.drawString(1.75 * inch, 1.25*inch, order_id_string)
    except Exception:
        pass

    c.drawString( 0.6 * inch, 3.60*inch, awb_string)
    routing_code = "N/A"
    if order.shipments[0].routing_code:
        routing_code = str(order.shipments[0].routing_code)
    c.drawString(2.1 * inch, 3.30*inch, routing_code)

    c.setFont('Helvetica', 9)
    c.drawString(0*inch, 4.75 * inch, order.shipments[0].courier.courier_name)
    full_address = order.delivery_address.address_one
    if order.delivery_address.address_two:
        full_address += " "+order.delivery_address.address_two
    full_address = split_string(full_address, 40)

    y_axis = 2.22
    for addr in full_address:
        c.drawString(-0.75 * inch, y_axis * inch, addr)
        y_axis -= 0.15

    try:
        c.drawString(-0.75 * inch, 1.20 * inch, order.delivery_address.city+", "+order.delivery_address.state)
        c.drawString(-0.75 * inch, 1.00 * inch, order.delivery_address.country+", PIN: "+order.delivery_address.pincode)
    except Exception:
        pass

    c.setFont('Helvetica', 8)
    if not client_name.hide_address or str(order.shipments[0].courier.courier_name).startswith("Bluedart"):
        try:
            if order.pickup_data:
                return_point = order.pickup_data.return_point
            else:
                return_point = order.shipments[0].return_point
            return_address = return_point.address
            if return_point.address_two:
                return_address += " "+ return_point.address_two

            return_point_name = client_name.client_name if client_name else str(return_point.name)
            return_address = return_point_name + " |  " + return_address

            return_address = split_string(return_address, 75)

            y_axis = -0.65
            for retn in return_address:
                c.drawString(-0.75 * inch, y_axis * inch, retn)
                y_axis -= 0.12

        except Exception:
            pass

    if not client_name.hide_products:
        c.setFont('Helvetica', 7)
        try:
            products_string = ""
            for prod in order.products:
                products_string += prod.master_product.name + " (" + str(prod.quantity) + ") + "
            products_string = products_string.rstrip(" + ")
            if order.payments[0].shipping_charges:
                products_string += " + Shipping"
            products_string = split_string(products_string, 45)
            if len(products_string) > 7:
                products_string = products_string[:7]
                products_string[6] += "..."

            y_axis = 0.6
            for prod in products_string:
                c.drawString(-0.75 * inch, y_axis * inch, prod)
                y_axis -= 0.15

            c.drawString(1.6 * inch, 0.5 * inch, str(order.payments[0].amount))
        except Exception:
            pass

    c.setFont('Helvetica', 8)

    try:
        dimension_str = str(order.shipments[0].dimensions['length']) + \
                        " x " + str(order.shipments[0].dimensions['breadth']) + \
                        " x " + str(order.shipments[0].dimensions['height'])

        weight_str = str(order.shipments[0].weight) + " kg"

        c.drawString(-0.08 * inch, 3.05 * inch, dimension_str)
        c.drawString(-0.15 * inch, 2.77 * inch, weight_str)
    except Exception:
        pass

    c.setFont('Helvetica', 10)


def generate_picklist(canvas, products, order_count):
    time_now = datetime.utcnow() + timedelta(hours=5.5)
    time_now = time_now.strftime('%Y-%m-%d %I:%M %p')
    y_axis = 11.25
    canvas.setFont('Helvetica-Bold', 14)
    canvas.drawString(3.5 * inch, y_axis * inch, "PICK LIST")
    y_axis -= 0.3
    canvas.setFont('Helvetica-Bold', 12)
    canvas.drawString(2.75 * inch, y_axis * inch, "Generated at: " + time_now)
    y_axis -= 0.3
    x_axis = (0.25, 2.05, 5.20, 6.30, 7.10, 8.0)
    for client, prod_dict in products.items():
        try:
            prod_dict = sorted(prod_dict.items(),key=lambda x: x[1]['quantity'],reverse=True)
            if y_axis < 4:
                canvas.drawString((x_axis[0] + 0.2) * inch, 0.6 * inch, "Picked By:")
                canvas.showPage()
                y_axis = 11.1
            canvas.setFont('Helvetica-Bold', 12)
            canvas.drawString(x_axis[0] * inch, y_axis * inch, "Client: " + str(client))
            y_axis -= 0.20
            canvas.drawString(x_axis[0] * inch, y_axis * inch, "Orders Selected: " + str(order_count[client]))
            y_axis -= 0.20

            canvas.line(x_axis[0] * inch, y_axis * inch, x_axis[5] * inch, y_axis * inch)

            canvas.drawString((x_axis[0]+0.1) * inch, (y_axis - 0.20)* inch, "SKU")
            canvas.drawString((x_axis[1]+0.1) * inch, (y_axis - 0.20)* inch, "Description")
            canvas.drawString((x_axis[2]+0.1) * inch, (y_axis - 0.20)* inch, "Shelf")
            canvas.drawString((x_axis[3]+0.05) * inch, (y_axis- 0.20) * inch, "Quantity")
            canvas.drawString((x_axis[4]+0.1) * inch, (y_axis - 0.20) * inch, "Picked?")

            new_y_axis = y_axis - 0.30

            for x in x_axis:
                canvas.line(x * inch, new_y_axis * inch, x * inch, y_axis * inch)

            canvas.line(x_axis[0] * inch, new_y_axis * inch, x_axis[5] * inch, new_y_axis * inch)

            y_axis = new_y_axis
            canvas.setFont('Helvetica', 10)
            for prod_info in prod_dict:
                prod_info = prod_info[1]
                if y_axis < 1:
                    canvas.drawString((x_axis[0] + 0.2) * inch, 0.6 * inch, "Picked By:")
                    canvas.showPage()
                    y_axis = 11.1

                canvas.setFont('Helvetica', 8)
                canvas.drawString((x_axis[0] + 0.1) * inch, (y_axis - 0.20) * inch, str(prod_info['sku']))
                canvas.drawString((x_axis[2] + 0.1) * inch, (y_axis - 0.20) * inch, str(prod_info['shelf']))
                canvas.drawString((x_axis[3] + 0.3) * inch, (y_axis - 0.20) * inch, str(prod_info['quantity']))

                canvas.setFont('Helvetica', 7)
                prod_name = split_string(str(prod_info['name']), 60)
                old_y_axis = y_axis
                y_axis += 0.13
                for addr in prod_name:
                    y_axis -= 0.13
                    canvas.drawString((x_axis[1] + 0.1) * inch, (y_axis - 0.20) * inch, addr)

                canvas.setFont('Helvetica', 10)

                new_y_axis = y_axis - 0.30

                for x in x_axis:
                    canvas.line(x * inch, new_y_axis * inch, x * inch, old_y_axis * inch)

                canvas.line(x_axis[0] * inch, new_y_axis * inch, x_axis[5] * inch, new_y_axis * inch)
                y_axis = new_y_axis

            y_axis -= 0.5

        except Exception:
            pass

    canvas.drawString((x_axis[0] + 0.2) * inch, 0.6 * inch, "Picked By:")

    return canvas


def generate_packlist(canvas, orders, order_count):
    time_now = datetime.utcnow() + timedelta(hours=5.5)
    time_now = time_now.strftime('%Y-%m-%d %I:%M %p')
    y_axis = 11.25
    canvas.setFont('Helvetica-Bold', 14)
    canvas.drawString(3.5 * inch, y_axis * inch, "PACK LIST")
    y_axis -= 0.3
    canvas.setFont('Helvetica-Bold', 12)
    canvas.drawString(2.75 * inch, y_axis * inch, "Generated at: " + time_now)
    y_axis -= 0.3
    x_axis = (0.25, 1.4, 3.20, 6.20, 7.10, 8.0)
    for client, order_dict in orders.items():
        try:
            if y_axis < 4:
                canvas.drawString((x_axis[0] + 0.2) * inch, 0.6 * inch, "Packed By:")
                canvas.showPage()
                y_axis = 11.1
            canvas.setFont('Helvetica-Bold', 12)
            canvas.drawString(x_axis[0] * inch, y_axis * inch, "Client: " + str(client))
            y_axis -= 0.20
            canvas.drawString(x_axis[0] * inch, y_axis * inch, "Orders Selected: " + str(order_count[client]))
            y_axis -= 0.20

            canvas.line(x_axis[0] * inch, y_axis * inch, x_axis[5] * inch, y_axis * inch)

            canvas.drawString((x_axis[0]+0.1) * inch, (y_axis - 0.20)* inch, "Order ID")
            canvas.drawString((x_axis[1]+0.1) * inch, (y_axis - 0.20)* inch, "SKU")
            canvas.drawString((x_axis[2]+0.1) * inch, (y_axis - 0.20)* inch, "Description")
            canvas.drawString((x_axis[3]+0.1) * inch, (y_axis- 0.20) * inch, "Quantity")
            canvas.drawString((x_axis[4]+0.1) * inch, (y_axis - 0.20) * inch, "Packed?")

            new_y_axis = y_axis - 0.30

            for x in x_axis:
                canvas.line(x * inch, new_y_axis * inch, x * inch, y_axis * inch)

            canvas.line(x_axis[0] * inch, new_y_axis * inch, x_axis[5] * inch, new_y_axis * inch)

            y_axis = new_y_axis
            for order_id, prod_dict in order_dict.items():
                prod_dict = sorted(prod_dict.items(), key=lambda x: x[1]['quantity'], reverse=True)
                if y_axis < 1:
                    canvas.drawString((x_axis[0] + 0.2) * inch, 0.6 * inch, "Packed By:")
                    canvas.showPage()
                    y_axis = 11.1
                canvas.setFont('Helvetica', 10)
                order_id_str = [str(order_id)[i:i+12] for i in range(0, len(str(order_id)), 12)]
                y_axis_order = y_axis
                for addr in order_id_str:
                    canvas.drawString((x_axis[0] + 0.1) * inch, (y_axis_order - 0.20) * inch, addr)
                    y_axis_order -= 0.13

                y_axis_order -= 0.13

                for prod_info in prod_dict:
                    prod_info = prod_info[1]
                    if y_axis < 1:
                        canvas.drawString((x_axis[0] + 0.2) * inch, 0.6 * inch, "Packed By:")
                        canvas.showPage()
                        y_axis = 11.1
                        y_axis_order = 11.1

                    canvas.drawString((x_axis[1] + 0.1) * inch, (y_axis - 0.20) * inch, str(prod_info['sku']))
                    canvas.drawString((x_axis[3] + 0.3) * inch, (y_axis - 0.20) * inch, str(prod_info['quantity']))

                    canvas.setFont('Helvetica', 8)
                    prod_name = split_string(str(prod_info['name']), 50)
                    old_y_axis = y_axis
                    y_axis += 0.13
                    for addr in prod_name:
                        y_axis -= 0.13
                        canvas.drawString((x_axis[2] + 0.1) * inch, (y_axis - 0.20) * inch, addr)

                    canvas.setFont('Helvetica', 10)

                    new_y_axis = y_axis - 0.30

                    if y_axis_order < new_y_axis:
                        new_y_axis = y_axis_order

                    for x in x_axis:
                        canvas.line(x * inch, new_y_axis * inch, x * inch, old_y_axis * inch)

                    canvas.line(x_axis[1] * inch, new_y_axis * inch, x_axis[5] * inch, new_y_axis * inch)
                    y_axis = new_y_axis

                canvas.line(x_axis[0] * inch, y_axis * inch, x_axis[5] * inch, y_axis * inch)

            y_axis -= 0.5

        except Exception:
            pass

    canvas.drawString((x_axis[0] + 0.2) * inch, 0.6 * inch, "Packed By:")

    return canvas


def create_invoice_blank_page(canvas):
    canvas.setFont('Helvetica', 9)
    canvas.translate(inch, inch)
    canvas.setLineWidth(0.4)
    canvas.line(-0.80 * inch, 6.7 * inch, 6.9 * inch, 6.7 * inch)
    canvas.line(-0.80 * inch, 6.4 * inch, 6.9 * inch, 6.4 * inch)
    canvas.line(-0.80 * inch, 9.7 * inch, 2.0 * inch, 9.7 * inch)
    canvas.line(-0.80 * inch, 9.2 * inch, 2.0 * inch, 9.2 * inch)
    canvas.setFont('Helvetica', 18)
    canvas.drawString(-0.75 * inch, 9.35 * inch, "TAX INVOICE")
    canvas.setFont('Helvetica-Bold', 9)
    canvas.drawString(-0.75 * inch, 6.5 * inch, "Product(s)")
    canvas.drawString(2.00 * inch, 6.5 * inch, "Qty")
    canvas.drawString(2.40 * inch, 6.5 * inch, "MRP")
    canvas.drawString(3.00 * inch, 6.5 * inch, "Taxable value")
    canvas.drawString(4.10 * inch, 6.5 * inch, "Tax Description")
    canvas.drawString(6.20 * inch, 6.5 * inch, "Total")
    canvas.drawString(-0.75 * inch, 8.8 * inch, "SOLD BY:")
    canvas.drawString(-0.75 * inch, 7.1 * inch, "GSTIN:")
    canvas.drawString(1.0 * inch, 8.5 * inch, "Billing Address:")
    canvas.drawString(3.0 * inch, 8.5 * inch, "Shipping Address:")
    canvas.setFont('Helvetica', 8)
    canvas.drawString(2.5 * inch, 9.7 * inch, "INVOICE DATE:")
    canvas.drawString(4.7 * inch, 9.7 * inch, "INVOICE NO.")
    canvas.drawString(2.5 * inch, 9.45 * inch, "ORDER DATE:")
    canvas.drawString(4.7 * inch, 9.45 * inch, "ORDER NO.")
    canvas.drawString(2.5 * inch, 9.2 * inch, "PAYMENT METHOD:")
    canvas.drawString(4.7 * inch, 9.2 * inch, "AWB NO.")
    canvas.setLineWidth(0.8)


def create_wro_label_blank_page(canvas):
    canvas.translate(inch, inch)
    canvas.drawImage("wareiq.jpg", -0.3 * inch, 7.8 * inch, width=200, height=200)
    canvas.rect(-0.6*inch, -0.6*inch, 7.45*inch, 10.95*inch, stroke=1, fill=0)


def fill_wro_label_data(c, wro_obj, page_no, total_pages):
    c.setFont('Helvetica-Bold', 13)
    wro_id = str(wro_obj[0].id)
    awb_barcode = code128.Code128(wro_id, barHeight=0.8 * inch, barWidth=0.8 * mm)
    awb_barcode.drawOn(c, 3.8 * inch, 8.85 * inch)
    c.drawString(3.5 * inch, 8.55 * inch, "Warehouse Receiving Order #"+wro_id)

    wro_page = wro_id + " " + str(page_no)
    awb_barcode = code128.Code128(wro_page, barHeight=0.8 * inch, barWidth=0.7 * mm)
    awb_barcode.drawOn(c, 1.75 * inch, 5.5 * inch)
    c.drawString(2.9 * inch, 5.3 * inch, wro_page)

    c.setFont('Helvetica-Bold', 10)
    c.drawString(0 * inch, 8.05 * inch, "Created Date:")
    c.drawString(0 * inch, 7.85 * inch, "Created By:")
    c.drawString(0 * inch, 7.65 * inch, "Estimated Arrival:")

    c.drawString(3.5 * inch, 8.05 * inch, "Destination:")
    c.drawString(3.5 * inch, 7.00 * inch, "Phone:")
    c.drawString(3.5 * inch, 6.80 * inch, "Email:")

    c.drawString(0 * inch, 6.80 * inch, "Box "+str(page_no)+" of "+str(total_pages))

    c.setFont('Helvetica', 10)
    full_address = wro_obj[1].address
    if wro_obj[1].address_two:
        full_address += " " + wro_obj[1].address_two
    full_address = split_string(full_address, 35)
    y_axis = 7.90
    for addr in full_address:
        c.drawString(3.5 * inch, y_axis * inch, addr)
        y_axis -= 0.15

    try:
        c.drawString(3.5 * inch, y_axis * inch, str(wro_obj[1].city) + ", " + str(wro_obj[1].state))
        c.drawString(3.5 * inch, (y_axis-0.15) * inch,
                     str(wro_obj[1].country) + ", PIN: " + str(wro_obj[1].pincode))
    except Exception:
        pass

    c.drawString(4.0 * inch, 7.00 * inch, str(wro_obj[1].phone))
    c.drawString(4.0 * inch, 6.80 * inch, "support@wareiq.com")

    c.drawString(1.3 * inch, 8.05 * inch, wro_obj[0].date_created.strftime('%Y-%m-%d') if wro_obj[0].date_created else "")
    c.drawString(1.3 * inch, 7.85 * inch, wro_obj[0].created_by if wro_obj[0].created_by else "")
    c.drawString(1.3 * inch, 7.65 * inch, wro_obj[0].edd.strftime('%Y-%m-%d') if wro_obj[0].edd else "")


def fill_invoice_data(c, order, client_name):
    c.setFont('Helvetica', 20)
    if client_name:
        c.drawString(-0.80 * inch,10.10 * inch, client_name.legal_name if client_name.legal_name else str(client_name.client_name))

    c.setFont('Helvetica', 8)
    order_date = order.order_date.strftime("%d/%m/%Y")
    if order.orders_invoice:
        invoice_no = order.orders_invoice[-1].invoice_no_text
        invoice_date = order.orders_invoice[-1].date_created if order.orders_invoice[-1].date_created else datetime.utcnow() + timedelta(hours=5.5)
        invoice_date = invoice_date.strftime("%d/%m/%Y")
    else:
        invoice_no = invoice_order(order)
        invoice_date = datetime.utcnow() + timedelta(hours=5.5)
        invoice_date = invoice_date.strftime("%d/%m/%Y")
    c.drawString(3.6 * inch, 9.7 * inch, invoice_date)
    c.drawString(3.6 * inch, 9.45 * inch, order_date)
    c.drawString(3.7 * inch, 9.2 * inch, order.payments[0].payment_mode.lower())
    c.drawString(5.5 * inch, 9.45 * inch, order.channel_order_id)
    if order.shipments and order.shipments[0].awb:
        c.drawString(5.5 * inch, 9.2 * inch, order.shipments[0].awb)

    c.drawString(5.5 * inch, 9.7 * inch, invoice_no)

    if order.pickup_data.gstin:
        c.drawString(-0.28 * inch, 7.1 * inch, order.pickup_data.gstin)

    c.setFont('Helvetica', 7)

    try:
        full_name = order.delivery_address.first_name
        if order.delivery_address.last_name:
            full_name += " " + order.delivery_address.last_name

        str_full_address = [full_name]
        full_address = order.delivery_address.address_one
        if order.delivery_address.address_two:
            full_address += " "+order.delivery_address.address_two
        full_address = split_string(full_address, 33)
        str_full_address += full_address
        str_full_address.append(order.delivery_address.city+", "+order.delivery_address.state)
        str_full_address.append(order.delivery_address.country+", PIN: "+order.delivery_address.pincode)
        y_axis = 8.3
        for addr in str_full_address:
            c.drawString(3.0 * inch, y_axis * inch, addr)
            y_axis -= 0.15

    except Exception:
        pass

    billing_address = order.billing_address if order.billing_address else order.delivery_address
    try:
        full_name = billing_address.first_name
        if billing_address.last_name:
            full_name += " " + billing_address.last_name

        str_full_address = [full_name]
        full_address = billing_address.address_one
        if billing_address.address_two:
            full_address += " "+billing_address.address_two
        full_address = split_string(full_address, 33)
        str_full_address += full_address
        str_full_address.append(billing_address.city+", "+billing_address.state)
        str_full_address.append(billing_address.country+", PIN: "+billing_address.pincode)
        y_axis = 8.3
        for addr in str_full_address:
            c.drawString(1.0 * inch, y_axis * inch, addr)
            y_axis -= 0.15

    except Exception:
        pass

    try:
        full_name = client_name.client_name if client_name.client_name else order.pickup_data.pickup.name
        str_full_address = [full_name]
        full_address = order.pickup_data.pickup.address
        if order.pickup_data.pickup.address_two:
            full_address += " "+order.pickup_data.pickup.address_two
        full_address = split_string(full_address, 35)
        str_full_address += full_address
        str_full_address.append(order.pickup_data.pickup.city+", "+order.pickup_data.pickup.state)
        str_full_address.append(order.pickup_data.pickup.country+", PIN: "+str(order.pickup_data.pickup.pincode))
        y_axis = 8.6
        for addr in str_full_address:
            c.drawString(-0.75 * inch, y_axis * inch, addr)
            y_axis -= 0.15

    except Exception:
        pass

    if order.orders_invoice:
        qr_url = order.orders_invoice[-1].qr_url
        qr_code = qr.QrCodeWidget(qr_url)
        bounds = qr_code.getBounds()
        width = bounds[2] - bounds[0]
        height = bounds[3] - bounds[1]
        d = Drawing(60, 60, transform=[60. / width, 0, 0, 60. / height, 0, 0])
        d.add(qr_code)
        d.drawOn(c, 5.2 * inch, 7.8 * inch)

    y_axis = 6.1
    s_no = 1
    prod_total_value = 0
    for prod in order.products:
        try:
            c.setFont('Helvetica-Bold', 7)
            product_name = str(s_no) + ". " + prod.master_product.name
            product_name = split_string(product_name, 40)
            for addr in product_name:
                c.drawString(-0.75 * inch, y_axis * inch, addr)
                y_axis -= 0.15
            c.setFont('Helvetica', 7)
            if prod.master_product.sku:
                c.drawString(0.45 * inch, y_axis* inch, "SKU: " + prod.master_product.sku)
            if prod.master_product.hsn_code:
                c.drawString(-0.65 * inch, y_axis* inch, "HSN: " + prod.master_product.hsn_code)

            c.drawString(2.02 * inch, (y_axis + 0.08) * inch, str(prod.quantity))

            if prod.tax_lines:
                des_str = ""
                total_tax = 0
                for tax_lines in prod.tax_lines:
                    total_tax += tax_lines['rate']

                taxable_val = prod.amount if prod.amount is not None else prod.master_product.price*prod.quantity

                taxable_val = taxable_val/(1+total_tax)
                c.drawString(3.02 * inch, (y_axis + 0.08) * inch, str(round(taxable_val, 2)))
                c.drawString(2.42 * inch, (y_axis + 0.08) * inch, str(round(prod.master_product.price, 2)) if prod.master_product.price else "")

                for tax_lines in prod.tax_lines:
                    des_str += tax_lines['title'] + "(_a_%): _b_".replace('_a_', str(round(tax_lines['rate']*100, 1))).replace('_b_', str(round(tax_lines['rate']*taxable_val, 2))) + " | "

                des_str = des_str.rstrip('| ')

                c.drawString(4.12 * inch, (y_axis + 0.08) * inch, des_str)

                c.drawString(6.22 * inch, (y_axis + 0.08) * inch, str(round(prod.amount, 2)) if prod.amount is not None else str(round(prod.master_product.price*prod.quantity, 2)))

            elif order.shipments and (prod.amount or prod.master_product.price):
                total_tax = 0.18

                taxable_val = prod.amount if prod.amount is not None else prod.master_product.price*prod.quantity

                taxable_val = taxable_val / (1 + total_tax)
                c.drawString(3.02 * inch, (y_axis + 0.08) * inch, str(round(taxable_val, 2)))
                c.drawString(2.42 * inch, (y_axis + 0.08) * inch, str(round(prod.master_product.price, 2)) if prod.master_product.price else "")

                if order.shipments[0].same_state:
                    des_str = "SGST(9.0%): _a_ | CGST(9.0%): _b_".replace('_a_', str(round(taxable_val*0.09, 2))).replace('_b_',str(round(taxable_val*0.09, 2)))
                else:
                    des_str = "IGST(18.0%): _a_".replace('_a_',str(round(taxable_val*0.18, 1)))

                des_str = des_str.rstrip('| ')

                c.drawString(4.12 * inch, (y_axis + 0.08) * inch, des_str)

                c.drawString(6.22 * inch, (y_axis + 0.08) * inch, str(round(prod.amount, 2)) if prod.amount is not None else str(round(prod.master_product.price*prod.quantity, 2)))

            else:
                taxable_val = prod.amount if prod.amount is not None else prod.master_product.price*prod.quantity
                c.drawString(3.02 * inch, (y_axis + 0.08) * inch, str(round(taxable_val, 2)))
                c.drawString(2.42 * inch, (y_axis + 0.08) * inch, str(round(prod.master_product.price, 2)) if prod.master_product.price else "")
                c.drawString(6.22 * inch, (y_axis + 0.08) * inch, str(round(prod.amount, 2)) if prod.amount is not None else str(round(prod.master_product.price*prod.quantity, 2)))

            prod_total_value += prod.amount if prod.amount is not None else prod.master_product.price*prod.quantity
        except Exception:
            pass

        s_no += 1
        y_axis -= 0.30

        if y_axis<-0.1:
            c.showPage()
            y_axis=10.1
            c.translate(inch, inch)

    if order.payments[0].shipping_charges:
        c.drawString(4.82 * inch, y_axis * inch, "Shipping Charges:")
        c.drawString(6.22 * inch, y_axis * inch, str(round(order.payments[0].shipping_charges, 2)))
        y_axis -= 0.20
        prod_total_value += order.payments[0].shipping_charges

    if prod_total_value-order.payments[0].amount > 1:
        c.drawString(4.82 * inch, y_axis * inch, "Discount:")
        c.drawString(6.16 * inch, y_axis * inch, "-")
        c.drawString(6.22 * inch, y_axis * inch, str(round(prod_total_value-order.payments[0].amount, 2)))
        y_axis -= 0.20

    c.setLineWidth(0.1)
    c.line(2.02 * inch, y_axis * inch, 6.9 * inch, y_axis * inch)
    y_axis -= 0.25
    c.setFont('Helvetica-Bold', 10)

    c.drawString(4.82 * inch, y_axis * inch, "NET TOTAL:")
    c.drawString(6.12 * inch, y_axis * inch, "Rs. "+ str(round(order.payments[0].amount, 2)))

    y_axis -= 0.175

    c.line(-0.75 * inch, y_axis * inch, 6.9 * inch, y_axis * inch)

    c.setFont('Helvetica', 7)
    c.drawString(4.82 * inch, (y_axis+0.09) * inch, "(incl. of all taxes)")

    y_axis -= 1.5
    c.drawString(-0.70 * inch, y_axis * inch, "This is computer generated invoice no signature required.")

    c.setFont('Helvetica', 8)


def invoice_order(order):
    try:
        last_inv_no = order.pickup_data.invoice_last
        if not last_inv_no:
            last_inv_no = 0
        inv_no = last_inv_no+1
        inv_text = str(inv_no)
        inv_text = inv_text.zfill(5)
        if order.pickup_data.invoice_prefix:
            inv_text = order.pickup_data.invoice_prefix + "-" + inv_text

        invoice_obj = OrdersInvoice(order=order,
                                    pickup_data=order.pickup_data,
                                    invoice_no_text=inv_text,
                                    invoice_no=inv_no,
                                    date_created=datetime.utcnow()+timedelta(hours=5.5),
                                    qr_url="https://track.wareiq.com/orders/v1/invoice/%s?uid=%s"%(str(order.id), ''.join(random.choices(string.ascii_lowercase+string.ascii_uppercase + string.digits, k=6))))
        order.pickup_data.invoice_last = inv_no
        db.session.add(invoice_obj)
        db.session.commit()
        return inv_text
    except Exception:
        return False


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


def pagination_validator(page_size, page_number):
    if page_size is None:
        page_size = 10
    else:
        page_size = int(page_size)
    if page_number is None:
        page_number = 1
    else:
        page_number = int(page_number)
    return page_size, page_number


def tracking_get_xpressbees_details(shipment, awb):
    xpressbees_url = "http://xbclientapi.xbees.in/TrackingService.svc/GetShipmentSummaryDetails"
    body = {"AWBNo": awb, "XBkey": shipment.courier.api_key}
    return_details = dict()
    req = requests.post(xpressbees_url, json=body).json()
    for each_scan in req[0]['ShipmentSummary']:
        return_details_obj = dict()
        return_details_obj['status'] = each_scan['Status']
        if each_scan['Comment']:
            return_details_obj['status'] += " - " + each_scan['Comment']
        return_details_obj['city'] = each_scan['Location']
        if each_scan['Location']:
            return_details_obj['city'] = each_scan['Location'].split(", ")[1]
        status_time = each_scan['StatusDate'] + " " + each_scan['StatusTime']
        if status_time:
            status_time = datetime.strptime(status_time, '%d-%m-%Y %H%M')

        time_str = status_time.strftime("%d %b %Y, %H:%M:%S")
        return_details_obj['time'] = time_str
        if time_str[:11] not in return_details:
            return_details[time_str[:11]] = [return_details_obj]
        else:
            return_details[time_str[:11]].append(return_details_obj)

        for key in return_details:
            return_details[key] = sorted(return_details[key], key=lambda k: k['time'], reverse=True)

    return return_details


def tracking_get_delhivery_details(shipment, awb):
    delhivery_url = "https://track.delhivery.com/api/status/packages/json/?waybill=%s&token=%s" \
                    % (str(awb), shipment.courier.api_key)
    return_details = dict()
    req = requests.get(delhivery_url).json()
    for each_scan in req['ShipmentData'][0]['Shipment']["Scans"]:
        return_details_obj = dict()
        return_details_obj['status'] = each_scan['ScanDetail']['Scan'] + \
                                       ' - ' + each_scan['ScanDetail']['Instructions']
        return_details_obj['city'] = each_scan['ScanDetail']['CityLocation']
        status_time = each_scan['ScanDetail']['StatusDateTime']
        if status_time:
            if len(status_time) == 19:
                status_time = datetime.strptime(status_time, '%Y-%m-%dT%H:%M:%S')
            else:
                status_time = datetime.strptime(status_time, '%Y-%m-%dT%H:%M:%S.%f')
        time_str = status_time.strftime("%d %b %Y, %H:%M:%S")
        return_details_obj['time'] = time_str
        if time_str[:11] not in return_details:
            return_details[time_str[:11]] = [return_details_obj]
        else:
            return_details[time_str[:11]].append(return_details_obj)

        for key in return_details:
            return_details[key] = sorted(return_details[key], key=lambda k: k['time'], reverse=True)

    return return_details


def tracking_get_bluedart_details(shipment, awb):
    bluedart_url = "https://api.bluedart.com/servlet/RoutingServlet?handler=tnt&action=custawbquery&loginid=HYD50082&awb=awb&numbers=%s&format=xml&lickey=eguvjeknglfgmlsi5ko5hn3vvnhoddfs&verno=1.3&scan=1" % awb
    return_details = dict()
    req = requests.get(bluedart_url)
    req = xmltodict.parse(req.content)
    try:
        if type(req['ShipmentData']['Shipment']['Scans']['ScanDetail'])==list:
            scan_list = req['ShipmentData']['Shipment']['Scans']['ScanDetail']
        else:
            scan_list = [req['ShipmentData']['Shipment']['Scans']['ScanDetail']]
    except Exception:
        scan_list = req['ShipmentData']['Shipment'][0]['Scans']['ScanDetail']

    for each_scan in scan_list:
        return_details_obj = dict()
        return_details_obj['status'] = each_scan['Scan']
        return_details_obj['city'] = each_scan['ScannedLocation']
        status_time = each_scan['ScanDate'] + " " +each_scan['ScanTime']
        if status_time:
            status_time = datetime.strptime(status_time, '%d-%b-%Y %H:%M')

        time_str = status_time.strftime("%d %b %Y, %H:%M:%S")
        return_details_obj['time'] = time_str
        if time_str[:11] not in return_details:
            return_details[time_str[:11]] = [return_details_obj]
        else:
            return_details[time_str[:11]].append(return_details_obj)

        for key in return_details:
            return_details[key] = sorted(return_details[key], key=lambda k: k['time'], reverse=True)

    return return_details


def tracking_get_ecomxp_details(shipment, awb):
    ecomxp_url = "https://plapi.ecomexpress.in/track_me/api/mawbd/?awb=%s&username=%s&password=%s" % (awb, shipment.courier.api_key, shipment.courier.api_password)
    return_details = dict()
    req = requests.get(ecomxp_url)
    req = xmltodict.parse(req.content)

    scan_list = list()
    for obj in req['ecomexpress-objects']['object']['field']:
        if obj['@name'] == 'scans':
            scan_list = obj['object']

    for each_scan in scan_list:
        each_scan = {item.get('@name'):item.get('#text') for item in each_scan['field']}
        return_details_obj = dict()
        return_details_obj['status'] = each_scan['status']
        return_details_obj['city'] = each_scan['location_city']
        status_time = each_scan['updated_on']
        if status_time:
            status_time = datetime.strptime(status_time, '%d %b, %Y, %H:%M')

        time_str = status_time.strftime("%d %b %Y, %H:%M:%S")
        return_details_obj['time'] = time_str
        if time_str[:11] not in return_details:
            return_details[time_str[:11]] = [return_details_obj]
        else:
            return_details[time_str[:11]].append(return_details_obj)

        for key in return_details:
            return_details[key] = sorted(return_details[key], key=lambda k: k['time'], reverse=True)

    return return_details


def check_client_order_ids(order_ids, auth_data, cur):
    if len(order_ids)==1:
        order_tuple_str = "("+str(order_ids[0])+")"
    else:
        order_tuple_str = str(tuple(order_ids))

    query_to_run = """SELECT array_agg(id) FROM orders WHERE id in __ORDER_IDS__ __CLIENT_FILTER__;""".replace("__ORDER_IDS__", order_tuple_str)

    if auth_data['user_group'] == 'client':
        query_to_run = query_to_run.replace('__CLIENT_FILTER__', "AND client_prefix='%s'"%auth_data['client_prefix'])
    elif auth_data['user_group'] == 'multi-vendor':
        cur.execute("SELECT vendor_list FROM multi_vendor WHERE client_prefix='%s';" % auth_data['client_prefix'])
        vendor_list = cur.fetchone()[0]
        query_to_run = query_to_run.replace("__CLIENT_FILTER__",
                                            "AND client_prefix in %s" % str(tuple(vendor_list)))
    else:
        query_to_run = query_to_run.replace("__CLIENT_FILTER__","")

    cur.execute(query_to_run)
    order_ids = cur.fetchone()[0]
    if not order_ids:
        return ""

    if len(order_ids)==1:
        order_tuple_str = "("+str(order_ids[0])+")"
    else:
        order_tuple_str = str(tuple(order_ids))

    return order_tuple_str


def cancel_order_on_couriers(order):
    if order.shipments and order.shipments[0].awb:
        if order.shipments[0].courier.id in (1, 2, 8, 11, 12):  # Cancel on delhievry #todo: cancel on other platforms too
            cancel_body = json.dumps({"waybill": order.shipments[0].awb, "cancellation": "true"})
            headers = {"Authorization": "Token " + order.shipments[0].courier.api_key,
                       "Content-Type": "application/json"}
            req_can = requests.post("https://track.delhivery.com/api/p/edit", headers=headers, data=cancel_body)
        if order.shipments[0].courier.id in (5, 13):  # Cancel on Xpressbees
            cancel_body = json.dumps({"AWBNumber": order.shipments[0].awb, "XBkey": order.shipments[0].courier.api_key,
                                      "RTOReason": "Cancelled by seller"})
            headers = {"Authorization": "Basic " + order.shipments[0].courier.api_key,
                       "Content-Type": "application/json"}
            req_can = requests.post("http://xbclientapi.xbees.in/POSTShipmentService.svc/RTONotifyShipment",
                                    headers=headers, data=cancel_body)


def cancel_order_on_channels(order):
    if order.client_channel and order.client_channel.mark_canceled and order.order_id_channel_unique:
        if order.client_channel.channel_id == 6: # cancel on magento
            cancel_header = {'Content-Type': 'application/json',
                             'Authorization': 'Bearer ' + order.client_channel.api_key}
            cancel_data = {
                "entity": {
                    "entity_id": int(order.order_id_channel_unique),
                    "status": "canceled"
                }
            }
            cancel_url = order.client_channel.shop_url + "/rest/V1/orders/%s/cancel" % str(order.order_id_channel_unique)
            req_ful = requests.post(cancel_url, data=json.dumps(cancel_data),
                                    headers=cancel_header, verify=False)
        if order.client_channel.channel_id == 1: # cancel on shopify
            get_cancel_url = "https://%s:%s@%s/admin/api/2021-01/orders/%s/cancel.json" % (
                order.client_channel.api_key, order.client_channel.api_password,
                order.client_channel.shop_url, order.order_id_channel_unique)

            tra_header = {'Content-Type': 'application/json'}
            cancel_data = {}
            req_ful = requests.post(get_cancel_url, data=json.dumps(cancel_data),
                                    headers=tra_header)
        if order.client_channel.channel_id == 5: # cancel on woocommerce
            wcapi = API(
                url=order.client_channel.shop_url,
                consumer_key=order.client_channel.api_key,
                consumer_secret=order.client_channel.api_password,
                version="wc/v3"
            )
            status_mark = "cancelled"
            r = wcapi.post('orders/%s' % str(order[5]), data={"status": status_mark})

        if order.client_channel.channel_id == 7: # cancel on Easyecom
            cancel_order_url = "%s/orders/cancelOrder?api_token=%s" % (order.client_channel.shop_url, order.client_channel.api_key)
            ful_header = {'Content-Type': 'application/json'}
            fulfil_data = {
                "api_token": order.client_channel.api_key,
                "reference_code": order.channel_order_id
            }
            req_ful = requests.post(cancel_order_url, data=json.dumps(fulfil_data),
                                    headers=ful_header)