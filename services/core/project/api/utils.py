# project/api/utils.py


import json
from functools import wraps
from datetime import datetime, timedelta

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
    canvas.drawString(2.40 * inch, 6.5 * inch, "Tax Description")
    canvas.drawString(3.60 * inch, 6.5 * inch, "Taxable value")
    canvas.drawString(4.80 * inch, 6.5 * inch, "Tax (value | %)")
    canvas.drawString(6.20 * inch, 6.5 * inch, "Total")
    canvas.drawString(-0.75 * inch, 8.8 * inch, "SOLD BY:")
    canvas.drawString(-0.75 * inch, 7.1 * inch, "GSTIN:")
    canvas.drawString(2.2 * inch, 8.5 * inch, "Billing Address:")
    canvas.drawString(5.0 * inch, 8.5 * inch, "Shipping Address:")
    canvas.setFont('Helvetica', 8)
    canvas.drawString(2.5 * inch, 9.7 * inch, "INVOICE DATE:")
    canvas.drawString(4.7 * inch, 9.7 * inch, "INVOICE NO.")
    canvas.drawString(2.5 * inch, 9.45 * inch, "ORDER DATE:")
    canvas.drawString(4.7 * inch, 9.45 * inch, "ORDER NO.")
    canvas.drawString(2.5 * inch, 9.2 * inch, "PAYMENT METHOD:")
    canvas.drawString(4.7 * inch, 9.2 * inch, "AWB NO.")
    canvas.setLineWidth(0.8)


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

    c.setFont('Helvetica', 8)
    try:
        products_string = ""
        for prod in order.products:
            products_string += prod.product.name + " (" + str(prod.quantity) + ") + "
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


def fill_shiplabel_data_thermal(c, order):
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
        order_id_string = order.channel_order_id
        order_id_barcode = code128.Code128(order_id_string, barHeight=0.6*inch, barWidth=0.3*mm)
        order_id_barcode.drawOn(c, 1.5*inch, 1.5*inch)
        c.drawString(1.85 * inch, 1.35*inch, order_id_string)
    except Exception:
        pass

    c.drawString( 0.6 * inch, 3.60*inch, awb_string)
    routing_code = "N/A"
    if order.shipments[0].routing_code:
        routing_code = str(order.shipments[0].routing_code)
    c.drawString(2.1 * inch, 3.30*inch, routing_code)

    c.setFont('Helvetica', 9)
    c.drawString(0*inch, 4.75 * inch, order.shipments[0].courier.courier_name)
    c.drawString(1.6 * inch, 0.5 * inch, str(order.payments[0].amount))
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
    try:
        if order.pickup_data:
            return_point = order.pickup_data.return_point
        else:
            return_point = order.shipments[0].return_point
        return_address = return_point.address
        if return_point.address_two:
            return_address += " "+ return_point.address_two

        return_address = split_string(return_address, 75)

        y_axis = -0.65
        for retn in return_address:
            c.drawString(-0.75 * inch, y_axis * inch, retn)
            y_axis -= 0.12

    except Exception:
        pass

    c.setFont('Helvetica', 7)
    try:
        products_string = ""
        for prod in order.products:
            products_string += prod.product.name + " (" + str(prod.quantity) + ") + "
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


def fill_invoice_data(c, order, client_name):
    c.setFont('Helvetica', 20)
    if client_name and client_name.client_name:
        c.drawString(-0.80 * inch,10.10 * inch, client_name.client_name)

    c.setFont('Helvetica', 8)
    order_date = order.order_date.strftime("%d/%m/%Y")
    invoice_date = datetime.utcnow() + timedelta(hours=5.5)
    invoice_date = invoice_date.strftime("%d/%m/%Y")
    c.drawString(3.6 * inch, 9.7 * inch, invoice_date)
    c.drawString(3.6 * inch, 9.45 * inch, order_date)
    c.drawString(3.7 * inch, 9.2 * inch, order.payments[0].payment_mode.lower())
    c.drawString(5.5 * inch, 9.45 * inch, order.channel_order_id)
    if order.shipments and order.shipments[0].awb:
        c.drawString(5.5 * inch, 9.2 * inch, order.shipments[0].awb)

    invoice_no = order.client_prefix.lower() + order.channel_order_id
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
            c.drawString(5.0 * inch, y_axis * inch, addr)
            y_axis -= 0.15

    except Exception:
        pass

    try:
        full_name = order.billing_address.first_name
        if order.billing_address.last_name:
            full_name += " " + order.billing_address.last_name

        str_full_address = [full_name]
        full_address = order.billing_address.address_one
        if order.billing_address.address_two:
            full_address += " "+order.billing_address.address_two
        full_address = split_string(full_address, 33)
        str_full_address += full_address
        str_full_address.append(order.billing_address.city+", "+order.billing_address.state)
        str_full_address.append(order.billing_address.country+", PIN: "+order.billing_address.pincode)
        y_axis = 8.3
        for addr in str_full_address:
            c.drawString(2.2 * inch, y_axis * inch, addr)
            y_axis -= 0.15

    except Exception:
        pass

    try:
        full_name = order.pickup_data.pickup.name
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

    y_axis = 6.1
    s_no = 1
    prod_total_value = 0
    for prod in order.products:
        try:
            c.setFont('Helvetica-Bold', 7)
            product_name = str(s_no) + ". " + prod.product.name
            product_name = split_string(product_name, 40)
            for addr in product_name:
                c.drawString(-0.75 * inch, y_axis * inch, addr)
                y_axis -= 0.15
            c.setFont('Helvetica', 7)
            if prod.product.master_sku:
                c.drawString(-0.65 * inch, y_axis* inch, "SKU: " + prod.product.master_sku)

            c.drawString(2.02 * inch, (y_axis + 0.08) * inch, str(prod.quantity))

            if prod.tax_lines:
                des_str = ""
                total_tax = 0
                for tax_lines in prod.tax_lines:
                    des_str += tax_lines['title'] + ": " + str(tax_lines['rate']*100) + "% | "
                    total_tax += tax_lines['rate']

                des_str = des_str.rstrip('| ')

                c.drawString(2.42 * inch, (y_axis + 0.08) * inch, des_str)

                taxable_val = prod.amount

                taxable_val = taxable_val/(1+total_tax)
                c.drawString(3.82 * inch, (y_axis + 0.08) * inch, str(round(taxable_val, 2)))

                tax_val = taxable_val*total_tax

                c.drawString(4.82 * inch, (y_axis + 0.08) * inch, str(round(tax_val, 2))+" | "+str(round(total_tax*100, 1)) + "%")
                c.drawString(6.22 * inch, (y_axis + 0.08) * inch, str(round(prod.amount, 2)))

            else:

                taxable_val = prod.amount
                c.drawString(3.62 * inch, (y_axis + 0.08) * inch, str(round(taxable_val, 2)))
                c.drawString(6.22 * inch, (y_axis + 0.08) * inch, str(round(prod.amount, 2)))

            prod_total_value += prod.amount
        except Exception:
            pass

        s_no += 1
        y_axis -= 0.30

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

    y_axis -= 1.5
    c.setFont('Helvetica', 7)

    c.drawString(-0.70 * inch, y_axis * inch, "This is computer generated invoice no signature required.")

    c.setFont('Helvetica', 8)


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
