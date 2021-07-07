from datetime import datetime, timedelta
import json, re, random, string, csv, boto3, os
from app.db_utils import DbConnection
from reportlab.lib.pagesizes import landscape, A4
from reportlab.lib.units import inch, mm
from reportlab.graphics.barcode import code128, qr
from reportlab.graphics.shapes import Drawing

from reportlab.pdfgen import canvas

conn = DbConnection.get_db_connection_instance()
session = boto3.Session(
    aws_access_key_id='AKIAWRT2R3KC3YZUBFXY',
    aws_secret_access_key='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',
)


def download_flag_func_orders(query_to_run, get_selected_product_details, auth_data, ORDERS_DOWNLOAD_HEADERS, hide_weights, report_id):
    cur = conn.cursor()
    try:
        client_prefix = auth_data.get('client_prefix') if auth_data.get('client_prefix') else auth_data.get('warehouse_prefix')
        query_to_run = re.sub(r"""__.+?__""", "", query_to_run)
        cur.execute(query_to_run)
        orders_qs_data = cur.fetchall()
        order_id_data = ','.join([str(it[1]) for it in orders_qs_data])
        product_detail_by_order_id = {}
        if order_id_data:
            update_product_details_query = get_selected_product_details.replace('__FILTERED_ORDER_ID__',
                                                                                order_id_data)
            cur.execute(update_product_details_query)
            product_detail_data = cur.fetchall()
            for it in product_detail_data:
                product_detail_by_order_id[it[0]] = [it[1], it[2], it[3], it[4], it[5], it[6], it[7], it[8]]

        filename = str(client_prefix)+"_EXPORT_orders_"+ ''.join(random.choices(string.ascii_letters + string.digits, k=8)) + ".csv"
        with open(filename, 'w') as mycsvfile:
            cw = csv.writer(mycsvfile)
            cw.writerow(ORDERS_DOWNLOAD_HEADERS)
            for order in orders_qs_data:
                try:
                    product_data = product_detail_by_order_id[order[1]] if order[1] in product_detail_by_order_id else []
                    if product_data and product_data[0]:
                        for idx, val in enumerate(product_data[0]):
                            order_disc = "N/A"
                            try:
                                order_disc = sum(product_data[5])-order[25]
                                if order[43]:
                                    order_disc+=order[43]
                            except Exception:
                                pass
                            new_row = list()
                            new_row.append(str(order[0]))
                            new_row.append(str(order[13]))
                            new_row.append(str(order[15]))
                            new_row.append(str(order[14]))
                            new_row.append(order[2].strftime("%Y-%m-%d %H:%M:%S") if order[2] else "N/A")
                            new_row.append(str(order[7]))
                            new_row.append(str(order[9]) if not hide_weights else "")
                            new_row.append(str(order[5]))
                            new_row.append(order[8].strftime("%Y-%m-%d") if order[8] else "N/A")
                            new_row.append(str(order[3]))
                            new_row.append(str(order[16]))
                            new_row.append(str(order[17]))
                            new_row.append(str(order[18]))
                            new_row.append(str(order[19]))
                            new_row.append(str(order[20]))
                            new_row.append(str(order[21]))
                            new_row.append(order[26])
                            new_row.append(str(val))
                            new_row.append(str(product_data[1][idx]))
                            new_row.append(str(product_data[2][idx]))
                            new_row.append(str(order[24]))
                            new_row.append(order[25])
                            new_row.append(order[34].strftime("%Y-%m-%d %H:%M:%S") if order[34] else "N/A")
                            new_row.append(order[23].strftime("%Y-%m-%d %H:%M:%S") if order[23] else "N/A")
                            new_row.append(order[22].strftime("%Y-%m-%d %H:%M:%S") if order[22] else "N/A")
                            if order[27] and order[28] is not None:
                                new_row.append("Confirmed" if order[28] else "Cancelled")
                                new_row.append(str(order[29]))
                            else:
                                new_row.append("N/A")
                                new_row.append("N/A")
                            if order[30] and order[31] is not None:
                                new_row.append("Cancelled" if order[31] else "Re-attempt")
                                new_row.append(str(order[32]))
                            else:
                                new_row.append("N/A")
                                new_row.append("N/A")
                            new_row.append(order[39].strftime("%Y-%m-%d %H:%M:%S") if order[39] else "N/A")
                            new_row.append(str(order[43]) if order[43] else "0")
                            new_row.append(str(order[44]) if order[44] else "N/A")
                            new_row.append(order[45].strftime("%Y-%m-%d %H:%M:%S") if order[45] else "N/A")
                            new_row.append(str(order_disc))
                            prod_amount = product_data[5][idx] if product_data[5][idx] is not None else product_data[7][idx]
                            cgst, sgst, igst = "", "", ""
                            try:
                                taxable_amount = prod_amount / (1 + product_data[6][idx])
                                if prod_amount and product_data[6][idx] and order[48]:
                                    cgst = taxable_amount * product_data[6][idx] / 2
                                    sgst = cgst
                                elif prod_amount and product_data[6][idx] and not order[48]:
                                    igst = taxable_amount * product_data[6][idx]
                            except Exception:
                                pass
                            new_row.append(str(prod_amount))
                            new_row.append(str(cgst))
                            new_row.append(str(sgst))
                            new_row.append(str(igst))
                            new_row.append(str(order[1]))
                            not_shipped = None
                            if not product_data[4][idx]:
                                not_shipped = "Weight/dimensions not entered for product(s)"
                            elif order[12] == "Pincode not serviceable":
                                not_shipped = "Pincode not serviceable"
                            elif not order[26]:
                                not_shipped = "Pickup point not assigned"
                            elif order[12] and "incorrect phone" in order[12].lower():
                                not_shipped = "Invalid contact number"
                            if not_shipped:
                                new_row.append(not_shipped)
                            if auth_data.get('user_group') == 'super-admin':
                                new_row.append(order[38])
                            cw.writerow(new_row)
                except Exception as e:
                    pass

        s3 = session.resource('s3')
        bucket = s3.Bucket("wareiqfiles")
        bucket.upload_file(filename, "downloads/"+filename, ExtraArgs={'ACL': 'public-read'})
        invoice_url = "https://wareiqfiles.s3.amazonaws.com/downloads/" + filename
        file_size = os.path.getsize(filename)
        file_size = int(file_size/1000)
        os.remove(filename)
        cur.execute("UPDATE downloads SET download_link='%s', status='processed', file_size=%s where id=%s"%(invoice_url, file_size, report_id))
        conn.commit()
        return invoice_url

    except Exception:
        conn.rollback()


def shiplabel_download_util(orders_qs, auth_data, report_id):
    shiplabel_type = "A4"
    cur = conn.cursor()
    if auth_data['user_group'] in ('client', 'super-admin', 'multi-vendor'):
        cur.execute("SELECT shipping_label FROM client_mapping WHERE client_prefix='%s'"%auth_data.get('client_prefix'))
        qs = cur.fetchone()
        if qs and qs[0]:
            shiplabel_type = qs[0]
    if auth_data['user_group'] == 'warehouse':
        cur.execute("SELECT shipping_label FROM warehouse_mapping WHERE warehouse_prefix='%s'" % auth_data.get('warehouse_prefix'))
        qs = cur.fetchone()
        if qs and qs[0]:
            shiplabel_type = qs[0]

    file_pref = auth_data['client_prefix'] if auth_data['client_prefix'] else auth_data['warehouse_prefix']
    file_name = "shiplabels_" + str(file_pref) + "_" + str(datetime.now().strftime("%d_%b_%Y_%H_%M_%S")) + ".pdf"
    if shiplabel_type == 'TH1':
        c = canvas.Canvas(file_name, pagesize=(288, 432))
        create_shiplabel_blank_page_thermal(c)
    else:
        c = canvas.Canvas(file_name, pagesize=landscape(A4))
        create_shiplabel_blank_page(c)
    failed_ids = dict()
    idx = 0
    for ixx, order in enumerate(orders_qs):
        try:
            if not order[0].shipments or not order[0].shipments[0].awb:
                continue
            if shiplabel_type == 'TH1':
                try:
                    fill_shiplabel_data_thermal(c, order[0], order[1])
                except Exception:
                    pass

                if idx != len(orders_qs) - 1:
                    c.showPage()
                    create_shiplabel_blank_page_thermal(c)

            elif shiplabel_type == 'A41':
                offset = 3.913
                try:
                    fill_shiplabel_data(c, order[0], offset, order[1])
                except Exception:
                    pass
                c.setFillColorRGB(1, 1, 1)
                c.rect(6.680 * inch, -1.0 * inch, 10 * inch, 10 * inch, fill=1)
                c.rect(-1.0 * inch, -1.0 * inch, 3.907 * inch, 10 * inch, fill=1)
                if idx != len(orders_qs) - 1:
                    c.showPage()
                    create_shiplabel_blank_page(c)
            else:
                offset_dict = {0: 0.20, 1: 3.913, 2: 7.676}
                try:
                    fill_shiplabel_data(c, order[0], offset_dict[idx % 3], order[1])
                except Exception:
                    pass
                if idx % 3 == 2 and ixx != (len(orders_qs) - 1):
                    c.showPage()
                    create_shiplabel_blank_page(c)
            idx += 1
        except Exception as e:
            failed_ids[order[0].channel_order_id] = str(e.args[0])
            pass

    if not (shiplabel_type in ('A41', 'TH1')):
        c.setFillColorRGB(1, 1, 1)
        if idx % 3 == 1:
            c.rect(2.917 * inch, -1.0 * inch, 10 * inch, 10 * inch, fill=1)
        if idx % 3 == 2:
            c.rect(6.680 * inch, -1.0 * inch, 10 * inch, 10 * inch, fill=1)

    c.save()
    s3 = session.resource('s3')
    bucket = s3.Bucket("wareiqshiplabels")
    bucket.upload_file(file_name, file_name, ExtraArgs={'ACL': 'public-read'})
    shiplabel_url = "https://wareiqshiplabels.s3.us-east-2.amazonaws.com/" + file_name
    file_size = os.path.getsize(file_name)
    file_size = int(file_size / 1000)
    os.remove(file_name)
    cur.execute("UPDATE downloads SET download_link='%s', status='processed', file_size=%s where id=%s" % (shiplabel_url, file_size, report_id))
    conn.commit()
    return shiplabel_url


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
        order_id_string = order.channel_order_id
        if order.client_prefix!='DHANIPHARMACY':
            order_id_barcode = code128.Code128(order_id_string, barHeight=0.6 * inch, barWidth=0.3 * mm)
            order_id_barcode.drawOn(c, (offset+0.2) * inch, -0.6 * inch)
        else:
            c.drawImage("Dhanipharmacy.png", (offset-0.3) * inch, -0.85 * inch, width=250, height=75, mask='auto')
        c.drawString((offset+0.2) * inch, -0.75 * inch, order_id_string)
        if order.orders_invoice:
            qr_url = order.orders_invoice[-1].qr_url
            qr_code = qr.QrCodeWidget(qr_url)
            bounds = qr_code.getBounds()
            width = bounds[2] - bounds[0]
            height = bounds[3] - bounds[1]
            d = Drawing(60, 60, transform=[60. / width, 0, 0, 60. / height, 0, 0])
            d.add(qr_code)
            d.drawOn(c, (offset-0.8) * inch, -0.80*inch)
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
        order_id_string = order.channel_order_id
        c.drawString(1.75 * inch, 1.25 * inch, order_id_string)
        if order.orders_invoice:
            qr_url = order.orders_invoice[-1].qr_url
            qr_code = qr.QrCodeWidget(qr_url)
            bounds = qr_code.getBounds()
            width = bounds[2] - bounds[0]
            height = bounds[3] - bounds[1]
            d = Drawing(60, 60, transform=[60. / width, 0, 0, 60. / height, 0, 0])
            d.add(qr_code)
            d.drawOn(c, 1.8 * inch, 1.4 * inch)
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