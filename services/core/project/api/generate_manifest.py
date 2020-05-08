
import boto3, os, string, random
from datetime import datetime, timedelta
from reportlab.lib.units import inch, mm
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.graphics.barcode import code128

session = boto3.Session(
    aws_access_key_id='AKIAWRT2R3KC3YZUBFXY',
    aws_secret_access_key='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',
)


def fill_manifest_data(orders, courier, store, warehouse):
    file_name = "/tmp/MANIFEST_" + warehouse+"_"+str(datetime.now().strftime("%d_%b_%Y_"))+''.join(random.choices(string.ascii_uppercase, k=8))+".pdf"
    c = canvas.Canvas(file_name, pagesize=A4)
    pickup_date = datetime.now()
    pickup_date = pickup_date.strftime('%d-%m-%Y')
    order_count = len(orders)
    if order_count:
        create_manifests_blank_page(c, courier, pickup_date, store, order_count)

    offset = 0.0
    for idx, order in enumerate(orders):
        try:
            fill_row_data(c, order, offset)
        except Exception:
            pass
        offset += 1.02
        if idx%10==9 and idx != (len(orders)-1):
            offset = 0.0
            c.showPage()
            create_manifests_blank_page(c, courier, pickup_date, store, order_count)

    c.setFillColorRGB(1, 1, 1)
    c.rect(-1.05 * inch, -1.05 * inch, 10 * inch, (10.55-offset) * inch, fill=1)

    c.save()
    s3 = session.resource('s3')
    bucket = s3.Bucket("wareiqmanifests")
    bucket.upload_file(file_name, file_name, ExtraArgs={'ACL': 'public-read'})
    manifest_url = "https://wareiqmanifests.s3.us-east-2.amazonaws.com/" + file_name
    os.remove(file_name)
    return manifest_url


def fill_row_data(c, order, offset):
    c.drawString(-0.70 * inch, (9.30 - offset) * inch, order[0])

    try:
        payment_string = order[9]
        if order[9].lower() == "cod":
            payment_string += " (Rs. " + str(order[10]) + ")"
        payment_string = split_string(payment_string, 12)
        y_axis = 9.30
        for pyst in payment_string:
            c.drawString(0 * inch, (y_axis - offset) * inch, pyst)
            y_axis -= 0.15
    except Exception:
        pass

    c.drawString(1 * inch, (8.60 - offset) * inch, str(order[16]))
    c.drawString(1 * inch, (8.75 - offset) * inch, str(order[17]))
    c.drawString(1 * inch, (8.90 - offset) * inch, str(order[15]))

    customer_name = order[11]
    if order[12]:
        customer_name += " " + order[12]

    c.drawString(1 * inch, (9.30 - offset) * inch, customer_name)
    c.drawString(1 * inch, (9.15 - offset) * inch, str(order[19]))

    c.setFont('Helvetica', 8)

    try:
        contents_string = ""

        for iddx, prod in enumerate(order[7]):
            contents_string += prod + " (" + str(order[8][iddx]) + ") "

        contents_string = split_string(contents_string, 28)
        contents_string = contents_string[:7]

        y_axis = 9.30
        for cont_str in contents_string:
            c.drawString(2.60 * inch, (y_axis - offset) * inch, cont_str)
            y_axis -= 0.12

    except Exception:
        pass

    c.setFont('Helvetica', 10)

    c.drawString(4.20 * inch, (9.30 - offset) * inch, str(order[3]) + " kg")

    awb_string = order[20]
    awb_barcode = code128.Code128(awb_string, barHeight=0.4 * inch, barWidth=0.35 * mm)
    awb_barcode.drawOn(c, 4.70 * inch, (8.85 - offset) * inch)

    c.drawString(5.30 * inch, (8.70 - offset) * inch, awb_string)


def create_manifests_blank_page(canvas, courier, date, store, shipments):
    canvas.setLineWidth(.8)
    canvas.translate(inch, inch)
    canvas.rect(-0.75 * inch, -0.70 * inch, 7.77 * inch, 10.59 * inch, fill=0)
    y_axis = 9.50
    for i in range(10):
        canvas.line(-0.75 * inch, y_axis * inch, 7.02 * inch, y_axis * inch)
        y_axis -= 1.02

    for i in (-0.05, 0.95, 2.55, 4.15, 4.65):
        canvas.line(i * inch, -0.70 * inch, i * inch, 9.89 * inch)

    canvas.setFont('Helvetica-Bold', 12)
    canvas.drawString(-0.70 * inch, 9.65 * inch, "Order #")
    canvas.drawString(0 * inch, 9.65 * inch, "Payment")
    canvas.drawString(1 * inch, 9.65 * inch, "Customer Details")
    canvas.drawString(2.60 * inch, 9.65 * inch, "Contents")
    canvas.drawString(4.20 * inch, 9.65 * inch, "Wt")
    canvas.drawString(4.70 * inch, 9.65 * inch, "AWB #")

    canvas.setFont('Helvetica', 10)
    canvas.drawString(-0.75 * inch, 10.05 * inch, "Courier: "+str(courier))
    canvas.drawString(5.55 * inch, 10.05 * inch, "No. of Shipments: "+str(shipments))
    canvas.drawString(2 * inch, 10.05 * inch, "Date: "+date)
    canvas.drawString(3.30 * inch, 10.05 * inch, "Store: "+store)


def split_string(str, limit, sep=" "):
    words = str.split()
    if max(map(len, words)) > limit:
        raise ValueError("limit is too small")
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