import smtplib, logging
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from .order_shipped import order_shipped

logger = logging.getLogger()
logger.setLevel(logging.INFO)

email_server = smtplib.SMTP_SSL('smtpout.secureserver.net', 465)
email_server.login("noreply@wareiq.com", "Berlin@123")


def send_bulk_emails(emails):
    logger.info("Sending Emails....count: " + str(len(emails)) + "  Time: " + str(datetime.utcnow()))
    for email in emails:
        try:
            email_server.sendmail(email[0]['From'], email[1], email[0].as_string())
        except Exception as e:
            logger.error("Couldn't send email: " + str(email['TO'])+"\nError: "+str(e.args[0]))


def create_email(order, edd, email):
    try:
        background_color = str(order[24]) if order[24] else "#B5D0EC"
        client_logo = str(order[21]) if order[21] else "https://logourls.s3.amazonaws.com/client_logos/logo_ane.png"
        client_name = str(order[20]) if order[20] else "WareIQ"
        email_title = str(order[22]) if order[22] else "Your order has been shipped!"
        order_id = str(order[12]) if order[12] else ""
        customer_name = str(order[18]) if order[18] else "Customer"
        courier_name = "WareIQ"
        if order[23] in (1,2,8,11,12):
            courier_name = "Delhivery"
        elif order[23] in (5,13):
            courier_name = "Xpressbees"
        elif order[23] in (4):
            courier_name = "Shadowfax"

        edd = edd if edd else ""
        awb_number = str(order[1]) if order[1] else ""
        tracking_link = "http://webapp.wareiq.com/tracking/" + str(order[1])

        html = order_shipped.replace('__CLIENT_LOGO__', client_logo)\
            .replace('__CLIENT_NAME__',  client_name)\
            .replace('__BACKGROUND_COLOR__', background_color)\
            .replace('__EMAIL_TITLE__', email_title)\
            .replace('__CUSTOMER_NAME__', customer_name)\
            .replace('__ORDER_ID__', order_id)\
            .replace('__COURIER_NAME__', courier_name)\
            .replace('__EDD__', edd)\
            .replace('__AWB_NUMBER__', awb_number).replace('__TRACKING_LINK__', tracking_link)

        # create message object instance
        msg = MIMEMultipart('alternative')

        recipients = [email]
        msg['From'] = "%s <noreply@wareiq.com>"%client_name
        msg['To'] = ", ".join(recipients)
        msg['Subject'] = email_title

        # write the HTML part

        part2 = MIMEText(html, "html")
        msg.attach(part2)
        return msg
    except Exception as e:
        logger.error("Couldn't send email: " + str(order[1]) + "\nError: " + str(e.args))
        return None