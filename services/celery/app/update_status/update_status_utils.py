import logging, boto3, requests, json
from datetime import datetime
from time import sleep
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from .order_shipped import order_shipped

logger = logging.getLogger()
logger.setLevel(logging.INFO)

RAVEN_URL = "https://api.ravenapp.dev/v1/apps/ccaaf889-232e-49df-aeb8-869e3153509d/events/send"
RAVEN_HEADERS = {"Content-Type": "application/json", "Authorization": "AuthKey K4noY3GgzaW8OEedfZWAOyg+AmKZTsqO/h/8Y4LVtFA="}

email_client = boto3.client('ses', region_name="us-east-1", aws_access_key_id='AKIAWRT2R3KC3YZUBFXY',
    aws_secret_access_key='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')


def send_shipped_event(mobile, email, order, edd, courier_name, tracking_link=None):
    background_color = str(order[24]) if order[24] else "#B5D0EC"
    client_logo = str(order[21]) if order[21] else "https://logourls.s3.amazonaws.com/client_logos/logo_ane.png"
    client_name = str(order[20]) if order[20] else "WareIQ"
    email_title = str(order[22]) if order[22] else "Your order has been shipped!"
    order_id = str(order[12]) if order[12] else ""
    customer_name = str(order[18]) if order[18] else "Customer"

    edd = edd if edd else ""
    awb_number = str(order[1]) if order[1] else ""
    if not tracking_link:
        tracking_link = "http://webapp.wareiq.com/tracking/" + str(order[1])

    payload = {
        "event": "shipped",
        "user": {
            "mobile": mobile,
            "email": email if email else ""
        },
        "data": {
            "client_name": client_name,
            "customer_name": customer_name,
            "courier_name": courier_name,
            "tracking_link": tracking_link,
            "email_title": email_title,
            "order_id": order_id,
            "edd": edd,
            "awb_number": awb_number,
            "background_color": background_color,
            "client_logo": client_logo
        },
        "override": {
            "email": {
                "from": {
                    "name": client_name
                }
            }
        }
    }

    req = requests.post(RAVEN_URL, headers=RAVEN_HEADERS, data=json.dumps(payload))


def send_delivered_event(mobile, order, courier_name, tracking_link=None):
    client_name = str(order[20]) if order[20] else "WareIQ"
    if not tracking_link:
        tracking_link = "http://webapp.wareiq.com/tracking/" + str(order[1])
    payload = {
        "event": "delivered",
        "user": {
            "mobile": mobile,
        },
        "data": {
            "client_name": client_name,
            "courier_name": courier_name,
            "tracking_link": tracking_link
        }
    }

    req = requests.post(RAVEN_URL, headers=RAVEN_HEADERS, data=json.dumps(payload))


def send_ndr_event(mobile, order, verification_link):
    client_name = str(order[20]) if order[20] else "WareIQ"
    payload = {
        "event": "ndr_verification",
        "user": {
            "mobile": mobile,
        },
        "data": {
            "client_name": client_name,
            "verification_link": verification_link,
        }
    }

    req = requests.post(RAVEN_URL, headers=RAVEN_HEADERS, data=json.dumps(payload))


def send_bulk_emails(emails):
    logger.info("Sending Emails....count: " + str(len(emails)) + "  Time: " + str(datetime.utcnow()))
    for email in emails:
        try:
            response = email_client.send_raw_email(
                Source=email[0]['From'],
                Destinations=email[1],
                RawMessage={
                    'Data': email[0].as_string(),
                },
            )
            sleep(0.08)
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
        elif order[23] in (4,):
            courier_name = "Shadowfax"
        elif order[23] in (9,):
            courier_name = "Bluedart"

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


def webhook_updates(order, cur, status, status_text, location, status_time, ndr_id=None):
    if order[38]:
        try:
            if ndr_id:
                cur.execute("SELECT reason FROM ndr_reasons WHERE id=%s"%str(ndr_id))
                status_text = cur.fetchone()[0]
            cur.execute("SELECT webhook_url, header_key, header_value, webhook_secret, id FROM webhooks WHERE status='active' and client_prefix='%s'"%order[3])
            all_webhooks = cur.fetchall()
            for webhook in all_webhooks:
                try:
                    req_body = {"awb": order[1],
                                "status": status,
                                "event_time": status_time,
                                "location": location,
                                "order_id": order[12],
                                "status_text": status_text}

                    headers = {"Content-Type": "application/json"}
                    if webhook[1] and webhook[2]:
                        headers[webhook[1]] = webhook[2]

                    req = requests.post(webhook[0], headers=headers, json=req_body, timeout=5)
                    if not str(req.status_code).startswith('2'):
                        cur.execute("UPDATE webhooks SET fail_count=fail_count+1 WHERE id=%s" % str(webhook[4]))
                except Exception:
                    cur.execute("UPDATE webhooks SET fail_count=fail_count+1 WHERE id=%s"%str(webhook[4]))
                    pass
        except Exception:
            pass
