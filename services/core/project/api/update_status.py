import psycopg2, requests, os
from datetime import datetime
from .queries import *
"""
host = os.environ('DTATBASE_HOST')
database = os.environ('DTATBASE_NAME')
user = os.environ('DTATBASE_USER')
password = os.environ('DTATBASE_PASSWORD')
conn = psycopg2.connect(host=host, database=database, user=user, password=password)
"""
conn = psycopg2.connect(host="wareiq-core-prod.cvqssxsqruyc.us-east-1.rds.amazonaws.com", database="core_prod", user="postgres", password="postgres")
conn_2 = psycopg2.connect(host="wareiq-core-prod.cvqssxsqruyc.us-east-1.rds.amazonaws.com", database="users_prod", user="postgres", password="postgres")

def lambda_handler():
    cur = conn.cursor()
    cur_2 =conn_2.cursor()
    cur.execute(get_courier_id_and_key_query)
    for courier in cur.fetchall():
        cur.execute(get_status_update_orders_query%str(courier[0]))
        all_orders = cur.fetchall()
        exotel_idx = 0
        exotel_sms_data = {
            'From': 'LM-WAREIQ'
        }
        for order in all_orders:
            try:
                if not order[1]:
                    continue

                check_status_url = "https://track.delhivery.com/api/status/packages/json/?waybill=%s&token=%s"%(order[1], courier[2])
                req = requests.get(check_status_url).json()

                new_status = req['ShipmentData'][0]['Shipment']['Status']['Status']

                if new_status=="Manifested":
                    continue

                if order[2] in ('READY TO SHIP', 'PICKUP REQUESTED') and new_status=='IN TRANSIT':
                    edd = req['ShipmentData'][0]['Shipment']['expectedDate']
                    edd = datetime.strptime(edd, '%Y-%m-%dT%H:%M:%S')
                    edd = edd.strftime('%-d %b')
                    cur_2.execute("select client_name from clients where client_prefix=%s"%order[3])
                    client_name = cur_2.fetchone()
                    customer_phone = order[4].replace(" ","")
                    customer_phone = "0"+customer_phone[-10:]

                    sms_to_key = "Messages[%s][To]" % str(exotel_idx)
                    sms_body_key = "Messages[%s][Body]" % str(exotel_idx)

                    exotel_sms_data[sms_to_key] = customer_phone
                    exotel_sms_data[sms_body_key] = "Dear Customer, your %s order has been shipped via Delhivery with AWB number %d. It is expected to arrive by %s. Thank you for Ordering.." % (
                    client_name, order[1], edd)
                    exotel_idx += 1

                new_status = new_status.upper()

                status_update_tuple = (new_status, order[0])
                cur.execute(order_status_update_query, status_update_tuple)

            except Exception as e:
                print("status update failed for " + str(order[0]) + "err:" + str(e.args[0]))

        if exotel_idx:
            lad = requests.post(
                'https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend',
                data=exotel_sms_data)

        conn.commit()
    cur.close()
