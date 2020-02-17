import psycopg2, requests, os, json
from datetime import datetime, timedelta

from .queries import *
from .generate_manifest import fill_manifest_data
"""
host = os.environ('DTATBASE_HOST')
database = os.environ('DTATBASE_NAME')
user = os.environ('DTATBASE_USER')
password = os.environ('DTATBASE_PASSWORD')
conn = psycopg2.connect(host=host, database=database, user=user, password=password)
"""
conn = psycopg2.connect(host="wareiq-core-prod2.cvqssxsqruyc.us-east-1.rds.amazonaws.com", database="core_prod", user="postgres", password="aSderRFgd23")

def lambda_handler():
    cur = conn.cursor()
    cur.execute(get_pickup_requests_query)
    all_pickups = cur.fetchall()
    for pick_req in all_pickups:
        pickup_request_dict = dict()

        time_now = datetime.utcnow()
        time_before = time_now + timedelta(hours=5.5) - timedelta(hours=pick_req[5] if pick_req[5] else 0)

        time_before = time_before.strftime("%Y-%m-%d %H:%M")

        get_orders_data_tuple = (pick_req[0], time_before)
        cur.execute(get_request_pickup_orders_data_query, get_orders_data_tuple)
        all_orders = cur.fetchall()
        if not all_orders:
            continue

        order = None
        for order in all_orders:
            if order[4] not in pickup_request_dict:
                pickup_request_dict[order[4]] = {"orders": [order], "api_key": order[5], "api_url":order[6], "courier_id":order[22]}
            else:
                pickup_request_dict[order[4]]['orders'].append(order)

            cur.execute(update_order_status_query%str(order[21]))

        if order:
            last_picked_update_id = order[21]
        else:
            continue


        for courier, values in pickup_request_dict.items():
            manifest_url = fill_manifest_data(values['orders'], courier, pick_req[2], pick_req[2])
            current_time = datetime.now()
            pickup_date = datetime.today()
            manifest_id = current_time.strftime('%Y_%m_%d_%H_%M_%S_') + pick_req[1]
            pickup_date_string = pickup_date.strftime("%Y-%m-%d ")+ "14:00:00"
            manifest_data_tuple = (manifest_id, pick_req[2], values['courier_id'], pick_req[3], len(values['orders']),
                                   pickup_date_string, manifest_url, current_time)
            cur.execute(insert_manifest_data_query, manifest_data_tuple)

            if courier in ("Delhivery", "Delhivery Surface Standard", "Delhivery Bulk", "Delhivery Heavy", "Delhivery Heavy 2"):
                pickup_request_api_body = json.dumps({ "pickup_time": "14:00:00",
                                            "pickup_date": pickup_date.strftime("%Y-%m-%d"),
                                            "pickup_location": pick_req[2],
                                            "expected_package_count": len(values['orders'])})

                pickup_request_api_url = "https://track.delhivery.com/fm/request/new/"

                headers = {"Authorization": "Token " + values['api_key'],
                           "Content-Type": "application/json"}

                req = requests.post(pickup_request_api_url, headers=headers, data=pickup_request_api_body)
        if last_picked_update_id:
            update_pickup_requests_tuple = (last_picked_update_id, datetime.now(), pick_req[1])
            cur.execute(update_pickup_requests_query, update_pickup_requests_tuple)

        conn.commit()

    cur.close()
