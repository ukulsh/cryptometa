from math import ceil
from datetime import datetime, timedelta
from app.order_price_reconciliation.query import *
from app.db_utils import DbConnection
import csv
import io
import logging

conn = DbConnection.get_db_connection_instance()
recon_status = 'reconciliation'
logger = logging.getLogger()
logger.setLevel(logging.INFO)

zone_column_mapping = {
    'A': 'zone_a',
    'B': 'zone_b',
    'C': 'zone_c',
    'D': 'zone_d',
    'E': 'zone_e',
}

zone_step_charge_column_mapping = {
    'A': 'a_step',
    'B': 'b_step',
    'C': 'c_step',
    'D': 'd_step',
    'E': 'e_step'
}


def calculate_new_charge(current_data, charged_weight, source_courier_id, total_charged_data, cur):
    delivery_zone = current_data[1]
    try:
        cost_select_tuple = (current_data[16], source_courier_id)
        cur.execute(
            "SELECT __ZONE__, cod_min, cod_ratio, rto_ratio, __ZONE_STEP__, rvp_ratio from cost_to_clients WHERE client_prefix=%s and courier_id=%s;".replace(
                '__ZONE__', zone_column_mapping[delivery_zone]).replace('__ZONE_STEP__',
                                                                        zone_step_charge_column_mapping[
                                                                            delivery_zone]), cost_select_tuple)
        charge_rate_values = cur.fetchone()
        if not charge_rate_values:
            cur.execute(
                "SELECT __ZONE__, cod_min, cod_ratio, rto_ratio, __ZONE_STEP__, rvp_ratio from cost_to_clients WHERE client_prefix=%s and courier_id=%s;".replace(
                    '__ZONE__', zone_column_mapping[delivery_zone]).replace('__ZONE_STEP__',
                                                                            zone_step_charge_column_mapping[
                                                                                delivery_zone]),
                (current_data[16], 16))  # 16 is rate for all
            charge_rate_values = cur.fetchone()

        if not charge_rate_values:
            cur.execute(
                "SELECT __ZONE__, cod_min, cod_ratio, rto_ratio, __ZONE_STEP__, rvp_ratio from client_default_cost WHERE courier_id=%s;".replace(
                    '__ZONE__', zone_column_mapping[delivery_zone]).replace('__ZONE_STEP__',
                                                                            zone_step_charge_column_mapping[
                                                                                delivery_zone]),
                (source_courier_id,))
            charge_rate_values = cur.fetchone()

        if not charge_rate_values:
            return

        cur.execute("select weight_offset, additional_weight_offset from master_couriers where id=%s;",
                    (source_courier_id,))
        courier_data = cur.fetchone()
        charge_rate = charge_rate_values[0]
        forward_charge = charge_rate
        per_step_charge = charge_rate_values[4] if charge_rate_values and len(charge_rate_values) >= 5 else 0.0
        per_step_charge = 0.0 if per_step_charge is None else per_step_charge
        if courier_data[0] != 0 and courier_data[1] != 0:
            if not per_step_charge:
                per_step_charge = charge_rate
            if charged_weight > courier_data[0]:
                forward_charge = charge_rate + ceil(
                    (charged_weight - courier_data[0] * 1.0) / courier_data[1]) * per_step_charge
        else:
            multiple = ceil(charged_weight / 0.5)
            forward_charge = charge_rate * multiple
        forward_charge_gst = forward_charge * 1.18
        rto_charge = 0
        rto_charge_gst = 0
        cod_charge = 0
        cod_charged_gst = 0
        if current_data[17] == 'RTO':
            rto_charge = forward_charge * charge_rate_values[3]
            rto_charge_gst = forward_charge_gst * charge_rate_values[3]
        elif current_data[17] == 'DTO':
            rto_charge = forward_charge * charge_rate_values[5]
            rto_charge_gst = forward_charge_gst * charge_rate_values[5]
        else:
            if current_data[18] and current_data[18].lower() == 'cod':
                if current_data[19]:
                    cod_charge = current_data[19] * (charge_rate_values[2] / 100)
                    if charge_rate_values[1] > cod_charge:
                        cod_charge = charge_rate_values[1]
                else:
                    cod_charge = charge_rate_values[1]

                cod_charged_gst = cod_charge * 1.18
        deduction_time = datetime.now()
        if current_data[17] == "DTO":
            forward_charge = 0
            forward_charge_gst = 0
        cod_charge = max(cod_charge-total_charged_data['cod_charge'], 0)
        cod_charged_gst = max(cod_charged_gst-total_charged_data['cod_charged_gst'], 0)
        forward_charge = max(forward_charge-total_charged_data['forward_charge'], 0)
        forward_charge_gst = max(forward_charge_gst-total_charged_data['forward_charge_gst'], 0)
        rto_charge = max(rto_charge-total_charged_data['rto_charge'], 0)
        rto_charge_gst = max(rto_charge_gst-total_charged_data['rto_charge_gst'], 0)
        total_charge = forward_charge + cod_charge + rto_charge
        total_charge_gst = forward_charge_gst + rto_charge_gst + cod_charged_gst
        if total_charge:
            cur.execute(get_client_balance, (current_data[16],))
            client_data = cur.fetchone()
            if client_data[1] and client_data[1].lower() == 'prepaid':
                current_balance = client_data[0]
                current_balance -= total_charge_gst
                cur.execute(update_client_balance, (current_balance, current_data[16],))
            insert_rates_tuple = (charged_weight, delivery_zone, deduction_time, cod_charge, cod_charged_gst,
                                  forward_charge, forward_charge_gst, rto_charge, rto_charge_gst, current_data[8],
                                  total_charge, total_charge_gst, datetime.now(), datetime.now(), recon_status,)
            cur.execute(insert_into_deduction_query, insert_rates_tuple)
            conn.commit()
    except Exception as e:
        logger.error("couldn't calculate courier cost order: " + str(current_data[8]) + "\nError: " + str(e))


def process_order_price_reconciliation(file_ref):
    stream = io.StringIO(file_ref.stream.read().decode("UTF8"), newline=None)
    reader = csv.DictReader(stream)
    order_data = {}
    cur = conn.cursor()
    for row in reader:
        awb = row['awb']
        charged_weight = float(row['charged_weight'])
        courier_id = row['courier_id']
        order_data[awb] = [charged_weight, courier_id]
    awb_values = ",".join(map(repr, order_data.keys()))
    modified_query = get_client_deduction_row.replace('__AWB_VALUES__', awb_values)
    cur.execute(modified_query)
    all_deduction_data = cur.fetchall()

    group_by_awb = {}
    previous_charge_data = {}

    for iterator in all_deduction_data:
        if iterator[12] in group_by_awb:
            if group_by_awb[iterator[12]][20] < iterator[20]:
                group_by_awb[iterator[12]] = iterator
            previous_charge_data[iterator[12]]['cod_charge'] += iterator[2]
            previous_charge_data[iterator[12]]['cod_charged_gst'] += iterator[3]
            previous_charge_data[iterator[12]]['forward_charge'] += iterator[4]
            previous_charge_data[iterator[12]]['forward_charge_gst'] += iterator[5]
            previous_charge_data[iterator[12]]['rto_charge'] += iterator[6]
            previous_charge_data[iterator[12]]['rto_charge_gst'] += iterator[7]
        else:
            group_by_awb[iterator[12]] = iterator
            previous_charge_data[iterator[12]] = {'cod_charge': iterator[2], 'cod_charged_gst': iterator[3],
                                                  'forward_charge': iterator[4], 'forward_charge_gst': iterator[5],
                                                  'rto_charge': iterator[6], 'rto_charge_gst': iterator[7],
                                                  }

    for _, iterator in group_by_awb.items():
        if order_data[iterator[12]][0] > iterator[0]:
            calculate_new_charge(iterator, order_data[iterator[12]][0], order_data[iterator[12]][1], previous_charge_data[iterator[12]], cur)
    cur.close()