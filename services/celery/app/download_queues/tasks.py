from datetime import datetime, timedelta
import json, re, random, string, csv, boto3, os
from app.db_utils import DbConnection

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
                            not_shipped = None
                            if not product_data[4][idx]:
                                not_shipped = "Weight/dimensions not entered for product(s)"
                            elif order[12] == "Pincode not serviceable":
                                not_shipped = "Pincode not serviceable"
                            elif not order[26]:
                                not_shipped = "Pickup point not assigned"
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