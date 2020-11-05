
get_client_deduction_row = """select x.weight_charged, x.zone, x.cod_charge, x.cod_charged_gst, x.forward_charge, 
x.forward_charge_gst, x.rto_charge, x.rto_charge_gst, x.shipment_id, x.total_charge, x.total_charged_gst, x.type, y.awb,
 y.zone, cc.pincode as pickup_pincode, dd.pincode as delivery_pincode, bb.client_prefix, bb.status, gg.payment_mode, gg.amount,
 x.date_created, y.courier_id from client_deductions as x inner join shipments as y on y.id=x.shipment_id and y.awb in (__AWB_VALUES__) inner join orders bb 
 on y.order_id=bb.id inner join pickup_points cc on y.pickup_id=cc.id inner join shipping_address dd on 
 bb.delivery_address_id=dd.id inner join orders_payments gg on bb.id=gg.order_id where bb.status in ('DELIVERED', 'RTO', 'DTO')"""


insert_into_deduction_query = """INSERT INTO client_deductions (weight_charged,zone,deduction_time,cod_charge,
                                cod_charged_gst,forward_charge,forward_charge_gst,rto_charge,
                                rto_charge_gst,shipment_id,total_charge,total_charged_gst,date_created,date_updated,type) VALUES (%s,%s,%s,%s,%s,
                                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""

update_client_balance = """UPDATE client_mapping SET current_balance=coalesce(current_balance, 0)-%s WHERE client_prefix=%s AND account_type ilike 'prepaid';"""