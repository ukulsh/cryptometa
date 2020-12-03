get_order_details_query = """select aa.id, bb.awb, aa.status, aa.client_prefix, aa.customer_phone, 
                                    aa.order_id_channel_unique, bb.channel_fulfillment_id, cc.api_key, 
                                    cc.api_password, cc.shop_url, bb.id, aa.pickup_data_id, aa.channel_order_id, ee.payment_mode, 
                                    cc.channel_id, gg.location_id, mm.item_list, mm.sku_quan_list, aa.customer_name, aa.customer_email, 
                                    nn.client_name, nn.client_logo, nn.custom_email_subject, bb.courier_id, nn.theme_color, cc.unique_parameter,
                                    cc.mark_shipped, cc.shipped_status, cc.mark_invoiced, cc.invoiced_status, cc.mark_delivered, 
                                    cc.delivered_status, cc.mark_returned, cc.returned_status, cc.id, ee.amount, oo.warehouse_prefix, 
                                    nn.client_name, bb.courier_id
                                    from orders aa
                                    left join shipments bb
                                    on aa.id=bb.order_id
                                    left join (select order_id, array_agg(channel_item_id) as item_list, array_agg(quantity) as sku_quan_list from
                                      		  (select kk.order_id, kk.channel_item_id, kk.quantity
                                              from op_association kk
                                              left join products ll on kk.product_id=ll.id) nn
                                              group by order_id) mm
                                    on aa.id=mm.order_id
                                    left join client_channel cc
                                    on aa.client_channel_id=cc.id
                                    left join client_pickups dd
                                    on aa.pickup_data_id=dd.id
                                    left join pickup_points oo
                                    on dd.pickup_id=oo.id
                                    left join orders_payments ee
                                    on aa.id=ee.order_id
                                    left join client_channel_locations gg
                                    on aa.client_channel_id=gg.client_channel_id
                                    and aa.pickup_data_id=gg.pickup_data_id
                                    left join client_mapping nn
                                    on aa.client_prefix=nn.client_prefix   
                                    where bb.awb = '%s';"""

insert_scan_query = """INSERT INTO order_scans (order_id, courier_id, shipment_id, status_code, status, status_text, 
                    location, location_city, status_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (order_id, courier_id, shipment_id, status, status_time) 
                    DO NOTHING;"""

insert_status_query = """INSERT INTO order_status (order_id, courier_id, shipment_id, status_code, status, status_text, 
                      location, location_city, status_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) 
                      ON CONFLICT (order_id, courier_id, shipment_id, status) 
                      DO UPDATE SET status_time = EXCLUDED.status_time, location_city=EXCLUDED.location_city, 
                      location=EXCLUDED.location, status_text=EXCLUDED.status_text,status_code=EXCLUDED.status_code;"""


select_orders_to_calculate_query = """select aa.id, aa.awb, aa.courier_id, aa.volumetric_weight, aa.weight, 
                                        bb.channel_order_id, bb.client_prefix, cc.pincode as pickup_pincode,
                                        dd.pincode as delivery_pincode, ff.status_time, bb.order_date, gg.payment_mode, 
                                        gg.amount, bb.status, aa.dimensions, aa.zone from shipments aa
                                        left join orders bb on aa.order_id=bb.id
                                        left join pickup_points cc on aa.pickup_id=cc.id
                                        left join shipping_address dd on bb.delivery_address_id=dd.id
                                        left join client_deductions ee on ee.shipment_id=aa.id
                                        left join (select * from order_status where status in ('Delivered', 'RTO', 'DTO')) ff
                                        on aa.id = ff.shipment_id
                                        left join orders_payments gg on bb.id=gg.order_id
                                        where bb.status in ('DELIVERED', 'RTO', 'DTO')
                                        and ee.shipment_id is null
                                        and (ff.status_time>'__STATUS_TIME__')"""

insert_into_deduction_query = """INSERT INTO client_deductions (weight_charged,zone,deduction_time,cod_charge,
                                cod_charged_gst,forward_charge,forward_charge_gst,rto_charge,
                                rto_charge_gst,shipment_id,total_charge,total_charged_gst,date_created,date_updated) VALUES (%s,%s,%s,%s,%s,
                                %s,%s,%s,%s,%s,%s,%s,%s,%s);"""

insert_into_courier_cost_query = """INSERT INTO courier_charges (weight_charged,zone,deduction_time,cod_charge,
                                forward_charge,rto_charge,shipment_id,total_charge,date_created,date_updated) VALUES 
                                (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""

update_client_balance = """UPDATE client_mapping SET current_balance=coalesce(current_balance, 0)-%s WHERE client_prefix=%s AND account_type ilike 'prepaid';"""

select_remittance_amount_query = """select * from
                                        (select xx.unique_id, xx.client_prefix, xx.remittance_id, xx.date as remittance_date, 
                                         xx.status, xx.transaction_id, sum(yy.amount) as remittance_total from
                                        (select id as unique_id, client_prefix, remittance_id, transaction_id, DATE(remittance_date), 
                                        ((DATE(remittance_date)) - INTERVAL '8 DAY') AS order_start,
                                        ((DATE(remittance_date)) - INTERVAL '1 DAY') AS order_end,
                                        status from cod_remittance) xx 
                                        left join 
                                        (select client_prefix, channel_order_id, order_date, payment_mode, amount, cc.status_time as delivered_date from orders aa
                                        left join orders_payments bb on aa.id=bb.order_id
                                        left join (select * from order_status where status='Delivered') cc
                                        on aa.id=cc.order_id
                                        where aa.status = 'DELIVERED'
                                        and bb.payment_mode ilike 'cod') yy
                                        on xx.client_prefix=yy.client_prefix 
                                        and yy.delivered_date BETWEEN xx.order_start AND xx.order_end
                                        group by xx.unique_id, xx.client_prefix, xx.remittance_id, xx.date, xx.status, xx.transaction_id) zz
                                        WHERE remittance_total is not null
                                        and remittance_date='__REMITTANCE_DATE__'
                                        and status='processing'
                                        order by remittance_date DESC, remittance_total DESC"""

fetch_inventory_quantity_query = """select yy.*, zz.combo_prods, zz.combo_prods_quan from
                                    (select product_id, status, warehouse_prefix, sum(quantity) from 
                                    (select * from op_association aa
                                    left join orders bb on aa.order_id=bb.id
                                    left join client_pickups cc on bb.pickup_data_id=cc.id
                                    left join pickup_points dd on cc.pickup_id=dd.id     
                                    where status not in ('CANCELED', 'NOT PICKED', 'NOT SHIPPED', 'NEW - FAILED', 'NEW - SHIPPED')) xx
                                    group by product_id, status, warehouse_prefix
                                    order by product_id, status, warehouse_prefix) yy
                                    left join (select combo_id, array_agg(combo_prod_id) as combo_prods, 
                                    array_agg(quantity) as combo_prods_quan from products_combos group by combo_id) zz
                                    on yy.product_id=zz.combo_id"""

update_inventory_quantity_query = """UPDATE products_quantity SET available_quantity=COALESCE(approved_quantity, 0)+%s,
									current_quantity=COALESCE(approved_quantity, 0)+%s, inline_quantity=%s, rto_quantity=%s
                                    WHERE product_id=%s and warehouse_prefix=%s;"""