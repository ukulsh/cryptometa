get_order_details_query = """select aa.id, bb.awb, aa.status, aa.client_prefix, aa.customer_phone, 
                                    aa.order_id_channel_unique, bb.channel_fulfillment_id, cc.api_key, 
                                    cc.api_password, cc.shop_url, bb.id, aa.pickup_data_id, aa.channel_order_id, ee.payment_mode, 
                                    cc.channel_id, gg.location_id, mm.item_list, mm.sku_quan_list, aa.customer_name, aa.customer_email, 
                                    nn.client_name, nn.client_logo, nn.custom_email_subject, bb.courier_id, nn.theme_color, cc.unique_parameter,
                                    cc.mark_shipped, cc.shipped_status, cc.mark_invoiced, cc.invoiced_status, cc.mark_delivered, 
                                    cc.delivered_status, cc.mark_returned, cc.returned_status, cc.id, ee.amount, oo.warehouse_prefix, nn.verify_ndr, pp.webhook_id,
                                    nn.client_name, bb.courier_id, oo.city, oo.warehouse_prefix, clc.subdomain
                                    from orders aa
                                    left join shipments bb
                                    on aa.id=bb.order_id
                                    left join (select order_id, array_agg(channel_item_id) as item_list, array_agg(quantity) as sku_quan_list from
                                      		  (select kk.order_id, kk.channel_item_id, kk.quantity
                                              from op_association kk
                                              left join master_products ll on kk.master_product_id=ll.id) nn
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
                                    left join (select client_prefix, max(id) as webhook_id from webhooks where status='active' group by client_prefix) pp
                                    on pp.client_prefix=aa.client_prefix
                                    left join client_customization clc
 									on aa.client_prefix=clc.client_prefix
                                    where __FILTER_ORDER__;"""

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

update_client_balance = """UPDATE client_mapping SET current_balance=coalesce(current_balance, 0)-%s WHERE client_prefix=%s AND account_type ilike 'prepaid' RETURNING current_balance;"""

select_remittance_amount_query = """select unique_id, client_prefix, remittance_id, remittance_date, status, transaction_id, remittance_total from
                                        (select xx.unique_id, xx.client_prefix, xx.remittance_id, xx.date as remittance_date, 
                                         xx.status, xx.transaction_id, xx.payout_id, sum(yy.amount) as remittance_total from
                                        (select id as unique_id, client_prefix, remittance_id, transaction_id, DATE(remittance_date), 
                                        DATE(del_from) AS order_start,
                                        DATE(del_to) AS order_end,
                                        status, payout_id from cod_remittance) xx 
                                        left join 
                                        (select client_prefix, channel_order_id, order_date, payment_mode, amount, cc.status_time as delivered_date from orders aa
                                        left join orders_payments bb on aa.id=bb.order_id
                                        left join (select * from order_status where status='Delivered') cc
                                        on aa.id=cc.order_id
                                        left join shipments dd on dd.order_id=aa.id
                                        left join master_couriers ee on dd.courier_id=ee.id
                                        where aa.status = 'DELIVERED'
                                        and bb.payment_mode ilike 'cod'
                                        and ee.integrated=true) yy
                                        on xx.client_prefix=yy.client_prefix 
                                        and yy.delivered_date BETWEEN xx.order_start AND xx.order_end
                                        group by xx.unique_id, xx.client_prefix, xx.remittance_id, xx.date, xx.status, xx.transaction_id, xx.payout_id) zz
                                        WHERE remittance_total is not null
                                        and remittance_date<='__REMITTANCE_DATE__'
                                        and remittance_date>'2021-01-01'
                                        and payout_id is null
                                        and status='processing'
                                        order by remittance_date DESC, remittance_total DESC"""

fetch_inventory_quantity_query = """select yy.*, zz.combo_prods, zz.combo_prods_quan from
                                    (select master_product_id, status, warehouse_prefix, sum(quantity) from 
                                    (select * from op_association aa
                                    left join orders bb on aa.order_id=bb.id
                                    left join client_pickups cc on bb.pickup_data_id=cc.id
                                    left join pickup_points dd on cc.pickup_id=dd.id     
                                    where status not in ('CANCELED', 'NOT PICKED', 'NOT SHIPPED', 'CLOSED')
                                    and (easyecom_loc_code is null or easyecom_loc_code='')) xx
                                    group by master_product_id, status, warehouse_prefix
                                    order by master_product_id, status, warehouse_prefix) yy
                                    left join (select combo_id, array_agg(combo_prod_id) as combo_prods, 
                                    array_agg(quantity) as combo_prods_quan from products_combos group by combo_id) zz
                                    on yy.master_product_id=zz.combo_id"""

update_inventory_quantity_query = """UPDATE products_quantity SET available_quantity=COALESCE(approved_quantity, 0)+%s,
									current_quantity=COALESCE(approved_quantity, 0)+%s, inline_quantity=%s, rto_quantity=%s
                                    WHERE product_id=%s and warehouse_prefix=%s;"""

update_easyecom_inventory_query = """update products_quantity
                                    set available_quantity=%s,
                                    inline_quantity=%s,
                                    current_quantity=%s
                                    WHERE warehouse_prefix=%s
                                    and product_id in (select id from master_products where sku=%s and client_prefix=%s);"""

insert_easyecom_inventory_query = """INSERT into products_quantity (product_id, available_quantity, warehouse_prefix, status, current_quantity, inline_quantity, total_quantity, approved_quantity) 
                                    select aa.id, %s, %s, 'APPROVED', %s, %s, %s, %s from master_products aa 
                                    where client_prefix=%s and sku=%s"""

get_pickup_requests_query = """select aa.pickup_data_id,  bb.courier_id, dd.warehouse_prefix, aa.id, dd.id from orders aa
                                left join shipments bb on aa.id=bb.order_id
                                left join client_pickups cc on aa.pickup_data_id=cc.id
                                left join pickup_points dd on cc.pickup_id=dd.id
                                left join client_mapping ee on ee.client_prefix=aa.client_prefix
                                where aa.status in ('READY TO SHIP', 'PICKUP REQUESTED')
                                and bb.id is not null
                                and ee.auto_pur!=false"""

insert_manifest_query = """INSERT into manifests (manifest_id, warehouse_prefix, courier_id, client_pickup_id, 
                            pickup_id, pickup_date, manifest_url, total_scheduled) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) returning id;"""

insert_order_pickups_query = """INSERT INTO order_pickups (manifest_id, order_id, picked) VALUES __INSERT_STR__ ON CONFLICT 
                                (order_id, manifest_id) DO NOTHING;"""

mark_30_days_old_orders_not_shipped = """update orders set status='NOT SHIPPED' where id in
                                        (select order_id from order_status aa
                                        left join orders bb on aa.order_id=bb.id
                                        where aa.status='Received'
                                        and aa.status_time<(NOW() - interval '30 day')
                                        and bb.status in ('READY TO SHIP', 'PICKUP REQUESTED'));
                                        
                                        update orders set status='NEW' where id in
                                        (select aa.id from orders aa 
                                        left join shipments bb on aa.id=bb.order_id
                                        where aa.status in ('READY TO SHIP', 'PICKUP REQUESTED')
                                        and bb.id is null);"""

update_pincode_serviceability_query = """INSERT INTO pincode_serviceability (pincode, courier_id, serviceable, 
                                         cod_available, reverse_pickup, pickup, sortcode, last_updated) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                                         ON CONFLICT (pincode, courier_id) DO UPDATE
                                         SET serviceable=EXCLUDED.serviceable, cod_available=EXCLUDED.cod_available,
                                         reverse_pickup=EXCLUDED.reverse_pickup, pickup=EXCLUDED.pickup, 
                                         last_updated=EXCLUDED.last_updated;"""

create_pincode_serv_file_query = """select aa.pincode, city, state, bool_or(serviceable) as serviceable, 
                                                bool_or(cod_available) as cod_available, bool_or(reverse_pickup) as reverse_pickup 
                                                from  pincode_serviceability aa
                                                left join pincode_mapping bb on aa.pincode=bb.pincode
                                                where serviceable=true
                                                and state is not null
                                                and length(aa.pincode)=6
                                                group by aa.pincode, city, state
                                                order by aa.pincode"""

wondersoft_push_query = """select cc.first_name, cc.last_name, aa.customer_phone, aa.customer_email, ff.gstin, 
                            cc.address_one,cc.address_two, cc.city, cc.state, cc.pincode, aa.order_date, 
                            aa.channel_order_id, gg.warehouse_prefix, dd.amount, dd.payment_mode, ee.products_sku,
                            ee.quan, ee.prod_amount, dd.discount_amount, dd.discount_code, dd.discount_type  from orders aa
                            left join shipping_address cc
                            on aa.delivery_address_id=cc.id
                            left join orders_payments dd
                            on dd.order_id=aa.id
                            left join 
                            (select order_id, array_agg(quantity) as quan, array_agg(pp.sku) as products_sku, 
                             array_agg(amount) as prod_amount
                             from op_association opa 
                             left join master_products pp
                             on opa.master_product_id = pp.id
                             group by order_id) ee
                            on aa.id=ee.order_id
                            left join shipments ll
                            on ll.order_id=aa.id
                            left join client_pickups ff
                            on ff.id=aa.pickup_data_id
                            left join pickup_points gg
                            on gg.id=ff.pickup_id
                            where aa.id=%s"""