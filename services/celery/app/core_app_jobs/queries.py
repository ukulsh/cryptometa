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

insert_scan_query = "INSERT INTO order_scans (order_id, courier_id, shipment_id, status_code, status, status_text, " \
                    "location, location_city, status_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);"

insert_status_query = """INSERT INTO order_status (order_id, courier_id, shipment_id, status_code, status, status_text, 
                      location, location_city, status_time) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) 
                      ON CONFLICT (order_id, courier_id, shipment_id, status) 
                      DO UPDATE SET status_time = EXCLUDED.status_time, location_city=EXCLUDED.location_city, 
                      location=EXCLUDED.location, status_text=EXCLUDED.status_text,status_code=EXCLUDED.status_code;"""