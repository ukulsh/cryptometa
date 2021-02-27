get_courier_id_and_key_query = """SELECT id, courier_name, api_key, api_password FROM master_couriers;"""

get_status_update_orders_query = """select aa.id, bb.awb, aa.status, aa.client_prefix, aa.customer_phone, 
                                    aa.order_id_channel_unique, bb.channel_fulfillment_id, cc.api_key, 
                                    cc.api_password, cc.shop_url, bb.id, aa.pickup_data_id, aa.channel_order_id, ee.payment_mode, 
                                    cc.channel_id, gg.location_id, mm.item_list, mm.sku_quan_list, aa.customer_name, aa.customer_email, 
                                    nn.client_name, nn.client_logo, nn.custom_email_subject, bb.courier_id, nn.theme_color, cc.unique_parameter,
                                    cc.mark_shipped, cc.shipped_status, cc.mark_invoiced, cc.invoiced_status, cc.mark_delivered, 
                                    cc.delivered_status, cc.mark_returned, cc.returned_status, cc.id, ee.amount, oo.warehouse_prefix
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
                                    where aa.status not in ('NEW','DELIVERED','NOT SHIPPED','RTO','CANCELED','CLOSED','DTO','LOST','DAMAGED','SHORTAGE','SHIPPED')
                                    and aa.status_type is distinct from 'DL'
                                    and bb.awb != ''
                                    and bb.awb is not null
                                    and bb.courier_id=%s;"""

order_status_update_query = """UPDATE orders SET status=%s, status_type=%s, status_detail=%s WHERE id=%s;"""

select_statuses_query = """SELECT  id, status_code, status, status_text, location, status_time, location_city from order_status
                            WHERE order_id=%s AND shipment_id=%s AND courier_id=%s
                            ORDER BY status_time DESC"""

update_prod_quantity_query_rto = """DO
                                    $do$
                                        declare
                                            temprow record;
                                        BEGIN
                                           FOR temprow IN
                                                    SELECT product_id, quantity FROM op_association WHERE order_id=%s
                                           LOOP
                                                UPDATE products_quantity 
                                                SET available_quantity=COALESCE(available_quantity, 0)+temprow.quantity, 
                                                    current_quantity=COALESCE(current_quantity, 0)+temprow.quantity,
                                                    rto_quantity=COALESCE(rto_quantity, 0)+temprow.quantity                              
                                                WHERE product_id=temprow.product_id;
                                           END LOOP;
                                        END
                                    $do$;"""

update_prod_quantity_query_pickup = """DO
                                        $do$
                                        declare
                                            temprow record;
                                        BEGIN
                                           FOR temprow IN
                                                    SELECT product_id, quantity FROM op_association WHERE order_id=%s
                                           LOOP
                                                UPDATE products_quantity 
                                                SET current_quantity=COALESCE(current_quantity, 0)-temprow.quantity,
                                                    inline_quantity=COALESCE(inline_quantity, 0)-temprow.quantity                              
                                                WHERE product_id=temprow.product_id;
                                           END LOOP;
                                        END
                                        $do$;"""

update_pickup_count_query = """UPDATE manifests SET total_picked=COALESCE(total_picked, 0)+%s
                                WHERE courier_id=%s AND client_pickup_id=%s
                                AND pickup_date::date=%s;"""
