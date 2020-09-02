fetch_client_channels_query = """select aa.id,aa.client_prefix,aa.channel_id,aa.api_key,aa.api_password,aa.shop_url,
                                aa.last_synced_order,aa.last_synced_time,aa.date_created,aa.date_updated,
                                bb.id,bb.channel_name,bb.logo_url,bb.date_created,bb.date_updated,aa.fetch_status, cc.unique_parameter
                                from client_channel aa
                                left join master_channels bb
                                on aa.channel_id=bb.id
                                left join client_mapping cc
                                on aa.client_prefix=cc.client_prefix"""

insert_shipping_address_query = """INSERT INTO shipping_address (first_name, last_name, address_one, address_two, city,	
                                            pincode, state, country, phone, latitude, longitude, country_code)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;
                            """

insert_billing_address_query = """INSERT INTO billing_address (first_name, last_name, address_one, address_two, city,	
                                            pincode, state, country, phone, latitude, longitude, country_code)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;
                            """

insert_orders_data_query = """INSERT INTO orders (channel_order_id, order_date, customer_name, customer_email, 
                                customer_phone, delivery_address_id, billing_address_id, date_created, status, client_prefix, client_channel_id, 
                                order_id_channel_unique, pickup_data_id)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;
                            """

insert_payments_data_query = """INSERT INTO orders_payments (payment_mode, amount, subtotal, shipping_charges, currency, order_id)
                                VALUES (%s,%s,%s,%s,%s,%s) RETURNING id"""

select_products_query = """SELECT id from products where sku=%s and client_prefix=%s;"""

insert_op_association_query = """INSERT INTO op_association (product_id, order_id, quantity, amount, channel_item_id, tax_lines)
                                    VALUES (%s,%s,%s,%s,%s,%s) RETURNING id"""

update_last_fetched_data_query = """UPDATE client_channel SET last_synced_order=%s, last_synced_time=%s WHERE id=%s"""

update_product_quantity_query = """UPDATE products_quantity 
                                    SET available_quantity=COALESCE(available_quantity, 0)-%s,
                                        inline_quantity=COALESCE(inline_quantity, 0)+%s
                                    WHERE product_id=%s;"""

insert_product_query = """INSERT INTO products (name, sku, active, channel_id, client_prefix, date_created, 
                          dimensions, price, weight, master_sku, subcategory_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;"""

insert_product_quantity_query = """INSERT INTO products_quantity (product_id,total_quantity,approved_quantity,
                                    available_quantity,warehouse_prefix,status,date_created)
                                    VALUES (%s,%s,%s,%s,%s,%s,%s);"""

get_orders_to_assign_pickups = """select aa.id, aa.client_prefix, bb.pincode, xx.sku_list, xx.quan_list, cc.order_split, cc.default_warehouse from orders aa
                                left join shipping_address bb on aa.delivery_address_id=bb.id
                                left join 
                                (select order_id, array_agg(mm.sku) as sku_list, array_agg(ll.quantity) as quan_list 
                                from op_association ll
                                left join products mm on ll.product_id=mm.id
                                group by ll.order_id) xx
                                on aa.id=xx.order_id
                                left join client_mapping cc on aa.client_prefix=cc.client_prefix
                                where aa.pickup_data_id is null
                                and aa.status='NEW'
                                and sku_list is not null
                                and aa.date_created>%s"""

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

available_warehouse_product_quantity = """select pp.*, qq.pincode from
                                        (select ll.warehouse_prefix, ll.product_id, mm.sku, approved_quantity-COALESCE(xx.unavailable, 0) as available_count, 
                                         kk.id as courier_id, mm.weight from products_quantity ll left join
                                        (select dd.warehouse_prefix, product_id, sum(quantity) as unavailable from op_association aa
                                        left join orders bb on aa.order_id=bb.id
                                        left join client_pickups cc on bb.pickup_data_id=cc.id
                                        left join pickup_points dd on cc.pickup_id=dd.id
                                        where bb.status in
                                        ('DELIVERED','DISPATCHED','IN TRANSIT','ON HOLD','PENDING','NEW','NOT PICKED','PICKUP REQUESTED','READY TO SHIP')
                                        and aa.product_id in 
                                        (select id from products where sku in __SKU_STR__ and client_prefix='__CLIENT_PREFIX__') 
                                         group by dd.warehouse_prefix, product_id) as xx
                                        on ll.product_id=xx.product_id and ll.warehouse_prefix=xx.warehouse_prefix
                                        left join products mm on ll.product_id=mm.id
                                        left join master_couriers kk on mm.inactive_reason=kk.courier_name
                                        where ll.product_id in 
                                        (select id from products where sku in __SKU_STR__ and client_prefix='__CLIENT_PREFIX__')) pp
                                        left join pickup_points qq on pp.warehouse_prefix=qq.warehouse_prefix"""

fetch_warehouse_to_pick_from = """with temp_table (warehouse, pincode) as (VALUES __WAREHOUSE_PINCODES__)
                                    select warehouse, tat, zone_value from
                                    (select * from temp_table aa
                                    left join (select * from city_pin_mapping where pincode in (select pincode from temp_table)) bb 
                                    on aa.pincode=bb.pincode) yy
                                    left join
                                    (select zone, tat, zone_value from city_zone_mapping
                                    where zone in (select city from city_pin_mapping where pincode in (select pincode from temp_table))
                                    and city in (select city from city_pin_mapping where pincode='__DELIVERY_PINCODE__')
                                    and courier_id=__COURIER_ID__) xx
                                    on yy.city=xx.zone
                                    order by tat,zone_value
                                    limit 1"""