fetch_client_channels_query = """select aa.id,aa.client_prefix,aa.channel_id,aa.api_key,aa.api_password,aa.shop_url,
                                aa.last_synced_order,aa.last_synced_time,aa.date_created,aa.date_updated,
                                bb.id,bb.channel_name,bb.logo_url,bb.date_created,bb.date_updated,aa.fetch_status, 
                                cc.unique_parameter, cc.loc_assign_inventory
                                from client_channel aa
                                left join master_channels bb
                                on aa.channel_id=bb.id
                                left join client_mapping cc
                                on aa.client_prefix=cc.client_prefix
                                WHERE aa.status=true"""

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
                                order_id_channel_unique, pickup_data_id, master_channel_id, date_updated)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;
                            """

insert_payments_data_query = """INSERT INTO orders_payments (payment_mode, amount, subtotal, shipping_charges, currency, order_id)
                                VALUES (%s,%s,%s,%s,%s,%s) RETURNING id"""

insert_order_extra_details_query = """INSERT INTO orders_extra_details (order_id, ip_address, user_agent, session_id, user_id, 
                                    user_created_at, order_count, verified_email, payment_id, payment_gateway, payment_method)
                                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""

select_products_query = """SELECT id, master_product_id from products where sku=%s and client_prefix=%s;"""

select_master_products_query = """SELECT id from master_products where sku=%s and client_prefix=%s;"""

insert_op_association_query = """INSERT INTO op_association (product_id, order_id, quantity, amount, channel_item_id, tax_lines, master_product_id)
                                    VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id"""

update_last_fetched_data_query = """UPDATE client_channel SET last_synced_order=%s, last_synced_time=%s WHERE id=%s"""

update_product_quantity_query = """UPDATE products_quantity 
                                    SET available_quantity=COALESCE(available_quantity, 0)-%s,
                                        inline_quantity=COALESCE(inline_quantity, 0)+%s
                                    WHERE product_id=%s;"""

insert_product_query = """INSERT INTO products (name, sku, channel_id, client_prefix, date_created, 
                          dimensions, price, weight, master_sku, subcategory_id, master_product_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;"""

insert_master_product_query = """INSERT INTO master_products (name, sku, active, client_prefix, date_created, 
                          dimensions, price, weight, subcategory_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;"""

insert_product_quantity_query = """INSERT INTO products_quantity (product_id,total_quantity,approved_quantity,
                                    available_quantity,warehouse_prefix,status,date_created)
                                    VALUES (%s,%s,%s,%s,%s,%s,%s);"""

get_orders_to_assign_pickups = """select aa.id, aa.client_prefix, bb.pincode, xx.sku_list, xx.quan_list, cc.order_split, cc.default_warehouse from orders aa
                                left join shipping_address bb on aa.delivery_address_id=bb.id
                                left join 
                                (select order_id, array_agg(mm.sku) as sku_list, array_agg(ll.quantity) as quan_list 
                                from op_association ll
                                left join master_products mm on ll.master_product_id=mm.id
                                group by ll.order_id) xx
                                on aa.id=xx.order_id
                                left join client_mapping cc on aa.client_prefix=cc.client_prefix
                                left join (select * from client_channel where channel_id=7) dd on dd.client_prefix=aa.client_prefix
                                where aa.pickup_data_id is null
                                and aa.status='NEW'
                                and dd.id is null
                                and sku_list is not null
                                and aa.date_created>%s"""


available_warehouse_product_quantity = """select aa.warehouse_prefix, aa.product_id, bb.sku, COALESCE(aa.available_quantity, 0) as available_count,  null as courier_id, 
                                         bb.weight, cc.pincode from products_quantity aa 
                                         left join master_products bb on aa.product_id=bb.id 
                                         left join pickup_points cc on aa.warehouse_prefix=cc.warehouse_prefix
                                         left join client_pickups kk on kk.client_prefix=bb.client_prefix and kk.pickup_id=cc.id
                                         where bb.sku in __SKU_STR__ and bb.client_prefix='__CLIENT_PREFIX__'
                                         and kk.active=true;"""

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

select_thirdwatch_check_orders_query = """select cc.ip_address, cc.session_id, cc.user_agent, cc.user_id, cc.user_created_at, 
                                        aa.customer_email, dd.first_name, dd.last_name, aa.customer_phone, cc.order_count, cc.verified_email,
                                        ee.cod_verified, aa.id, aa.order_date, bb.amount, bb.payment_mode, 
                                        hh.sku, hh.prod_name, hh.prod_amount,  hh.quantity, hh.master_sku, dd.phone, dd.address_one, 
                                        dd.address_two, dd.city, dd.state, dd.pincode, dd.country, ii.client_name, cc.payment_id, 
                                        cc.payment_gateway, cc.payment_method, ii.thirdwatch_cod_only, aa.channel_order_id, aa.client_prefix from orders aa
                                        left join orders_payments bb on aa.id=bb.order_id
                                        left join orders_extra_details cc on aa.id=cc.order_id
                                        left join shipping_address dd on dd.id=aa.delivery_address_id     
                                        left join cod_verification ee on ee.order_id=aa.id
                                        left join (select order_id, array_agg(sku) as sku, array_agg(name) as prod_name, array_agg(amount) as prod_amount, 
                                                   array_agg(quantity) as quantity, array_agg(master_sku) as master_sku from op_association ff 
                                                  left join products gg on ff.product_id=gg.id group by order_id) hh on hh.order_id=aa.id
                                        left join client_mapping ii on ii.client_prefix=aa.client_prefix
                                        left join thirdwatch_data jj on jj.order_id=aa.id
                                        where jj.order_id is null
                                        and ii.thirdwatch=true
                                        and aa.order_date>'__ORDER_TIME__'"""


insert_failed_order_query = """INSERT INTO failed_orders (channel_order_id, order_date, customer_name, customer_email, 
                                customer_phone, error, synced, client_prefix, client_channel_id, master_channel_id, 
                                order_id_channel_unique, date_created) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                ON CONFLICT (order_id_channel_unique, client_channel_id) 
                                DO NOTHING;"""