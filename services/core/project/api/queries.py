
fetch_client_channels_query = """select aa.id,aa.client_prefix,aa.channel_id,aa.api_key,aa.api_password,aa.shop_url,
                                aa.last_synced_order,aa.last_synced_time,aa.date_created,aa.date_updated,
                                bb.id,bb.channel_name,bb.logo_url,bb.date_created,bb.date_updated 
                                from client_channel aa
                                left join master_channels bb
                                on aa.channel_id=bb.id"""

insert_shipping_address_query = """INSERT INTO shipping_address (first_name, last_name, address_one, address_two, city,	
                                            pincode, state, country, phone, latitude, longitude, country_code)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;
                            """

insert_orders_data_query = """INSERT INTO orders (channel_order_id, order_date, customer_name, customer_email, 
                                customer_phone, delivery_address_id, date_created, status, client_prefix, client_channel_id, 
                                order_id_channel_unique, pickup_data_id)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;
                            """

insert_payments_data_query = """INSERT INTO orders_payments (payment_mode, amount, subtotal, shipping_charges, currency, order_id)
                                VALUES (%s,%s,%s,%s,%s,%s) RETURNING id"""

select_products_query = """SELECT id from products where sku=%s and client_prefix=%s;"""

insert_op_association_query = """INSERT INTO op_association (product_id, order_id, quantity)
                                    VALUES (%s,%s,%s) RETURNING id"""

update_last_fetched_data_query = """UPDATE client_channel SET last_synced_order=%s, last_synced_time=%s WHERE id=%s"""

update_product_quantity_query = """UPDATE products_quantity 
                                    SET available_quantity=COALESCE(available_quantity, 0)-%s,
                                        inline_quantity=COALESCE(inline_quantity, 0)+%s
                                    WHERE product_id=%s;"""

insert_product_query = """INSERT INTO products (name, sku, active, channel_id, client_prefix, date_created, 
                          dimensions, price, weight) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;"""

insert_product_quantity_query = """INSERT INTO products_quantity (product_id,total_quantity,approved_quantity,
                                    available_quantity,warehouse_prefix,status,date_created)
                                    VALUES (%s,%s,%s,%s,%s,%s,%s);"""

get_orders_to_assign_pickups = """select aa.id, aa.client_prefix, bb.pincode, xx.sku_list, xx.quan_list from orders aa
                                left join shipping_address bb on aa.delivery_address_id=bb.id
                                left join 
                                (select order_id, array_agg(mm.sku) as sku_list, array_agg(ll.quantity) as quan_list 
                                from op_association ll
                                left join products mm on ll.product_id=mm.id
                                group by ll.order_id) xx
                                on aa.id=xx.order_id
                                where aa.pickup_data_id is null
                                and aa.status='NEW'
                                and sku_list is not null
                                and aa.order_date>%s"""


########################create shipments

fetch_client_couriers_query = """select aa.id,aa.client_prefix,aa.courier_id,aa.priority,aa.last_shipped_order_id,
                                aa.last_shipped_time,aa.date_created,aa.date_updated,aa.unique_parameter,bb.id,
                                bb.courier_name,bb.logo_url,bb.date_created,bb.date_updated,bb.api_key,bb.api_password,bb.api_url
		                        from client_couriers aa
                                left join master_couriers bb
                                on aa.courier_id=bb.id"""

get_pickup_points_query = """select aa.id, aa.pickup_id, aa.return_point_id, 
                                bb.phone, bb.address, bb.address_two, bb.city,
                                bb.country, bb.pincode, bb.warehouse_prefix, bb.state, bb.name,
                                cc.phone, cc.address, cc.address_two, cc.city,
                                cc.country, cc.pincode, cc.warehouse_prefix, cc.state, cc.name
                                from client_pickups aa
                                left join pickup_points bb
                                on aa.pickup_id=bb.id
                                left join return_points cc
                                on aa.return_point_id=cc.id
                                where aa.id=%s"""

get_orders_to_ship_query = """select aa.id,aa.channel_order_id,aa.order_date,aa.customer_name,aa.customer_email,aa.customer_phone,
                                aa.date_created,aa.date_updated,aa.status,aa.client_prefix,aa.client_channel_id,aa.delivery_address_id,
                                cc.id,cc.first_name,cc.last_name,cc.address_one,cc.address_two,cc.city,cc.pincode,cc.state,cc.country,cc.phone,
                                cc.latitude,cc.longitude,cc.country_code,dd.id,dd.payment_mode,dd.amount,dd.currency,dd.order_id,dd.shipping_charges,
                                dd.subtotal,dd.order_id,ee.dimensions,ee.weights,ee.quan, ff.api_key, ff.api_password, 
                                ff.shop_url, aa.order_id_channel_unique, ee.products_name, aa.pickup_data_id, xx.cod_verified, xx.id, ee.ship_courier
                                from orders aa
                                left join shipping_address cc
                                on aa.delivery_address_id=cc.id
                                left join orders_payments dd
                                on dd.order_id=aa.id
                                left join 
                                (select order_id, array_agg(dimensions) as dimensions, array_agg(weight) as weights, 
                                array_agg(quantity) as quan, array_agg(pp.name) as products_name, 
                                array_agg(pp.inactive_reason ORDER BY pp.weight DESC) as ship_courier
                                 from op_association opa 
                                 left join products pp
                                 on opa.product_id = pp.id
                                 where client_prefix=%s
                                 group by order_id) ee
                                on aa.id=ee.order_id
                                left join client_channel ff
                                on aa.client_channel_id=ff.id
                                left join shipments ll
                                on ll.order_id=aa.id
                                left join cod_verification xx
                                on aa.id=xx.order_id
                                where aa.client_prefix=%s
                                and aa.status='NEW'
                                and ll.id is null
                                __PRODUCT_FILTER__
                                order by order_date"""

update_last_shipped_order_query = """UPDATE client_couriers SET last_shipped_order_id=%s, last_shipped_time=%s WHERE client_prefix=%s"""

update_orders_status_query = """UPDATE orders SET status='READY TO SHIP' WHERE id in %s;"""

#########################request pickups


get_pickup_requests_query = """select aa.id,aa.client_prefix,bb.warehouse_prefix,aa.pickup_id, cc.id as pr_id,
                                cc.pickup_after_hours
                                from client_pickups aa
                                left join pickup_points bb
                                on aa.pickup_id=bb.id
                                left join pickup_requests cc
                                on aa.client_prefix=cc.client_prefix;"""

get_request_pickup_orders_data_query = """select aa.channel_order_id, aa.order_date, aa.client_prefix, 
                                bb.weight, cc.courier_name, cc.api_key, cc.api_url, dd.prod_names, 
                                dd.prod_quan, ee.payment_mode, ee.amount, ff.first_name, ff.last_name, 
                                ff.address_one, ff.address_two, ff.city, ff.pincode, ff.state, ff.country, ff.phone,
                                bb.awb, aa.id, cc.id from orders aa
                              	left join shipments bb
                                on aa.id=bb.order_id
                                left join master_couriers cc
                                on bb.courier_id=cc.id
                                left join 
                                (select order_id, array_agg(pp.name) as prod_names, array_agg(quantity) as prod_quan 
                                 from op_association opa 
                                 left join products pp
                                 on opa.product_id = pp.id
                                 where order_id>3116
                                 group by order_id) dd
                                on aa.id=dd.order_id
                                left join orders_payments ee
                                on aa.id=ee.order_id
                                left join shipping_address ff
                                on aa.delivery_address_id=ff.id
                                where aa.status in __ORDER_STATUS__
                                and aa.pickup_data_id=%s
                                and aa.order_date<%s
                                order by aa.id;"""

update_order_status_query = """UPDATE orders SET status='PICKUP REQUESTED' WHERE id=%s"""

insert_manifest_data_query = """INSERT INTO manifests (manifest_id, warehouse_prefix, courier_id, pickup_id, 
                                total_scheduled, pickup_date, manifest_url, date_created) VALUES (%s,%s,%s,%s,%s,
                                %s,%s,%s)"""

update_pickup_requests_query = """UPDATE pickup_requests SET last_picked_order_id=%s, last_pickup_request_date=%s
                                  WHERE client_prefix=%s"""


#########################update status

get_courier_id_and_key_query = """SELECT id, courier_name, api_key FROM master_couriers;"""

get_status_update_orders_query = """select aa.id, bb.awb, aa.status, aa.client_prefix, aa.customer_phone, 
                                    aa.order_id_channel_unique, bb.channel_fulfillment_id, cc.api_key, 
                                    cc.api_password, cc.shop_url, bb.id, dd.pickup_id, aa.channel_order_id, ee.payment_mode from orders aa
                                    left join shipments bb
                                    on aa.id=bb.order_id
                                    left join client_channel cc
                                    on aa.client_channel_id=cc.id
                                    left join client_pickups dd
                                    on aa.pickup_data_id=dd.id
                                    left join orders_payments ee
                                    on aa.id=ee.order_id
                                    where aa.status not in ('NEW','DELIVERED','NOT SHIPPED','RTO','CANCELED')
                                    and aa.status_type is distinct from 'DL'
                                    and bb.awb != ''
                                    and bb.status != 'Fail'
                                    and bb.status != 'Failure'
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
                                WHERE courier_id=%s AND pickup_id=%s
                                AND pickup_date::date=%s;"""


######################### Calculate rates

select_orders_to_calculate_query = """select aa.id, aa.awb, aa.courier_id, aa.volumetric_weight, aa.weight, 
                                        bb.channel_order_id, bb.client_prefix, cc.pincode as pickup_pincode,
                                        dd.pincode as delivery_pincode, ff.status_time, bb.order_date, gg.payment_mode, 
                                        gg.amount, bb.status, aa.dimensions from shipments aa
                                        left join orders bb on aa.order_id=bb.id
                                        left join pickup_points cc on aa.pickup_id=cc.id
                                        left join shipping_address dd on bb.delivery_address_id=dd.id
                                        left join client_deductions ee on ee.shipment_id=aa.id
                                        left join (select * from order_status where status in ('Delivered', 'Returned')) ff
                                        on aa.id = ff.shipment_id
                                        left join orders_payments gg on bb.id=gg.order_id
                                        where bb.status in ('DELIVERED', 'RTO')
                                        and ee.shipment_id is null
                                        and (ff.status_time>'__STATUS_TIME__' or ff.status_time is null)"""

insert_into_deduction_query = """INSERT INTO client_deductions (weight_charged,zone,deduction_time,cod_charge,
                                cod_charged_gst,forward_charge,forward_charge_gst,rto_charge,
                                rto_charge_gst,shipment_id,total_charge,total_charged_gst,date_created,date_updated) VALUES (%s,%s,%s,%s,%s,
                                %s,%s,%s,%s,%s,%s,%s,%s,%s);"""


######################### Ivr verification

get_details_cod_verify_ivr = """select aa.order_id, bb.customer_phone from cod_verification aa
                            left join orders bb on aa.order_id=bb.id
                            where bb.status='NEW'
                            and order_date>'__ORDER_TIME__'
                            and cod_verified is null
                            order by bb.id"""

get_details_ndr_verify_ivr = """select aa.order_id, bb.customer_phone from ndr_verification aa
                            left join orders bb on aa.order_id=bb.id
                            where bb.status='PENDING'
                            and aa.date_created>'__ORDER_TIME__'
                            and ndr_verified is null
                            order by bb.id"""

################## app queries

product_count_query = """select product_id, status, sum(quantity) from 
                        (select * from op_association aa
                        left join orders bb on aa.order_id=bb.id
                        left join client_pickups cc on bb.pickup_data_id=cc.id
                        left join pickup_points dd on cc.pickup_id=dd.id
                        where aa.product_id in __PRODUCT_IDS__
                        and status != 'CANCELED'
                        __WAREHOUSE_FILTER__ ) xx
                        group by product_id, status"""

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