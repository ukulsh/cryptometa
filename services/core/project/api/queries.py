
fetch_client_channels_query = """select aa.id,aa.client_prefix,aa.channel_id,aa.api_key,aa.api_password,aa.shop_url,
                                aa.last_synced_order,aa.last_synced_time,aa.date_created,aa.date_updated,
                                bb.id,bb.channel_name,bb.logo_url,bb.date_created,bb.date_updated,aa.fetch_status, 
                                cc.unique_parameter, cc.loc_assign_inventory
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


########################create shipments

fetch_client_couriers_query = """select aa.id,aa.client_prefix,aa.courier_id,aa.priority,aa.last_shipped_order_id,
                                aa.last_shipped_time,aa.date_created,aa.date_updated,aa.unique_parameter,bb.id,
                                bb.courier_name,bb.logo_url,bb.date_created,bb.date_updated,bb.api_key,bb.api_password,bb.api_url
		                        from client_couriers aa
                                left join master_couriers bb
                                on aa.courier_id=bb.id
                                where aa.active=true
                                order by aa.client_prefix, priority;"""

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
                                ff.shop_url, aa.order_id_channel_unique, ee.products_name, aa.pickup_data_id, xx.cod_verified, 
                                xx.id, ee.ship_courier, gg.location_id, ff.channel_id, yy.verify_cod, yy.essential, ee.subcategories, 
                                yy.cod_ship_unconfirmed, yy.client_name, aa.chargeable_weight, yy.cod_man_ver
                                from orders aa
                                left join shipping_address cc
                                on aa.delivery_address_id=cc.id
                                left join orders_payments dd
                                on dd.order_id=aa.id
                                left join 
                                (select order_id, array_agg(dimensions) as dimensions, array_agg(weight) as weights, 
                                array_agg(quantity) as quan, array_agg(pp.name) as products_name, 
                                array_agg(pp.inactive_reason ORDER BY pp.weight DESC) as ship_courier,
                                array_agg(qq.name ORDER BY pp.weight DESC) as subcategories
                                 from op_association opa 
                                 left join products pp
                                 on opa.product_id = pp.id
                                 left join products_subcategories qq
                                 on pp.subcategory_id=qq.id
                                 where client_prefix=%s
                                 group by order_id) ee
                                on aa.id=ee.order_id
                                left join client_channel ff
                                on aa.client_channel_id=ff.id
                                left join shipments ll
                                on ll.order_id=aa.id
                                left join client_channel_locations gg
                                on aa.client_channel_id=gg.client_channel_id
                                and aa.pickup_data_id=gg.pickup_data_id
                                left join cod_verification xx
                                on aa.id=xx.order_id
                                left join client_mapping yy
                                on aa.client_prefix=yy.client_prefix
                                where aa.client_prefix=%s
                                __ORDER_SELECT_FILTERS__
                                and NOT EXISTS (SELECT 1 FROM unnest(ee.weights) x WHERE x IS NULL)
                                and (xx.id is null or (xx.id is not null and xx.cod_verified = true) 
                                     or (yy.cod_ship_unconfirmed=true and aa.order_date<(NOW() - interval '1 day')))
                                order by order_date"""

update_last_shipped_order_query = """UPDATE client_couriers SET last_shipped_order_id=%s, last_shipped_time=%s WHERE client_prefix=%s"""

update_orders_status_query = """UPDATE orders SET status='READY TO SHIP' WHERE id in %s;"""

delete_failed_shipments_query = """DELETE FROM 	order_status where shipment_id in 
                                    (select id  from shipments where remark like 'Crashing while saving package%' or remark like 'COD%');
                                    delete  from shipments where remark like 'Crashing while saving package%' or remark like 'COD%';"""

#########################request pickups


get_pickup_requests_query = """select aa.id,aa.client_prefix,bb.warehouse_prefix,aa.pickup_id, auto_pur, auto_pur_time
                                from client_pickups aa
                                left join pickup_points bb on aa.pickup_id=bb.id
                                left join client_mapping cc on aa.client_prefix=cc.client_prefix;"""

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
                                order by aa.id;"""

update_order_status_query = """UPDATE orders SET status='PICKUP REQUESTED' WHERE id=%s;
                                INSERT INTO order_pickups (manifest_id, order_id, picked, date_created)
                                VALUES (%s,%s,%s,%s) ON CONFLICT (manifest_id, order_id) DO NOTHING;"""

insert_manifest_data_query = """INSERT INTO manifests (manifest_id, warehouse_prefix, courier_id, pickup_id, 
                                total_scheduled, pickup_date, manifest_url, date_created, client_pickup_id) VALUES 
                                (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;"""

#########################update status

get_courier_id_and_key_query = """SELECT id, courier_name, api_key FROM master_couriers;"""

get_status_update_orders_query = """select aa.id, bb.awb, aa.status, aa.client_prefix, aa.customer_phone, 
                                    aa.order_id_channel_unique, bb.channel_fulfillment_id, cc.api_key, 
                                    cc.api_password, cc.shop_url, bb.id, aa.pickup_data_id, aa.channel_order_id, ee.payment_mode, 
                                    cc.channel_id, gg.location_id, mm.item_list, mm.sku_quan_list, aa.customer_name, aa.customer_email, 
                                    nn.client_name, nn.client_logo, nn.custom_email_subject, bb.courier_id, nn.theme_color, cc.unique_parameter,
                                    cc.mark_shipped, cc.shipped_status, cc.mark_invoiced, cc.invoiced_status, cc.mark_delivered, 
                                    cc.delivered_status, cc.mark_returned, cc.returned_status, cc.id, ee.amount
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
                                    left join orders_payments ee
                                    on aa.id=ee.order_id
                                    left join client_channel_locations gg
                                    on aa.client_channel_id=gg.client_channel_id
                                    and aa.pickup_data_id=gg.pickup_data_id
                                    left join client_mapping nn
                                    on aa.client_prefix=nn.client_prefix
                                    where aa.status not in ('NEW','DELIVERED','NOT SHIPPED','RTO','CANCELED','CLOSED','DTO','LOST')
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


######################### Calculate rates

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

get_client_balance = """select current_balance, account_type from client_mapping where client_prefix=%s"""

update_client_balance = """update client_mapping set current_balance=%s where client_prefix=%s"""

######################### Ivr verification

get_details_cod_verify_ivr = """select aa.order_id, bb.customer_phone from cod_verification aa
                            left join orders bb on aa.order_id=bb.id
                            left join client_mapping cc on cc.client_prefix=bb.client_prefix
                            where bb.status='NEW'
                            and order_date>'__ORDER_TIME__'
                            and cod_verified is null
                            and cc.cod_man_ver is not true
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

select_product_list_query = """SELECT aa.id, aa.name as product_name, aa.product_image, aa.master_sku as channel_sku, cc.sku as master_sku, cc.price, bb.total_quantity,  bb.available_quantity,
                             bb.current_quantity, bb.inline_quantity, bb.rto_quantity,cc.dimensions, cc.weight, null as channel_logo FROM products aa
                             LEFT JOIN master_products cc on aa.master_product_id=cc.id
                            __JOIN_TYPE__ (select product_id, sum(approved_quantity) as total_quantity, sum(available_quantity) as available_quantity,
                                       sum(current_quantity) as current_quantity, sum(inline_quantity) as inline_quantity, sum(rto_quantity) as rto_quantity
                                      FROM products_quantity __WAREHOUSE_FILTER__ 
                                      GROUP BY product_id) bb
                            ON cc.id=bb.product_id
                            WHERE (aa.name ilike '%__SEARCH_KEY__%' or aa.sku ilike '%__SEARCH_KEY__%' or aa.master_sku ilike '%__SEARCH_KEY__%')
                            __CLIENT_FILTER__
                            __MV_CLIENT_FILTER__
                            ORDER BY __ORDER_BY__ __ORDER_TYPE__ 
                            __PAGINATION__
                            """

select_product_list_channel_query = """SELECT aa.id, aa.name as product_name,aa.sku as channel_product_id, aa.product_image, aa.master_sku as channel_sku, cc.sku as master_sku, cc.price, dd.logo_url as channel_logo, dd.channel_name, cc.id as master_product_id FROM products aa
                             LEFT JOIN master_products cc on aa.master_product_id=cc.id
                             LEFT JOIN master_channels dd on aa.channel_id=dd.id
                             WHERE (aa.name ilike '%__SEARCH_KEY__%' or aa.master_sku ilike '%__SEARCH_KEY__%' or cc.sku ilike '%__SEARCH_KEY__%')
                            __CLIENT_FILTER__
                            __MV_CLIENT_FILTER__
                            __CHANNEL_FILTER__
                            __STATUS_FILTER__
                            ORDER BY __ORDER_BY__ __ORDER_TYPE__ 
                            __PAGINATION__
                            """

select_orders_list_query = """select distinct on (aa.order_date, aa.id) aa.channel_order_id as order_id, aa.id as unique_id, aa.order_date, aa.status, 
                              aa.status_detail, bb.awb, CONCAT('http://webapp.wareiq.com/tracking/', bb.awb) as tracking_link, cc.courier_name, bb.edd, 
                              bb.weight, bb.dimensions, bb.volumetric_weight,bb.remark, aa.customer_name, aa.customer_phone, aa.customer_email, dd.address_one, 
                              dd.address_two, dd.city, dd.state, dd.country, dd.pincode, ee.delivered_time, ff.pickup_time, gg.payment_mode, gg.amount, ii.warehouse_prefix,
                             mm.id,  mm.cod_verified, mm.verified_via, nn.id,  nn.ndr_verified, nn.verified_via, vv.logo_url, qq.manifest_time, rr.reason_id, 
                             rr.reason, rr.date_created, aa.client_prefix, bb.pdd, uu.flag, uu.score, uu.reasons, gg.shipping_charges, ww.invoice_no
                             from orders aa
                             left join shipments bb
                             on aa.id=bb.order_id
                             left join master_couriers cc on bb.courier_id=cc.id
                             left join shipping_address dd on aa.delivery_address_id=dd.id
                             left join (select order_id, status_time as delivered_time from order_status where status in ('Delivered','RTO','DTO')) ee
                             on aa.id=ee.order_id
                             left join (select order_id, status_time as pickup_time from order_status where status='Picked') ff
                             on aa.id=ff.order_id
                             left join (select order_id, status_time as manifest_time from order_status where status='Received') qq
                             on aa.id=qq.order_id
                             left join orders_payments gg on aa.id=gg.order_id
                             left join client_pickups hh on aa.pickup_data_id=hh.id
                             left join pickup_points ii on hh.pickup_id=ii.id
                             left join op_association jj on aa.id=jj.order_id
                             left join products kk on jj.product_id=kk.id
                             left join cod_verification mm on mm.order_id=aa.id
                             left join ndr_verification nn on nn.order_id=aa.id
                             left join thirdwatch_data uu on uu.order_id=aa.id
                             left join master_channels vv on vv.id=aa.master_channel_id
                             left join orders_invoice ww on ww.order_id=aa.id
                             left join (select ss.id, ss.order_id, tt.id as reason_id, tt.reason, ss.date_created from ndr_shipments ss left join ndr_reasons tt on ss.reason_id=tt.id ) rr
                             on aa.id=rr.order_id
                             __THIRDWATCH_SCORE_FILTER__
                             __THIRDWATCH_FLAG_FILTER__
                             __SEARCH_KEY_FILTER__
                             __SEARCH_KEY_FILTER_ON_CUSTOMER__
                             __ORDER_DATE_FILTER__
                             __MANIFEST_DATE_FILTER__
                             __PICKUP_TIME_FILTER__
                             __DELIVERED_TIME_FILTER__
                             __COURIER_FILTER__
                             __STATUS_FILTER__
                             __TAB_STATUS_FILTER__
                             __NDR_REASON_FILTER__
                             __NDR_TYPE_FILTER__
                             __PICKUP_FILTER__
                             __TYPE_FILTER__
                             __CLIENT_FILTER__
                             __MV_CLIENT_FILTER__
                             __SINCE_ID_FILTER__
                             __MASTER_CHANNEL__
                             __EDD_FILTER__
                             order by order_date DESC, aa.id DESC
                             __PAGINATION__"""

get_selected_product_details = """select ll.order_id, ll.product_names, ll.skus, ll.quantity, ll.weights, ll.dimensions  from (SELECT order_id, array_agg(name) as product_names, array_agg(master_sku) as skus, 
                                        array_agg(quantity) as quantity, array_agg(weight) as weights, array_agg(dimensions) as dimensions from op_association jj 
                                       left join products kk on jj.product_id=kk.id
                                       group by order_id) ll where ll.order_id in (__FILTERED_ORDER_ID__)"""

select_wallet_deductions_query = """SELECT aa.status_time, aa.status, bb.courier_name, cc.awb, dd.channel_order_id, dd.id, ee.cod_charge, 
                                    ee.forward_charge, ee.rto_charge, ee.total_charge, ee.zone, ee.weight_charged, 
                                    (CASE WHEN (ff.management_fee_static is not null) THEN ff.management_fee_static 
                                     WHEN (ff.management_fee is not null) THEN COALESCE((ff.management_fee/100)*ee.forward_charge, 0) ELSE 
                                     5 END) tot_amount from order_status aa
                                    LEFT JOIN master_couriers bb on aa.courier_id=bb.id
                                    LEFT JOIN shipments cc on aa.shipment_id=cc.id
                                    LEFT JOIN orders dd on aa.order_id=dd.id
                                    LEFT JOIN client_deductions ee on ee.shipment_id=aa.shipment_id
                                    LEFT JOIN cost_to_clients ff on dd.client_prefix=ff.client_prefix and bb.id=ff.courier_id
                                    WHERE aa.status in ('Delivered', 'RTO', 'DTO')
                                    AND (cc.awb ilike '%__SEARCH_KEY__%' or dd.channel_order_id ilike '%__SEARCH_KEY__%')
                                    __CLIENT_FILTER__
                                    __MV_CLIENT_FILTER__
                                    __COURIER_FILTER__
                                    __DATE_TIME_FILTER__
                                    AND aa.status_time>'2020-04-01'
                                    ORDER BY status_time DESC
                                    __PAGINATION__"""


select_wallet_reconciliation_query = """SELECT ee.channel_order_id, ee.id, aa.raised_date, cc.courier_name, bb.awb, dd.weight_charged,
                                    aa.charged_weight, aa.expected_amount, aa.charged_amount, ff.status, aa.remarks, aa.dispute_date, ee.client_prefix
                                    from weight_discrepency aa
                                    LEFT JOIN shipments bb on aa.shipment_id=bb.id
                                    LEFT JOIN master_couriers cc on bb.courier_id=cc.id
                                    LEFT JOIN (select * from client_deductions where type is null) dd on dd.shipment_id=aa.shipment_id
                                    LEFT JOIN orders ee on ee.id=bb.order_id
                                	LEFT JOIN discrepency_status ff on ff.id=aa.status_id
                                  	WHERE (bb.awb ilike '%__SEARCH_KEY__%' or ee.channel_order_id ilike '%__SEARCH_KEY__%')
                                    __CLIENT_FILTER__
                                    __MV_CLIENT_FILTER__
                                    __COURIER_FILTER__
                                    __DATE_TIME_FILTER__
                                    __STATUS_FILTER__
                                    ORDER BY aa.raised_date DESC
                                    __PAGINATION__"""


select_pickups_list_query = """select aa.id, aa.manifest_id, bb.courier_name, ee.total_picked, ee.total_scheduled, aa.pickup_date, 
                                dd.warehouse_prefix, aa.total_picked, aa.total_scheduled, aa.manifest_url from manifests aa
                                left join master_couriers bb on aa.courier_id=bb.id
                                left join client_pickups cc on aa.client_pickup_id=cc.id
                                left join pickup_points dd on cc.pickup_id=dd.id
                                left join (select manifest_id, count(1) as total_scheduled, count(1) filter (where picked is true) as total_picked 
                                           from order_pickups group by manifest_id) ee on aa.id=ee.manifest_id
                                where 1=1
                                 __PICKUP_FILTER__
                                 __CLIENT_FILTER__
                                 __MV_CLIENT_FILTER__
                                 __COURIER_FILTER__
                                 __PICKUP_TIME_FILTER__
                                order by pickup_date DESC, ee.total_scheduled DESC
                                __PAGINATION__"""


select_wallet_remittance_query = """select * from
                                    (select xx.unique_id, xx.client_prefix, xx.remittance_id, xx.date as remittance_date, 
                                     xx.status, xx.transaction_id, sum(yy.amount) as remittance_total from
                                    (select id as unique_id, client_prefix, remittance_id, transaction_id, DATE(remittance_date), 
                                    DATE(del_from) AS order_start,
                                    DATE(del_to) AS order_end,
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
                                    __SEARCH_KEY_FILTER__
                                    __CLIENT_FILTER__
                                    __MV_CLIENT_FILTER__
                                    __STATUS_FILTER__
                                    __REMITTANCE_DATE_FILTER__
                                    order by remittance_date DESC, remittance_total DESC
                                    __PAGINATION__"""

select_wallet_remittance_orders_query = """select yy.* from
                                        (select id as unique_id, client_prefix, remittance_id, transaction_id, DATE(remittance_date), 
                                        DATE(del_from) AS order_start,
                                        DATE(del_to) AS order_end,
                                        status from cod_remittance) xx 
                                        left join 
                                        (select client_prefix, channel_order_id, order_date, ee.courier_name, dd.awb, payment_mode, amount, cc.status_time as delivered_date from orders aa
                                        left join orders_payments bb on aa.id=bb.order_id
                                        left join (select * from order_status where status='Delivered') cc
                                        on aa.id=cc.order_id
                                        left join shipments dd on dd.order_id=aa.id
                                        left join master_couriers ee on dd.courier_id=ee.id
                                        where aa.status = 'DELIVERED'
                                        and bb.payment_mode ilike 'cod') yy
                                        on xx.client_prefix=yy.client_prefix 
                                        and yy.delivered_date BETWEEN xx.order_start AND xx.order_end
                                         where xx.unique_id=__REMITTANCE_ID__
                                         order by delivered_date"""