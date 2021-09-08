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

update_last_shipped_order_query = (
    """UPDATE client_couriers SET last_shipped_order_id=%s, last_shipped_time=%s WHERE client_prefix=%s"""
)

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
                                              left join master_products ll on kk.master_product_id=ll.id) nn
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

available_warehouse_product_quantity = """select aa.warehouse_prefix, aa.product_id, bb.sku, aa.available_quantity as available_count,  null as courier_id, 
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

select_product_list_query = """SELECT aa.id, aa.name as product_name, aa.product_image, aa.sku as master_sku, aa.price, bb.total_quantity,  bb.available_quantity,
                             bb.current_quantity, bb.inline_quantity, bb.rto_quantity,aa.dimensions, aa.weight, aa.hsn_code as hsn, aa.tax_rate FROM master_products aa
                            __JOIN_TYPE__ (select product_id, sum(approved_quantity) as total_quantity, sum(available_quantity) as available_quantity,
                                       sum(current_quantity) as current_quantity, sum(inline_quantity) as inline_quantity, sum(rto_quantity) as rto_quantity
                                      FROM products_quantity __WAREHOUSE_FILTER__ 
                                      GROUP BY product_id) bb
                            ON aa.id=bb.product_id
                            WHERE (aa.name ilike '%__SEARCH_KEY__%' or aa.sku ilike '%__SEARCH_KEY__%')
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

select_wro_list_query = """select aa.id, aa.warehouse_prefix, aa.client_prefix, aa.created_by, aa.no_of_boxes, aa.tracking_details, aa.edd, aa.status,
                            aa.date_created, bb.master_id, bb.master_sku, bb.ro_quantity, bb.received_quantity from warehouse_ro aa
                            left join (select wro_id, array_agg(yy.id) as master_id, array_agg(yy.sku) as master_sku, array_agg(xx.ro_quantity) as ro_quantity, 
                            array_agg(xx.received_quantity) as received_quantity from products_wro xx
                            left join master_products yy on xx.master_product_id=yy.id
                            group by wro_id) bb on aa.id=bb.wro_id
                            WHERE 1=1
                            __SEARCH_FILTER__
                            __CLIENT_FILTER__
                            __WAREHOUSE_FILTER__
                            __STATUS_FILTER__
                            __MV_CLIENT_FILTER__
                             ORDER BY __ORDER_BY__ __ORDER_TYPE__ 
                            __PAGINATION__"""

select_combo_list_query = """select parent_id, array_agg(child_id) as child_id, name, sku, date_created, 
                            array_agg(child_sku) as child_sku, array_agg(child_name) as child_name, array_agg(child_qty) as child_qty from
                            (select bb.id as parent_id, cc.id as child_id, bb.name, bb.sku, aa.date_created::date as date_created, 
                            cc.sku as child_sku, cc.name as child_name, aa.quantity as child_qty from products_combos aa
                            left join master_products bb on aa.combo_id=bb.id
                            left join master_products cc on aa.combo_prod_id=cc.id
                            WHERE (bb.name ilike '%__SEARCH_KEY__%' or bb.sku ilike '%__SEARCH_KEY__%')
                            __CLIENT_FILTER__
                            __MV_CLIENT_FILTER__
                            __WAREHOUSE_FILTER__) xx
                            GROUP BY parent_id,name, sku, date_created
                            ORDER BY __ORDER_BY__ __ORDER_TYPE__ 
                            __PAGINATION__
                            """

select_inventory_history_query = """select bb.sku, aa.warehouse_prefix, aa.user, aa.quantity, aa.type, aa.date_created as update_date, aa.remark from inventory_update aa
                                    left join master_products bb on aa.product_id=bb.id
                                    where (bb.name ilike '%__SEARCH_KEY__%' or bb.sku ilike '%__SEARCH_KEY__%')
                                    __CLIENT_FILTER__
                                    __WAREHOUSE_FILTER__
                                    __TYPE_FILTER__
                                    ORDER BY __ORDER_BY__ __ORDER_TYPE__ 
                                    __PAGINATION__"""

select_orders_list_query = """select distinct on (aa.order_date, aa.id) aa.channel_order_id as order_id, aa.id as unique_id, aa.order_date, aa.status, 
                              aa.status_detail, bb.awb, CONCAT('https://webapp.wareiq.com/tracking/', bb.awb) as tracking_link, cc.courier_name, bb.edd, 
                              bb.weight, bb.dimensions, bb.volumetric_weight,bb.remark, aa.customer_name, aa.customer_phone, aa.customer_email, dd.address_one, 
                              dd.address_two, dd.city, dd.state, dd.country, dd.pincode, ee.delivered_time, ff.pickup_time, gg.payment_mode, gg.amount, ii.warehouse_prefix,
                             mm.id,  mm.cod_verified, mm.verified_via, nn.id,  nn.ndr_verified, nn.verified_via, vv.logo_url, qq.manifest_time, __NDR_AGG_SEL_1__ 
                             aa.client_prefix, bb.pdd, uu.flag, uu.score, uu.reasons, gg.shipping_charges, ww.invoice_no_text, ww.date_created, __NDR_AGG_SEL_2__ uu.tags, 
                             bb.same_state, bb.tracking_link as pod_link, aa.date_updated
                             from orders aa
                             left join shipments bb
                             on aa.id=bb.order_id
                             left join master_couriers cc on bb.courier_id=cc.id
                             left join shipping_address dd on aa.delivery_address_id=dd.id
                             left join (select order_id, status_time as delivered_time from order_status where status in ('Delivered','RTO','DTO')) ee
                             on aa.id=ee.order_id
                             left join (select order_id, status_time as pickup_time from order_status where status in ('Picked', 'Picked RVP', 'Shipped')) ff
                             on aa.id=ff.order_id
                             left join (select order_id, status_time as manifest_time from order_status where status='Received') qq
                             on aa.id=qq.order_id
                             left join orders_payments gg on aa.id=gg.order_id
                             left join client_pickups hh on aa.pickup_data_id=hh.id
                             left join pickup_points ii on hh.pickup_id=ii.id
                             left join cod_verification mm on mm.order_id=aa.id
                             left join ndr_verification nn on nn.order_id=aa.id
                             left join thirdwatch_data uu on uu.order_id=aa.id
                             left join master_channels vv on vv.id=aa.master_channel_id
                             left join orders_invoice ww on ww.order_id=aa.id
                             __NDR_AGGREGATION__
                             __SEARCH_KEY_FILTER__
                             __SEARCH_KEY_FILTER_ON_CUSTOMER__
                             __THIRDWATCH_SCORE_FILTER__
                             __THIRDWATCH_FLAG_FILTER__
                             __THIRDWATCH_TAGS_FILTER__
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
                             __UPDATED_AFTER__
                             order by order_date DESC, aa.id DESC
                             __PAGINATION__"""

get_selected_product_details = """select ll.order_id, ll.product_names, ll.skus, ll.quantity, ll.weights, ll.dimensions, ll.prod_price, ll.tax_rate, ll.mrp  from (SELECT order_id, array_agg(name) as product_names, array_agg(sku) as skus, 
                                        array_agg(quantity) as quantity, array_agg(weight) as weights, array_agg(dimensions) as dimensions, array_agg(jj.amount) as prod_price, array_agg(tax_rate) as tax_rate, array_agg(kk.price) as mrp from op_association jj 
                                       left join master_products kk on jj.master_product_id=kk.id
                                       group by order_id) ll where ll.order_id in (__FILTERED_ORDER_ID__)"""

select_wallet_deductions_query = """SELECT aa.status_time, aa.status, bb.courier_name, cc.awb, dd.channel_order_id, dd.id, ee.cod_charge, 
                                    ee.forward_charge, ee.rto_charge, ee.total_charge, ee.zone, ee.weight_charged, 
                                    (CASE WHEN (ff.management_fee_static is not null) THEN ff.management_fee_static 
                                     WHEN (ff.management_fee is not null) THEN COALESCE((ff.management_fee/100)*ee.forward_charge, 0) ELSE 
                                     5 END) tot_amount, ee.type from order_status aa
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
                                    left join shipments dd on dd.order_id=aa.id
                                    left join master_couriers ee on dd.courier_id=ee.id
                                    where aa.status = 'DELIVERED'
                                    and bb.payment_mode ilike 'cod'
                                    and ee.integrated=true) yy
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

select_wallet_remittance_orders_query = """select yy.*, xx.transaction_id from
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
                                        and bb.payment_mode ilike 'cod'
                                        and ee.integrated=true) yy
                                        on xx.client_prefix=yy.client_prefix 
                                        and yy.delivered_date BETWEEN xx.order_start AND xx.order_end
                                        __REMITTANCE_ID_FILTER__
                                        __CLIENT_FILTER__
                                        order by delivered_date"""

select_state_performance_query = """select state, order_count, ROUND((order_count*100 / SUM(order_count) OVER ()), 1) AS perc_total, 
                                    ROUND(shipping_cost::numeric/nullif(ship_cost_count, 0), 2) as avg_ship_cost,
                                    ROUND(transit_days::numeric/nullif(delivered_count, 0), 1) as avg_tras_days, 
                                    ROUND(rto_count*100::numeric/nullif(order_count, 0), 1) as rto_perc, 
                                    ROUND(revenue::numeric/nullif(delivered_count, 0), 2) as avg_revenue, freq_zone, 
                                    ROUND(cod_count*100::numeric/nullif(order_count, 0), 1) as cod_perc, freq_wh from 
                                    (select cc.state, count(*) as order_count, sum(case when aa.status='DELIVERED' then forward_charge+rto_charge else 0 end) as shipping_cost, 
                                    sum(dd.status_time::date-ee.status_time::date) as transit_days, sum(case when aa.status='DELIVERED' then 1 else 0 end) as delivered_count,
                                    sum(case when aa.status='RTO' then 1 else 0 end) as rto_count, sum(case when aa.status='DELIVERED' then ii.amount else 0 end) as revenue,
                                    sum(case when gg.forward_charge is not null and aa.status='DELIVERED' then 1 else 0 end) as ship_cost_count,
                                    sum(case when ii.payment_mode ilike 'cod' then 1 else 0 end) as cod_count,
                                    mode() within group (order by hh.zone) as freq_zone,
                                    mode() within group (order by kk.warehouse_prefix) as freq_wh
                                    from orders aa
                                    left join shipping_address bb on aa.delivery_address_id=bb.id
                                    left join pincode_mapping cc on bb.pincode=cc.pincode
                                    left join shipments hh on hh.order_id=aa.id
                                    left join (select * from order_status where status in ('Delivered')) dd on dd.order_id=aa.id
                                    left join (select * from order_status where status in ('Picked')) ee on ee.order_id=aa.id
                                    left join (select * from order_status where status in ('RTO')) ff on ff.order_id=aa.id
                                    left join client_deductions gg on gg.shipment_id=hh.id
                                    left join orders_payments ii on aa.id=ii.order_id
                                    left join client_pickups jj on aa.pickup_data_id=jj.id
                                    left join pickup_points kk on jj.pickup_id=kk.id
                                    where aa.status in ('DELIVERED','RTO')
                                    and hh.courier_id!=19
                                    and aa.order_date>'%s' and aa.order_date<'%s'
                                    __CLIENT_FILTER__
                                    __MODE_FILTER__
                                    and cc.state is not null
                                    group by cc.state
                                    order by order_count DESC) xx"""

select_courier_performance_query = """select courier_name, order_count, ROUND((order_count*100 / SUM(order_count) OVER ()), 1) AS perc_total, 
                                    ROUND(shipping_cost::numeric/nullif(ship_cost_count, 0), 2) as avg_ship_cost,
                                    ROUND(transit_days::numeric/nullif(delivered_count, 0), 1) as avg_tras_days, 
                                    ROUND(rto_count*100::numeric/nullif(order_count, 0), 1) as rto_perc, 
                                    ROUND(delivered_count*100::numeric/nullif(order_count, 0), 1) as delivered_perc,
                                    ROUND(del_within_sla*100::numeric/nullif(pdd_count, 0), 1) as del_sla_perc, 
                                    ROUND(ndr_count*100::numeric/nullif(order_count, 0), 1) as ndr_perc from 
                                    (select jj.courier_name, count(*) as order_count, sum(case when aa.status='DELIVERED' then forward_charge+rto_charge else 0 end) as shipping_cost, 
                                    sum(dd.status_time::date-ee.status_time::date) as transit_days, sum(case when aa.status='DELIVERED' then 1 else 0 end) as delivered_count,
                                    sum(case when aa.status='RTO' then 1 else 0 end) as rto_count, sum(case when aa.status='DELIVERED' then ii.amount else 0 end) as revenue,
                                    sum(case when gg.forward_charge is not null and aa.status='DELIVERED' then 1 else 0 end) as ship_cost_count,
                                    sum(case when dd.status_time <=hh.pdd + interval '1' day and hh.pdd is not null and aa.status='DELIVERED' then 1 else 0 end) as del_within_sla,
                                    sum(case when hh.pdd is not null and aa.status='DELIVERED' then 1 else 0 end) as pdd_count,
                                    sum(case when kk.order_id is not null then 1 else 0 end) as ndr_count
                                    from orders aa
                                    left join shipping_address bb on aa.delivery_address_id=bb.id
                                    left join pincode_mapping cc on bb.pincode=cc.pincode
                                    left join shipments hh on hh.order_id=aa.id
                                    left join master_couriers jj on hh.courier_id=jj.id
                                    left join (select order_id, count(*) from ndr_shipments group by order_id) kk on kk.order_id=aa.id
                                    left join (select * from order_status where status in ('Delivered')) dd on dd.order_id=aa.id
                                    left join (select * from order_status where status in ('Picked')) ee on ee.order_id=aa.id
                                    left join (select * from order_status where status in ('RTO')) ff on ff.order_id=aa.id
                                    left join client_deductions gg on gg.shipment_id=hh.id
                                    left join orders_payments ii on aa.id=ii.order_id
                                    where aa.status in ('DELIVERED','RTO')
                                    and hh.courier_id!=19
                                    and aa.order_date>'%s' and aa.order_date<'%s'
                                    and jj.courier_name is not null
                                    __CLIENT_FILTER__
                                    __MODE_FILTER__
                                    __ZONE_FILTER__
                                    group by jj.courier_name
                                    order by order_count DESC) xx"""

select_zone_performance_query = """select zone, order_count, ROUND((order_count*100 / SUM(order_count) OVER ()), 1) AS perc_total, 
                                    ROUND(shipping_cost::numeric/nullif(ship_cost_count, 0), 2) as avg_ship_cost,
                                    ROUND(transit_days::numeric/nullif(delivered_count, 0), 1) as avg_tras_days, 
                                    ROUND(rto_count*100::numeric/nullif(order_count, 0), 1) as rto_perc, 
                                    ROUND(delivered_count*100::numeric/nullif(order_count, 0), 1) as delivered_perc,
                                    ROUND(del_within_sla*100::numeric/nullif(pdd_count, 0), 1) as del_sla_perc, 
                                    ROUND(ndr_count*100::numeric/nullif(order_count, 0), 1) as ndr_perc from 
                                    (select hh.zone, count(*) as order_count, sum(case when aa.status='DELIVERED' then forward_charge+rto_charge else 0 end) as shipping_cost, 
                                    sum(dd.status_time::date-ee.status_time::date) as transit_days, sum(case when aa.status='DELIVERED' then 1 else 0 end) as delivered_count,
                                    sum(case when aa.status='RTO' then 1 else 0 end) as rto_count, sum(case when aa.status='DELIVERED' then ii.amount else 0 end) as revenue,
                                    sum(case when gg.forward_charge is not null and aa.status='DELIVERED' then 1 else 0 end) as ship_cost_count,
                                    sum(case when dd.status_time <=hh.pdd + interval '1' day and hh.pdd is not null and aa.status='DELIVERED' then 1 else 0 end) as del_within_sla,
                                    sum(case when hh.pdd is not null and aa.status='DELIVERED' then 1 else 0 end) as pdd_count,
                                    sum(case when kk.order_id is not null then 1 else 0 end) as ndr_count
                                    from orders aa
                                    left join shipping_address bb on aa.delivery_address_id=bb.id
                                    left join pincode_mapping cc on bb.pincode=cc.pincode
                                    left join shipments hh on hh.order_id=aa.id
                                    left join master_couriers jj on hh.courier_id=jj.id
                                    left join (select order_id, count(*) from ndr_shipments group by order_id) kk on kk.order_id=aa.id
                                    left join (select * from order_status where status in ('Delivered')) dd on dd.order_id=aa.id
                                    left join (select * from order_status where status in ('Picked')) ee on ee.order_id=aa.id
                                    left join (select * from order_status where status in ('RTO')) ff on ff.order_id=aa.id
                                    left join client_deductions gg on gg.shipment_id=hh.id
                                    left join orders_payments ii on aa.id=ii.order_id
                                    where aa.status in ('DELIVERED','RTO')
                                    and hh.courier_id!=19
                                    and aa.order_date>'%s' and aa.order_date<'%s'
                                    and hh.zone is not null
                                    __CLIENT_FILTER__
                                    group by hh.zone
                                    order by order_count DESC) xx"""

select_top_selling_state_query = """select cc.state, count(*) as order_count ,ROUND((count(*)*100 / SUM(count(*)) OVER ()), 1)::numeric AS order_perc
                                    from orders aa
                                    left join shipping_address bb on aa.delivery_address_id=bb.id
                                    left join pincode_mapping cc on bb.pincode=cc.pincode
                                    where aa.status = 'DELIVERED'
                                    and aa.order_date>'%s' and aa.order_date<'%s'
                                    and cc.state is not null
                                    __CLIENT_FILTER__
                                    group by cc.state
                                    order by order_count DESC"""

select_transit_delays_query = """select * from (select aa.id, aa.channel_order_id, aa.status, bb.awb, cc.courier_name, dd.status_time as shipped_date,
                                  bb.pdd, (now()::date-bb.pdd::date) as delayed_by_days, bb.zone, ee.status_time as last_scan_time, 
                                  aa.customer_name, aa.customer_phone, aa.customer_email from orders aa
                                  left join shipments bb on aa.id=bb.order_id
                                  left join master_couriers cc on bb.courier_id=cc.id
                                  left join orders_payments gg on gg.order_id=aa.id
                                  left join (select * from order_status where status='Picked') dd on aa.id=dd.order_id
                                  left join (select order_id, max(status_time) as status_time from order_status group by order_id) ee on aa.id=ee.order_id
                                  left join (select * from order_status where status='Returned') ff on aa.id=ff.order_id
                                  where bb.pdd is not null
                                  and aa.status in ('IN TRANSIT', 'DISPATCHED', 'PENDING')
                                  and aa.status_type not in ('RT', 'DL')
                                  __MODE_FILTER__
                                  __CLIENT_FILTER__
                                  and ff.id is null) xx
                                  where delayed_by_days>0
                                  order by __ORDER_BY__ __ORDER_TYPE__
                                  __PAGINATION__"""


select_rto_delays_query = """select * from (select aa.id, aa.channel_order_id, aa.status, bb.awb, cc.courier_name, ff.status_time as return_mark_date,
               				 (now()::date-ff.status_time::date)-10 as delayed_by_days, bb.zone, ee.status_time as last_scan_time, 
                              aa.customer_name, aa.customer_phone, aa.customer_email from orders aa
                              left join shipments bb on aa.id=bb.order_id
                              left join master_couriers cc on bb.courier_id=cc.id
                              left join orders_payments gg on gg.order_id=aa.id
                              left join (select order_id, max(status_time) as status_time from order_status group by order_id) ee on aa.id=ee.order_id
                              left join (select * from order_status where status='Returned') ff on aa.id=ff.order_id
                              where ff.id is not null
                              and aa.status not in ('RTO', 'DELIVERED', 'DTO', 'DAMAGED', 'LOST', 'SHORTAGE', 'NOT SHIPPED', 'DISPATCHED')
                              __MODE_FILTER__
                              __CLIENT_FILTER__            
                              ) xx
                              where delayed_by_days>10
                              order by __ORDER_BY__ __ORDER_TYPE__
                              __PAGINATION__"""

select_ndr_reason_query = """select bb.reason, count(*) as total_count, sum(case when aa.current_status='reattempt' then 1 else 0 end) as reattempt_requested, 
                            sum(case when aa.current_status='cancelled' then 1 else 0 end) as cancellation_confirmed, 
                            sum(case when cc.status='DISPATCHED' then 1 else 0 end) as current_out_for_delivery
                            from (select distinct on (order_id) order_id, reason_id, current_status from ndr_shipments
                            order by order_id DESC, id DESC) aa
                            left join ndr_reasons bb on aa.reason_id=bb.id
                            left join orders cc on aa.order_id=cc.id
                            where cc.status in ('PENDING', 'DISPATCHED')
                            and (cc.status_type='UD' or cc.status_type is null)
                            __CLIENT_FILTER__ 
                            group by bb.reason
                            order by total_count DESC"""

select_ndr_reason_orders_query = """select cc.channel_order_id, cc.status, dd.awb, ee.courier_name, aa.current_status, 
                                    ff.verified_via, attempt_count, bb.reason, defer_dd, updated_add, updated_phone, cc.customer_phone from
                                    (select distinct on (order_id) order_id, reason_id, current_status, defer_dd, updated_add, updated_phone from ndr_shipments
                                    order by order_id DESC, id DESC) aa
                                    left join (select order_id, count(*) as attempt_count from ndr_shipments group by order_id) gg on gg.order_id=aa.order_id
                                    left join ndr_reasons bb on aa.reason_id=bb.id
                                    left join orders cc on aa.order_id=cc.id
                                    left join shipments dd on aa.order_id=dd.order_id
                                    left join master_couriers ee on dd.courier_id=ee.id
                                    left join ndr_verification ff on aa.order_id=ff.order_id
                                    where cc.status in ('PENDING', 'DISPATCHED')
                                    and (cc.status_type='UD' or cc.status_type is null)
                                    __CLIENT_FILTER__
                                    order by cc.order_date"""

select_serviceable_couriers_orders = """
SELECT
    courier_name,
    id,
    pickup,
    CASE WHEN (payment_mode ILIKE 'cod') THEN
        cod_available
    WHEN (payment_mode ILIKE 'pickup') THEN
        reverse_pickup
    ELSE
        serviceable
    END AS delivery
FROM (
    SELECT
        aa.id,
        aa.pickup_pincode,
        aa.delivery_pincode,
        dd.courier_name,
        aa.payment_mode,
        bool_or(pickup) AS pickup,
        bool_or(serviceable) AS serviceable,
        bool_or(cod_available) AS cod_available,
        bool_or(reverse_pickup) AS reverse_pickup
    FROM (
        SELECT
            aa.id,
            cc.pincode::varchar AS pickup_pincode,
            ee.pincode AS delivery_pincode,
            ff.payment_mode
        FROM
            orders aa
        LEFT JOIN client_pickups bb ON aa.pickup_data_id = bb.id
        LEFT JOIN pickup_points cc ON bb.pickup_id = cc.id
        LEFT JOIN shipping_address ee ON ee.id = aa.delivery_address_id
        LEFT JOIN orders_payments ff ON ff.order_id = aa.id
    WHERE
        aa.id IN __ORDER_IDS__) aa
    LEFT JOIN (
        SELECT
            aa.pincode,
            bb.courier_name,
            pickup
        FROM
            pincode_serviceability aa
            LEFT JOIN master_couriers bb ON aa.courier_id = bb.id) dd ON aa.pickup_pincode = dd.pincode
        LEFT JOIN (
            SELECT
                aa.pincode,
                bb.courier_name,
                serviceable,
                cod_available,
                reverse_pickup
            FROM
                pincode_serviceability aa
                LEFT JOIN master_couriers bb ON aa.courier_id = bb.id) ff ON aa.delivery_pincode = ff.pincode
        WHERE
            dd.courier_name = ff.courier_name
        GROUP BY
            aa.id,
            aa.pickup_pincode,
            aa.delivery_pincode,
            dd.courier_name,
            aa.payment_mode) xx
"""

inventory_analytics_query = """
SELECT
    *
FROM (
    SELECT
        aa.client_prefix,
        aa.master_product_id,
        aa.sku,
        aa.product_name,
        aa.warehouse_prefix,
        SUM(aa.available_quantity) available_quantity,
        SUM(aa.sales) sales,
        SUM(COALESCE(cc.ro_quantity, 0) - COALESCE(cc.received_quantity, 0)) in_transit_quantity,
        MIN(bb.edd) ead
    FROM ((
            SELECT
                aa.client_prefix,
                aa.id master_product_id,
                aa.sku,
                aa.name product_name,
                bb.warehouse_prefix,
                bb.available_quantity available_quantity,
                CAST(NULL AS int) sales
            FROM
                master_products aa
                LEFT JOIN products_quantity bb ON aa.id = bb.product_id
            WHERE
                aa.client_prefix = '{0}'
        )
        UNION ALL (
            SELECT
                aa.client_prefix,
                aa.id master_product_id,
                aa.sku,
                aa.name product_name,
                ee.warehouse_prefix,
                CAST(NULL AS int) available_quantity,
                COUNT(bb.order_id) sales
            FROM
                master_products aa
                LEFT JOIN op_association bb ON aa.id = bb.master_product_id
                LEFT JOIN orders cc ON bb.order_id = cc.id
                LEFT JOIN client_pickups dd ON cc.pickup_data_id = dd.id
                LEFT JOIN pickup_points ee ON dd.pickup_id = ee.id
            WHERE
                aa.client_prefix = '{0}'
                AND cc.order_date >= '{1}'
                AND cc.order_date <= '{2}'
                AND cc.pickup_data_id IS NOT NULL
            GROUP BY
                aa.id,
                aa.client_prefix,
                aa.name,
                aa.sku,
                dd.id,
                ee.warehouse_prefix
        )) aa
        LEFT JOIN (
            SELECT
                *
            FROM
                warehouse_ro
            WHERE
                status = 'awaiting'
        ) bb ON aa.client_prefix = bb.client_prefix AND aa.warehouse_prefix = bb.warehouse_prefix
        LEFT JOIN products_wro cc ON aa.master_product_id = cc.master_product_id AND bb.id = cc.wro_id
    GROUP BY
        aa.client_prefix,
        aa.master_product_id,
        aa.sku,
        aa.product_name,
        aa.warehouse_prefix
) aa
WHERE
    NOT (aa.warehouse_prefix IS NULL
        AND aa.available_quantity = 0
        AND aa.sales IS NULL
        AND aa.in_transit_quantity = 0)
    __WAREHOUSE_FILTER__
    __STOCK_OUT_FILTER__
    __OVER_STOCK_FILTER__
    __BEST_SELLER_FILTER__
    __SEARCH_KEY_FILTER__
__SORT_BY__
__PAGINATION__
"""

inventory_analytics_filters_query = """
SELECT
    aa.warehouse_prefix,
    COUNT(DISTINCT (aa.sku)) product_count
FROM ((
        SELECT
            cc.warehouse_prefix,
            aa.sku sku
        FROM
            master_products aa
            LEFT JOIN products_wro bb ON aa.id = bb.master_product_id
            LEFT JOIN warehouse_ro cc ON bb.wro_id = cc.id
        WHERE
            aa.client_prefix = '{0}'
        GROUP BY
            cc.warehouse_prefix,
            aa.sku
    )
    UNION ALL (
        SELECT
            ee.warehouse_prefix,
            aa.sku sku
        FROM
            master_products aa
            LEFT JOIN op_association bb ON aa.id = bb.master_product_id
            LEFT JOIN orders cc ON bb.order_id = cc.id
            LEFT JOIN client_pickups dd ON cc.pickup_data_id = dd.id
            LEFT JOIN pickup_points ee ON dd.pickup_id = ee.id
        WHERE
            aa.client_prefix = '{0}'
            AND cc.pickup_data_id IS NOT NULL
        GROUP BY
            ee.warehouse_prefix,
            aa.sku)
) aa
WHERE
    aa.warehouse_prefix IS NOT NULL
GROUP BY
    aa.warehouse_prefix
"""

inventory_analytics_in_transit_query = """
SELECT 
    aa.client_prefix,
    aa.id master_product_id,
    aa.sku,
    aa.name product_name,
    bb.warehouse_prefix,
    SUM(COALESCE(cc.ro_quantity, 0) - COALESCE(cc.received_quantity, 0)) in_transit_quantity,
    MIN(bb.edd) ead
FROM
    master_products aa  
    LEFT JOIN warehouse_ro bb ON aa.client_prefix = bb.client_prefix
    LEFT JOIN products_wro cc ON aa.id = cc.master_product_id AND bb.id = cc.wro_id
WHERE
    aa.client_prefix = '{0}' AND bb.status = 'awaiting'
    __WAREHOUSE_FILTER__
    __SEARCH_KEY_FILTER__
GROUP BY
    aa.client_prefix,
    aa.id,
    aa.sku,
    aa.name,
    bb.warehouse_prefix
ORDER BY
	ead ASC NULLS LAST,
    in_transit_quantity DESC
__PAGINATION__
"""
