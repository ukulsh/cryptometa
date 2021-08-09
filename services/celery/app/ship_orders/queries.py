fetch_client_couriers_query = """select aa.id,aa.client_prefix,aa.courier_id,aa.priority,aa.last_shipped_order_id,
                                aa.last_shipped_time,aa.date_created,aa.date_updated,aa.unique_parameter,bb.id,
                                bb.courier_name,bb.logo_url,bb.date_created,bb.date_updated,bb.api_key,bb.api_password,bb.api_url
		                        from client_couriers aa
                                left join master_couriers bb
                                on aa.courier_id=bb.id
                                left join client_mapping cc 
                                on aa.client_prefix=cc.client_prefix
                                where aa.active=true
                                and (cc.account_type != 'prepaid' or cc.current_balance>=500) 
                                __CLIENT_FILTER__ 
                                order by aa.client_prefix, priority;"""

get_pickup_points_query = """select aa.id, aa.pickup_id, aa.return_point_id, 
                                bb.phone, bb.address, bb.address_two, bb.city,
                                bb.country, bb.pincode, bb.warehouse_prefix, bb.state, bb.name,
                                cc.phone, cc.address, cc.address_two, cc.city,
                                cc.country, cc.pincode, cc.warehouse_prefix, cc.state, cc.name, 
                                aa.enable_sdd, aa.invoice_last, aa.invoice_prefix, bb.latitude, bb.longitude
                                from client_pickups aa
                                left join pickup_points bb
                                on aa.pickup_id=bb.id
                                left join return_points cc
                                on aa.return_point_id=cc.id
                                where aa.id=%s"""

get_orders_to_ship_query = """select distinct on (aa.id) aa.id,aa.channel_order_id,aa.order_date,aa.customer_name,aa.customer_email,aa.customer_phone,
                                aa.date_created,aa.date_updated,aa.status,aa.client_prefix,aa.client_channel_id,aa.delivery_address_id,
                                cc.id,cc.first_name,cc.last_name,cc.address_one,cc.address_two,cc.city,cc.pincode,cc.state,cc.country,cc.phone,
                                cc.latitude,cc.longitude,cc.country_code,dd.id,dd.payment_mode,dd.amount,dd.currency,dd.order_id,dd.shipping_charges,
                                dd.subtotal,dd.order_id,ee.dimensions,ee.weights,ee.quan, ff.api_key, ff.api_password, 
                                ff.shop_url, aa.order_id_channel_unique, ee.products_name, aa.pickup_data_id, xx.cod_verified, 
                                xx.id, ee.ship_courier, gg.location_id, ff.channel_id, yy.verify_cod, yy.essential, ee.subcategories, 
                                yy.cod_ship_unconfirmed, yy.client_name, aa.chargeable_weight, yy.cod_man_ver, zz.id, 
                                ff.unique_parameter, ff.id, ppa.warehouse_prefix, ppa.pincode, ee.products_sku
                                from orders aa
                                left join shipping_address cc
                                on aa.delivery_address_id=cc.id
                                left join orders_payments dd
                                on dd.order_id=aa.id
                                left join 
                                (select order_id, array_agg(dimensions) as dimensions, array_agg(weight) as weights, 
                                array_agg(quantity) as quan, array_agg(pp.name) as products_name, 
                                array_agg(1) as ship_courier,
                                array_agg(qq.name ORDER BY pp.weight DESC) as subcategories,
                                array_agg(pp.sku) as products_sku
                                 from op_association opa 
                                 left join master_products pp
                                 on opa.master_product_id = pp.id
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
                                left join client_pickups cpa
                                on cpa.id=aa.pickup_data_id
                                left join pickup_points ppa
                                on ppa.id=cpa.pickup_id
                                left join (select * from orders_invoice where cancelled is not true) zz
                                on zz.order_id=aa.id
                                where aa.client_prefix=%s
                                and aa.status='NEW'
                                __ORDER_SELECT_FILTERS__
                                and NOT EXISTS (SELECT 1 FROM unnest(ee.weights) x WHERE x IS NULL)
                                and (xx.id is null or (xx.id is not null and xx.cod_verified = true) 
                                     or (yy.cod_ship_unconfirmed=true and aa.order_date<(NOW() - interval '1 day')))
                                order by aa.id"""

update_last_shipped_order_query = """UPDATE client_couriers SET last_shipped_order_id=%s, last_shipped_time=%s WHERE client_prefix=%s"""

update_orders_status_query = """UPDATE orders SET status='READY TO SHIP' WHERE id in %s;"""

delete_failed_shipments_query = """DELETE FROM 	order_status where shipment_id in 
                                    (select id  from shipments where remark like 'Crashing while saving package%' or remark like 'COD%');
                                    delete  from shipments where remark like 'Crashing while saving package%' or remark like 'COD%';"""

update_same_state_query = """update shipments aa
                                set same_state=true
                                from orders bb 
                                left join shipping_address cc on bb.delivery_address_id=cc.id
                                left join client_pickups dd on dd.id=bb.pickup_data_id
                                left join pickup_points ee on dd.pickup_id=ee.id
                                left join pincode_mapping ff on ee.pincode::varchar=ff.pincode
                                left join pincode_mapping gg on cc.pincode=gg.pincode
                                where bb.id=aa.order_id
                                and aa.same_state is null 
                                and ff.state=gg.state"""

fetch_client_shipping_rules_query = """select aa.id,aa.client_prefix,aa.rule_name,aa.priority,aa.condition_type,
                                        aa.conditions,aa.courier_1_id,aa.courier_2_id,aa.courier_3_id,aa.courier_4_id,
                                        bb.courier_name as courier_1_name, dd.courier_name as courier_2_name,
                                        ee.courier_name as courier_3_name, ff.courier_name as courier_4_name
                                        from shipping_rules aa
                                        left join master_couriers bb
                                        on aa.courier_1_id=bb.id
                                        left join master_couriers dd
                                        on aa.courier_2_id=dd.id
                                        left join master_couriers ee
                                        on aa.courier_3_id=ee.id
                                        left join master_couriers ff
                                        on aa.courier_4_id=ff.id
                                        left join client_mapping cc
                                        on aa.client_prefix=cc.client_prefix
                                        where aa.active=true
                                        and (cc.account_type != 'prepaid' or cc.current_balance>=500) 
                                        __CLIENT_FILTER__ 
                                        order by aa.client_prefix, priority;"""