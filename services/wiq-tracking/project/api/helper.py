def get_courier_details(awb, cur):
    cur.execute(
        """
        SELECT bb.courier_name, bb.logo_url
        FROM shipments aa
        LEFT JOIN master_couriers bb on aa.courier_id=bb.id
        WHERE aa.awb=%s
        """,
        (awb,),
    )
    return cur.fetchone()


def check_subdomain_exists(subdomain, cur):
    cur.execute(
        """
            SELECT 
                client_prefix, client_logo_url, theme_color, background_image_url, 
                client_name, client_url, nav_links, support_url, privacy_url, nps_enabled, 
                banners  
            FROM client_customization 
            WHERE subdomain=%s
            """,
        (subdomain,),
    )
    client_details = cur.fetchone()

    return client_details
