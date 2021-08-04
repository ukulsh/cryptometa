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
