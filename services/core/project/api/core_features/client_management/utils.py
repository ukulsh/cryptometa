from project.api.models import CostToClients, MasterCouriers


def get_cost_to_clients(posted_data):
    client_prefix = posted_data.get('client_prefix')
    courier_data = posted_data.get('courier_data') or []
    cost_to_client_ref_data = []
    for it in courier_data:
        courier_name = it.get('courier_name')
        courier_ref = MasterCouriers.query.filter_by(courier_name=courier_name).first()
        zone_a = it.get('zone_a')
        zone_b = it.get('zone_b')
        zone_c = it.get('zone_c')
        zone_d = it.get('zone_d')
        zone_e = it.get('zone_e')
        a_step = it.get('a_step')
        b_step = it.get('b_step')
        c_step = it.get('c_step')
        d_step = it.get('d_step')
        e_step = it.get('e_step')
        cod_min = it.get('cod_min')
        cod_ratio = it.get('cod_ratio')
        rvp_ratio = it.get('rvp_ratio')
        rto_ratio = it.get('rto_ratio')
        management_fee = it.get('management_fee')
        cost_to_client_ref = CostToClients(client_prefix, courier_ref.id, zone_a, zone_b, zone_c, zone_d, zone_e, a_step,
                                           b_step, c_step, d_step, e_step, cod_min, cod_ratio, rvp_ratio, rto_ratio,
                                           management_fee)
        cost_to_client_ref_data.append(cost_to_client_ref)
    return cost_to_client_ref_data
