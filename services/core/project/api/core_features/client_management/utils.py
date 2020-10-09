from project.api.models import CostToClients, MasterCouriers


def get_cost_to_clients(posted_data):
    client_prefix = posted_data.get('client_prefix')
    courier_name = posted_data.get('courier_name')
    courier_ref = MasterCouriers.query.filter_by(courier_name=courier_name).first()
    zone_a = posted_data.get('zone_a')
    zone_b = posted_data.get('zone_b')
    zone_c = posted_data.get('zone_c')
    zone_d = posted_data.get('zone_d')
    zone_e = posted_data.get('zone_e')
    a_step = posted_data.get('a_step')
    b_step = posted_data.get('b_step')
    c_step = posted_data.get('c_step')
    d_step = posted_data.get('d_step')
    e_step = posted_data.get('e_step')
    cod_min = posted_data.get('cod_min')
    cod_ratio = posted_data.get('cod_ratio')
    rvp_ratio = posted_data.get('rvp_ratio')
    cost_to_client_ref = CostToClients(client_prefix, courier_ref.id, zone_a, zone_b, zone_c, zone_d, zone_e, a_step,
                                       b_step, c_step, d_step, e_step, cod_min, cod_ratio, rvp_ratio)
    return cost_to_client_ref
