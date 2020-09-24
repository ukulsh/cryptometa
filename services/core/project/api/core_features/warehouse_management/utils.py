from project.api.models import PickupPoints, ReturnPoints, ClientPickups


def parse_client_mapping(data):
    ref_pickup = PickupPoints(data.get('pickup_location'), data.get('pickup_name'), data.get('pickup_phone'),
                              data.get('pickup_address'), data.get('pickup_address_two'), data.get('pickup_city'),
                              data.get('pickup_state'), data.get('pickup_country'),  data.get('pickup_pincode'),
                              data.get('pickup_warehouse_prefix'))
    ref_return = ReturnPoints(data.get('return_location'), data.get('return_name'), data.get('return_phone'),
                              data.get('return_address'), data.get('return_address_two'), data.get('return_city'),
                              data.get('return_state'), data.get('return_country'), data.get('return_pincode'),
                              data.get('return_warehouse_prefix'))
    objects_to_create = [ref_pickup, ref_return, data.get('gstin')]
    return objects_to_create
