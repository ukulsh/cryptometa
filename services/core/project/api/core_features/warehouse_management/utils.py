from project.api.models import PickupPoints, ReturnPoints, ClientPickups


def parse_client_mapping(data, client_prefix):
    objects_to_create = []
    for iterator in data:
        existence_ref = PickupPoints.query.filter_by(warehouse_prefix=iterator.get('pickup_warehouse_prefix')).first()
        if existenc_ref:
            raise Exception("WareHouse Pickups prefix is already existing..")

        ref_pickup = PickupPoints(iterator.get('pickup_location'), iterator.get('pickup_name'), iterator.get('pickup_phone'),
                                  iterator.get('pickup_address'), iterator.get('pickup_address_two'), iterator.get('pickup_city'),
                                  iterator.get('pickup_state'), iterator.get('pickup_country'),  iterator.get('pickup_pincode'),
                                  iterator.get('pickup_warehouse_prefix'))
        ref_return = ReturnPoints(iterator.get('return_location'), iterator.get('return_name'), iterator.get('return_phone'),
                                  iterator.get('return_address'), iterator.get('return_address_two'), iterator.get('return_city'),
                                  iterator.get('return_state'), iterator.get('return_country'), iterator.get('return_pincode'),
                                  iterator.get('return_warehouse_prefix'))
        existence_ref = ReturnPoints.query.filter_by(warehouse_prefix=iterator.get('return_warehouse_prefix')).fist()
        if existenc_ref:
            raise Exception("WareHouse Returns prefix is already existing..")
        objects_to_create.append([ref_pickup, ref_return, iterator.get('gstin')])
    return objects_to_create
