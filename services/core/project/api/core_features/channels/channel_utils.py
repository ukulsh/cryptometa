from project.api.models import ClientChannel


def get_channel_integration_object(post_data, client_prefix,  channel_id):
    store_name = post_data.get('store_name')
    api_key = post_data.get('api_key').strip()
    api_password = post_data.get('api_password').strip()
    shop_url = post_data.get('shop_url').strip()
    shared_secret = post_data.get('shared_secret')
    mark_shipped = post_data.get('mark_shipped')
    shipped_status = post_data.get('shipped_status')
    mark_invoiced = post_data.get('mark_invoiced')
    invoiced_status = post_data.get('invoiced_status')
    mark_canceled = post_data.get('mark_canceled')
    canceled_status = post_data.get('canceled_status')
    mark_delivered = post_data.get('mark_delivered')
    delivered_status = post_data.get('delivered_status')
    fetch_status = post_data.get('fetch_status')
    sync_inventory = post_data.get('sync_inventory')
    mark_returned = post_data.get('mark_returned')
    returned_status = post_data.get('returned_status')
    if not isinstance(fetch_status, list):
        fetch_status = []
    channel_int = ClientChannel(client_prefix=client_prefix, store_name=store_name, channel_id=channel_id, api_key=api_key,
                                api_password=api_password, shop_url=shop_url, shared_secret=shared_secret,
                                mark_shipped=mark_shipped, shipped_status=shipped_status, mark_invoiced=mark_invoiced,
                                invoiced_status=invoiced_status, mark_canceled=mark_canceled, canceled_status=canceled_status,
                                mark_delivered=mark_delivered, delivered_status=delivered_status, mark_returned=mark_returned,
                                returned_status=returned_status, sync_inventory=sync_inventory, fetch_status=fetch_status)
    return channel_int