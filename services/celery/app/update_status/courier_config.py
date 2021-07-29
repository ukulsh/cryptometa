def delhivery_status_mapper(scan):
    to_record_status = ""
    if scan['ScanDetail']['Scan'] == "Manifested" \
            and scan['ScanDetail']['Instructions'] == "Consignment Manifested":
        to_record_status = "Received"
    elif scan['ScanDetail']['Scan'] == "In Transit" \
            and "picked" in str(scan['ScanDetail']['Instructions']).lower():
        to_record_status = "Picked"
    elif scan['ScanDetail']['Scan'] == "In Transit" \
            and scan['ScanDetail']['StatusCode']=='EOD-77':
        to_record_status = "Picked RVP"
    elif scan['ScanDetail']['Scan'] == "In Transit" \
            and scan['ScanDetail']['ScanType'] == "UD":
        to_record_status = "In Transit"
    elif scan['ScanDetail']['Scan'] == "In Transit" \
            and scan['ScanDetail']['ScanType'] == "PU":
        to_record_status = "In Transit"
    elif scan['ScanDetail']['Scan'] == "Dispatched" \
            and scan['ScanDetail']['ScanType'] == "PU":
        to_record_status = "Dispatched for DTO"
    elif scan['ScanDetail']['Scan'] == "Dispatched" \
            and scan['ScanDetail']['Instructions'] == "Out for delivery":
        to_record_status = "Out for delivery"
    elif scan['ScanDetail']['Scan'] == "Delivered":
        to_record_status = "Delivered"
    elif scan['ScanDetail']['Scan'] == "Pending" \
            and scan['ScanDetail'][
        'Instructions'] == "Customer Refused to accept/Order Cancelled":
        to_record_status = "Cancelled"
    elif scan['ScanDetail']['ScanType'] == "RT":
        to_record_status = "Returned"
    elif scan['ScanDetail']['Scan'] == "RTO":
        to_record_status = "RTO"
    elif scan['ScanDetail']['Scan'] == "DTO":
        to_record_status = "DTO"
    elif scan['ScanDetail']['Scan'] == "Canceled":
        to_record_status = "Canceled"
    
    return to_record_status

def xpressbees_status_mapper(scan, flags):
    to_record_status = ""
    if scan['StatusCode'] == "DRC":
        to_record_status = "Received"
    elif scan['StatusCode'] == "PUD" or (scan['StatusCode'] == "PKD" and scan.get('PickUpTime')):
        to_record_status = "Picked"
        flags['order_picked_check'] = True
    elif scan['StatusCode'] in ("IT", "RAD"):
        to_record_status = "In Transit"
        flags['order_picked_check'] = True
    elif scan['StatusCode'] == "OFD":
        to_record_status = "Out for delivery"
    elif scan['StatusCode'] == "DLVD":
        to_record_status = "Delivered"
    elif scan['StatusCode'] == "UD" and scan['Status'] in \
            ("Consignee Refused To Accept", "Consignee Refused to Pay COD Amount"):
        to_record_status = "Cancelled"
    elif scan['StatusCode'] == "RTO":
        to_record_status = "Returned"
    elif scan['StatusCode'] == "RTD":
        to_record_status = "RTO"
    
    return to_record_status, flags

def xpressbees_ndr_mapper(requested_order):
    ndr_reason = None
    if requested_order['ShipmentSummary'][0]['Status'].lower() in config['Xpressbees']['ndr_reasons']:
        ndr_reason = config['Xpressbees']['ndr_reasons'][requested_order['ShipmentSummary'][0]['Status'].lower()]
    elif "future delivery" in requested_order['ShipmentSummary'][0]['Status'].lower():
        ndr_reason = 4
    elif "evening delivery" in requested_order['ShipmentSummary'][0]['Status'].lower():
        ndr_reason = 4
    elif "open delivery" in requested_order['ShipmentSummary'][0]['Status'].lower():
        ndr_reason = 10
    elif "address incomplete" in requested_order['ShipmentSummary'][0]['Status'].lower():
        ndr_reason = 2
    elif "amount not ready" in requested_order['ShipmentSummary'][0]['Status'].lower():
        ndr_reason = 15
    elif "customer not available" in requested_order['ShipmentSummary'][0]['Status'].lower():
        ndr_reason = 1
    elif "entry not permitted" in requested_order['ShipmentSummary'][0]['Status'].lower():
        ndr_reason = 7
    elif "customer refused to accept" in requested_order['ShipmentSummary'][0]['Status'].lower():
        ndr_reason = 3
    else:
        ndr_reason = 14
    
    return ndr_reason

config = {
    "Delhivery": {
        'status_url': "https://track.delhivery.com/api/status/packages/json/?waybill=%s&token=%s",
        'status_mapper_fn': delhivery_status_mapper,
        'status_time_format': '%Y-%m-%dT%H:%M:%S',
        'edd_time_format': '%Y-%m-%dT%H:%M:%S',
        'status_mapping': {
            "DLYDC-107": 6,
            "DLYDC-110": 4,
            "DLYDC-132": 8,
            "EOD-104": 7,
            "EOD-11": 1,
            "EOD-111": 11,
            "EOD-3": 4,
            "EOD-40": 9,
            "EOD-6": 3,
            "EOD-69": 11,
            "EOD-74": 2,
            "EOD-86": 12,
            "FMEOD-106": 12,
            "FMEOD-118": 3,
            "RDPD-17": 12,
            "RT-101": 12,
            "ST-108": 13,
        },
    },
    "Xpressbees": {
        'status_url': "http://xbclientapi.xbees.in/TrackingService.svc/GetShipmentSummaryDetails",
        'status_mapper_fn': xpressbees_status_mapper,
        'ndr_mapper_fn': xpressbees_ndr_mapper,
        'status_time_format': '%d-%m-%YT%H%M',
        'edd_time_format': '%m/%d/%Y %I:%M:%S %p',
        'status_mapping': {
            "DRC": ("READY TO SHIP", "UD", ""),
            "PUC": ("READY TO SHIP", "UD", ""),
            "OFP": ("READY TO SHIP", "UD", ""),
            "PUD": ("IN TRANSIT", "UD", ""),
            "PND": ("READY TO SHIP", "UD", ""),
            "PKD": ("IN TRANSIT", "UD", ""),
            "IT": ("IN TRANSIT", "UD", ""),
            "RAD": ("IN TRANSIT", "UD", ""),
            "OFD": ("DISPATCHED", "UD", ""),
            "RTON": ("IN TRANSIT", "RT", ""),
            "RTO": ("IN TRANSIT", "RT", ""),
            "RTO-IT": ("IN TRANSIT", "RT", ""),
            "RAO": ("IN TRANSIT", "RT", ""),
            "RTU": ("IN TRANSIT", "RT", ""),
            "RTO-OFD": ("DISPATCHED", "RT", ""),
            "STD": ("DAMAGED", "UD", ""),
            "STG": ("SHORTAGE", "UD", ""),
            "RTO-STG": ("SHORTAGE", "RT", ""),
            "DLVD": ("DELIVERED", "DL", ""),
            "RTD": ("RTO", "DL", ""),
            "LOST": ("LOST", "UD", ""),
            "UD": ("PENDING", "UD", "")
        },
        'ndr_reasons': {
            "customer refused to accept": 3,
            "consignee refused to accept": 3,
            "customer refused to pay cod amount": 9,
            "add incomplete/incorrect & mobile not reachable": 1,
            "add incomplete/incorrect": 2,
            "customer not available & mobile not reachable": 1,
            "customer not available": 1,
            "consignee not available": 1,
            "oda (out of delivery area)": 8
        },
    },
}