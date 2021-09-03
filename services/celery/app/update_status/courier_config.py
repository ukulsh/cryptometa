from datetime import datetime, timedelta


def delhivery_status_mapper(scan):
    to_record_status = ""
    if scan["ScanDetail"]["Scan"] == "Manifested" and scan["ScanDetail"]["Instructions"] == "Consignment Manifested":
        to_record_status = "Received"
    elif scan["ScanDetail"]["Scan"] == "In Transit" and "picked" in str(scan["ScanDetail"]["Instructions"]).lower():
        to_record_status = "Picked"
    elif scan["ScanDetail"]["Scan"] == "In Transit" and scan["ScanDetail"]["StatusCode"] == "EOD-77":
        to_record_status = "Picked RVP"
    elif scan["ScanDetail"]["Scan"] == "In Transit" and scan["ScanDetail"]["ScanType"] == "UD":
        to_record_status = "In Transit"
    elif scan["ScanDetail"]["Scan"] == "In Transit" and scan["ScanDetail"]["ScanType"] == "PU":
        to_record_status = "In Transit"
    elif scan["ScanDetail"]["Scan"] == "Dispatched" and scan["ScanDetail"]["ScanType"] == "PU":
        to_record_status = "Dispatched for DTO"
    elif scan["ScanDetail"]["Scan"] == "Dispatched" and scan["ScanDetail"]["Instructions"] == "Out for delivery":
        to_record_status = "Out for delivery"
    elif scan["ScanDetail"]["Scan"] == "Delivered":
        to_record_status = "Delivered"
    elif (
        scan["ScanDetail"]["Scan"] == "Pending"
        and scan["ScanDetail"]["Instructions"] == "Customer Refused to accept/Order Cancelled"
    ):
        to_record_status = "Cancelled"
    elif scan["ScanDetail"]["ScanType"] == "RT":
        to_record_status = "Returned"
    elif scan["ScanDetail"]["Scan"] == "RTO":
        to_record_status = "RTO"
    elif scan["ScanDetail"]["Scan"] == "DTO":
        to_record_status = "DTO"
    elif scan["ScanDetail"]["Scan"] == "Canceled":
        to_record_status = "Canceled"

    return to_record_status


def xpressbees_status_mapper(scan, flags):
    to_record_status = ""
    if scan["StatusCode"] == "DRC":
        to_record_status = "Received"
    elif scan["StatusCode"] == "PUD" or (scan["StatusCode"] == "PKD" and scan.get("PickUpTime")):
        to_record_status = "Picked"
        flags["order_picked_check"] = True
    elif scan["StatusCode"] in ("IT", "RAD"):
        to_record_status = "In Transit"
        flags["order_picked_check"] = True
    elif scan["StatusCode"] == "OFD":
        to_record_status = "Out for delivery"
    elif scan["StatusCode"] == "DLVD":
        to_record_status = "Delivered"
    elif scan["StatusCode"] == "UD" and scan["Status"] in (
        "Consignee Refused To Accept",
        "Consignee Refused to Pay COD Amount",
    ):
        to_record_status = "Cancelled"
    elif scan["StatusCode"] == "RTO":
        to_record_status = "Returned"
    elif scan["StatusCode"] == "RTD":
        to_record_status = "RTO"

    return to_record_status, flags


def bluedart_status_mapper(scan, new_status, flags):
    to_record_status = ""
    if scan["ScanCode"] == "015" and not flags["is_return"]:
        to_record_status = "Picked"
    elif scan["ScanCode"] == "001" and not flags["is_return"]:
        to_record_status = "Picked"
    elif new_status == "IN TRANSIT" and scan["ScanType"] == "UD" and not flags["is_return"]:
        to_record_status = "In Transit"
    elif scan["ScanCode"] in ("002", "092") and not flags["is_return"]:
        to_record_status = "Out for delivery"
    elif scan["ScanCode"] in ("000", "090", "099") and not flags["is_return"]:
        to_record_status = "Delivered"
    elif scan["ScanType"] == "RT" and not flags["is_return"]:
        to_record_status = "Returned"
    elif scan["ScanCode"] == "000" and flags["is_return"]:
        to_record_status = "RTO"
    elif scan["ScanCode"] == "188" and scan["ScanType"] == "RT":
        to_record_status = "RTO"

    return to_record_status, flags


def ecom_status_mapper(scan, new_data, requested_order, status_time):
    to_record_status = ""
    if scan["reason_code_number"] == "0011":
        to_record_status = "Picked"
    elif scan["reason_code_number"] == "002":
        to_record_status = "Picked"
    elif scan["reason_code_number"] == "003":
        to_record_status = "In Transit"
    elif scan["reason_code_number"] == "006":
        to_record_status = "Out for delivery"
    elif scan["reason_code_number"] == "999":
        to_record_status = "Delivered"
    elif scan["reason_code_number"] == "777":
        to_record_status = "Returned"
    elif (
        requested_order.get("rts_reason_code_number")
        and requested_order.get("rts_last_update")
        and requested_order.get("rts_reason_code_number") == "999"
    ):
        to_record_status = "RTO"
        if requested_order["rts_last_update"]:
            status_time = requested_order["rts_last_update"]
            status_time = datetime.strptime(status_time, "%d %b, %Y, %H:%M")
        else:
            status_time = datetime.utcnow() + timedelta(hours=5.5)
        new_data["new_status"] = "RTO"
        new_data["status_type"] = "DL"

    return to_record_status, status_time


def dtdc_status_mapper(scan):
    to_record_status = ""
    #! Check if this is correct map
    if scan["strCode"] == "BKD":
        to_record_status = "Ready to ship"
    elif scan["strCode"] == "OUTDLV":
        to_record_status = "Out for delivery"
    elif scan["strCode"] == "DLV":
        to_record_status = "Delivered"
    elif scan["strCode"].startswith("RTO"):
        to_record_status = "RTO"
    else:
        to_record_status = "In Transit"

    return to_record_status


def xpressbees_ndr_mapper(requested_order):
    ndr_reason = None
    if requested_order["ShipmentSummary"][0]["Status"].lower() in config["Xpressbees"]["ndr_reasons"]:
        ndr_reason = config["Xpressbees"]["ndr_reasons"][requested_order["ShipmentSummary"][0]["Status"].lower()]
    elif "future delivery" in requested_order["ShipmentSummary"][0]["Status"].lower():
        ndr_reason = 4
    elif "evening delivery" in requested_order["ShipmentSummary"][0]["Status"].lower():
        ndr_reason = 4
    elif "open delivery" in requested_order["ShipmentSummary"][0]["Status"].lower():
        ndr_reason = 10
    elif "address incomplete" in requested_order["ShipmentSummary"][0]["Status"].lower():
        ndr_reason = 2
    elif "amount not ready" in requested_order["ShipmentSummary"][0]["Status"].lower():
        ndr_reason = 15
    elif "customer not available" in requested_order["ShipmentSummary"][0]["Status"].lower():
        ndr_reason = 1
    elif "entry not permitted" in requested_order["ShipmentSummary"][0]["Status"].lower():
        ndr_reason = 7
    elif "customer refused to accept" in requested_order["ShipmentSummary"][0]["Status"].lower():
        ndr_reason = 3
    else:
        ndr_reason = 14

    return ndr_reason


# TODO Make all keys in config uniform across delivery partners
config = {
    "Delhivery": {
        "auth_token_api": None,
        "status_url": "https://track.delhivery.com/api/status/packages/json/?waybill=%s&token=%s",
        "api_type": "bulk",
        "status_mapper_fn": delhivery_status_mapper,
        "ndr_mapper_fn": None,
        "status_time_format": "%Y-%m-%dT%H:%M:%S",
        "edd_time_format": "%Y-%m-%dT%H:%M:%S",
        "exotel_url": "https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend",
        "status_mapping": {
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
        "auth_token_api": None,
        "status_url": "http://xbclientapi.xbees.in/TrackingService.svc/GetShipmentSummaryDetails",
        "api_type": "bulk",
        "status_mapper_fn": xpressbees_status_mapper,
        "ndr_mapper_fn": xpressbees_ndr_mapper,
        "status_time_format": "%d-%m-%YT%H%M",
        "edd_time_format": "%m/%d/%Y %I:%M:%S %p",
        "exotel_url": "https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend",
        "status_mapping": {
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
            "UD": ("PENDING", "UD", ""),
        },
        "ndr_reasons": {
            "customer refused to accept": 3,
            "consignee refused to accept": 3,
            "customer refused to pay cod amount": 9,
            "add incomplete/incorrect & mobile not reachable": 1,
            "add incomplete/incorrect": 2,
            "customer not available & mobile not reachable": 1,
            "customer not available": 1,
            "consignee not available": 1,
            "oda (out of delivery area)": 8,
        },
    },
    "Bluedart": {
        "auth_token_api": None,
        "status_url": "https://api.bluedart.com/servlet/RoutingServlet?handler=tnt&action=custawbquery&loginid=HYD50082&awb=awb&numbers=%s&format=xml&lickey=eguvjeknglfgmlsi5ko5hn3vvnhoddfs&verno=1.3&scan=1",
        "api_type": "bulk",
        "status_mapper_fn": bluedart_status_mapper,
        "ndr_mapper_fn": None,
        "status_time_format": "%d-%b-%YT%H:%M",
        "edd_time_format": "%d %B %Y",
        "exotel_url": "https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend",
        "status_mapping": {
            "S": {
                "002": ("DISPATCHED", "UD", "SHIPMENT OUTSCAN"),
                "001": ("IN TRANSIT", "UD", "SHIPMENT INSCAN"),
                "003": ("IN TRANSIT", "UD", "SHIPMENT OUTSCANNED TO NETWORK"),
                "004": ("IN TRANSIT", "UD", "PLASTIC BAG INSCAN"),
                "005": ("IN TRANSIT", "UD", "POD SLIP INSCAN"),
                "006": ("IN TRANSIT", "UD", "UNDELIVERED INSCAN"),
                "007": ("IN TRANSIT", "UD", "CANVAS BAG CONSOLIDATED SCAN"),
                "008": ("IN TRANSIT", "UD", "OVERAGE DELETED"),
                "009": ("IN TRANSIT", "UD", "SHIPMENT-AUTOSCAN"),
                "010": ("IN TRANSIT", "UD", "SHIPMENT SHORTAGE"),
                "011": ("IN TRANSIT", "UD", "TRANSIT CONNECTION SCAN"),
                "012": ("IN TRANSIT", "UD", "PLASTIC BAG - AUTO TALLY"),
                "013": ("IN TRANSIT", "UD", "SHIPMENT CONNECTED THRU SCL APPLICATION"),
                "014": ("IN TRANSIT", "UD", "PAPER WORK INSCAN"),
                "015": ("IN TRANSIT", "UD", "PICK UP SCAN ON FIELD"),
                "020": ("IN TRANSIT", "UD", "DIRECT CANVAS BAG SCANNED"),
                "021": ("IN TRANSIT", "UD", "MIXED CANVAS BAG SCANNED"),
                "022": ("IN TRANSIT", "UD", "CANVAS BAG IN SCAN AT DESTINATION LOC"),
                "023": ("IN TRANSIT", "UD", "CLUBBED CANVAS BAG SCAN"),
                "024": ("IN TRANSIT", "UD", "UNDELIVERED SHIPMENT HELD AT LOCATION"),
                "025": ("IN TRANSIT", "UD", "SHIPMENT SCAN TALLIED/ SLAH TALLY"),
                "026": ("IN TRANSIT", "UD", "TRANSIT SCAN"),
                "027": ("IN TRANSIT", "UD", "LOAD/VEHICLE ARRIVED AT DELIVERY LOC"),
                "100": ("IN TRANSIT", "UD", "CANVAS BAG RECEIVED AS OVERAGE"),
                "106": ("IN TRANSIT", "UD", "CHANGE IN WEIGHT EFFECTED"),
            },
            "T": {
                "098": ("CANCELED", "DL", "TCL PICKUP CANCELLED"),
                "135": ("CONFISCATED", "DL", "SHPT.CONFISCATED,CASE CLOSED"),
                "129": ("DAMAGED", "UD", "DAMAGED SHIPMENT, CASE CLOSED"),
                "130": ("IN TRANSIT", "UD", "CONTACT CUSTOMER CARE"),
                "178": ("DAMAGED", "DL", "SHIPMENT SPOILED-SHIPPER RECONSTRUCTING"),
                "000": ("DELIVERED", "DL", "SHIPMENT DELIVERED"),
                "090": ("DELIVERED", "DL", "FORWARDED TO 3RD PARTY-NO POD AVAILABLE"),
                "099": ("DELIVERED", "DL", "MOVED TO HISTORY FILES"),
                "025": ("DESTROYED", "DL", "SHIPMENT DESTROYED/ABANDONED"),
                "070": ("DESTROYED", "DL", "ABANDONED/FORFEITED;DISPOSAL POLICY"),
                "141": ("DESTROYED", "DL", "SHIPMENT  DESTROYED/SENT FOR DISPOSAL"),
                "092": ("DISPATCHED", "UD", "SHIPMENT OUT FOR DELIVERY"),
                "027": ("IN TRANSIT", "RD", "SHIPMENT REDIRECTED ON FRESH AWB"),
                "028": ("IN TRANSIT", "UD", "RELEASED FROM CUSTOMS"),
                "029": ("IN TRANSIT", "UD", "DELIVERY  SCHEDULED FOR NEXT WORKING DAY"),
                "030": ("IN TRANSIT", "UD", "PKG HELD FOR TAXES"),
                "031": ("IN TRANSIT", "UD", "PACKAGE INTERCHANGED AT ORIGIN"),
                "032": ("IN TRANSIT", "UD", "PROCEDURAL DELAY IN DELIVERY EXPECTED"),
                "033": ("IN TRANSIT", "UD", "APX/SFC AWB RECD,SHIPMENT NOT RECEIVED"),
                "034": ("IN TRANSIT", "UD", "RTO SHPT HAL AS PER CUSTOMERS REQUEST"),
                "035": ("IN TRANSIT", "UD", "HANDED OVER TO AD-HOC/AGENT/SUB-COURIER"),
                "036": ("IN TRANSIT", "UD", "LATE ARRIVAL/SCHED. FOR NEXT WORKING DAY"),
                "037": ("IN TRANSIT", "UD", "PACKAGE WRONGLY ROUTED IN NETWORK"),
                "038": ("IN TRANSIT", "UD", "CLEARANCE PROCESS DELAYED"),
                "039": ("IN TRANSIT", "UD", "SHIPMENT INSPECTED FOR SECURITY PURPOSES"),
                "040": ("IN TRANSIT", "UD", "CNEE CUSTOMS BROKER NOTIFIED FOR CLRNCE"),
                "041": ("IN TRANSIT", "UD", "SHPT/PAPERWORK HANDED OVER TO CNEE BRKR"),
                "042": ("IN TRANSIT", "UD", "CNEE NAME / SURNAME MIS-MATCH"),
                "043": ("IN TRANSIT", "UD", "SHIPMENT RETURNED TO SHIPPER/ORIGIN"),
                "044": ("IN TRANSIT", "UD", "CNEE REFUSING TO PAY OCTROI/TAX/DEMURRAG"),
                "045": ("IN TRANSIT", "UD", "HELD FOR CLARITY ON HANDLING CHARGES"),
                "046": ("IN TRANSIT", "UD", "HELD AT PUD/HUB;REGULATORY PAPERWORK REQ"),
                "047": ("IN TRANSIT", "UD", "CONTENTS MISSING"),
                "048": ("IN TRANSIT", "UD", "MISROUTE DUE TO SHIPPER FAULT/WRONG PIN"),
                "049": ("IN TRANSIT", "UD", "MISROUTE DUE TO BDE FAULT"),
                "050": ("IN TRANSIT", "RD", "SHPT REDIRECTED ON SAME AWB"),
                "051": ("IN TRANSIT", "UD", "CHANGE IN MODE - AIR SHPT. BY SFC"),
                "052": ("IN TRANSIT", "UD", "MISSED CONNECTION"),
                "053": ("IN TRANSIT", "UD", "SHIPMENT SUB-COURIERED"),
                "054": ("IN TRANSIT", "UD", "NOT CONNECTED AS PER CUTOFF"),
                "055": ("IN TRANSIT", "UD", "SHIPMENT OFF-LOADED BY AIRLINE"),
                "056": ("IN TRANSIT", "UD", "P.O. BOX ADDRESS,UNABLE TO DELIVER"),
                "057": ("IN TRANSIT", "UD", "FLIGHT CANCELLED"),
                "058": ("IN TRANSIT", "UD", "MISROUTE;WRONG PIN/ZIP BY SHIPPER"),
                "059": ("IN TRANSIT", "UD", "COMM FLIGHT,VEH/TRAIN; DELAYED/CANCELLED"),
                "060": ("IN TRANSIT", "UD", "REDIRECTED ON SAME AWB TO SHIPPER"),
                "061": ("IN TRANSIT", "UD", "CMENT WITHOUT PINCODE;SHPR FAILURE"),
                "062": ("IN TRANSIT", "UD", "OCTROI/TAXES/CHEQUE/DD/COD AMT NOT READY"),
                "063": ("IN TRANSIT", "UD", "INCOMPLETE ST WAYBILL;DELIVERY DELAYED"),
                "064": ("IN TRANSIT", "UD", "HELD FOR DUTY/TAXES/FEES PAYMENT"),
                "065": ("IN TRANSIT", "UD", "IN TRANSIT"),
                "066": ("IN TRANSIT", "UD", "TIME CONSTRAINT;UNABLE TO DELIVER"),
                "067": ("IN TRANSIT", "UD", "TRANSPORT STRIKE"),
                "068": ("IN TRANSIT", "UD", "MISROUTE IN NETWORK"),
                "069": ("IN TRANSIT", "UD", "CNEE OFFICE CLOSED;UNABLE TO DELIVER"),
                "071": ("IN TRANSIT", "UD", "UNABLE TO DELIVER:DUE NATURAL DISASTER"),
                "072": ("IN TRANSIT", "UD", "FREIGHT SHIPMENT:RECD AT BOMBAY"),
                "073": ("IN TRANSIT", "UD", "CCU HUB;TRANSHIPMENT PERMIT AWAITED"),
                "075": ("IN TRANSIT", "UD", "SHIPMENT TRANSITED THRU DHL FACILITY"),
                "076": ("IN TRANSIT", "UD", "CREDIT CARD;CNEE REFUSING IDENTIFICATION"),
                "077": ("IN TRANSIT", "UD", "PACKAGE INTERCHANGED"),
                "078": ("IN TRANSIT", "UD", "SHP IMPOUNDED BY REGULATORY AUTHORITY"),
                "079": ("IN TRANSIT", "UD", "DELIVERY NOT ATTEMPTED AT DESTINATION"),
                "080": ("IN TRANSIT", "UD", "NOT CONNECTED, SPACE CONSTRAINT"),
                "081": ("IN TRANSIT", "UD", "INCOMPLETE CREDIT CARD POD"),
                "082": ("IN TRANSIT", "UD", "DELAY AT DESTINATION;POD AWAITED"),
                "083": ("IN TRANSIT", "UD", "SHIPMENT HELD IN NETWORK"),
                "084": ("IN TRANSIT", "UD", "ALL/PART/PACKAGING OF SHIPMENT DAMAGED"),
                "085": ("IN TRANSIT", "UD", "SCHEDULED FOR MOVEMENT IN NETWORK"),
                "086": ("IN TRANSIT", "UD", "SHPT DELIVERED/CNEE CONSIDERS DAMAGED"),
                "087": ("IN TRANSIT", "UD", "SHPT PROCESSED AT LOCATION"),
                "088": ("IN TRANSIT", "UD", "SHPT DEPARTED FM DHL FACILITY"),
                "089": ("IN TRANSIT", "UD", "SHPT REACHED DHL TRANSIT FACILITY"),
                "091": ("IN TRANSIT", "UD", "CONSIGNMENT PARTIALLY DELIVERED"),
                "093": ("IN TRANSIT", "UD", "DELIVERED TO WRONG ADDRESS AND RETRIEVED"),
                "094": ("IN TRANSIT", "UD", "SHIPMENT/PIECE MISSING"),
                "095": ("IN TRANSIT", "UD", "ADMIN OVERRIDE ON NSL FAILURES"),
                "096": ("IN TRANSIT", "UD", "LATE POD/STATUS UPDATE"),
                "097": ("IN TRANSIT", "UD", "DOD SHIPMENT DELIVERED, DD PENDING DELY."),
                "100": ("IN TRANSIT", "UD", "SHIPMENT CANT TRAVEL ON DESIRED MODE"),
                "101": ("IN TRANSIT", "UD", "APEX CONNECTED ON COMMERCIAL FLIGHT"),
                "102": ("IN TRANSIT", "UD", "DUTS IN DOX SHIPMENT"),
                "103": ("IN TRANSIT", "UD", "SHPT CANT TRAVEL ON DESIRED MODE"),
                "104": ("IN TRANSIT", "RT", "RETURN TO SHIPPER"),
                "105": ("IN TRANSIT", "RT", "SHIPMENT RETURNED BACK TO SHIPPER"),
                "106": ("IN TRANSIT", "UD", "LINEHAUL DELAYED; ACCIDENT/TRAFFIC-JAM"),
                "107": ("IN TRANSIT", "UD", "LINEHAUL DELAYED;TRAFFICJAM ENROUTE"),
                "110": ("IN TRANSIT", "UD", "DETAINED AT ORIGIN"),
                "111": ("IN TRANSIT", "UD", "SECURITY CLEARED"),
                "120": ("IN TRANSIT", "UD", "DELIVERY BY APPOINTMENT"),
                "121": ("IN TRANSIT", "UD", "SHIPMENT BOOKED FOR EMBARGO LOCATION"),
                "123": ("IN TRANSIT", "RT", "RTO FROM HUB ON FRESH AWB"),
                "132": ("IN TRANSIT", "RD", "CHANGE IN MODE/NEW AWB CUT"),
                "133": ("IN TRANSIT", "UD", "AWB INFORMATION MODIFIED"),
                "136": ("IN TRANSIT", "UD", "APEX TRANSIT ON COMM FLT;CCU HUB"),
                "140": ("IN TRANSIT", "UD", "SHIPMENT UNDER COOLING BY AIRLINE"),
                "142": ("IN TRANSIT", "UD", "SHIPMENT PARTIALLY DELIVERED"),
                "143": ("IN TRANSIT", "UD", "SPECIAL SHIPPER ODA DELV-DELAY EXPECTED"),
                "145": ("IN TRANSIT", "UD", "AWB WRONGLY INSCANNED"),
                "146": ("IN TRANSIT", "UD", "UNDER SECURITY INVESTIGATION"),
                "147": ("IN TRANSIT", "UD", "DP DUTS HELD AT CCU W/H"),
                "148": ("IN TRANSIT", "UD", "PLEASE CONTACT CUSTOMER SERVICE"),
                "149": ("IN TRANSIT", "UD", "CMENT WITHOUT PINCODE/DELIVERY DELAYED"),
                "150": ("IN TRANSIT", "UD", "CORRECTION OF WRONG POD DETAILS"),
                "151": ("IN TRANSIT", "UD", "AWAITING CNEE FEEDBACK TO SORRY CARD"),
                "152": ("IN TRANSIT", "UD", "ATTEMPT AT SECONDARY ADDRESS"),
                "154": ("IN TRANSIT", "UD", "SHPT DETAINED/SEIZED BY REGULATORY"),
                "155": ("IN TRANSIT", "UD", "CHECK IN SCAN"),
                "156": ("IN TRANSIT", "UD", "SHPT REACHED DHL DESTINATION LOCATION"),
                "157": ("IN TRANSIT", "UD", "MISCODE;DELIVERY DELAYED"),
                "159": ("IN TRANSIT", "UD", "SERVICE CHANGE;SHPT IN TRANSIT"),
                "160": ("IN TRANSIT", "UD", "SHPT U/D:NO SERVICE INCIDNET REPORTED"),
                "161": ("IN TRANSIT", "UD", "AWAITING CONX ON SCHEDULED FLT:IN TRANST"),
                "162": ("IN TRANSIT", "UD", "TRACE INITIATED"),
                "163": ("IN TRANSIT", "UD", "DHL TRACE CLOSED"),
                "166": ("IN TRANSIT", "UD", "CAPACITY CONSTRAINT; BULK DESPATCH"),
                "169": ("IN TRANSIT", "UD", "FLFM SHIPMENT;APEX/SFC MODE"),
                "170": ("IN TRANSIT", "UD", "FREIGHT SHIPMENT:AWAITING CUSTOMS P/W"),
                "171": ("IN TRANSIT", "UD", "FREIGHT SHPT:CUSTOMS CLEARANCE ON DATE"),
                "172": ("IN TRANSIT", "UD", "FREIGHT SHIPMENT:CLEARED CUSTOMS"),
                "173": ("IN TRANSIT", "UD", "SHIPMENT NOT LOCATED"),
                "174": ("IN TRANSIT", "UD", "SHIPMENT RECEIVED;PAPERWORK NOT RECEIVED"),
                "175": ("IN TRANSIT", "UD", "CONSIGNEE NOT AVAILABLE; CANT DELIVER"),
                "176": ("IN TRANSIT", "UD", "ATA/TP SHIPMENTS;DAY DEFERRED DELIVERY"),
                "177": ("IN TRANSIT", "UD", "SHIPMENT  DESTROYED/SENT FOR DISPOSAL"),
                "179": ("IN TRANSIT", "UD", "DC DESCREPANCY"),
                "180": ("IN TRANSIT", "UD", "DC RECEIVED FROM CNEE"),
                "181": ("IN TRANSIT", "UD", "ADMIN OVER-RIDE OF DC COUNT"),
                "182": ("IN TRANSIT", "UD", "POD/DC COPY SENT"),
                "183": ("IN TRANSIT", "UD", "POD/DC ACCURACY"),
                "184": ("IN TRANSIT", "UD", "SHIPMENT HANDEDOVER TO DHL"),
                "185": ("IN TRANSIT", "UD", "APEX / SFC SHPT OVERCARRIED IN NETWORK"),
                "186": ("IN TRANSIT", "UD", "APX/SFC SHPT MISPLACED AT DST/WAREHOUSE"),
                "187": ("IN TRANSIT", "UD", "DEMURRAGE CHARGES NOT READY"),
                "189": ("IN TRANSIT", "UD", "GSTN SITE NOT WORKING"),
                "190": ("IN TRANSIT", "UD", "SHIPMENT UNTRACEABLE AT DESTINATION"),
                "206": ("IN TRANSIT", "UD", "SHIPMENT KEPT IN PARCEL LOCKER"),
                "207": ("IN TRANSIT", "UD", "SHIPMENT RETRIEVED FROM PARCEL LOCKER"),
                "208": ("IN TRANSIT", "UD", "SHIPMENT KEPT IN PARCEL SHOP FOR COLLECT"),
                "209": ("IN TRANSIT", "UD", "SHPT RETRIEVED FROM PARCEL SHOP FOR RTO"),
                "210": ("IN TRANSIT", "UD", "DG SHIPMENT SCAN IN LOCATION"),
                "211": ("IN TRANSIT", "UD", "LOAD ON HOLD;SPACE CONSTRAINT-DELVRY LOC"),
                "212": ("IN TRANSIT", "UD", "LOAD ON HOLD;SPACE CONSTRAINT IN NET VEH"),
                "213": ("IN TRANSIT", "UD", "LOAD ON HOLD;SPACE CONSTRAINT-COMML FLT"),
                "214": ("IN TRANSIT", "UD", "LOAD ON HOLD; EMBARGO ON COMML UPLIFT"),
                "215": ("IN TRANSIT", "UD", "LOAD ON HOLD; SPACE CONSTRAINT IN TRAIN"),
                "216": ("IN TRANSIT", "UD", "HELD IN DHLe NETWORK DPS CHECK"),
                "220": ("IN TRANSIT", "UD", "SHIPMENT HANDED OVER TO ASSOCIATE"),
                "221": ("IN TRANSIT", "UD", "SHPT RCD IN TRANSIT LOC; BEING CONNECTED"),
                "222": ("IN TRANSIT", "UD", "SHPT RCVD AT DESTN LOC FOR DLVRY ATTEMPT"),
                "223": ("IN TRANSIT", "UD", "UD SHPT SENDING BACK TO BDE FOR PROCESS"),
                "224": ("IN TRANSIT", "UD", "UD SHPT RCVD FRM ASSOCIATE FOR PROCESSNG"),
                "301": ("IN TRANSIT", "UD", "TRAFFIC JAM ENROUTE"),
                "302": ("IN TRANSIT", "UD", "ACCIDENT ENROUTE"),
                "303": ("IN TRANSIT", "UD", "DETAINED AT CHECK-POST"),
                "304": ("IN TRANSIT", "UD", "POLITICAL DISTURBANCE"),
                "305": ("IN TRANSIT", "UD", "HEAVY RAIN"),
                "306": ("IN TRANSIT", "UD", "VEHICLE BREAK-DOWN ENROUTE"),
                "307": ("IN TRANSIT", "UD", "HEAVY FOG"),
                "309": ("IN TRANSIT", "UD", "DETAINED BY RTO"),
                "310": ("IN TRANSIT", "UD", "VENDOR FAULT"),
                "311": ("IN TRANSIT", "UD", "ENDORSEMENT NOT DONE AT CHECK-POST"),
                "312": ("IN TRANSIT", "UD", "CAUGHT FIRE INSIDE VEHICLE"),
                "313": ("IN TRANSIT", "UD", "DELAYED BY ENROUTE SECTOR"),
                "314": ("IN TRANSIT", "UD", "DETAINED BY SALES TAX"),
                "315": ("IN TRANSIT", "UD", "ANY OTHER CONTROLABLE REASON"),
                "316": ("IN TRANSIT", "UD", "ANY OTHER NON-CONTROLABLE REASON"),
                "021": ("LOST", "DL", "LOST SHIPMENT"),
                "001": ("PENDING", "UD", "CUSTOMER ASKED FUTURE DELIVERY: HAL", 4),
                "002": ("PENDING", "UD", "OUT OF DELIVERY AREA", 8),
                "003": ("PENDING", "UD", "RESIDENCE/OFFICE CLOSED;CANT DELIVER", 6),
                "004": ("PENDING", "UD", "COMPANY ON STRIKE, CANNOT DELIVER", 7),
                "005": ("PENDING", "UD", "HOLIDAY:DELIVERY ON NEXT WORKING DAY", 4),
                "006": ("PENDING", "UD", "SHIPPER PKGNG/MRKNG IMPROPER;SHPT HELD", 2),
                "007": ("IN TRANSIT", "UD", "SHIPT MANIFESTED;NOT RECD BY DESTINATION"),
                "008": ("PENDING", "UD", "ADDRESS UNLOCATABLE; CANNOT DELIVER", 2),
                "009": ("PENDING", "UD", "ADDRESS INCOMPLETE, CANNOT DELIVER", 2),
                "010": ("PENDING", "UD", "ADDRESS INCORRECT; CANNOT DELIVER", 2),
                "011": ("PENDING", "UD", "CONSIGNEE REFUSED TO ACCEPT", 3),
                "012": ("PENDING", "UD", "NO SUCH CO./CNEE AT GIVEN ADDRESS", 2),
                "013": ("PENDING", "UD", "CONSIGNEE NOT AVAILABLE;CANT DELIVER", 1),
                "014": ("PENDING", "UD", "CNEE SHIFTED FROM THE GIVEN ADDRESS", 2),
                "016": ("IN TRANSIT", "RT", "RTO FROM ORIGIN S.C. ON SAME AWB"),
                "017": ("PENDING", "UD", "DISTURBANCE/NATURAL DISASTER/STRIKE", 12),
                "019": ("PENDING", "UD", "CONSIGNEE NOT YET CHECKED IN", 4),
                "020": ("PENDING", "UD", "CONSIGNEE OUT OF STATION", 4),
                "022": ("IN TRANSIT", "UD", "BEING PROCESSED AT CUSTOMS"),
                "024": ("IN TRANSIT", "UD", "BD FLIGHT DELAYED; BAD WEATHER/TECH SNAG"),
                "137": ("PENDING", "UD", "DELIVERY AREA NOT ACCESSIBLE", 7),
                "139": ("PENDING", "UD", "NEED DEPT NAME/EXTN.NO:UNABLE TO DELIVER", 2),
                "201": ("PENDING", "UD", "E-TAIL; REFUSED TO ACCEPT SHIPMENT", 3),
                "202": ("IN TRANSIT", "UD", "E-TAIL; REFUSED - SHPTS ORDERED IN BULK"),
                "203": ("PENDING", "UD", "E-TAIL; REFUSED-OPEN DELIVERY REQUEST", 10),
                "204": ("PENDING", "UD", "E-TAIL; REFUSED-WRONG PROD DESP/NOT ORDE", 3),
                "205": ("PENDING", "UD", "E-TAIL: FAKE  BOOKING/FAKE ADDRESS", 2),
                "217": ("PENDING", "UD", "CONSIGNEE HAS GIVEN BDE HAL ADDRESS", 2),
                "218": ("PENDING", "UD", "CONSIGNEE ADD IS EDUCATIONAL INSTITUTION", 7),
                "219": ("IN TRANSIT", "UD", "SHIPMENT MOVED TO MOBILE OFFICE"),
                "308": ("PENDING", "UD", "NO ENTRY", 7),
                "777": ("PENDING", "UD", "CONSIGNEE REFUSED SHIPMENT DUE TO GST", 2),
                "026": ("POSTED", "DL", "SHIPMENT POSTED"),
                "074": ("IN TRANSIT", "RT", "RETURNED (SHIPPER REQUEST)"),
                "118": ("RTO", "RT", "DELIVERED BACK TO SHIPPER"),
                "188": ("RTO", "RT", "DELIVERED BACK TO SHIPPER"),
            },
        },
    },
    "Ecom Express": {
        "auth_token_api": None,
        "status_url": "https://plapi.ecomexpress.in/track_me/api/mawbd/?awb=%s&username=%s&password=%s",
        "api_type": "bulk",
        "status_mapper_fn": ecom_status_mapper,
        "ndr_mapper_fn": None,
        "status_time_format": "%d %b, %Y, %H:%M",
        "edd_time_format": "%d-%b-%Y",
        "exotel_url": "https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend",
        "status_mapping": {
            "303": ("IN TRANSIT", "UD", "In Transit", "Shipment In Transit"),
            "400": ("IN TRANSIT", "UD", "Picked", "Shipment picked up"),
            "003": ("IN TRANSIT", "UD", "In Transit", "Bag scanned at DC"),
            "002": ("IN TRANSIT", "UD", "In Transit", "Shipment in-scan"),
            "004": ("IN TRANSIT", "UD", "In Transit", "Shipment in-scan"),
            "005": ("IN TRANSIT", "UD", "In Transit", "Shipment in-scan at DC"),
            "0011": ("IN TRANSIT", "UD", "Picked", "Shipment picked up"),
            "21601": ("IN TRANSIT", "UD", "In Transit", "Late arrival-Misconnection/After cut off"),
            "006": ("DISPATCHED", "UD", "Out for delivery", "Shipment out for delivery"),
            "888": ("DAMAGED", "UD", "", "Transit Damage"),
            "302": ("DAMAGED", "UD", "", "Transit Damage"),
            "555": ("DESTROYED", "UD", "", "Destroyed Red Bus Shipment"),
            "88802": ("DESTROYED", "UD", "", "Shipment destroyed - contains liquid item"),
            "88803": ("DESTROYED", "UD", "", "Shipment destroyed - contains fragile item"),
            "88804": ("DESTROYED", "UD", "", "Shipment destroyed - empty packet"),
            "31701": ("DESTROYED", "UD", "", "Shipment destroyed - food item"),
            "311": ("SHORTAGE", "UD", "", "Shortage"),
            "313": ("SHORTAGE", "UD", "", "Shortage"),
            "314": ("DAMAGED", "UD", "", "DMG Lock - Damage"),
            "999": ("DELIVERED", "DL", "Delivered", "Shipment delivered"),
            "204": ("DELIVERED", "DL", "Delivered", "Shipment delivered"),
            "777": ("IN TRANSIT", "RT", "Returned", "Returned"),
            "333": ("LOST", "UD", "", "Shipment Lost"),
            "33306": ("LOST", "UD", "", "Shipment Lost"),
            "33307": ("LOST", "UD", "", "Shipment Lost"),
            "228": ("PENDING", "UD", "In Transit", "Out of Delivery Area"),
            "227": ("PENDING", "UD", "In Transit", "Residence/Office Closed"),
            "226": ("PENDING", "UD", "In Transit", "Holiday/Weekly off - Delivery on Next Working Day"),
            "224": ("PENDING", "UD", "In Transit", "Address Unlocatable"),
            "223": ("PENDING", "UD", "In Transit", "Address Incomplete"),
            "222": ("PENDING", "UD", "In Transit", "Address Incorrect"),
            "220": ("PENDING", "UD", "In Transit", "No Such Consignee At Given Address"),
            "418": ("PENDING", "UD", "In Transit", "Consignee Shifted, phone num wrong"),
            "417": ("PENDING", "UD", "In Transit", "PHONE NUMBER NOT ANSWERING/ADDRESS NOT LOCATABLE"),
            "219": ("PENDING", "UD", "In Transit", "Consignee Not Available"),
            "218": ("PENDING", "UD", "In Transit", "Consignee Shifted from the Given Address"),
            "231": ("PENDING", "UD", "In Transit", "Shipment attempted - Customer not available"),
            "212": ("PENDING", "UD", "In Transit", "Consignee Out Of Station"),
            "217": ("PENDING", "UD", "In Transit", "Delivery Area Not Accessible"),
            "213": ("PENDING", "UD", "In Transit", "Scheduled for Next Day Delivery"),
            "331": ("PENDING", "UD", "In Transit", "Consignee requested for future delivery "),
            "210": ("PENDING", "UD", "Cancelled", "Shipment attempted - Customer refused to accept"),
            "209": ("PENDING", "UD", "In Transit", "Consignee Refusing to Pay COD Amount"),
            "419": ("PENDING", "UD", "In Transit", "Three attempts made, follow up closed"),
            "401": ("PENDING", "UD", "In Transit", "CUSTOMER RES/OFF CLOSED"),
            "421": ("PENDING", "UD", "In Transit", "Customer Number not reachable/Switched off"),
            "23101": ("PENDING", "UD", "In Transit", "Customer out of station"),
            "23102": ("PENDING", "UD", "In Transit", "Customer not in office"),
            "23103": ("PENDING", "UD", "In Transit", "Customer not in residence"),
            "22701": ("PENDING", "UD", "In Transit", "Case with Legal team"),
            "20002": ("PENDING", "UD", "In Transit", "Forcefully opened by customer and returned"),
            "21002": ("PENDING", "UD", "Cancelled", "Order already cancelled"),
            "22301": ("PENDING", "UD", "In Transit", "Customer out of station"),
            "22303": ("PENDING", "UD", "In Transit", "No Such Consignee At Given Address"),
            "23401": ("PENDING", "UD", "In Transit", "Address pincode mismatch - Serviceable area"),
            "23402": ("PENDING", "UD", "In Transit", "Address pincode mismatch - Non Serviceable area"),
            "22702": ("PENDING", "UD", "In Transit", "Shipment attempted - Office closed"),
            "22801": ("PENDING", "UD", "In Transit", "Customer Address out of delivery area"),
            "22901": ("PENDING", "UD", "In Transit", "Customer requested for self collection"),
            "2447": ("PENDING", "UD", "In Transit", "No such addressee in the given address"),
            "2445": ("PENDING", "UD", "In Transit", "Cash amount Mismatch"),
            "12247": ("PENDING", "UD", "In Transit", "Delivery Attempt to be made - Escalations"),
            "12245": ("PENDING", "UD", "In Transit", "Delivery attempt to be made - FE Instructions"),
            "20701": ("PENDING", "UD", "In Transit", "Misroute due to wrong pincode given by customer"),
        },
        "ndr_reasons": {
            "228": 8,
            "227": 6,
            "226": 4,
            "224": 2,
            "223": 2,
            "222": 2,
            "220": 2,
            "418": 2,
            "417": 2,
            "219": 1,
            "218": 1,
            "231": 1,
            "212": 1,
            "217": 7,
            "213": 4,
            "331": 4,
            "210": 3,
            "209": 9,
            "419": 13,
            "401": 6,
            "421": 1,
            "23101": 1,
            "23102": 1,
            "23103": 1,
            "232": 2,
            "234": 2,
            "22701": 6,
            "20002": 11,
            "21002": 3,
            "22301": 2,
            "22303": 2,
            "23401": 2,
            "23402": 2,
            "2447": 2,
            "22702": 6,
            "22801": 8,
            "22901": 5,
            "2445": 9,
        },
    },
    "Pidge": {
        "auth_token_api": None,
        "status_url": "https://dev-release-v1.pidge.in/v2.0/vendor/order/",
        "api_type": "individual",
        "status_mapper_fn": None,
        "ndr_mapper_fn": None,
        "status_time_format": "%Y-%m-%dT%H:%M:%S.%fZ",
        "edd_time_format": None,
        "exotel_url": "https://ff2064142bc89ac5e6c52a6398063872f95f759249509009:783fa09c0ba1110309f606c7411889192335bab2e908a079@api.exotel.com/v1/Accounts/wareiq1/Sms/bulksend",
        "status_mapping": {
            130: ("IN TRANSIT", "UD", "Picked", "Shipment picked up"),
            150: ("IN TRANSIT", "UD", "In Transit", "Shipment in transit"),
            170: ("DISPATCHED", "UD", "Out for delivery", "Shipment out for delivery"),
            190: ("DELIVERED", "UD", "Delivered", "Shipment delivered"),
            5: ("PENDING", "UD", "In Transit", "Shipment not delivered"),
            0: ("CANCELED", "UD", "Cancelled", "order cancelled"),
        },
    },
    "DTDC": {
        # TODO Modify to prod urls
        "auth_token_api": "https://blktracksvc.dtdc.com/dtdc-api/api/dtdc/authenticate",
        "status_url": "https://blktracksvc.dtdc.com/dtdc-api/rest/JSONCnTrk/getTrackDetails",
        "api_type": "individual",
        "status_mapper_fn": dtdc_status_mapper,
        "ndr_mapper_fn": None,
        "status_time_format": "%d%m%Y-%H%M",
        "status_to_code_mapping": {
            "Booked": ("READY TO SHIP", "UD", "Received"),
            "In Transit": ("IN TRANSIT", "UD", "In Transit"),
            "Out For Delivery": ("DISPATCHED", "UD", "Out for delivery"),
            "Not Delivered": ("PENDING", "UD", "In Transit"),
            "Delivered": ("DELIVERED", "DL", "Delivered"),
            "RTO Processed & Forwarded": ("IN TRANSIT", "RT", "Returned"),
            "RTO In Transit": ("IN TRANSIT", "RT", "In Transit for RTO"),
            "RTO Out For Delivery": ("DISPATCHED", "RT", "Dispatched for RTO"),
            "RTO Not Delivered": ("PENDING", "RT", "In Transit for RTO"),
            "RTO Delivered": ("DELIVERED", "RT", "Delivered for RTO"),
        },
        "status_mapping": {
            "BKD": ("READY TO SHIP", "UD", "Received", "Booking done in DTDC hub"),
            "OPMF": ("IN TRANSIT", "UD", "In Transit", "Shipment in transit"),
            "IPMF": ("IN TRANSIT", "UD", "In Transit", "Shipment in transit"),
            "OBMD": ("IN TRANSIT", "UD", "In Transit", "Shipment in transit"),
            "IBMD": ("IN TRANSIT", "UD", "In Transit", "Shipment in transit"),
            "OBMN": ("IN TRANSIT", "UD", "In Transit", "Shipment in transit"),
            "IBMN": ("IN TRANSIT", "UD", "In Transit", "Shipment in transit"),
            "OMBM": ("IN TRANSIT", "UD", "In Transit", "Shipment in transit"),
            "IMBM": ("IN TRANSIT", "UD", "In Transit", "Shipment in transit"),
            "ORBO": ("IN TRANSIT", "UD", "In Transit", "Shipment in transit"),
            "IRBO": ("IN TRANSIT", "UD", "In Transit", "Shipment in transit"),
            "CDOUT": ("IN TRANSIT", "UD", "In Transit", "Shipment in transit"),
            "CDIN": ("IN TRANSIT", "UD", "In Transit", "Shipment in transit"),
            "OUTDLV": ("DISPATCHED", "UD", "Out for delivery", "Shipment out for delivery"),
            "NONDLV": ("PENDING", "UD", "In Transit", "Shipment not delivered"),
            "DLV": ("DELIVERED", "DL", "Delivered", "Shipment delivered"),
            "RTO": ("IN TRANSIT", "RT", "Returned", "Returned"),
            "RTOOPMF": ("IN TRANSIT", "RT", "Returned", "Returned"),
            "RTOIPMF": ("IN TRANSIT", "RT", "Returned", "Returned"),
            "RTOOBMD": ("IN TRANSIT", "RT", "Returned", "Returned"),
            "RTOIBMD": ("IN TRANSIT", "RT", "Returned", "Returned"),
            "RTOOBMN": ("IN TRANSIT", "RT", "Returned", "Returned"),
            "RTOIBMN": ("IN TRANSIT", "RT", "Returned", "Returned"),
            "RTOOMBM": ("IN TRANSIT", "RT", "Returned", "Returned"),
            "RTOIMBM": ("IN TRANSIT", "RT", "Returned", "Returned"),
            "RTOORBO": ("IN TRANSIT", "RT", "Returned", "Returned"),
            "RTOIRBO": ("IN TRANSIT", "RT", "Returned", "Returned"),
            "RTOCDOUT": ("IN TRANSIT", "RT", "Returned", "Returned"),
            "RTOCDIN": ("IN TRANSIT", "RT", "Returned", "Returned"),
            "RTOOUTDLV": ("DISPATCHED", "RT", "Returned", "Returned"),
            "RTONONDLV": ("PENDING", "RT", "Returned", "Returned"),
            "RTODLV": ("DELIVERED", "RT", "Returned", "Returned"),
        },
        "ndr_reasons": {
            "ADDRESS INCOMPLETE OR WRONG-(CIR)": 2,
            "RECEIVER REQUESTED DELIVERY ON ANOTHER DATE-(CIR)": 4,
            "COLLECTION AMOUNT NOT READY-(CIR)": 15,
            "COVID 19 - CUSTOMER REFUSED TO ACCEPT": 1,
            "ADDRESS CORRECT AND PINCODE WRONG-(CIR)": 2,
            "OFFICE CLOSED OR DOOR LOCK-(CIR)": 6,
            "CONTACT NAME / DEPT NOT MENTIONED-(CIR)": 2,
            "RECEIVER REFUSE DELIVERY DUE TO DAMAGE-(DIR)": 11,
            "LAST DATE OVER FOR SUBMISSION-(OTR)": 14,
            "LAST MILE MISROUTE-(OTR)": 2,
            "ADDRESS OK BUT NO SUCH PERSON-(CIR)": 1,
            "AREA NON SERVICEABLE-(DIR)": 8,
            "CUSTOMER WILL SELF COLLECT-(CIR)": 5,
            "RECEIVER NOT AVAILABLE-(CIR)": 1,
            "RECEIVER REFUSED DELIVERY(CIR)": 3,
            "RECEIVER SHIFTED FROM GIVEN ADDRESS-(CIR)": 2,
            "RESTRICTED ENTRY-(OTR)": 7,
            "CONSIGNMENT LOST-(OTR)": 14,
            "RESCHEDULED": 4,
            "COVID 19 - OFFICE CLOSED/DOOR LOCKED": 6,
            "RECEIVER WANTS OPEN DELIVERY-(CIR)": 10,
            "CONSIGNOR REFUSED RTO SHIPMENT-(CIR)": 14,
            "LOCAL HOLIDAY-(OTR)": 12,
            "PAPERWORK REQUIRED-(OTR)": 14,
            "COVID19 COULD NOT ATTEMPT": 12,
            "OTP NOT GENERATED": 14,
        },
    },
}
