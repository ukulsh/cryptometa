from datetime import datetime, timedelta
from sqlalchemy import or_, func
from flask import Blueprint, request, jsonify
from flask_restful import Api
import json, requests
from project import db
from project.api.models import MultiVendor, Orders, OrdersPayments, CodVerification, NDRVerification
from project.api.utils import authenticate_restful
from project.api.utilities.db_utils import DbConnection

webhooks_blueprint = Blueprint("webhooks", __name__)
api = Api(webhooks_blueprint)


@webhooks_blueprint.route("/core/v1/consume/status", methods=["POST"])
@authenticate_restful
def consume_status_info(resp):
    data = json.loads(request.data)
    auth_data = resp.get("data")
    if auth_data.get("user_group") != "courier":
        return jsonify({"success": False, "msg": "Auth Failed"}), 404

    if auth_data.get("username") == "ecomexpress":
        pass
        # do ecom things

    print("Yes In....")
    response = dict()
    auth_data = resp.get("data")
    if not auth_data:
        return jsonify({"msg": "Authentication Failed"}), 400

    if auth_data["user_group"] == "warehouse":
        response["today"] = {"orders": 0, "revenue": 0}
        response["yesterday"] = {"orders": 0, "revenue": 0}
        response["graph_data"] = list()
        return jsonify(response), 200

    client_prefix = auth_data.get("client_prefix")
    from_date = datetime.utcnow() + timedelta(hours=5.5)
    from_date = datetime(from_date.year, from_date.month, from_date.day)
    from_date = from_date - timedelta(hours=5.5)
    qs_data = (
        db.session.query(
            func.date_trunc("day", Orders.order_date).label("date"),
            func.count(Orders.id),
            func.sum(OrdersPayments.amount),
        )
        .join(OrdersPayments, Orders.id == OrdersPayments.order_id)
        .filter(Orders.order_date >= datetime.today() - timedelta(days=30))
    )
    cod_verification = (
        db.session.query(CodVerification)
        .join(Orders, Orders.id == CodVerification.order_id)
        .filter(or_(CodVerification.date_created >= from_date, CodVerification.verification_time >= from_date))
    )
    ndr_verification = (
        db.session.query(NDRVerification)
        .join(Orders, Orders.id == NDRVerification.order_id)
        .filter(or_(NDRVerification.date_created >= from_date, NDRVerification.verification_time >= from_date))
    )
    if auth_data["user_group"] == "client":
        qs_data = qs_data.filter(Orders.client_prefix == client_prefix)
        cod_verification = cod_verification.filter(Orders.client_prefix == client_prefix)
        ndr_verification = ndr_verification.filter(Orders.client_prefix == client_prefix)
    if auth_data["user_group"] == "multi-vendor":
        all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
        qs_data = qs_data.filter(Orders.client_prefix.in_(all_vendors.vendor_list))
        cod_verification = cod_verification.filter(Orders.client_prefix.in_(all_vendors.vendor_list))
        ndr_verification = ndr_verification.filter(Orders.client_prefix.in_(all_vendors.vendor_list))

    qs_data = qs_data.group_by("date").order_by("date").all()
    cod_verification = cod_verification.all()
    ndr_verification = ndr_verification.all()

    cod_check = {
        "total_checked": len(cod_verification),
        "confirmed_via_text": 0,
        "confirmed_via_call": 0,
        "total_cancelled": 0,
        "not_confirmed_yet": 0,
    }
    for cod_data in cod_verification:
        if cod_data.cod_verified is True:
            if cod_data.verified_via == "text":
                cod_check["confirmed_via_text"] += 1
            elif cod_data.verified_via == "call":
                cod_check["confirmed_via_call"] += 1
        elif cod_data.cod_verified is False:
            cod_check["total_cancelled"] += 1

        else:
            cod_check["not_confirmed_yet"] += 1

    ndr_check = {
        "total_checked": len(ndr_verification),
        "confirmed_via_text": 0,
        "confirmed_via_call": 0,
        "reattempt_requested": 0,
        "not_confirmed_yet": 0,
    }
    for ndr_data in ndr_verification:
        if ndr_data.ndr_verified is True:
            if ndr_data.verified_via == "text":
                ndr_check["confirmed_via_text"] += 1
            elif ndr_data.verified_via == "call":
                ndr_check["confirmed_via_call"] += 1
        elif ndr_data.ndr_verified is False:
            ndr_check["reattempt_requested"] += 1

        else:
            ndr_check["not_confirmed_yet"] += 1

    response["cod_verification"] = cod_check
    response["ndr_verification"] = ndr_check

    date_today = datetime.utcnow()
    date_today = date_today + timedelta(hours=5.5)
    date_yest = date_today - timedelta(days=1)

    date_today = datetime.strftime(date_today, "%d-%m-%Y")
    date_yest = datetime.strftime(date_yest, "%d-%m-%Y")

    response["today"] = {"orders": 0, "revenue": 0}
    response["yesterday"] = {"orders": 0, "revenue": 0}

    response["graph_data"] = list()

    for dat_obj in qs_data:
        date_str = datetime.strftime(dat_obj[0], "%d-%m-%Y")
        if date_str == date_today:
            response["today"] = {"orders": dat_obj[1], "revenue": dat_obj[2]}
        if date_str == date_yest:
            response["yesterday"] = {"orders": dat_obj[1], "revenue": dat_obj[2]}
        response["graph_data"].append(
            {"date": datetime.strftime(dat_obj[0], "%d-%m-%Y"), "orders": dat_obj[1], "revenue": dat_obj[2]}
        )

    return jsonify(response), 200
