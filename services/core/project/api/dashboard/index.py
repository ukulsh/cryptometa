from datetime import datetime, timedelta
from sqlalchemy import or_, func
from flask import Blueprint, request, jsonify
from flask_restful import Api
from project import db
from project.api.models import MultiVendor, Orders, OrdersPayments, CodVerification, NDRVerification
from project.api.utils import authenticate_restful
from project.api.utilities.db_utils import DbConnection

dashboard_blueprint = Blueprint('dashboard', __name__)
api = Api(dashboard_blueprint)

conn = DbConnection.get_db_connection_instance()
conn_2 = DbConnection.get_pincode_db_connection_instance()


@dashboard_blueprint.route('/dashboard', methods=['GET'])
@authenticate_restful
def get_dashboard(resp):
    print('Yes In....')
    response = dict()
    auth_data = resp.get('data')
    if not auth_data:
        return jsonify({"msg": "Authentication Failed"}), 400

    if auth_data['user_group'] == 'warehouse':
        response['today'] = {"orders": 0, "revenue": 0}
        response['yesterday'] = {"orders": 0, "revenue": 0}
        response['graph_data'] = list()
        return jsonify(response), 200

    client_prefix = auth_data.get('client_prefix')
    from_date = datetime.utcnow() + timedelta(hours=5.5)
    from_date = datetime(from_date.year, from_date.month, from_date.day)
    from_date = from_date - timedelta(hours=5.5)
    qs_data = db.session.query(func.date_trunc('day', Orders.order_date).label('date'), func.count(Orders.id), func.sum(OrdersPayments.amount))\
        .join(OrdersPayments, Orders.id==OrdersPayments.order_id)\
        .filter(Orders.order_date >= datetime.today()- timedelta(days=30))
    cod_verification = db.session.query(CodVerification).join(Orders, Orders.id==CodVerification.order_id)\
        .filter(or_(CodVerification.date_created >= from_date, CodVerification.verification_time >= from_date))
    ndr_verification = db.session.query(NDRVerification).join(Orders, Orders.id==NDRVerification.order_id)\
        .filter(or_(NDRVerification.date_created >= from_date, NDRVerification.verification_time >= from_date))
    if auth_data['user_group'] == 'client':
        qs_data = qs_data.filter(Orders.client_prefix == client_prefix)
        cod_verification = cod_verification.filter(Orders.client_prefix == client_prefix)
        ndr_verification = ndr_verification.filter(Orders.client_prefix == client_prefix)
    if auth_data['user_group'] == 'multi-vendor':
        all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix==client_prefix).first()
        qs_data = qs_data.filter(Orders.client_prefix.in_(all_vendors.vendor_list))
        cod_verification = cod_verification.filter(Orders.client_prefix.in_(all_vendors.vendor_list))
        ndr_verification = ndr_verification.filter(Orders.client_prefix.in_(all_vendors.vendor_list))

    qs_data = qs_data.group_by('date').order_by('date').all()
    cod_verification = cod_verification.all()
    ndr_verification = ndr_verification.all()

    cod_check = {"total_checked": len(cod_verification),
                 "confirmed_via_text": 0,
                 "confirmed_via_call": 0,
                 "total_cancelled": 0,
                 "not_confirmed_yet": 0}
    for cod_data in cod_verification:
        if cod_data.cod_verified is True:
            if cod_data.verified_via == 'text':
                cod_check['confirmed_via_text'] += 1
            elif cod_data.verified_via == 'call':
                cod_check['confirmed_via_call'] += 1
        elif cod_data.cod_verified is False:
            cod_check['total_cancelled'] += 1

        else:
            cod_check['not_confirmed_yet'] += 1

    ndr_check = {"total_checked": len(ndr_verification),
                 "confirmed_via_text": 0,
                 "confirmed_via_call": 0,
                 "reattempt_requested": 0,
                 "not_confirmed_yet": 0}
    for ndr_data in ndr_verification:
        if ndr_data.ndr_verified is True:
            if ndr_data.verified_via == 'text':
                ndr_check['confirmed_via_text'] += 1
            elif ndr_data.verified_via == 'call':
                ndr_check['confirmed_via_call'] += 1
        elif ndr_data.ndr_verified is False:
            ndr_check['reattempt_requested'] += 1

        else:
            ndr_check['not_confirmed_yet'] += 1

    response['cod_verification'] = cod_check
    response['ndr_verification'] = ndr_check

    date_today = datetime.utcnow()
    date_today = date_today + timedelta(hours=5.5)
    date_yest = date_today - timedelta(days=1)

    date_today = datetime.strftime(date_today, '%d-%m-%Y')
    date_yest = datetime.strftime(date_yest, '%d-%m-%Y')

    response['today'] = {"orders": 0, "revenue": 0}
    response['yesterday'] = {"orders": 0, "revenue": 0}

    response['graph_data'] = list()

    for dat_obj in qs_data:
        date_str = datetime.strftime(dat_obj[0], '%d-%m-%Y')
        if date_str==date_today:
            response['today'] = {"orders": dat_obj[1], "revenue": dat_obj[2]}
        if date_str==date_yest:
            response['yesterday'] = {"orders": dat_obj[1], "revenue": dat_obj[2]}
        response['graph_data'].append({"date":datetime.strftime(dat_obj[0], '%d-%m-%Y'),
                                       "orders":dat_obj[1],
                                       "revenue":dat_obj[2]})

    return jsonify(response), 200

@dashboard_blueprint.route('/dashboard/v1/performance', methods=['GET'])
@authenticate_restful
def get_dashboard_performance(resp):
    response = dict()
    cur = conn.cursor()
    try:
        auth_data = resp.get('data')
        if not auth_data:
            return jsonify({"msg": "Authentication Failed"}), 400

        if auth_data['user_group'] == 'warehouse':
            response['data'] = {}
            return jsonify(response), 200

        from_date = request.args.get('from')
        to_date = request.args.get('to')

        if not from_date:
            from_date = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=30)
            from_date = from_date.strftime('%Y-%m-%d')

        if to_date:
            to_date = datetime.strptime(to_date, '%Y-%m-%d')
            to_date = to_date+timedelta(days=1)
            to_date = to_date.strftime('%Y-%m-%d')
        else:
            to_date = datetime.utcnow() + timedelta(hours=5.5) + timedelta(days=1)
            to_date = to_date.strftime('%Y-%m-%d')

        client_prefix = auth_data.get('client_prefix')

        query_to_run = """select aa.status, count(*) from orders aa
                        left join shipments bb on aa.id=bb.order_id
                        where aa.order_date>'%s' and aa.order_date<'%s'
                        __CLIENT_FILTER__
                        group by aa.status"""%(from_date, to_date)

        all_vendors = None
        if auth_data['user_group'] == 'multi-vendor':
            all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
            all_vendors = all_vendors.vendor_list

        if auth_data['user_group'] == 'client':
            query_to_run = query_to_run.replace("__CLIENT_FILTER__",
                                                    "AND aa.client_prefix='%s'" % auth_data['client_prefix'])
        elif all_vendors:
            query_to_run = query_to_run.replace("__CLIENT_FILTER__",
                                                    "AND aa.client_prefix in %s" % str(tuple(all_vendors)))
        else:
            query_to_run = query_to_run.replace("__CLIENT_FILTER__", "")

        all_shipments = 0
        active_shipments = 0
        delivered_shipments = 0
        rto_shipments = 0
        cur.execute(query_to_run)
        status_qs = cur.fetchall()

        for st_obj in status_qs:
            if st_obj[0] in ('DELIVERED'):
                delivered_shipments += st_obj[1]
                all_shipments += st_obj[1]
            elif st_obj[0] in ('RTO','DTO'):
                rto_shipments += st_obj[1]
                all_shipments += st_obj[1]
            elif st_obj[0] in ('IN TRANSIT','PENDING', 'DISPATCHED'):
                active_shipments += st_obj[1]
                all_shipments += st_obj[1]
            elif st_obj[0] in ('PICKUP REQUESTED', 'READY TO SHIP'):
                all_shipments += st_obj[1]

        response['data'] = {'all':all_shipments, "active": active_shipments, "delivered":delivered_shipments, "rto": rto_shipments}

        return jsonify(response), 200
    except Exception as e:
        return jsonify(response), 400


@dashboard_blueprint.route('/dashboard/v1/verification', methods=['GET'])
@authenticate_restful
def get_dashboard_verification(resp):
    response = dict()
    cur = conn.cursor()
    try:
        auth_data = resp.get('data')
        if not auth_data:
            return jsonify({"msg": "Authentication Failed"}), 400

        if auth_data['user_group'] == 'warehouse':
            response['data'] = {}
            return jsonify(response), 200

        from_date = request.args.get('from')
        to_date = request.args.get('to')

        if not from_date:
            from_date = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=30)
            from_date = from_date.strftime('%Y-%m-%d')

        if to_date:
            to_date = datetime.strptime(to_date, '%Y-%m-%d')
            to_date = to_date+timedelta(days=1)
            to_date = to_date.strftime('%Y-%m-%d')
        else:
            to_date = datetime.utcnow() + timedelta(hours=5.5) + timedelta(days=1)
            to_date = to_date.strftime('%Y-%m-%d')

        client_prefix = auth_data.get('client_prefix')

        query_to_run_cod = """select verified_via, count(*) from cod_verification aa
                            left join orders bb on bb.id=aa.order_id
                            where aa.date_created+interval '5.5 hours'>'%s' 
                            and aa.date_created+interval '5.5 hours'<'%s'
                            __CLIENT_FILTER__
                            group by verified_via"""%(from_date, to_date)

        query_to_run_ndr = """select verified_via, count(*) from ndr_verification aa
                                    left join orders bb on bb.id=aa.order_id
                                    where aa.date_created+interval '5.5 hours'>'%s' 
                                    and aa.date_created+interval '5.5 hours'<'%s'
                                    __CLIENT_FILTER__
                                    group by verified_via""" % (from_date, to_date)

        all_vendors = None
        if auth_data['user_group'] == 'multi-vendor':
            all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
            all_vendors = all_vendors.vendor_list

        if auth_data['user_group'] == 'client':
            query_to_run_cod = query_to_run_cod.replace("__CLIENT_FILTER__",
                                                    "AND bb.client_prefix='%s'" % auth_data['client_prefix'])
            query_to_run_ndr = query_to_run_ndr.replace("__CLIENT_FILTER__",
                                                        "AND bb.client_prefix='%s'" % auth_data['client_prefix'])
        elif all_vendors:
            query_to_run_cod = query_to_run_cod.replace("__CLIENT_FILTER__",
                                                    "AND bb.client_prefix in %s" % str(tuple(all_vendors)))
            query_to_run_ndr = query_to_run_ndr.replace("__CLIENT_FILTER__",
                                                        "AND bb.client_prefix in %s" % str(tuple(all_vendors)))
        else:
            query_to_run_cod = query_to_run_cod.replace("__CLIENT_FILTER__", "")
            query_to_run_ndr = query_to_run_ndr.replace("__CLIENT_FILTER__", "")

        cur.execute(query_to_run_cod)
        cod_qs = cur.fetchall()
        cur.execute(query_to_run_ndr)
        ndr_qs = cur.fetchall()

        cod_obj = {'text':0, 'call':0, 'manual':0,'total':0}
        ndr_obj = {'text':0, 'call':0, 'manual':0,'total':0}

        for st_obj in cod_qs:
            cod_obj['total'] += st_obj[1]
            if st_obj[0] =='text':
                cod_obj['text'] += st_obj[1]
            elif st_obj[0] =='call':
                cod_obj['call'] += st_obj[1]
            elif st_obj[0] == 'manual':
                cod_obj['manual'] += st_obj[1]

        for st_obj in ndr_qs:
            ndr_obj['total'] += st_obj[1]
            if st_obj[0] =='text':
                ndr_obj['text'] += st_obj[1]
            elif st_obj[0] =='call':
                ndr_obj['call'] += st_obj[1]
            elif st_obj[0] == 'manual':
                ndr_obj['manual'] += st_obj[1]

        response['data'] = {'cod':cod_obj, "ndr": ndr_obj}

        return jsonify(response), 200
    except Exception as e:
        return jsonify(response), 400


@dashboard_blueprint.route('/dashboard/v1/zonewise', methods=['GET'])
@authenticate_restful
def get_dashboard_zonewise(resp):
    response = dict()
    cur = conn.cursor()
    try:
        auth_data = resp.get('data')
        if not auth_data:
            return jsonify({"msg": "Authentication Failed"}), 400

        if auth_data['user_group'] == 'warehouse':
            response['data'] = {}
            return jsonify(response), 200

        from_date = request.args.get('from')
        to_date = request.args.get('to')

        if not from_date:
            from_date = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=30)
            from_date = from_date.strftime('%Y-%m-%d')

        if to_date:
            to_date = datetime.strptime(to_date, '%Y-%m-%d')
            to_date = to_date+timedelta(days=1)
            to_date = to_date.strftime('%Y-%m-%d')
        else:
            to_date = datetime.utcnow() + timedelta(hours=5.5) + timedelta(days=1)
            to_date = to_date.strftime('%Y-%m-%d')

        client_prefix = auth_data.get('client_prefix')

        query_to_run = """select cc.zone, count(cc.zone) from orders aa
                                left join shipments bb on aa.id=bb.order_id
                                left join client_deductions cc on cc.shipment_id=bb.id
                                where aa.order_date>'%s' and aa.order_date<'%s'
                                and cc.zone is not null
                                __CLIENT_FILTER__
                                group by cc.zone"""%(from_date, to_date)

        all_vendors = None
        if auth_data['user_group'] == 'multi-vendor':
            all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
            all_vendors = all_vendors.vendor_list

        if auth_data['user_group'] == 'client':
            query_to_run = query_to_run.replace("__CLIENT_FILTER__",
                                                    "AND aa.client_prefix='%s'" % auth_data['client_prefix'])
        elif all_vendors:
            query_to_run = query_to_run.replace("__CLIENT_FILTER__",
                                                    "AND aa.client_prefix in %s" % str(tuple(all_vendors)))
        else:
            query_to_run = query_to_run.replace("__CLIENT_FILTER__", "")

        cur.execute(query_to_run)
        zone_qs = cur.fetchall()

        response['data'] = {'data':zone_qs}

        return jsonify(response), 200
    except Exception as e:
        return jsonify(response), 400


@dashboard_blueprint.route('/dashboard/v1/ndr', methods=['GET'])
@authenticate_restful
def get_dashboard_ndr(resp):
    response = dict()
    cur = conn.cursor()
    try:
        auth_data = resp.get('data')
        if not auth_data:
            return jsonify({"msg": "Authentication Failed"}), 400

        if auth_data['user_group'] == 'warehouse':
            response['data'] = {}
            return jsonify(response), 200

        from_date = request.args.get('from')
        to_date = request.args.get('to')

        if not from_date:
            from_date = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=30)
            from_date = from_date.strftime('%Y-%m-%d')

        if to_date:
            to_date = datetime.strptime(to_date, '%Y-%m-%d')
            to_date = to_date+timedelta(days=1)
            to_date = to_date.strftime('%Y-%m-%d')
        else:
            to_date = datetime.utcnow() + timedelta(hours=5.5) + timedelta(days=1)
            to_date = to_date.strftime('%Y-%m-%d')

        client_prefix = auth_data.get('client_prefix')

        query_to_run_count = """select bb.status, count(*) from ndr_shipments aa
                            left join orders bb on bb.id=aa.order_id
                            where aa.date_created+interval '5.5 hours'>'%s' 
                            and aa.date_created+interval '5.5 hours'<'%s'
                            __CLIENT_FILTER__
                            group by bb.status"""%(from_date, to_date)

        query_to_run_reason = """select cc.reason, count(*) from ndr_shipments aa
                                left join orders bb on bb.id=aa.order_id
                                left join ndr_reasons cc on cc.id=aa.reason_id
                                where aa.date_created+interval '5.5 hours'>'%s' 
                                and aa.date_created+interval '5.5 hours'<'%s'
                                __CLIENT_FILTER__
                                group by cc.reason""" % (from_date, to_date)

        all_vendors = None
        if auth_data['user_group'] == 'multi-vendor':
            all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
            all_vendors = all_vendors.vendor_list

        if auth_data['user_group'] == 'client':
            query_to_run_count = query_to_run_count.replace("__CLIENT_FILTER__",
                                                    "AND bb.client_prefix='%s'" % auth_data['client_prefix'])
            query_to_run_reason = query_to_run_reason.replace("__CLIENT_FILTER__",
                                                            "AND bb.client_prefix='%s'" % auth_data['client_prefix'])
        elif all_vendors:
            query_to_run_count = query_to_run_count.replace("__CLIENT_FILTER__",
                                                    "AND bb.client_prefix in %s" % str(tuple(all_vendors)))
            query_to_run_reason = query_to_run_reason.replace("__CLIENT_FILTER__",
                                                            "AND bb.client_prefix in %s" % str(tuple(all_vendors)))
        else:
            query_to_run_count = query_to_run_count.replace("__CLIENT_FILTER__", "")
            query_to_run_reason = query_to_run_reason.replace("__CLIENT_FILTER__", "")

        cur.execute(query_to_run_count)
        ndr_count_qs = cur.fetchall()
        count = {"raised":0, "active":0, "delivered":0, "rto":0}
        for ndr_qs in ndr_count_qs:
            if ndr_qs[0] in ('IN TRANSIT', 'PENDING'):
                count['raised'] += ndr_qs[1]
                count['active'] += ndr_qs[1]
            elif ndr_qs[0] == 'DELIVERED':
                count['raised'] += ndr_qs[1]
                count['delivered'] += ndr_qs[1]
            elif ndr_qs[0] == 'RTO':
                count['raised'] += ndr_qs[1]
                count['rto'] += ndr_qs[1]

        cur.execute(query_to_run_reason)
        ndr_reason_qs = cur.fetchall()
        reason = list()
        for ndr_qs in ndr_reason_qs:
            reason.append({"reason": ndr_qs[0], "count":ndr_qs[1]})

        response['data'] = {'data':{"ndr_count": count, "ndr_reason": reason}}

        return jsonify(response), 200
    except Exception as e:
        return jsonify(response), 400


@dashboard_blueprint.route('/dashboard/v1/delivery_timeline', methods=['GET'])
@authenticate_restful
def get_dashboard_delivery_timeline(resp):
    response = dict()
    cur = conn.cursor()
    try:
        auth_data = resp.get('data')
        if not auth_data:
            return jsonify({"msg": "Authentication Failed"}), 400

        if auth_data['user_group'] == 'warehouse':
            response['data'] = {}
            return jsonify(response), 200

        from_date = request.args.get('from')
        to_date = request.args.get('to')

        if not from_date:
            from_date = datetime.utcnow() + timedelta(hours=5.5) - timedelta(days=30)
            from_date = from_date.strftime('%Y-%m-%d')

        if to_date:
            to_date = datetime.strptime(to_date, '%Y-%m-%d')
            to_date = to_date+timedelta(days=1)
            to_date = to_date.strftime('%Y-%m-%d')
        else:
            to_date = datetime.utcnow() + timedelta(hours=5.5) + timedelta(days=1)
            to_date = to_date.strftime('%Y-%m-%d')

        client_prefix = auth_data.get('client_prefix')

        query_to_run_count = """select delivery_days, count(delivery_days) from
                                (select aa.status_time::date-dd.status_time::date as delivery_days from order_status aa
                                left join orders bb on aa.order_id=bb.id
                                left join shipments cc on aa.shipment_id=cc.id
                                left join (select * from order_status where status='Picked') dd on bb.id=dd.order_id
                                where aa.status='Delivered'
                                and aa.status_time>'%s' and aa.status_time<'%s'
                                __CLIENT_FILTER__) xx
                                where delivery_days is not null
                                group by delivery_days"""%(from_date, to_date)

        all_vendors = None
        if auth_data['user_group'] == 'multi-vendor':
            all_vendors = db.session.query(MultiVendor).filter(MultiVendor.client_prefix == client_prefix).first()
            all_vendors = all_vendors.vendor_list

        if auth_data['user_group'] == 'client':
            query_to_run_count = query_to_run_count.replace("__CLIENT_FILTER__",
                                                    "AND bb.client_prefix='%s'" % auth_data['client_prefix'])
        elif all_vendors:
            query_to_run_count = query_to_run_count.replace("__CLIENT_FILTER__",
                                                    "AND bb.client_prefix in %s" % str(tuple(all_vendors)))
        else:
            query_to_run_count = query_to_run_count.replace("__CLIENT_FILTER__", "")

        cur.execute(query_to_run_count)
        count_qs = cur.fetchall()
        count_dict = {"one":0,"two":0,"three":0,"four":0,"five":0,"gt_five":0}
        for count_qs_obj in count_qs:
            if count_qs_obj[0]<2:
                count_dict['one'] += count_qs_obj[1]
            elif count_qs_obj[0]==2:
                count_dict['two'] += count_qs_obj[1]
            elif count_qs_obj[0]==3:
                count_dict['three'] += count_qs_obj[1]
            elif count_qs_obj[0]==4:
                count_dict['four'] += count_qs_obj[1]
            elif count_qs_obj[0]==5:
                count_dict['five'] += count_qs_obj[1]
            else:
                count_dict['gt_five'] += count_qs_obj[1]

        response['data'] = {'data':count_dict}

        return jsonify(response), 200
    except Exception as e:
        return jsonify(response), 400
