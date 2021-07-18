# services/wiq/project/api/tracking.py

from flask import Blueprint, jsonify, redirect, render_template, current_app, \
    send_from_directory, request
import os, psycopg2
from hashids import Hashids

CORE_SERVICE_URL = os.environ.get('CORE_SERVICE_URL') or 'http://localhost:5010'

tracking_blueprint = Blueprint('tracking', __name__)

hashids = Hashids(min_length=6, salt="thoda namak shamak daalte hai")


@tracking_blueprint.route('/', methods=['GET'])
def tracking_page():
    try:
        url = request.url
        if '5000' not in url:
            client_track = url.split('.')[0].strip('https://')
        else:
            client_track = 'wareiq'
        conn = psycopg2.connect(host="wareiq-core-prod2.cvqssxsqruyc.us-east-1.rds.amazonaws.com", database="core_prod",
                                user="postgres", password="aSderRFgd23")
        cur = conn.cursor()

        cur.execute("SELECT client_prefix, client_logo, theme_color FROM client_mapping WHERE lower(client_prefix)=%s", (client_track, ))
        client_details = cur.fetchone()
        conn.close()
        if not client_details:
            return jsonify({"msg": "Invalid URL"}), 404

        data_obj = {"client_prefix": client_details[0],
                     "logo_url": client_details[1],
                     "theme_color": client_details[2]}

        return render_template("tracking.html", data=data_obj)
    except Exception as e:
        return jsonify({"msg": "Invalid URL"}), 400


@tracking_blueprint.route('/static/<path:path>')
def serve_static_files(path):
    return send_from_directory('static', path)


@tracking_blueprint.route('/tracking/<awb>', methods=['GET'])
def tracking_page(awb):
    try:
        url = request.url
        if '5000' not in url:
            client_track = url.split('.')[0].strip('https://')
        else:
            client_track = 'wareiq'
        conn = psycopg2.connect(host="wareiq-core-prod2.cvqssxsqruyc.us-east-1.rds.amazonaws.com", database="core_prod",
                                user="postgres", password="aSderRFgd23")
        cur = conn.cursor()

        cur.execute("SELECT client_prefix, client_logo, theme_color FROM client_mapping WHERE lower(client_prefix)=%s", (client_track, ))
        client_details = cur.fetchone()
        conn.close()
        if not client_details:
            return jsonify({"msg": "Invalid URL"}), 404

        data_obj = {"client_prefix": client_details[0],
                     "logo_url": client_details[1],
                     "theme_color": client_details[2]}

        return render_template("tracking.html", data=data_obj)
    except Exception as e:
        return jsonify({"msg": "Invalid URL"}), 400