# services/wiq/project/api/tracking.py

from flask import (
    Blueprint,
    jsonify,
    redirect,
    render_template,
    current_app,
    send_from_directory,
    request,
)
import os, psycopg2, requests, logging, json
from hashids import Hashids
from datetime import datetime

CORE_SERVICE_URL = os.environ.get("CORE_SERVICE_URL") or "https://track.wareiq.com"

tracking_blueprint = Blueprint("tracking", __name__)

hashids = Hashids(min_length=6, salt="thoda namak shamak daalte hai")

conn = psycopg2.connect(
    host="wareiq-core-prod2.cvqssxsqruyc.us-east-1.rds.amazonaws.com",
    database="core_prod",
    user="postgres",
    password="aSderRFgd23",
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@tracking_blueprint.route("/", methods=["GET"])
def tracking_page():
    try:
        url = request.url
        if "5000" not in url:
            subdomain = url.split(".")[0].replace("https://", "")
            subdomain = subdomain.replace("http://", "")
        else:
            subdomain = "wareiq"

        cur = conn.cursor()
        cur.execute(
            "SELECT client_prefix, client_logo_url, theme_color FROM client_customization WHERE subdomain=%s",
            (subdomain,),
        )
        client_details = cur.fetchone()
        if not client_details:
            return jsonify({"msg": "Invalid URL"}), 404

        data_obj = {
            "client_prefix": client_details[0],
            "client_logo_url": client_details[1],
            "theme_color": client_details[2],
        }

        return render_template("tracking.html", data=data_obj)
    except Exception as e:
        conn.rollback()
        return jsonify({"msg": "Invalid URL"}), 400


@tracking_blueprint.route("/static/<path:path>")
def serve_static_files(path):
    return send_from_directory("static", path)


@tracking_blueprint.route("/tracking/<awb>", methods=["GET"])
def tracking_page_detials(awb):
    try:
        url = request.url
        if "5000" not in url:
            subdomain = url.split(".")[0].replace("https://", "")
            subdomain = subdomain.replace("http://", "")
        else:
            subdomain = "wareiq"
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 
                aa.client_prefix, client_logo_url, theme_color, cc.id, background_image_url, 
                client_name, client_url, nav_links, support_url, privacy_url, nps_enabled, 
                banners 
            FROM client_customization aa 
            LEFT JOIN orders bb on aa.client_prefix=bb.client_prefix 
            LEFT JOIN shipments cc on bb.id=cc.order_id
            WHERE subdomain=%s and cc.awb=%s""",
            (subdomain, awb),
        )
        client_details = cur.fetchone()
        if not client_details:
            return jsonify({"msg": "Invalid URL"}), 404
        if not client_details[3]:
            return redirect(
                url.split("tracking")[0] + "?invalid=Tracking ID not found."
            )

        customization_details = {
            "client_prefix": client_details[0],
            "client_logo_url": client_details[1],
            "theme_color": client_details[2],
            "background_image_url": client_details[4],
            "client_name": client_details[5],
            "client_url": client_details[6],
            "nav_links": json.loads(client_details[7]),
            "support_url": client_details[2],
            "privacy_url": client_details[2],
            "nps_enabled": client_details[2],
            "banners": json.loads(client_details[2]),
        }

        req1 = requests.get(CORE_SERVICE_URL + "/orders/v1/track/%s" % awb)
        req2 = requests.get(CORE_SERVICE_URL + "/orders/v1/track/%s?details=true" % awb)

        if not req1.status_code == 200:
            return render_template("tracking.html", data=customization_details)

        data = req1.json()["data"]
        last_update_time = None
        for entry in data["order_track"]:
            if entry["time"]:
                last_update_time = entry["time"]

        if last_update_time:
            last_update_time = datetime.strptime(last_update_time, "%d %b %Y, %I:%M %p")
            last_update_time = last_update_time.strftime("%A, %d %b %Y at %I:%M %p")

        data["latest_update_time"] = last_update_time
        if req2.status_code == 200:
            data["details_data"] = req2.json()["data"]
            for key, value in data["details_data"].items():
                for each_scan in value:
                    scan_time = datetime.strptime(
                        each_scan["time"], "%d %b %Y, %H:%M:%S"
                    )
                    scan_time = scan_time.strftime("%I:%M %p")
                    each_scan["time"] = scan_time

        data.update(customization_details)
        return render_template("trackingDetails.html", data=data, enumerate=enumerate)
    except Exception as e:
        conn.rollback()
        return jsonify({"msg": "Invalid URL"}), 400


@tracking_blueprint.route("/tracking", methods=["GET"])
def tracking_page_details_id():
    try:
        url = request.url
        if "5000" not in url:
            client_track = url.split(".")[0].replace("https://", "")
            client_track = client_track.replace("http://", "")
        else:
            client_track = "justherbs"

        orderId = request.args.get("orderId")
        mobile = request.args.get("mobile")
        if not orderId or not mobile:
            return redirect(
                url.split("tracking")[0]
                + "?invalid=Order ID and phone number required."
            )

        mobile = "".join(e for e in str(mobile) if e.isalnum())
        mobile = "0" + mobile[-10:]

        cur = conn.cursor()
        cur.execute(
            """SELECT aa.client_prefix, client_logo, theme_color, cc.id, cc.awb FROM client_mapping aa 
                        LEFT JOIN orders bb on aa.client_prefix=bb.client_prefix 
                        LEFT JOIN shipments cc on bb.id=cc.order_id
                        WHERE tracking_url=%s 
                        and bb.channel_order_id=%s and bb.customer_phone=%s""",
            (client_track, orderId, mobile),
        )
        client_details = cur.fetchone()
        if not client_details or not client_details[3] or not client_details[4]:
            return redirect(
                url.split("tracking")[0]
                + "?invalid=No record found for given ID and phone number."
            )

        req1 = requests.get(
            CORE_SERVICE_URL + "/orders/v1/track/%s" % client_details[4]
        )
        req2 = requests.get(
            CORE_SERVICE_URL + "/orders/v1/track/%s?details=true" % client_details[4]
        )

        if not req1.status_code == 200:
            data_obj = {
                "client_prefix": client_details[0],
                "logo_url": client_details[1],
                "theme_color": client_details[2],
            }

            return render_template("tracking.html", data=data_obj)

        data = req1.json()["data"]
        last_update_time = None
        for entry in data["order_track"]:
            if entry["time"]:
                last_update_time = entry["time"]

        if last_update_time:
            last_update_time = datetime.strptime(last_update_time, "%d %b %Y, %I:%M %p")
            last_update_time = last_update_time.strftime("%A, %d %b %Y at %I:%M %p")

        data["latest_update_time"] = last_update_time
        if req2.status_code == 200:
            data["details_data"] = req2.json()["data"]
            for key, value in data["details_data"].items():
                for each_scan in value:
                    scan_time = datetime.strptime(
                        each_scan["time"], "%d %b %Y, %H:%M:%S"
                    )
                    scan_time = scan_time.strftime("%I:%M %p")
                    each_scan["time"] = scan_time

        return render_template("trackingDetails.html", data=data, enumerate=enumerate)
    except Exception as e:
        conn.rollback()
        return jsonify({"msg": "Invalid URL"}), 400
