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
from . import helper as helper

CORE_SERVICE_URL = os.environ.get("CORE_SERVICE_URL") or "https://track.wareiq.com"

tracking_blueprint = Blueprint("tracking", __name__)

hashids = Hashids(min_length=6, salt="thoda namak shamak daalte hai")

conn = psycopg2.connect(
    host=os.environ.get("DATABASE_HOST"),
    database=os.environ.get("DATABASE_NAME"),
    user=os.environ.get("DATABASE_USER"),
    password=os.environ.get("DATABASE_PASSWORD"),
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
        # If subdomain doesn't exist, return to WareIQ tracking page
        client_details = helper.check_subdomain_exists(subdomain, cur)
        if not client_details:
            return redirect("https://wareiq.wiq.app", code=301)

        customization_details = {
            "client_prefix": client_details[0],
            "client_logo_url": client_details[1],
            "theme_color": client_details[2],
            "background_image_url": client_details[3],
            "client_name": client_details[4],
            "client_url": client_details[5],
            "nav_links": json.loads(client_details[6]) if client_details[6] else [],
            "support_url": client_details[7],
            "privacy_url": client_details[8],
            "nps_enabled": client_details[9],
            "banners": json.loads(client_details[10]) if client_details[10] else [],
        }

        return render_template("tracking.html", data=customization_details)
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
        # If subdomain doesn't exist, return to WareIQ tracking page
        client_details = helper.check_subdomain_exists(subdomain, cur)
        if not client_details:
            return redirect("https://wareiq.wiq.app", code=301)

        if subdomain != "wareiq":
            cur.execute(
                """
                SELECT 
                    aa.client_prefix, aa.client_logo_url, aa.theme_color, aa.background_image_url, 
                    aa.client_name, aa.client_url, aa.nav_links, aa.support_url, aa.privacy_url, aa.nps_enabled, 
                    aa.banners, cc.id
                FROM client_customization aa 
                LEFT JOIN orders bb on aa.client_prefix=bb.client_prefix 
                LEFT JOIN shipments cc on bb.id=cc.order_id
                WHERE aa.subdomain=%s and cc.awb=%s""",
                (subdomain.lower(), awb),
            )
            client_details = cur.fetchone()
        else:
            # If the subdomain is "wareiq", combine two seperate queries for
            # client customization and awb
            cur.execute(
                """
                SELECT 
                    client_prefix, client_logo_url, theme_color, background_image_url, 
                    client_name, client_url, nav_links, support_url, privacy_url, nps_enabled, 
                    banners 
                FROM client_customization
                WHERE subdomain=%s""",
                (subdomain.lower(),),
            )
            client_details = list(cur.fetchone())
            cur.execute("""SELECT id FROM shipments WHERE awb=%s""", (awb,))
            client_details.append(cur.fetchone()[0])

        # If tracking id is not found
        if not client_details or not client_details[11]:
            return redirect(url.split("tracking")[0] + "?invalid=Tracking ID not found.")

        customization_details = {
            "client_prefix": client_details[0],
            "client_logo_url": client_details[1],
            "theme_color": client_details[2],
            "background_image_url": client_details[3],
            "client_name": client_details[4],
            "client_url": client_details[5],
            "nav_links": json.loads(client_details[6]) if client_details[6] else [],
            "support_url": client_details[7],
            "privacy_url": client_details[8],
            "nps_enabled": client_details[9],
            "banners": json.loads(client_details[10]) if client_details[10] else [],
            "id": client_details[11],
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

        courier = helper.get_courier_details(awb, cur)
        customization_details["courier_name"] = courier[0]
        customization_details["courier_logo"] = courier[1]

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
            subdomain = url.split(".")[0].replace("https://", "")
            subdomain = subdomain.replace("http://", "")
        else:
            subdomain = "wareiq"

        orderId = request.args.get("orderId")
        mobile = request.args.get("mobile")
        if not orderId or not mobile:
            return redirect(url.split("tracking")[0] + "?invalid=Order ID and phone number required.")

        mobile = "".join(e for e in str(mobile) if e.isalnum())
        mobile = "0" + mobile[-10:]

        cur = conn.cursor()
        if subdomain != "wareiq":
            cur.execute(
                """
                SELECT 
                    aa.client_prefix, client_logo_url, theme_color, background_image_url, 
                    client_name, client_url, nav_links, support_url, privacy_url, nps_enabled, 
                    banners, cc.id, cc.awb
                FROM client_customization aa 
                LEFT JOIN orders bb on aa.client_prefix=bb.client_prefix 
                LEFT JOIN shipments cc on bb.id=cc.order_id
                WHERE subdomain=%s and bb.channel_order_id=%s and bb.customer_phone=%s""",
                (subdomain, orderId, mobile),
            )
            client_details = cur.fetchall()
        else:
            # If the subdomain is "wareiq", combine two seperate queries for
            # client customization and order id, mobile
            cur.execute(
                """
                SELECT 
                    client_prefix, client_logo_url, theme_color, background_image_url, 
                    client_name, client_url, nav_links, support_url, privacy_url, nps_enabled, 
                    banners 
                FROM client_customization
                WHERE subdomain=%s""",
                (subdomain.lower(),),
            )
            client_details = list(cur.fetchone())
            cur.execute(
                """
                SELECT bb.id, bb.awb
                FROM orders aa
                LEFT JOIN shipments bb on aa.id=bb.order_id
                WHERE aa.channel_order_id=%s and aa.customer_phone=%s""",
                (orderId, mobile),
            )
            order_details = cur.fetchall()
            client_details = [client_details + list(ii) for ii in order_details]

        # Check for multiple AWBs connected to the user
        if len(client_details) > 1:
            awbs = [ii[12] for ii in client_details]
            awbString = ",".join(awbs)
            return redirect(url.split("tracking")[0] + "?awb=" + awbString)

        if len(client_details) == 1:
            client_details = client_details[0]

        # If no order exists for given details
        if not client_details or not client_details[11] or not client_details[12]:
            return redirect(url.split("tracking")[0] + "?invalid=No record found for given ID and phone number.")

        customization_details = {
            "client_prefix": client_details[0],
            "client_logo_url": client_details[1],
            "theme_color": client_details[2],
            "background_image_url": client_details[3],
            "client_name": client_details[4],
            "client_url": client_details[5],
            "nav_links": json.loads(client_details[6]) if client_details[6] else [],
            "support_url": client_details[7],
            "privacy_url": client_details[8],
            "nps_enabled": client_details[9],
            "banners": json.loads(client_details[10]) if client_details[10] else [],
            "id": client_details[11],
            "awb": client_details[12],
        }

        req1 = requests.get(CORE_SERVICE_URL + "/orders/v1/track/%s" % client_details[12])
        req2 = requests.get(CORE_SERVICE_URL + "/orders/v1/track/%s?details=true" % client_details[12])

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
                    scan_time = datetime.strptime(each_scan["time"], "%d %b %Y, %H:%M:%S")
                    scan_time = scan_time.strftime("%I:%M %p")
                    each_scan["time"] = scan_time

        courier = helper.get_courier_details(customization_details["awb"], cur)
        customization_details["courier_name"] = courier[0]
        customization_details["courier_logo"] = courier[1]

        data.update(customization_details)
        return render_template("trackingDetails.html", data=data, enumerate=enumerate)
    except Exception as e:
        conn.rollback()
        return jsonify({"msg": "Invalid URL"}), 400
