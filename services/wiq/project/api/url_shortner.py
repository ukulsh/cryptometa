# services/wiq/project/api/url_shortner.py

from flask import Blueprint, jsonify, redirect
import os, psycopg2
from hashids import Hashids

CORE_SERVICE_URL = os.environ.get('CORE_SERVICE_URL') or 'http://localhost:5010'

shortner_blueprint = Blueprint('shortner', __name__)

hashids = Hashids(min_length=6, salt="thoda namak shamak daalte hai")


@shortner_blueprint.route('/<id>')
def url_redirect(id):
    original_id = hashids.decode(id)
    if original_id:
        conn = psycopg2.connect(host="wareiq-core-prod2.cvqssxsqruyc.us-east-1.rds.amazonaws.com", database="core_prod",
                                user="postgres", password="aSderRFgd23")
        cur = conn.cursor()
        original_id = original_id[0]
        cur.execute("""SELECT url FROM url_shortner where id=%s;"""%str(original_id))
        original_url = cur.fetchone()[0]
        cur.execute("UPDATE url_shortner SET clicks = COALESCE(clicks, 0)+1 WHERE id = %s"%str(original_id))
        conn.commit()
        conn.close()
        return redirect(original_url)
    else:
        return jsonify({"msg": "Invalid URL"}), 400