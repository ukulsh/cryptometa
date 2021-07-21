from project.api.models import (
    ClientMapping,
    ClientDefaultCost,
    CostToClients,
    ClientChannel,
    ClientCustomization,
)
from flask_restful import Api, Resource
from flask import Blueprint, request, jsonify
from project import db
from datetime import datetime, timedelta
from project.api.utils import authenticate_restful
from project.api.utilities.s3_utils import process_upload_logo_file
from project.api.core_features.client_management.utils import get_cost_to_clients
import json
import logging

logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

client_management_blueprint = Blueprint("clientManagement", __name__)
api = Api(client_management_blueprint)


class ClientManagement(Resource):
    def post(self):
        response_object = {"status": "fail"}
        try:
            posted_data = request.get_json()
            client_prefix = posted_data.get("client_prefix")
            cl_obj = (
                db.session.query(ClientMapping)
                .filter(ClientMapping.client_prefix == client_prefix)
                .first()
            )

            # If ClientMapping with client_prefix already exists
            if cl_obj:
                return response_object, 201

            # Add ClientMapping object
            client_name = posted_data.get("client_name")
            account_type = posted_data.get("account_type")
            client_logo = posted_data.get("client_logo")
            theme_color = posted_data.get("theme_color")
            client_mapping_ref = ClientMapping(
                client_name,
                client_prefix,
                account_type,
                client_logo=client_logo,
                theme_color=theme_color,
            )
            db.session.add(client_mapping_ref)

            # Add CostToClients and MasterCouriers object
            cost_to_client_ref = get_cost_to_clients(posted_data)
            db.session.add_all(cost_to_client_ref)

            # Add ClientChannel object
            if client_prefix.startswith("bky_"):  # bky custom channel integration
                channel_int = ClientChannel(
                    client_prefix=client_prefix,
                    store_name=client_name,
                    channel_id=8,
                    api_key=client_prefix.split("ky_")[1],
                    api_password=None,
                    shop_url="https://asia-south1-bikai-d5ee5.cloudfunctions.net",
                    shared_secret=None,
                    mark_shipped=True,
                    shipped_status="SHIPPED",
                    mark_invoiced=False,
                    invoiced_status=None,
                    mark_canceled=True,
                    canceled_status="CANCELLED",
                    mark_delivered=True,
                    delivered_status="DELIVERED",
                    mark_returned=True,
                    returned_status="RETURNED",
                    sync_inventory=False,
                    fetch_status=None,
                )
                db.session.add(channel_int)

            # Add ClientCustomization object
            client_customization = ClientCustomization(
                client_prefix=client_prefix,
                subdomain=None,
                client_logo_url=None,
                theme_color=None,
                background_image_url=None,
                client_name=client_name,
                client_url=None,
                support_url=None,
                privacy_url=None,
            )
            db.session.add(client_customization)

            db.session.commit()
            response_object["status"] = "success"
            return response_object, 201
        except Exception as e:
            logger.error("Failed while inserting clients info", e)
            response_object["message"] = "failed while inserting client info"
            return response_object, 400

    def patch(self):
        response_object = {"status": "fail"}
        try:
            posted_data = request.get_json()
            client_prefix = posted_data.get("client_prefix")
            client_name = posted_data.get("client_name")
            client_mapping_ref = ClientMapping.query.filter_by(
                client_prefix=client_prefix
            ).first()
            if client_name:
                client_mapping_ref.client_name = client_name
            if posted_data.get("thirdwatch_active") != None:
                if client_mapping_ref.thirdwatch == None and posted_data.get(
                    "thirdwatch_active"
                ):
                    client_mapping_ref.thirdwatch_activate_time = (
                        datetime.utcnow() + timedelta(hours=5.5)
                    )
                client_mapping_ref.thirdwatch = posted_data.get("thirdwatch_active")
                client_mapping_ref.thirdwatch_cod_only = posted_data.get(
                    "thirdwatch_cod_only"
                )

            db.session.commit()
            response_object["status"] = "success"
            return response_object, 200
        except Exception as e:
            db.session.rollback()
            logger.error("Failed while updating clients info", e)
            response_object["message"] = "failed while updating client info"
            return response_object, 400

    def get(self):
        response_object = {"status": "fail"}
        try:
            client_prefix = request.args.get("client_prefix")
            client_mapping_ref = ClientMapping.query.filter_by(
                client_prefix=client_prefix
            ).first()
            response_object["thirdwatch"] = client_mapping_ref.thirdwatch
            response_object["status"] = "success"
            return response_object, 200
        except Exception as e:
            logger.error("Failed while fetching clients info", e)
            response_object["message"] = "couldn't get client data"
            return response_object, 400


api.add_resource(ClientManagement, "/core/v1/clientManagement")


class ClientGeneralInfo(Resource):

    method_decorators = {
        "post": [authenticate_restful],
        "get": [authenticate_restful],
        "patch": [authenticate_restful],
    }

    def post(self, resp):
        response_object = {"status": "fail"}
        try:
            authz_data = resp.get("data")
            client_prefix = authz_data.get("client_prefix")
            posted_data = json.loads(request.form.get("data"))
            client_mapping_ref = ClientMapping.query.filter_by(
                client_prefix=client_prefix
            ).first()
            client_mapping_ref.theme_color = posted_data.get("theme_color")
            client_mapping_ref.verify_ndr = posted_data.get("verify_ndr")
            client_mapping_ref.verify_cod = posted_data.get("verify_cod")
            client_mapping_ref.cod_ship_unconfirmed = posted_data.get(
                "cod_ship_unconfirmed"
            )
            client_mapping_ref.cod_man_ver = posted_data.get("verify_cod_manual")
            client_mapping_ref.hide_products = posted_data.get("hide_products")
            client_mapping_ref.hide_address = posted_data.get("hide_shipper_address")
            client_mapping_ref.shipping_label = posted_data.get("shipping_label")
            client_mapping_ref.default_warehouse = posted_data.get("default_warehouse")
            client_mapping_ref.order_split = posted_data.get("order_split")
            client_mapping_ref.auto_pur = posted_data.get("auto_pur")
            client_mapping_ref.auto_pur_time = posted_data.get("auto_pur_time")
            logo_file = request.files.get("logo_file")
            if logo_file:
                logo_url = process_upload_logo_file(client_prefix, logo_file)
                client_mapping_ref.client_logo = logo_url
            db.session.commit()
            response_object["status"] = "success"
            return response_object, 200
        except Exception as e:
            logger.error("Failed while updating client general info", e)
            response_object["message"] = "Failed while updating client general info"
            return response_object, 400

    def get(self, resp):
        response_object = {"status": "fail"}
        try:
            authz_data = resp.get("data")
            client_prefix = authz_data.get("client_prefix")
            client_mapping_ref = ClientMapping.query.filter_by(
                client_prefix=client_prefix
            ).first()
            response_object["data"] = client_mapping_ref.to_json()
            response_object["status"] = "success"
            return response_object, 200
        except Exception as e:
            logger.error("Failed while getting client general info", e)
            response_object["message"] = "Failed while getting the client general info"
            return response_object, 400


api.add_resource(ClientGeneralInfo, "/core/v1/clientGeneralSetting")


class ClientCustomizations(Resource):
    """This class handles api end points for client custom tracking page settings.
    :class:`models.ClientCustomization` entry is created by default when
    :class:`models.ClientMapping` entry is created. Hence fields that are not
    updated by the user contain exisiting data in post requests.
    """

    method_decorators = {"post": [authenticate_restful], "get": [authenticate_restful]}

    def post(self, resp):
        """This function handles updating the :class:`models.ClientCustomization` entry.

        :param resp: User related data added by `authenticate_restful` function.
        :type resp: Dictionary

        :returns: Response object with keys as status and message.
        :rtype: Object
        """
        response_object = {"status": "fail"}
        try:
            auth_data = resp.get("data")
            client_prefix = auth_data.get("client_prefix")
            customization_object = (
                db.session.query(ClientCustomization)
                .filter(ClientCustomization.client_prefix == client_prefix)
                .first()
            )

            # If the ClientCustomization object for a client_prefix is not available
            if not customization_object:
                return response_object, 201

            posted_data = request.form
            print(posted_data)
            customization_object.subdomain = posted_data.get("subdomain")
            customization_object.theme_color = posted_data.get("theme_color")
            customization_object.background_image_url = posted_data.get(
                "background_image_url"
            )
            customization_object.client_name = posted_data.get("client_name")
            customization_object.client_url = posted_data.get("client_url")
            customization_object.nav_links = json.dumps(
                json.loads(posted_data.get("nav_links"))
            )
            customization_object.support_url = posted_data.get("support_url")
            customization_object.privacy_url = posted_data.get("privacy_url")
            customization_object.nps_enabled = json.loads(
                posted_data.get("nps_enabled").lower()
            )

            # Handling logo files
            # * S3 bucket is version enabled
            if isinstance(posted_data.get("client_logo_url"), str):
                # If logo object is already available in S3
                customization_object.client_logo_url = posted_data.get(
                    "client_logo_url"
                )
            elif not json.loads(posted_data.get("client_logo_url").lower()):
                customization_object.client_logo_url = None
            else:
                customization_object.client_logo_url = process_upload_logo_file(
                    client_prefix,
                    request.files.get("client_logo_url"),
                )

            # Handling banner files
            banners = json.loads(posted_data.get("banners"))
            updated_banners = []
            for idx, banner in enumerate(banners):
                banner_object = {}

                # If banner_image is removed
                if not banner.get("banner_image_url"):
                    continue

                # If banner_image is url or attached as a file
                if "https://" not in banner.get("banner_image_url"):
                    banner_object["banner_image_url"] = process_upload_logo_file(
                        client_prefix,
                        request.files.get(banner["banner_image_url"]),
                        bucket="wareiqcustomization",
                        file_name="_banner_file_{0}".format(idx + 1),
                        master_bucket=None,
                    )
                else:
                    banner_object["banner_image_url"] = banner.get("banner_image_url")
                banner_object["image_redirect_url"] = json.dumps(
                    banner.get("image_redirect_url")
                )

                updated_banners.append(banner_object)
            customization_object.banners = json.dumps(updated_banners)

            db.session.commit()
            response_object["status"] = "success"
            return response_object, 200
        except Exception as e:
            db.session.rollback()
            logger.error("Failed while posting client customization info", e)
            response_object[
                "message"
            ] = "Failed while posting client customization info"
            return response_object, 400

    def get(self, resp):
        """This function handles getting the :class:`models.ClientCustomization` entry.

        :param resp: User related data added by `authenticate_restful` function.
        :type resp: Dictionary

        :returns: Response object with keys as status and message.
        :rtype: Object
        """
        response_object = {"status": "fail"}
        try:
            auth_data = resp.get("data")
            client_prefix = auth_data.get("client_prefix")
            customization_object = ClientCustomization.query.filter_by(
                client_prefix=client_prefix
            ).first()

            if customization_object:
                response_object["data"] = customization_object.to_json()
            else:
                # If ClientCustomization entry is not available, create one.
                # This is for the clients already on the platform before this feature went live.
                client_object = ClientMapping.query.filter_by(
                    client_prefix=client_prefix
                ).first()
                new_customization_object = ClientCustomization(
                    client_prefix=client_prefix, client_name=client_object.client_name
                )
                db.session.add(new_customization_object)
                db.session.commit()

                customization_object = ClientCustomization.query.filter_by(
                    client_prefix=client_prefix
                ).first()
                response_object["data"] = customization_object.to_json()

            response_object["status"] = "success"
            return response_object, 200
        except Exception as e:
            db.session.rollback()
            logger.error("Failed while getting client customization info", e)
            response_object[
                "message"
            ] = "Failed while getting the client customization info"
            return response_object, 400


api.add_resource(ClientCustomizations, "/core/v1/clientCustomizations")


@client_management_blueprint.route(
    "/core/v1/clientCustomizations/check_subdomain", methods=["GET"]
)
@authenticate_restful
def check_subdomain_availability(resp):
    response_object = {"status": "fail"}
    try:
        subdomain = request.args.get("subdomain")
        is_available = (
            ClientCustomization.query.filter_by(subdomain=subdomain).first() is None
        )
        response_object["message"] = is_available
        response_object["status"] = "success"
        return response_object, 200
    except Exception as e:
        logger.error("Failed while checking subdomain availability", e)
        response_object["message"] = "Failed while checking subdomain availability"
        return response_object, 400


@client_management_blueprint.route("/core/v1/getDefaultCost", methods=["GET"])
@authenticate_restful
def get_default_cost(resp):
    response_object = {"status": "fail"}
    try:
        cost_data_ref = ClientDefaultCost.query.all()
        response_object["data"] = [it.to_json() for it in cost_data_ref]
        response_object["status"] = "success"
        return jsonify(response_object), 200
    except Exception as e:
        logger.error("Failed while getting client default cost info", e)
        response_object["message"] = "Failed while getting the client default cost"
        return jsonify(response_object), 400


@client_management_blueprint.route("/core/v1/getClientCost", methods=["GET"])
@authenticate_restful
def get_client_cost(resp):
    response_object = {"status": "fail"}
    try:
        authz_data = resp.get("data")
        client_prefix = authz_data.get("client_prefix")
        cost_data_ref = CostToClients.query.filter_by(client_prefix=client_prefix)
        response_object["data"] = [it.to_json() for it in cost_data_ref]
        response_object["status"] = "success"
        return jsonify(response_object), 200
    except Exception as e:
        logger.error("Failed while getting client cost info", e)
        response_object["message"] = "Failed while getting the client cost info"
        return jsonify(response_object), 400
