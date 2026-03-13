import json
from flask import Blueprint, request, current_app, Response
from oasis_helper import conditional_login_required
from api.neo4j_interface import Neo4jDB, ReadRequest
from api.read_as_table.strategies import topological_rec_to_json, records_to_json, parse_request_params
import logging


def create_get_data_bp(parser=parse_request_params,
                       translator=topological_rec_to_json,#records_to_json,
                       log = logging.getLogger("[API] get_data_as_table")
                       ) -> Blueprint:
    """
    Returns the blueprint to create_data

    :param parser: Component which takes the request as an arg and returns a ReadRequest.
    :translator: Component that takes the Neo4j records as an Input and returns the json.
    """

    bp = Blueprint("get_data_bp", __name__)

    @bp.route("/get_data_as_table", methods=["GET"])
    @conditional_login_required
    def get_data_as_table2() -> Response:
        driver = current_app.config["driver"]

        log.debug(f"Parsing request: {request}")

        try:
            params: ReadRequest = parser(request)
            log.debug(f"Parsed request: {params}")

            interf_db = Neo4jDB(driver)
            data = interf_db.read_data(params)
            log.debug("data was read: %.100s", data)

            response = translator(data, params)

            log.debug("Response: %.200s", response)
            return response
        except ValueError as e:
            return Response(json.dumps({"error": str(e)}), status=400, mimetype="application/json")

    return bp

