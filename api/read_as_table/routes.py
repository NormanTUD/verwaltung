import json
from flask import Blueprint, request, current_app, Response
from oasis_helper import conditional_login_required
from api.neo4j_interface import Neo4jDB, ReadRequest
from api.read_as_table.strategies import topological_rec_to_json, parse_request_params
from api.read_as_table.debug_view import render_debug_view
import logging


def create_get_data_bp(
    parser=parse_request_params,
    translator=topological_rec_to_json,
    log=logging.getLogger("[API] read_as_table.routes"),
) -> Blueprint:
    bp = Blueprint("get_data_bp", __name__)

    @bp.route("/get_data_as_table", methods=["GET"])
    @conditional_login_required
    def get_data_as_table() -> Response:
        driver = current_app.config["driver"]
        log.debug(f"Parsing request: {request}")

        try:
            params: ReadRequest = parser(request)
            log.debug(f"Parsed request: {params}")

            interf_db = Neo4jDB(driver)
            data = interf_db.read_data(params)
            log.debug("data was read: %.100s", data)

            # ── NEW: debug/plot view ────────────────────────────────────
            plot = request.args.get("plot")
            if plot:
                return render_debug_view(data, params, mode=plot)
            # ────────────────────────────────────────────────────────────

            response = translator(data, params)
            log.debug("Response: %.200s", response)
            return response

        except ValueError as e:
            log.warning(f"Request failed: {e}")
            return Response(
                json.dumps({"error": str(e)}), status=500, mimetype="application/json"
            )

    return bp
