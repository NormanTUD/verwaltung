import json
from flask import Blueprint, request, jsonify, current_app
from oasis_helper import conditional_login_required

from api.neo4j_interface import Neo4jDB, ReadRequest
from neo4j import Record
from neo4j.graph import Node, Relationship
from logging import getLogger
from pandas import DataFrame
from typing import Any

log = getLogger("get_data_as_table")
def create_get_data_bp() -> Blueprint:
    log.info("Registering Blueprint")
    bp = Blueprint("get_data_bp", __name__)




    def parse_request_params(req) -> ReadRequest:
        nodes_param = req.args.get("nodes")
        if not nodes_param:
            raise ValueError("Parameter 'nodes' required")
        selected_labels = [n.strip() for n in nodes_param.split(",") if n.strip()]
        main_label = selected_labels[0]

        max_depth = int(req.args.get("maxDepth", max(3, len(selected_labels))))
        limit_raw = req.args.get("limit")
        limit = int(limit_raw) if limit_raw else None

        filter_labels_raw = req.args.get("filterLabels")
        filter_labels = [l.strip() for l in filter_labels_raw.split(",")] if filter_labels_raw else None

        relationships_raw = req.args.get("relationships")
        rel_filter = [r.strip() for r in relationships_raw.split(",")] if relationships_raw else None

        qb_raw = req.args.get("qb")
        where = None
        if qb_raw and qb_raw.lower() != "null":
            qb_json = json.loads(qb_raw)
            if qb_json:  # prüfen, dass es nicht None ist
                where = qb_to_cypher(qb_json)

        # allow manual where override
        manual_where = req.args.get("where")
        if manual_where:
            where = manual_where

        return ReadRequest(selected_labels, main_label, max_depth, limit, filter_labels, where, rel_filter)

    @bp.route("/get_data_as_table", methods=["GET"])
    @conditional_login_required
    def get_data_as_table2():
        log.debug("=====api.get_data_as_table=====")
        driver = current_app.config["driver"]
        # log.debug(driver)
        interf_db = Neo4jDB(driver)
        log.debug(interf_db)
        params = parse_request_params(request)

        # BUG: The next logging process produces a weird error
        # log.debug("Parsed Parameters from Front", params)

        data = interf_db.read_data(params)
        log.debug(f" data was read: {data}"[:100])

        # df = records_to_table(data)
        data_dict = records_to_json(data)

        log.debug(f"JSON Data: {data_dict}"[:200])
        return jsonify(data_dict)

    return bp


def records_to_json(data: list[Record]) -> dict[str, Any]:

    if not data:
        return {"columns": [], "rows": []}
    columns = []
    # If every Record has the same structure, we only need to consider the first to create our columns
    for record in data:
        for element in record:
            if isinstance(element, Node):
                label = list(element.labels)[0]
                if not label in [c["nodeType"] for c in columns]:
                    for prop, val in element.items():
                        columns.append({"nodeType": label,
                                        "property": prop})

    rows = []

    for record in data:
        row = {}
        cells = []
        relations: list = []
        for element in record:

            if isinstance(element, Node):

                label = list(element.labels)[0]
                for prop, val in element.items():
                    cells.append( {"nodeId": element.element_id,
                                   "nodyType": label,
                                   "value": val
                                   }
                                )
            elif isinstance(element, Relationship):
                relations.append({"fromId": element.nodes[0].element_id,
                                  "relation": element.type,
                                  "toId": element.nodes[1].element_id})


        row["cells"] = cells
        row["relations"] = relations
        rows.append(row)


    return {"columns": columns, "rows": rows}
