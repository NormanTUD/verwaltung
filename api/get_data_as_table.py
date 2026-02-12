import json
from flask import Blueprint, request, jsonify, current_app, Response, Parse_Error
from oasis_helper import conditional_login_required

from api.neo4j_interface import Neo4jDB, ReadRequest
from neo4j import Record
from neo4j.graph import Node, Relationship
import logging
from pandas import DataFrame
from typing import Any

log = logging.getLogger("[API] get_data_as_table")
log.setLevel(logging.INFO)

def parse_request_params(req) -> ReadRequest:
        nodes_param = req.args.get("nodes")
        if not nodes_param:
            raise ValueError("Parameter 'nodes' required")
        selected_labels = [n.strip() for n in nodes_param.split(",") if n.strip()]

        if req.args.get("maxDepth"):
            log.info("Max Depth is passed from the frontend but is ultimately not used")
            # We dont need it, as we alway want to search by relationship type
            # and not retrieve nodes simply because thei're connected in any way.
            # max_depth = int(req.args.get("maxDepth", max(3, len(selected_labels))))

        limit_raw = req.args.get("limit")
        limit = int(limit_raw) if limit_raw else None

        if req.args.get("filterLabels"):
            log.info("Frontend Send Deprecated field fiter_labels")

        property_filters = None

        relationships_raw = req.args.get("relationships")
        rel_filter = [r.strip() for r in relationships_raw.split(",")] if relationships_raw else None

        qb_raw = req.args.get("qb")
        if qb_raw and qb_raw.lower() != "null":
            property_filters = json.loads(qb_raw)



        # allow manual where override
        manual_where = req.args.get("where")
        if manual_where:
            raise NotImplementedError(f"Where clauses are not supported atm")
            # where = manual_where

        return ReadRequest(selected_labels, limit, property_filters, rel_filter)

def records_to_json(data: list[Record], params:ReadRequest) -> Response:
    if not data:
        return {"columns": [], "rows": []}

    columns = cols_from_data(data)
    indent_distances = distance_of_unrelated_node_types(columns)
    # get_data_print(data)

    log.debug(f"Counted Columns per Node Type: {indent_distances}")


    empty_cell = {"nodeId": None, "nodeType": None, "value": None}

    rows = []

    for record in data:
        row = {}
        cells = []
        relations: list = []

        # populate row
        for element in record:

            if isinstance(element, Node):
                label = list(element.labels)[0]
                for prop, val in element.items():
                    cells.append( {"nodeId": element.element_id,
                                   "nodeType": label,
                                   "value": val
                                   }
                                )

            elif isinstance(element, Relationship):
                relations.append({"fromId": element.nodes[0].element_id,
                                  "relation": element.type,
                                  "toId": element.nodes[1].element_id})
        row["cells"] = cells
        row["relations"] = relations
        if not row["relations"]:
            row_type = row["cells"][0]["nodeType"]
            indent = indent_distances.get(row_type)
            if indent:
                for i in range(indent):
                    row["cells"].insert(0, empty_cell)

        rows.append(row)

    return jsonify({"columns": columns, "rows": rows})

def create_get_data_bp(parser=parse_request_params,
                       translator=records_to_json,
                       ) -> Blueprint:
    """
    Returns the blueprint to create_data

    :param parser: Component which takes the request as an arg and returns a ReadRequest
    :translator: Component that takes the Neo4j records as an Input and returns the json.
    """

    log.debug("Registering Blueprint")
    bp = Blueprint("get_data_bp", __name__)

    # this is probably gross, as it handles everything and returns everything to the user
    # however error handeling should happen here?
    @bp.errorhandler(Exception)
    def handle_parse_error(error):
        return jsonify({'error': str(error)}), 400


    @bp.route("/get_data_as_table", methods=["GET"])
    @conditional_login_required
    def get_data_as_table2() -> Response:
        driver = current_app.config["driver"]
        interf_db = Neo4jDB(driver)

        params: ReadRequest = parser(request)
        data = interf_db.read_data(params)

        log.debug(f" data was read: {data}"[:100])

        data_dict = translator(data, params)

        log.debug(f"JSON Data: {data_dict}"[:200])
        return data_dict

    return bp

def cols_from_data(data: list[Record]) -> list[dict]:
    """ Parses the data for columns, where each column is a node:property"""
    # Create Columns
    columns:list = []
    known_node_types = set()
    for record in data:
        for element in record:
            if isinstance(element, Relationship): continue
            label = list(element.labels)[0]
            if label in known_node_types: continue
            for prop, val in element.items():
                columns.append({"nodeType": label,
                                "property": prop})
            known_node_types.add(label)
    return columns

def distance_of_unrelated_node_types(columns: list) -> dict[str, int]:
    """ Calculates how many columns each type of node has."""
    columns_per_node_type:dict = {}
    prev = None
    for i, c in enumerate(columns):
        node_type = c["nodeType"]
        if not prev:
            prev = node_type
            continue
        if node_type != prev:
            columns_per_node_type[node_type] = i
            prev = node_type
    return columns_per_node_type

def get_data_print(data):
    LIM = "    "
    for record in data:
        print("Record:")
        for element in record:
            print(f"{LIM}{element}")
            for property, value in element.items():
                print(LIM * 2, property, value)
