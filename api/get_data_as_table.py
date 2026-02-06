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
        log.info("db_read: request_parse: main_label is a attribute of the \
                 ReadRequest, however the frontend does not give one.\
                 defaulting to the first label.")


        max_depth = int(req.args.get("maxDepth", max(3, len(selected_labels))))
        limit_raw = req.args.get("limit")
        limit = int(limit_raw) if limit_raw else None

        filter_labels_raw = req.args.get("filterLabels")
        filter_labels = [l.strip() for l in filter_labels_raw.split(",")] if filter_labels_raw else None

        relationships_raw = req.args.get("relationships")
        rel_filter = [r.strip() for r in relationships_raw.split(",")] if relationships_raw else None

        qb_raw = req.args.get("qb")
        if qb_raw and qb_raw.lower() != "null":
            raise NotImplementedError(f"qbraw was in the args of get_data_as_table route")
            # qb_json = json.loads(qb_raw)
            # if qb_json:  # prüfen, dass es nicht None ist
            #     raise NotImplementedError
            #     where = qb_to_cypher(qb_json)

        # allow manual where override
        manual_where = req.args.get("where")
        if manual_where:
            raise NotImplementedError(f"Where clauses are not supported atm")
            # where = manual_where

        return ReadRequest(selected_labels, main_label, max_depth, limit, filter_labels, rel_filter)

    @bp.route("/get_data_as_table", methods=["GET"])
    @conditional_login_required
    def get_data_as_table2():
        log.debug("=====api.get_data_as_table=====")
        driver = current_app.config["driver"]
        # log.debug(driver)
        interf_db = Neo4jDB(driver)
        params = parse_request_params(request)

        # BUG: The next logging process produces a weird error
        # log.debug("Parsed Parameters from Front", params)

        data = interf_db.read_data(params)
        log.debug(f" data was read: {data}"[:100])

        # df = records_to_table(data)
        data_dict = records_to_json(data, params)

        log.debug(f"JSON Data: {data_dict}"[:200])
        return jsonify(data_dict)

    return bp

def cols_from_data(data) -> list[dict]:
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

def records_to_json(data: list[Record], params:ReadRequest) -> dict[str, Any]:
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
                # indentation for node_types that are not related
                # indent = columns_per_node_type.get(label)
                # if indent:
                #     for i in range(indent):
                #         cells.append(empty_cell)
                for prop, val in element.items():

                    # TTD
                    # col_label = columns[len(cells)]["nodeType"]
                    # assert label == col_label, f"expected {label} == {col_label}, but it aint in {len(cells)}"


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
        rows.append(row)

    for row in rows:
        if row["relations"]: continue
        row_type = row["cells"][0]["nodeType"]
        indent = indent_distances.get(row_type)
        if indent:
            for i in range(indent):
                row["cells"].insert(0, empty_cell)


    return {"columns": columns, "rows": rows}
