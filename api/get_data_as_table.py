import json
from flask import Blueprint, request, jsonify, current_app, Response
from oasis_helper import conditional_login_required
from enum import Enum
from api.neo4j_interface import Neo4jDB, ReadRequest
from neo4j import Record
from neo4j.graph import Node, Relationship
import logging

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
        return jsonify({"columns": [], "rows": []})

    columns = cols_from_data(data)
    indent_distances = distance_of_unrelated_node_types(columns)

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
        #log.debug(f"{row=}")
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




    @bp.route("/get_data_as_table", methods=["GET"])
    @conditional_login_required
    def get_data_as_table2() -> Response:
        driver = current_app.config["driver"]
        interf_db = Neo4jDB(driver)

        log.debug(f"Parsing request: {request}")
        params: ReadRequest = parser(request)
        log.debug(f"Parsed request: {params}")
        data = interf_db.read_data(params)

        # Experimental
        top = topology_detector(data)
        log.info(f"Topology Detector: {top=}")

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




from enum import Enum, auto
from dataclasses import dataclass

class NodeRole(Enum):
    LEAF = auto() # No Children, effectively a sub-table for every parent node
    CHAIN = auto() # One Child, creating a sub-table for each node
    FORK = auto() # 2+ Children, creating multiple sub-tables

class TopologyNode:
    def __init__(self, node_lbl):
        self.node_lbl = node_lbl
        self.connected_to = []
        self.incoming_con_n = 0

    @property
    def is_root(self) -> bool:
        return self.incoming_con_n == 0

    @property
    def is_leaf(self) -> bool:
        return len(self.connected_to) == 0

    def get_classification(self):
        if self.is_leaf: return NodeRole.LEAF
        if len(self.connected_to) > 1: return NodeRole.FORK
        else: return NodeRole.CHAIN

    def __repr__(self):
        return f"{self.node_lbl}: of type {self.get_classification()} with {len(self.connected_to)} children and {self.incoming_con_n} parents. "



@dataclass(frozen=True)
class AbstractRelation:
    from_node_type: str
    to_node_type:str



def extract_node_types_and_relations(data)-> tuple[set[str], set[AbstractRelation]]:
    known_nodes= set()
    relations = set()
    for record in data:
        for element in record:

            if isinstance(element, Relationship):
                n1 = element.nodes[0]
                l1, = n1.labels
                n2 = element.nodes[1]
                l2, = n2.labels

                r = AbstractRelation(l1, l2)
                if r in relations: continue
                relations.add(r)
                log.debug("Found a relation")
                continue

            label = list(element.labels)[0]
            if label in known_nodes: continue
            known_nodes.add(label)

    return known_nodes, relations



def topology_detector(data):
    node_types, relations = extract_node_types_and_relations(data)
    log.info(f"topology detect: \n    {node_types=}\n    {relations=} ")
    nodes = {n:TopologyNode(n) for n in node_types}

    for r in relations:
        # Wont work for relation between two nodes of same type
        # wed create a TopNode that points to itself, is no root
        from_node = nodes[r.from_node_type]
        to_node = nodes[r.to_node_type]
        from_node.connected_to.append(to_node)
        to_node.incoming_con_n += 1


    return sorted([n for n in nodes.values()], key=lambda node: len(node.connected_to))
