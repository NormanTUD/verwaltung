from neo4j import Driver, Record
from neo4j.exceptions import Neo4jError
import logging
from dataclasses import dataclass, asdict
from typing import Any
from abc import ABC

from api.neo4j_interface_helpers import label_validation, validate_labels
from api.qb_helpers import QueryBuilderProcessor, CypherWhereClause, old_where_clause

logger = logging.getLogger(__name__)


"""
Dataclasses
"""
@dataclass
class ReadRequest():
    """
    dataclass that holds query parameters for the db.
    :selected_labes: a list of the labels to be retrieved
    :limit: maximum amount of returned nodes
    :property_filters: JQueryBuider output
    :rel_filters: list of relations  between nodes

    some parameters like a max_depth are not implemented as of now.
    """

    selected_labels: list[str]
    limit: int | None
    property_filters: dict[str, str] | None
    rel_fitler: list[str] | None


"""
Helper Functions - Dataclasses End
"""


def construct_cypher_query(
    node_labels: list[str],
    property_filters: dict | None = None,
    rel_types: list[str] | None = None,
    limit: int | None = None,
    all_labels: bool = False
) -> tuple[str, dict[str, Any]]:

    # if not node_labels: raise ValueError("N4JDB: construct_cypher: Unvalid node labels")
    # validate
    validation = validate_labels(
        node_labels,
        property_filters.keys() if property_filters else None,
        rel_types or None,
        logger
    )
    if not validation.ok:
        raise ValueError(
            f"The following identifiers are not valid Neo4j names: {validation.bad}"
        )

    # Node Types
    if all_labels:
        node_label_clause = ":" + ":".join(node_labels)
    else:
        node_label_clause = ":" + "|".join(node_labels)
    node_pattern = f"(n{node_label_clause})"

    # Relationship clause (optional)

    if rel_types:
        rel_type_clause = ":" + "|".join(rel_types)
        rel_pattern = f"-[r{rel_type_clause}]->(m)"
        match_clause = f"MATCH {node_pattern}{rel_pattern}"
        return_clause = " RETURN n, r, m"
    else:
        match_clause = f"MATCH {node_pattern}"
        return_clause = " RETURN n"

    # where clause
    parameters: dict[str, Any] = {}


    if not property_filters: where_clause = ""
    else:
        qb_p = QueryBuilderProcessor("n")
        where = qb_p.process(property_filters)

        # TODO: Remove TDD Fallback
        if where.is_valid:
            where_clause, parameters = "WHERE " + where.clause, where.parameters
            logger.debug(f"DEV: qb-parser: valid: {where_clause=},\n    {parameters=}")
        else: raise ValueError(f"{where.errors}")
            # logger.warning("Outdated property_filters request")
            # where_clause, parameters = old_where_clause(property_filters)



    # limit
    limit_clause = ""
    if limit is not None:
        limit_clause = " LIMIT $limit"
        parameters["limit"] = limit

    # Construct
    cypher = (
        f"{match_clause}"
        f"{where_clause}"
        f"{return_clause}"
        f"{limit_clause}"
    )
    if 'limit' in parameters:
        parameters["limit"] = limit
    return cypher, parameters

"""
Interface
"""

class Neo4jDBInterface(ABC):
    def __init__(self, driver: Driver , logger=None):
        self._driver = driver

    def create_data(self):
        raise NotImplementedError("Method not implemented yet.")

    def read_data(self,
                  request: ReadRequest):

        raise NotImplementedError("Method not implemented yet.")

    def update_data(self):
        raise NotImplementedError("Method not implemented yet.")

    def delete_data(self):
        raise NotImplementedError("Method not implemented yet.")

class Neo4jDB(Neo4jDBInterface):
    def __init__(self, driver: Driver, logger=None):
        super().__init__(driver)
        self.logger = logger or logging.getLogger("[Database]") # This should probably get injected
        self.logger.setLevel(logging.INFO)

    """
    Main Interface Methods
    """

    def read_data(self, req_data: ReadRequest) -> list[Record]: #node_types, relationships = None, filters = None, limit=None
        node_types = req_data.selected_labels
        relationships = req_data.rel_fitler
        filters = req_data.property_filters
        limit = req_data.limit
        if not limit: limit = 1000

        # max_depth is not implemented
        self.logger.info("cypher construction: max_depth from ReadRequest is not implemented.")

        self.logger.debug(f"Read Query: {node_types=}, {relationships=}, {filters=}, {limit=}")

        # tightly coupled - but the cypher constructing is a bridge between request interface and db access
        cypher, params = construct_cypher_query(node_types, filters, relationships, limit)
        self.logger.debug(f"Cypher was created: {cypher} with paramets: {params}")

        with self._driver.session() as session:
            """
            converting the Result object to a list of Records is memory intensive
            should be no problem if we dont have results with over 1000s of Nodes
            However to fulfill the endpoint, we can validate the limit to be <1000?
            """

            r = session.run(cypher, params)
            result = list(r)

            r.consume()

        return result

