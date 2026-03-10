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
    :rel_as_filter: returns only nodes that have the requested relationships if True,
    else requests all Nodes regardless if they have the connection or not.

    some parameters like a max_depth are not implemented as of now.
    """

    selected_labels: list[str]
    limit: int | None
    property_filters: dict[str, str|int] | None
    rel_filter: list[str] | None
    rel_as_filter: bool | None = None # This is for logging that the frontend does not implement this endpoint, can be changed to True later on.

    def __post_init__(self):
        """ Basic Validation of the fields on a Type-Basis
        Cypher Validation/Safety is happening in another place"""
        for l in self.selected_labels:
            if not isinstance(l, str): raise ValueError("ReadRequest: Bad label")
        if not isinstance(self.limit, int) or not self.limit > 0:
            if self.limit is not None: raise ValueError("ReadRequest: Bad Limit")
        if self.property_filters:
            for k,v in self.property_filters.items():
                if not isinstance(k, str): raise ValueError("ReadRequest: Bad property filter")
        if self.rel_filter is not None:
            for rf in self.rel_filter:
                if not isinstance(rf, str): raise ValueError("ReadRequest: Bad relationship")
        if self.rel_as_filter is not None:
            if not isinstance(self.rel_as_filter, bool): raise ValueError("ReadRequest: Bad rel_as_filter switch")


"""
Helper Functions - Dataclasses End
"""


def construct_cypher_query(
    node_labels: list[str],
    property_filters: dict | None = None,
    rel_types: list[str] | None = None,
    limit: int | None = None,
    all_labels: bool = False,
    rel_as_filter = True
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

    # Relationship clause (optional)

    if rel_types:
        rel_type_clause = ":" + "|".join(rel_types)
        rel_pattern = f"-[r{rel_type_clause}]->(m)"
        if rel_as_filter:
            match_clause = f"MATCH {node_pattern}{rel_pattern} {where_clause}"
        else:
            match_clause = f"MATCH {node_pattern} {where_clause} OPTIONAL MATCH {node_pattern}{rel_pattern}"

        return_clause = " RETURN n, r, m"
    else:
        match_clause = f"MATCH {node_pattern} {where_clause}"
        return_clause = " RETURN n"



    # Construct
    cypher = (
        f"{match_clause}"
        #f"{where_clause}"
        f"{return_clause}"
        f"{limit_clause}"
    )
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
        relationships = req_data.rel_filter
        filters = req_data.property_filters
        limit = req_data.limit
        if not limit: limit = 1000

        rel_as_filter = req_data.rel_as_filter
        if rel_as_filter is None:
            from api.read_as_table.constants import REL_AS_FILTER
            rel_as_filter = REL_AS_FILTER
            self.logger.info(f"Rel_as_filter switch is not (yet) used by the frontend, defaulting to api.read_as_table.constants value: {REL_AS_FILTER}")
        self.logger.info("cypher construction: max_depth from ReadRequest is not implemented.")

        self.logger.debug(f"Read Query: {node_types=}, {relationships=}, {filters=}, {limit=}")

        # tightly coupled - but the cypher constructing is a bridge between request interface and db access
        cypher, params = construct_cypher_query(node_types, filters, relationships, limit, rel_as_filter=rel_as_filter)
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

