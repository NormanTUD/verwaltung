

from neo4j import Driver, Record
from neo4j.exceptions import Neo4jError
import logging
from dataclasses import dataclass, asdict
from typing import Any
from abc import ABC

logger = logging.getLogger(__name__)


"""
==========
Dataclasses
==========
"""
@dataclass
class ReadRequest():
    selected_labels: list[str]
    main_label: str
    max_depth: int
    limit: int | None
    filter_labels: dict[str, str] | None
    rel_fitler: list[str] | None

@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    bad: list[str]

@dataclass
class DBResult():
    pass
"""
==========
Helper Functions - Dataclasses End
==========
"""
def n4j_label_validation(label: str) -> bool:
    """Sketch. Validates that the label is alphanumeric with underscores only."""
    if not label.replace("_", "").isalnum():
        logger.error(f"Invalid label format: {label}")
        return False
    return True

def validate_labels(*label_groups) -> ValidationResult:
    bad: list[str] = []

    for group in label_groups:
        if not group:
            continue

        if isinstance(group, str):
            items = [group]
        else:
            try:
                items = list(group)
            except TypeError:
                bad.append(str(group))
                continue

        for item in items:
            if not isinstance(item, str):
                bad.append(repr(item))
                continue
            if not n4j_label_validation(item):
                bad.append(item)

    return ValidationResult(ok=not bad, bad=bad)


def construct_cypher_query(
    node_labels: list[str],
    node_filters: dict | None = None,
    rel_types: list[str] | None = None,
    limit: int | None = None,
    all_labels: bool = False
) -> tuple[str, dict[str, Any]]:
    """
    TODO:
    """

    # validate
    validation = validate_labels(
        node_labels,
        node_filters.keys() if node_filters else None,
        rel_types,
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
    where_clauses: list[str] = []
    parameters: dict[str, Any] = {}

    if node_filters:
        for prop, value in node_filters.items():
            param_name = f"prop_{prop}"
            where_clauses.append(f"n.{prop} = ${param_name}")
            parameters[param_name] = value

    where_clause = ""
    if where_clauses:
        where_clause = " WHERE " + " AND ".join(where_clauses)

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

def label_validation(label: str) -> bool:
    """Sketch. Validates that the label is alphanumeric with underscores only."""
    if not label.replace("_", "").isalnum():
        logger.error(f"Invalid label format: {label}")
        return False
    return True

class Neo4jDBInterface(ABC):
    def __init__(self, driver):
        self._driver: Driver = driver

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
    def __init__(self, driver: Driver):
        super().__init__(driver)
        self.logger = logging.getLogger("Database")

    """
    ==========
    Main Interface Methods
    ==========
    """

    def read_data(self, req_data: ReadRequest) -> list[Record]: #node_types, relationships = None, filters = None, limit=None
        node_types = req_data.selected_labels
        relationships = req_data.rel_fitler
        filters = req_data.filter_labels
        limit = req_data.limit

        self.logger.debug(f"Read Query: {node_types=}, {relationships=}, {filters=}, {limit=}")
        cypher, params = construct_cypher_query(node_types, filters, relationships, limit)
        self.logger.debug(f"Cypher was created: {cypher} with paramets: {params}")
        with self._driver.session() as session:
            # converting the Result object to a list of Records is memory intensive
            # should be no problem if we dont have results with over 1000s of Nodes
            r = session.run(cypher, params)
            result = list(r)
            if len(result) > 1000:
                logger.warning(" Length of results list is over 1k, switch to iterative approach?")

            r.consume()

        return result

    """
    ==========
    Read Data Helper Methods
    ==========
    """

    def _fetch_nodes(self,
                label: str,
                limit: int | None = None,
                where_props: dict | None = None) -> list[dict] | list:
        """
        Args:
            label: Node label to fetch
            limit: Optional limit on number of nodes to fetch
            where_props: Dict of property filters, e.g., {"age": 25, "f_name": "Alice"}
        Returns:
        a List of dictionaries with the node data OR an empty list if no nodes found.
        This is raw Node data.
        """
        logger.info(f"fetch_nodes(\n    {label=}\n    {limit=}\n    {where_props=}\n):")
        if not label_validation(label):
            logger.error(f"fetch_nodes(): Invalid label {label}.")
            raise ValueError(f"Invalid label {label}.")
        if not isinstance(limit, (type(None), int)) or (isinstance(limit, int) and limit <= 0):
            logger.error(f"fetch_nodes(): Invalid limit {limit}. Must be None or positive integer.")
            raise ValueError(f"Invalid limit {limit}. Must be None or positive integer.")
        # Cypher query construction
        base_query = f"MATCH (n:`{label}`)"
        params = {}
        if where_props:
            conditions = []
            for key, value in where_props.items():
                if not label_validation(key):
                    logger.error(f"fetch_nodes(): Invalid property key {key}.")
                    raise ValueError(f"Invalid property key {key}.")
                param_name = f"prop_{key}"
                conditions.append(f"n.{key} = ${param_name}")
                params[param_name] = value

            base_query += " WHERE " + " AND ".join(conditions)
        base_query += " RETURN n"
        if limit:
            base_query += " LIMIT $limit"
            params["limit"] = limit


        with self._driver.session() as session:
            try:
                records = session.run(base_query, params).data()
                if not records:
                    logger.info(f"fetch_nodes(): No nodes found with label {label}.")
                    return []
                else:
                    logger.info(f"fetch_nodes(): Retrieved {len(records)} nodes with label {label}.")
                    # remove the "n" dictionary layer
                    return [record["n"] for record in records]

            except Neo4jError as e:
                logger.error(f"Neo4jError fetching nodes: {e}")
                raise
            except Exception as e:
                logger.error(f"Error fetching nodes: {e}")
                raise

