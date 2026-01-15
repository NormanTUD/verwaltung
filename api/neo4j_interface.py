

from neo4j import Driver
from neo4j.exceptions import Neo4jError
import logging
from dataclasses import dataclass, asdict
from typing import Any
from abc import ABC
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

"""
==========
Dataclasses
==========
"""
@dataclass(frozen=True)
class Column():
    nodeType: str
    property: str

@dataclass(frozen=True)
class Cell():
    nodeId: int
    nodeType: str
    value: Any

@dataclass(frozen=True)
class Relation:
    fromId: int
    relation: str
    toId: int

@dataclass(frozen=True)
class Row:
    cells: list[Cell]
    relations: list[Relation]

@dataclass(frozen=True)
class TableResponse:
    columns: list[Column]
    rows: list[Row]

    def to_dict(self):
        return {
            "columns": [asdict(col) for col in self.columns],
            "rows": [
                {
                    "cells": [asdict(cell) for cell in row.cells],
                    "relations": [asdict(rel) for rel in row.relations]
                }
                for row in self.rows
            ]
        }
"""
==========
Dataclasses End
==========
"""

def label_validation(label: str) -> bool:
    """Sketch. Validates that the label is alphanumeric with underscores only."""
    if not label.replace("_", "").isalnum():
        logger.error(f"Invalid label format: {label}")
        return False
    return True

class Neo4jDBInterface(ABC):
    def __init__(self, driver):
        self._driver = driver

    def create_data(self):
        raise NotImplementedError("Method not implemented yet.")

    def read_data(self,
                  node_types: list[str]|str,
                  relationships: list[str]|str|None = None,
                  filters: dict | None = None,
                  limit=None):

        raise NotImplementedError("Method not implemented yet.")

    def update_data(self):
        raise NotImplementedError("Method not implemented yet.")

    def delete_data(self):
        raise NotImplementedError("Method not implemented yet.")

class Neo4jDB(Neo4jDBInterface):
    def __init__(self, driver: Driver):
        super().__init__(driver)

    def read_data(self, node_types, relationships = None, filters = None, limit=None):
        pass
