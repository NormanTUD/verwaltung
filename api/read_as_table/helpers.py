from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from neo4j.graph import Node
from logging import getLogger


def extract_node_label(
    element: Node, log=getLogger("[API] read_as_table.helpers: ")
) -> str:
    """get the single expected node label."""
    if len(element.labels) > 1:
        log.warning(
            f"Dealing with nodes with {len(element.labels)}, expected is one. This will cause errors down the line."
        )
    return list(element.labels)[0]
