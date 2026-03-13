from __future__ import annotations
from typing import TYPE_CHECKING
import logging
if TYPE_CHECKING:
    from api.read_as_table.topology_detector import TopologyTree
    from neo4j import Record
from neo4j.graph import Relationship
from api.read_as_table.helpers import extract_node_label

log = logging.getLogger("[Topology Helper]")


def _dfs_collect_labels(
    tree: TopologyTree, result: list[str], visited: set[str]
) -> None:

    if tree.node_label in visited:
        return

    visited.add(tree.node_label)
    result.append(tree.node_label)
    for child in tree.children:
        _dfs_collect_labels(child, result, visited)



def _discover_properties(data: list[Record]) -> dict[str, list[str]]:
    """Return {nodeLabel: [prop1, prop2, ...]} preserving first-seen order."""
    props_by_type: dict[str, list[str]] = {}
    for record in data:
        for element in record:
            if isinstance(element, Relationship):
                continue
            label = extract_node_label(element, log)
            if label not in props_by_type:
                props_by_type[label] = list(element.keys())
    return props_by_type



def _build_columns_from_trees(
    trees: list[TopologyTree],
    props_by_type: dict[str, list[str]],
) -> tuple[list[dict], dict[str, list[int]], int, list[str]]:
    """Builds columns directly from the tree structure, supporting duplicate layers."""
    columns: list[dict] = []
    col_offset: dict[str, list[int]] = {}
    ordered_labels: list[str] = []
    idx = 0
    visited_ids = set()

    def traverse(node: TopologyTree, depth: int):
        nonlocal idx
        # Track by memory ID to allow cloned self-loop nodes to be processed
        if id(node) in visited_ids:
            return
        visited_ids.add(id(node))

        label = node.node_label
        if label not in ordered_labels:
            ordered_labels.append(label)

        props = props_by_type.get(label)
        if props:
            if label not in col_offset:
                col_offset[label] = []
            col_offset[label].append(idx)

            for prop in props:
                # 'depth' is added here to help the frontend render nested headers
                columns.append({"nodeType": label, "property": prop, "depth": depth})
            idx += len(props)

        for child in node.children:
            traverse(child, depth + 1)

    for tree in trees:
        traverse(tree, 0)

    return columns, col_offset, idx, ordered_labels



def _grouping_sort_key2(
    row: dict,
    ordered_labels: list[str],
    col_offset: dict[str, list[int]],
) -> list[str]:
    """Produce a composite key that groups rows sharing the same parent."""
    key: list[str] = []
    for label in ordered_labels:
        if label not in col_offset or not col_offset[label]:
            key.append("")
            continue
        # Group by the FIRST occurrence (root-most) of the node label
        first_offset = col_offset[label][0]
        cell = row["cells"][first_offset]
        key.append(str(cell.get("nodeId") or ""))
    return key



def _topology_tree_to_dict(tree: TopologyTree) -> dict:
    """Recursively serialise a TopologyTree into a plain dict for JSON."""
    return {
        "nodeLabel": tree.node_label,
        "roles": [r.name for r in tree.roles],
        "cycleType": tree.cycle_type.name,
        "sameTypeLoop": (
            [r.label for r in tree.same_type_info.relations]
            if tree.same_type_info
            else None
        ),
        "relationFromParent": (
            {
                "label": tree.relation_from_parent.label,
                "from": tree.relation_from_parent.from_node_type,
                "to": tree.relation_from_parent.to_node_type,
            }
            if tree.relation_from_parent
            else None
        ),
        "children": [_topology_tree_to_dict(c) for c in tree.children],
    }
