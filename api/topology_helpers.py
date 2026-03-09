from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from api.topology_detector import TopologyTree
    from neo4j import Record
    from neo4j.graph import Relationship

def _ordered_labels_from_trees(trees: list[TopologyTree]) -> list[str]:
    """Pre-order DFS across all root trees → deterministic label list."""
    ordered: list[str] = []
    visited: set[str] = set()
    for tree in trees:
        _dfs_collect_labels(tree, ordered, visited)
    return ordered


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
    """Return {nodeLabel: [prop1, prop2, …]} preserving first-seen order."""
    props_by_type: dict[str, list[str]] = {}
    for record in data:
        for element in record:
            if isinstance(element, Relationship):
                continue
            label = list(element.labels)[0]
            if label not in props_by_type:
                props_by_type[label] = list(element.keys())
    return props_by_type


def _build_columns(
    ordered_labels: list[str],
    props_by_type: dict[str, list[str]],
) -> tuple[list[dict], dict[str, int], int]:
    """Build the column list, a {label→offset} index, and the total width."""
    columns: list[dict] = []
    col_offset: dict[str, int] = {}
    idx = 0
    for label in ordered_labels:
        props = props_by_type.get(label)
        if not props:
            continue
        col_offset[label] = idx
        for prop in props:
            columns.append({"nodeType": label, "property": prop})
        idx += len(props)
    return columns, col_offset, idx


def _grouping_sort_key(
    row: dict,
    ordered_labels: list[str],
    col_offset: dict[str, int],
) -> list[str]:
    """Produce a composite key that groups rows sharing the same parent."""
    key: list[str] = []
    for label in ordered_labels:
        if label not in col_offset:
            key.append("")
            continue
        cell = row["cells"][col_offset[label]]
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
