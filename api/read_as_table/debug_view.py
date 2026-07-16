# debug_view.py  —  rewritten with interactive visual morphs + Venn diagrams
from __future__ import annotations
from typing import TYPE_CHECKING
import html
import json
import logging
import math
from copy import deepcopy

if TYPE_CHECKING:
    from neo4j import Record
    from api.neo4j_interface import ReadRequest

from flask import Response
from neo4j.graph import Node, Relationship

from api.read_as_table.helpers import extract_node_label
from api.read_as_table.topology_detector import (
    TopologyTranslator,
    TopologyTree,
    TopologyNode,
    AbstractRelation,
    CycleType,
    NodeRole,
    get_longest_path,
)
from api.read_as_table.topology_helpers import (
    _build_columns_from_trees,
    _discover_properties,
    _grouping_sort_key2,
    _topology_tree_to_dict,
)
from api.read_as_table.topology_cli import render_tree as _cli_render_tree
from api.read_as_table.constants import SAME_TYPE_DEPTH

log = logging.getLogger("[API] read_as_table.debug_view")


# ══════════════════════════════════════════════════════════════════════════
#  1. DOT generators  (unchanged, but node IDs now embed the label so JS
#     can select and highlight them without re-parsing SVG text nodes)
# ══════════════════════════════════════════════════════════════════════════
def _raw_records_to_dot(data: list["Record"]) -> str:
    lines = ['digraph raw {', '  rankdir=LR; node [shape=box, style=rounded];']
    seen_nodes: set[str] = set()
    seen_edges: set[tuple[str, str, str]] = set()
    for record in data:
        for element in record:
            if isinstance(element, Node):
                nid = element.element_id
                if nid in seen_nodes:
                    continue
                seen_nodes.add(nid)
                label = extract_node_label(element, log)
                props = ", ".join(f"{k}={v}" for k, v in element.items())
                lines.append(
                    f'  "{nid}" [id="raw::{html.escape(label)}::{html.escape(nid)}",'
                    f' label=<<b>{html.escape(label)}</b>'
                    f'<br/><font point-size="9">{html.escape(props)}</font>>];'
                )
            elif isinstance(element, Relationship):
                a = element.nodes[0].element_id
                b = element.nodes[1].element_id
                key = (a, element.type, b)
                if key in seen_edges:
                    continue
                seen_edges.add(key)
                lines.append(
                    f'  "{a}" -> "{b}" '
                    f'[id="rawrel::{html.escape(element.type)}::{a}::{b}",'
                    f' label="{html.escape(element.type)}"];'
                )
    lines.append('}')
    return "\n".join(lines)


def _topology_to_dot(trees: list[TopologyTree]) -> str:
    lines = ['digraph topology {', '  rankdir=TB; node [shape=box];']
    counter = {"i": 0}

    def walk(t: TopologyTree, parent_id: str | None, edge_label: str | None):
        counter["i"] += 1
        nid = f"n{counter['i']}"
        fill = "white"
        if t.cycle_type == CycleType.CROSS_TYPE:
            fill = "lightpink"
        elif t.cycle_type == CycleType.SAME_TYPE:
            fill = "lightyellow"
        roles = ",".join(sorted(r.name for r in t.roles))
        lines.append(
            f'  {nid} [id="tree::{html.escape(t.node_label)}::{nid}",'
            f' label=<<b>{html.escape(t.node_label)}</b>'
            f'<br/><font point-size="9">{html.escape(roles)}</font>>,'
            f' style=filled, fillcolor="{fill}"];'
        )
        if parent_id is not None:
            lines.append(f'  {parent_id} -> {nid} [label="{html.escape(edge_label or "")}"];')
        if t.same_type_info:
            via = ",".join(r.label for r in t.same_type_info.relations)
            lines.append(
                f'  {nid} -> {nid} [label="⟳ {html.escape(via)}", '
                f'style=dashed, color=orange];'
            )
        for c in t.children:
            rel = c.relation_from_parent.label if c.relation_from_parent else ""
            walk(c, nid, rel)

    for tr in trees:
        walk(tr, None, None)
    lines.append('}')
    return "\n".join(lines)


def _node_graph_to_dot(top: list[TopologyNode]) -> str:
    lines = ['digraph nodegraph {', '  rankdir=LR; node [shape=ellipse];']
    for n in top:
        roles = ",".join(sorted(r.name for r in n.get_classification()))
        fill = "lightgreen" if n.is_root else ("lightgrey" if n.is_leaf else "white")
        lines.append(
            f'  "{n.node_lbl}" [id="ng::{html.escape(n.node_lbl)}",'
            f' label=<<b>{html.escape(n.node_lbl)}</b>'
            f'<br/><font point-size="9">{html.escape(roles)}<br/>'
            f'in={n.incoming_con_n} out={len(n.connected_to)}</font>>,'
            f' style=filled, fillcolor="{fill}"];'
        )
    for n in top:
        for child, rel in n.connected_to:
            style = "dashed,color=orange" if child is n else "solid"
            lines.append(
                f'  "{n.node_lbl}" -> "{child.node_lbl}" '
                f'[id="nge::{html.escape(n.node_lbl)}::{html.escape(rel.label)}::{html.escape(child.node_lbl)}",'
                f' label="{html.escape(rel.label)}", style={style}];'
            )
    lines.append('}')
    return "\n".join(lines)


def _capture_ascii_tree(trees: list[TopologyTree]) -> str:
    import io, contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        _cli_render_tree(trees)
    return buf.getvalue() or "(empty)"


# ══════════════════════════════════════════════════════════════════════════
#  2. Instrumented pipeline replays
#     (unchanged from before + a couple of extra "visual state" fields)
# ══════════════════════════════════════════════════════════════════════════
def _trace_schema_extraction(data: list["Record"]) -> list[dict]:
    known_nodes: set[str] = set()
    relations: set = set()
    events: list[dict] = []
    for ri, record in enumerate(data):
        for ei, element in enumerate(record):
            if isinstance(element, Relationship):
                if not element.nodes[0] or not element.nodes[1]:
                    continue
                l1 = extract_node_label(element.nodes[0])
                l2 = extract_node_label(element.nodes[1])
                rel = AbstractRelation(element.type, l1, l2)
                is_new = rel not in relations
                if is_new:
                    relations.add(rel)
                events.append({
                    "record": ri, "pos": ei, "kind": "REL", "new": is_new,
                    "value": f"({l1})-[{rel.label}]->({l2})",
                    # Visual hooks:
                    "highlight_edge": f"rawrel::{rel.label}::{element.nodes[0].element_id}::{element.nodes[1].element_id}",
                    "highlight_nodes": [l1, l2],
                    "nodes_snapshot": sorted(known_nodes),
                    "rels_snapshot": [
                        f"({r.from_node_type})-[{r.label}]->({r.to_node_type})"
                        for r in relations
                    ],
                })
            else:
                lbl = extract_node_label(element)
                is_new = lbl not in known_nodes
                known_nodes.add(lbl)
                events.append({
                    "record": ri, "pos": ei, "kind": "NODE", "new": is_new,
                    "value": lbl,
                    "highlight_nodes": [lbl],
                    "highlight_raw_id": element.element_id,
                    "nodes_snapshot": sorted(known_nodes),
                    "rels_snapshot": [
                        f"({r.from_node_type})-[{r.label}]->({r.to_node_type})"
                        for r in relations
                    ],
                })
    return events


def _trace_tree_build(top_translator: TopologyTranslator) -> list[dict]:
    events: list[dict] = []

    def build(node: TopologyNode, ancestors: set, path: list[str]):
        same_type = [(c, r) for c, r in node.connected_to if c.node_lbl == node.node_lbl]
        cross_type = [(c, r) for c, r in node.connected_to if c.node_lbl != node.node_lbl]

        events.append({
            "action": "enter", "node": node.node_lbl,
            "ancestors": sorted(ancestors), "path": list(path),
            "stack_snapshot": list(path) + [node.node_lbl],
            "roles": [r.name for r in node.get_classification()],
            "same_type_relations": [r.label for _, r in same_type],
            "cross_type_children": [c.node_lbl for c, _ in cross_type],
            # Venn: same vs cross vs total children
            "venn_children": {
                "same": [r.label for _, r in same_type],
                "cross": [c.node_lbl for c, _ in cross_type],
            },
        })

        if node.node_lbl in ancestors:
            events.append({
                "action": "cross_type_cycle_hit",
                "node": node.node_lbl,
                "ancestors": sorted(ancestors), "path": list(path),
                "stack_snapshot": list(path) + [node.node_lbl],
                "verdict": "CROSS_TYPE — stop recursion, no children",
            })
            return

        if same_type:
            events.append({
                "action": "same_type_loop_recorded",
                "node": node.node_lbl,
                "stack_snapshot": list(path) + [node.node_lbl],
                "relations": [r.label for _, r in same_type],
            })

        ancestors.add(node.node_lbl)
        events.append({
            "action": "ancestor_push", "node": node.node_lbl,
            "stack_snapshot": list(path) + [node.node_lbl],
            "ancestors": sorted(ancestors),
        })

        for child, rel in cross_type:
            events.append({
                "action": "descend", "from": node.node_lbl,
                "to": child.node_lbl, "via": rel.label,
                "stack_snapshot": list(path) + [node.node_lbl],
                "ancestors": sorted(ancestors),
            })
            build(child, ancestors, path + [node.node_lbl])

        ancestors.discard(node.node_lbl)
        events.append({
            "action": "ancestor_pop", "node": node.node_lbl,
            "stack_snapshot": list(path),
            "ancestors": sorted(ancestors),
        })

    roots = top_translator.roots or (
        [top_translator.top[0]] if top_translator.top else []
    )
    for r in roots:
        build(r, set(), [])
    return events


def _trace_column_build(trees, props_by_type):
    events: list[dict] = []
    columns: list[dict] = []
    col_offset: dict[str, list[int]] = {}
    ordered_labels: list[str] = []
    idx = 0
    visited_ids: set[int] = set()

    def traverse(node: TopologyTree, depth: int, path: list[str]):
        nonlocal idx
        if id(node) in visited_ids:
            events.append({"action": "skip_visited", "node": node.node_label,
                           "depth": depth, "idx": idx,
                           "columns_so_far": [dict(c) for c in columns]})
            return
        visited_ids.add(id(node))

        label = node.node_label
        first_time_label = label not in ordered_labels
        if first_time_label:
            ordered_labels.append(label)

        props = props_by_type.get(label)
        event = {
            "action": "visit", "node": label, "depth": depth,
            "path": list(path), "idx_before": idx,
            "first_time_label": first_time_label,
            "props": list(props) if props else [],
        }
        added_cols: list[dict] = []
        if props:
            col_offset.setdefault(label, []).append(idx)
            event["offset_assigned"] = idx
            for prop in props:
                nc = {"nodeType": label, "property": prop, "depth": depth}
                columns.append(nc)
                added_cols.append(nc)
            idx += len(props)
        event["idx_after"] = idx
        event["added_columns"] = added_cols
        event["columns_so_far"] = [dict(c) for c in columns]
        event["col_offset_snapshot"] = {k: list(v) for k, v in col_offset.items()}
        event["ordered_labels_snapshot"] = list(ordered_labels)
        events.append(event)

        for child in node.children:
            traverse(child, depth + 1, path + [label])

    for tree in trees:
        traverse(tree, 0, [])

    return events, columns, col_offset, idx, ordered_labels


def _trace_row_build(data, columns, col_offset, props_by_type, total_cols):
    empty_cell = {"nodeId": None, "nodeType": None, "value": None}
    all_traces: list[dict] = []
    rows: list[dict] = []

    for ri, record in enumerate(data):
        cells = [dict(empty_cell) for _ in range(total_cols)]
        relations: list[dict] = []
        same_type_extras: list[list[dict]] = []
        seen_counts: dict[str, int] = {}
        events: list[dict] = []

        for ei, element in enumerate(record):
            if isinstance(element, Node):
                label = list(element.labels)[0]
                if label not in col_offset:
                    events.append({"kind": "skip_unknown_label", "label": label,
                                   "seen_counts": dict(seen_counts),
                                   "cells_snapshot": [dict(c) for c in cells],
                                   "highlighted_cols": []})
                    continue
                occ = seen_counts.get(label, 0)
                seen_counts[label] = occ + 1
                offsets = col_offset[label]
                props = props_by_type.get(label, [])
                if occ >= len(offsets):
                    overflow = [{"nodeId": element.element_id, "nodeType": label,
                                 "value": element.get(p)} for p in props]
                    same_type_extras.append(overflow)
                    events.append({"kind": "overflow_to_sameTypeNodes",
                                   "label": label, "occurrence": occ,
                                   "available_layers": len(offsets),
                                   "overflow": overflow,
                                   "cells_snapshot": [dict(c) for c in cells],
                                   "highlighted_cols": [],
                                   "seen_counts": dict(seen_counts)})
                    continue
                offset = offsets[occ]
                placed = []
                highlighted = []
                for i, prop in enumerate(props):
                    v = element.get(prop)
                    cells[offset + i] = {"nodeId": element.element_id,
                                         "nodeType": label, "value": v}
                    placed.append({"col": offset + i, "prop": prop, "value": v})
                    highlighted.append(offset + i)
                events.append({"kind": "place", "label": label,
                               "occurrence": occ, "offset": offset,
                               "placed": placed,
                               "highlighted_cols": highlighted,
                               "seen_counts": dict(seen_counts),
                               "cells_snapshot": [dict(c) for c in cells]})
            elif isinstance(element, Relationship):
                a, b = element.nodes[0], element.nodes[1]
                if not a or not b:
                    events.append({"kind": "rel_missing_node"})
                    continue
                relations.append({"fromId": a.element_id, "relation": element.type,
                                  "toId": b.element_id})
                events.append({"kind": "relation",
                               "from_label": extract_node_label(a),
                               "to_label": extract_node_label(b),
                               "type": element.type,
                               "cells_snapshot": [dict(c) for c in cells],
                               "highlighted_cols": []})

        row = {"cells": cells, "relations": relations}
        if same_type_extras:
            row["sameTypeNodes"] = same_type_extras
        rows.append(row)
        all_traces.append({"record": ri, "events": events,
                           "final_cells": cells, "final_relations": relations,
                           "seen_counts_end": seen_counts,
                           "overflow": same_type_extras})

    return all_traces, rows


def _trace_sort_key(rows, ordered_labels, col_offset):
    events = []
    for i, r in enumerate(rows):
        key_parts = []
        detail = []
        for label in ordered_labels:
            offsets = col_offset.get(label)
            if not offsets:
                key_parts.append("")
                detail.append({"label": label, "offset": None,
                               "value": None, "key_part": ""})
                continue
            first_offset = offsets[0]
            cell = r["cells"][first_offset]
            k = str(cell.get("nodeId") or "")
            key_parts.append(k)
            detail.append({"label": label, "offset": first_offset,
                           "value": cell.get("value"), "node_id": cell.get("nodeId"),
                           "key_part": k})
        events.append({"row_index": i, "key": key_parts, "detail": detail})
    return events


# ══════════════════════════════════════════════════════════════════════════
#  3. VENN SET COMPUTATION
#     Whenever the pipeline naturally deals with two-or-three related sets,
#     ship a {name -> [members]} dict to the frontend, which renders it as
#     an interactive SVG Venn diagram.
# ══════════════════════════════════════════════════════════════════════════
def _compute_all_venns(
    data,
    top_translator: TopologyTranslator,
    trees: list[TopologyTree],
    props_by_type: dict[str, list[str]],
    col_offset: dict[str, list[int]],
    ordered_labels: list[str],
    rows: list[dict],
) -> dict[str, dict]:
    """Every Venn diagram used on the page.  Key = target DIV id."""
    venns: dict[str, dict] = {}

    # ── (a) Stage 1: labels-as-nodes  vs  labels-as-rel-endpoints ──
    labels_as_nodes: set[str] = set()
    labels_as_from: set[str] = set()
    labels_as_to: set[str] = set()
    for record in data:
        for e in record:
            if isinstance(e, Node):
                labels_as_nodes.add(extract_node_label(e))
            elif isinstance(e, Relationship):
                if e.nodes[0]:
                    labels_as_from.add(extract_node_label(e.nodes[0]))
                if e.nodes[1]:
                    labels_as_to.add(extract_node_label(e.nodes[1]))
    venns["venn-s1-endpoints"] = {
        "title": "Where each label appears",
        "sets": {
            "as node in record": sorted(labels_as_nodes),
            "as source of rel":  sorted(labels_as_from),
            "as target of rel":  sorted(labels_as_to),
        },
    }

    # ── (b) Stage 2: roots  vs  leaves  vs  forks (out-degree ≥ 2) ──
    roots = {n.node_lbl for n in top_translator.top if n.is_root}
    leaves = {n.node_lbl for n in top_translator.top if n.is_leaf}
    forks = {n.node_lbl for n in top_translator.top if len(n.connected_to) >= 2}
    venns["venn-s2-roles"] = {
        "title": "Structural roles",
        "sets": {
            "ROOT (no non-self incoming)": sorted(roots),
            "LEAF (out-degree 0)":         sorted(leaves),
            "FORK (out-degree ≥ 2)":       sorted(forks),
        },
    }

    # ── (c) Stage 4: for each FORK t, the descendant-labels
    #     of every branch, plotted as an N-way Venn (up to 3 branches).
    #     Where any intersection is non-empty → the diamond problem is present.
    fork_venns: list[dict] = []
    for t in _walk_forest(trees):
        if len(t.children) >= 2:
            branch_sets = {}
            for c in t.children[:3]:  # 2- or 3-way Venn
                rel = c.relation_from_parent.label if c.relation_from_parent else "?"
                key = f"via {rel} → {c.node_label}"
                branch_sets[key] = sorted(_collect_descendant_labels(c))
            fork_venns.append({
                "fork_label": t.node_label,
                "title": f"Descendants of each branch of «{t.node_label}»",
                "sets": branch_sets,
                "note": "Any non-empty intersection = diamond problem "
                        "(same descendant reachable via ≥ 2 branches).",
            })
    venns["_fork_venns"] = fork_venns  # rendered as multiple divs

    # ── (d) Stage 6: property overlap between (up to 3) node types ──
    prop_pairs = list(props_by_type.items())
    for i in range(len(prop_pairs)):
        for j in range(i + 1, len(prop_pairs)):
            for k in range(j + 1, len(prop_pairs) + 1):
                # take groups of 2 or 3 with non-empty overlap
                pass
    # Simpler: show up to 3 label→props sets in one venn
    if props_by_type:
        top_three = list(props_by_type.items())[:3]
        venns["venn-s6-props"] = {
            "title": "Property names shared across the first "
                     f"{len(top_three)} node type(s)",
            "sets": {lbl: sorted(props) for lbl, props in top_three},
        }

    # ── (e) Stage 8: per-record → labels-in-record  ∩  labels-in-schema ──
    #      One combined venn showing labels-in-schema, labels-actually-seen,
    #      and labels-that-caused-overflow.
    schema_labels = set(labels_as_nodes)
    seen_labels: set[str] = set()
    overflow_labels: set[str] = set()
    for r in rows:
        for c in r["cells"]:
            if c.get("nodeType"):
                seen_labels.add(c["nodeType"])
        for extra in r.get("sameTypeNodes", []) or []:
            for c in extra:
                overflow_labels.add(c["nodeType"])
    venns["venn-s8-labels"] = {
        "title": "Label participation across the run",
        "sets": {
            "in schema":     sorted(schema_labels),
            "placed in row": sorted(seen_labels),
            "overflowed":    sorted(overflow_labels),
        },
    }

    # ── (f) Stage 10: rows-containing-each-label (2- or 3-way) ──
    if ordered_labels:
        picks = ordered_labels[:3]
        pick_sets = {lbl: set() for lbl in picks}
        for i, r in enumerate(rows):
            for c in r["cells"]:
                if c.get("nodeType") in pick_sets and c.get("nodeId"):
                    pick_sets[c["nodeType"]].add(str(i))
        venns["venn-s10-rows"] = {
            "title": f"Which rows contain a given label? "
                     f"({', '.join(picks)})",
            "sets": {lbl: sorted(v, key=int) for lbl, v in pick_sets.items()},
        }

    return venns


def _collect_descendant_labels(t: TopologyTree) -> set[str]:
    out = {t.node_label}
    stack = list(t.children)
    seen = {id(t)}
    while stack:
        n = stack.pop()
        if id(n) in seen:
            continue
        seen.add(id(n))
        out.add(n.node_label)
        stack.extend(n.children)
    return out


# ══════════════════════════════════════════════════════════════════════════
#  4. Variable glossary  (unchanged)
# ══════════════════════════════════════════════════════════════════════════
GLOSSARY: list[tuple[str, str, str, str]] = [
    ("D",       r"\mathcal{D}",        "input data",
     "The full list of Neo4j records returned by the query. "
     "Each record is an ordered tuple of Node / Relationship objects."),
    ("r",       r"r",                  "one record",
     "A single record r ∈ D — Neo4j returns one per row of the driver result."),
    ("e",       r"e",                  "one element of a record",
     "Either a Node or a Relationship object; found by iterating a record."),
    ("n",       r"n",                  "a Node",
     "A Neo4j Node with (element_id, labels, properties)."),
    ("rho",     r"\rho",               "a Relationship",
     "A Neo4j Relationship with (type, start-node, end-node)."),
    ("ell",     r"\ell",               "a node label (string)",
     "The single string label of a node, e.g. 'Person'. "
     "extract_node_label() picks it out of element.labels."),
    ("N",       r"\mathcal{N}",        "set of node labels",
     "All distinct labels ever seen in D — the schema's vertex-types."),
    ("R",       r"\mathcal{R}",        "set of AbstractRelations",
     "Deduplicated (from-label, type, to-label) triples — the schema's edge-types."),
    ("v",       r"v",                  "a TopologyNode",
     "One vertex in the abstracted TopologyNode graph "
     "(one per distinct label in N)."),
    ("A",       r"A",                  "ancestor set (DFS)",
     "The set of labels on the current DFS path — used to detect cross-type cycles."),
    ("C",       r"C",                  "children of v",
     "v.connected_to — a list of (child_TopologyNode, AbstractRelation) pairs."),
    ("Csame",   r"C_{\text{same}}",    "self-loop children",
     "Members of C where the child label equals v's own label (Type-C loops)."),
    ("Ccross",  r"C_{\text{cross}}",   "cross-type children",
     "Members of C where the child label differs from v's own label."),
    ("t",       r"t",                  "a TopologyTree node",
     "One node of the built TopologyTree — carries roles, cycle_type, "
     "children and same_type_info."),
    ("k",       r"k",                  "SAME_TYPE_DEPTH",
     "Constant from constants.py controlling how many clone-layers "
     "_expand_same_type() adds for each Type-C loop."),
    ("P",       r"P",                  "properties-by-label map",
     "P(ℓ) = ordered list of property names for label ℓ, from the first "
     "instance seen. Built by _discover_properties()."),
    ("O",       r"O",                  "column-offsets map",
     "O(ℓ) = list of starting column indices, one per layer of ℓ in the tree. "
     "Same key as col_offset in the code."),
    ("i",       r"i",                  "running column index",
     "Cursor that walks the columns list as _build_columns_from_trees() lays it out."),
    ("W",       r"W",                  "total number of columns",
     "Final table width = final value of i = |columns|."),
    ("S",       r"S",                  "seen_counts (per record)",
     "S(ℓ) = how many nodes of label ℓ we've encountered so far *within one record*. "
     "Resets for every new record."),
    ("j",       r"j",                  "property index",
     "Index into P(ℓ); ranges over 0 … |P(ℓ)|−1."),
    ("row",     r"\text{row}",         "an output row",
     "Row object being built for the current record; row.cells is a fixed-width array."),
    ("eps",     r"\varepsilon",        "empty-string sentinel",
     "Used as a default when a label has no cell in a given row, "
     "so the sort key is still lexicographically well-defined."),
]


def _glossary_html() -> str:
    rows = []
    for gid, sym, short, long_ in GLOSSARY:
        rows.append(
            f'<li data-var="{gid}">'
            f'  <div class="glossary-sym math">{html.escape(sym)}</div>'
            f'  <div class="glossary-body">'
            f'    <div class="glossary-short">{html.escape(short)}</div>'
            f'    <div class="glossary-long">{html.escape(long_)}</div>'
            f'  </div>'
            f'</li>'
        )
    return '<ul class="glossary-list">' + "".join(rows) + "</ul>"


# ══════════════════════════════════════════════════════════════════════════
#  5. Equations  (unchanged, kept verbatim)
# ══════════════════════════════════════════════════════════════════════════
EQ_S0_D = r"""
\underbrace{\mathcal{D}}_{\text{all records}} \;=\; \bigl\{\;
  \underbrace{r_1}_{\text{record 1}},\;
  \underbrace{r_2}_{\text{record 2}},\; \dots,\;
  \underbrace{r_n}_{\text{record }n}
\;\bigr\}
"""
EQ_S0_R = r"""
\underbrace{r_i}_{\text{one record}} \;=\; \bigl(\;
  \underbrace{e_{i,1}}_{\text{element 1}},\;
  \underbrace{e_{i,2}}_{\text{element 2}},\; \dots
\;\bigr)
"""
EQ_S0_E = r"""
\underbrace{e_{i,j}}_{\text{element } j \text{ in record } i}
\;\in\;
\underbrace{\text{Node}}_{\text{Neo4j node}} \;\cup\;
\underbrace{\text{Rel}}_{\text{Neo4j relationship}}
"""
EQ_S1_N = r"""
\underbrace{\mathcal{N}}_{\text{set of labels}}
\;=\;
\bigcup_{\underbrace{r}_{\text{each record}} \in \underbrace{\mathcal{D}}_{\text{records}}}
\Bigl\{\;
  \underbrace{\text{label}(n)}_{\text{the node's label}}
  \;:\;
  \underbrace{n}_{\text{a node}} \in
  \underbrace{r}_{\text{record}} \cap
  \underbrace{\text{Node}}_{\text{filter: nodes only}}
\;\Bigr\}
"""
EQ_S1_R = r"""
\begin{aligned}
\underbrace{\mathcal{R}}_{\text{set of abstract relations}}
\;=\;
\bigcup_{\underbrace{r}_{\text{each record}} \in \underbrace{\mathcal{D}}_{\text{records}}}
\Bigl\{\;\bigl(&
  \underbrace{\text{label}(n_1)}_{\text{from-label}},\;
  \underbrace{\rho.\text{type}}_{\text{rel type-name}},\;
  \underbrace{\text{label}(n_2)}_{\text{to-label}}
\bigr) \\[4pt]
&:\;
  \underbrace{\rho}_{\text{a relationship}} \in
  \underbrace{r}_{\text{record}} \cap \underbrace{\text{Rel}}_{\text{filter: rels only}},\;
  \underbrace{\rho}_{\text{same }\rho} = \bigl(
    \underbrace{n_1}_{\text{start node}} \to \underbrace{n_2}_{\text{end node}}
  \bigr)
\;\Bigr\}
\end{aligned}
"""
EQ_S2_ADD = r"""
\begin{aligned}
&\underbrace{\text{node}[a]}_{\text{TopologyNode for label }a}.\underbrace{\text{connected\_to}}_{\text{out-edges list}}
   \;\mathrel{+}{=}\; \bigl(
     \underbrace{\text{node}[b]}_{\text{TopologyNode for label }b},\;
     \underbrace{\ell}_{\text{rel label}}
   \bigr) \\[6pt]
&\underbrace{\text{node}[b]}_{\text{TopologyNode for label }b}.\underbrace{\text{incoming\_con\_n}}_{\text{in-degree}}
   \;\mathrel{+}{=}\; \underbrace{1}_{\text{one edge}}
\end{aligned}
"""
EQ_S3_ROOT = r"""
\underbrace{\text{is\_root}(v)}_{\text{predicate: is }v\text{ a root?}}
\;\iff\;
\underbrace{\Bigl|\bigl\{\,c : (c,\cdot)\in\underbrace{v.C}_{\text{connected\_to}},\; c=v \bigr\}\Bigr|}_{\text{count of self-loop children}}
\;=\;
\underbrace{v.\text{incoming\_con\_n}}_{\text{in-degree of }v}
"""
EQ_S3_FALLBACK = r"""
\underbrace{v^{\star}}_{\text{fallback root}}
\;=\;
\underbrace{\arg\max}_{\underbrace{v}_{\text{over all vertices}} \in \underbrace{\mathcal{N}}_{\text{node graph}}}
\;\;\underbrace{|v.C|}_{\text{out-degree}}
"""
EQ_S4_SPLIT = r"""
\underbrace{C_{\text{same}}}_{\text{self-loop children}}
\;=\;
\bigl\{\, (c,\ell) \in \underbrace{C}_{\text{connected\_to}} \;:\;
  \underbrace{c}_{\text{child node}} = \underbrace{v}_{\text{current node}}
\bigr\}
\qquad
\underbrace{C_{\text{cross}}}_{\text{cross-type children}}
\;=\;
\underbrace{C}_{\text{connected\_to}} \;\setminus\;
\underbrace{C_{\text{same}}}_{\text{self-loops}}
"""
EQ_S4_BUILD = r"""
\underbrace{\text{build}(v, A)}_{\text{recursive tree-builder}}
\;=\;
\begin{cases}
  \underbrace{\text{CROSS\_TYPE stub}}_{\text{no children, marks cycle}}
    & \text{if } \underbrace{v}_{\text{current}} \in \underbrace{A}_{\text{ancestor set}} \\[8pt]
  \underbrace{\text{node}\Bigl(v,\;\bigl\{\text{build}(c,\; A \cup \{v\}) : (c,\cdot)\in C_{\text{cross}}\bigr\}\Bigr)}_{\text{recurse into each cross-type child}}
    & \text{otherwise}
\end{cases}
"""
EQ_S5_CHAIN = r"""
\underbrace{t}_{\text{original tree-node}}
\;\xrightarrow{\text{clone}}\;
\underbrace{t'}_{\text{layer 1}}
\;\xrightarrow{\text{clone}}\;
\underbrace{t''}_{\text{layer 2}}
\;\xrightarrow{\text{clone}}\;
\dots
\;\;\bigl(\;\underbrace{k}_{\text{SAME\_TYPE\_DEPTH}}\text{ layers}\;\bigr)
"""
EQ_S6_P = r"""
\underbrace{P}_{\text{property map}}
\;:\;
\underbrace{\mathcal{N}}_{\text{labels}}
\;\longrightarrow\;
\underbrace{\text{List}[\text{String}]}_{\text{ordered prop names}}
\qquad
\underbrace{P(\ell)}_{\text{props for label }\ell}
\;=\;
\underbrace{\text{keys}\Bigl(\text{first}\bigl\{\,n\in\mathcal{D}:\text{label}(n)=\ell\bigr\}\Bigr)}_{\text{keys of first-seen instance of }\ell}
"""
EQ_S7_APPEND = r"""
\begin{aligned}
&\underbrace{O(\ell)}_{\text{offsets for label }\ell}
  \;\mathrel{+}{=}\;
  \bigl[\,\underbrace{i}_{\text{current col index}}\,\bigr]
  \\[4pt]
&\underbrace{\text{columns}}_{\text{running column list}}
  \;\mathrel{+}{=}\;
  \Bigl[\;\bigl(
    \underbrace{\ell}_{\text{label}},\;
    \underbrace{p}_{\text{property}},\;
    \underbrace{\text{depth}}_{\text{tree depth}}
  \bigr) \;:\; \underbrace{p}_{\text{iterate}} \in \underbrace{P(\ell)}_{\text{props of }\ell}\;\Bigr]
  \\[4pt]
&\underbrace{i}_{\text{col cursor}}
  \;\mathrel{+}{=}\;
  \underbrace{|P(\ell)|}_{\text{how many props }\ell\text{ has}}
\end{aligned}
"""
EQ_S7_WIDTH = r"""
\underbrace{W}_{\text{total \# columns}}
\;=\;
\underbrace{|\text{columns}|}_{\text{length of columns list}}
\;=\;
\sum_{\underbrace{\ell}_{\text{each label}} \in \underbrace{\mathcal{N}}_{\text{labels}}}
  \underbrace{|O(\ell)|}_{\text{\# layers of }\ell}
  \;\cdot\;
  \underbrace{|P(\ell)|}_{\text{\# props of }\ell}
"""
EQ_S8_COUNTER = r"""
\underbrace{k}_{\text{occurrence index}}
\;=\;
\underbrace{S(\ell)}_{\text{how many }\ell\text{s so far in this record}}
\qquad
\underbrace{S(\ell)}_{\text{counter}}
\;\mathrel{+}{=}\;
\underbrace{1}_{\text{consumed one occurrence}}
"""
EQ_S8_PLACE = r"""
\underbrace{\text{row.cells}\bigl[\, \underbrace{O(\ell)_k}_{\text{start-column for layer }k} + \underbrace{j}_{\text{prop offset}} \,\bigr]}_{\text{one destination cell}}
\;=\;
\Bigl(\;
  \underbrace{n.\text{id}}_{\text{node id}},\;
  \underbrace{\ell}_{\text{label}},\;
  \underbrace{n.P(\ell)_j}_{\text{value of prop } j}
\Bigr)
\quad
\text{for } \underbrace{j}_{\text{each prop index}} = 0, \dots, \underbrace{|P(\ell)|}_{\text{\# props}} - 1
"""
EQ_S8_OVERFLOW = r"""
\text{if}\;\;
\underbrace{k}_{\text{occurrence}}
\;\geq\;
\underbrace{|O(\ell)|}_{\text{available layers}}
\;\;\Rightarrow\;\;
\underbrace{\text{row.sameTypeNodes}}_{\text{overflow bucket}}
\;\mathrel{+}{=}\;
\Bigl[\,\text{cells for }\underbrace{n}_{\text{this node}}\,\Bigr]
"""
EQ_S10_KEY = r"""
\underbrace{\text{key}(r)}_{\text{sort key for row }r}
\;=\;
\Bigl(\;
  \underbrace{\text{str}\bigl(\;
    \underbrace{r.\text{cells}\bigl[\,\underbrace{O(\ell)_0}_{\text{root-most offset}}\,\bigr].\text{nodeId}}_{\text{cell's node id}}
    \;\lor\; \underbrace{\varepsilon}_{\text{empty fallback}}
  \bigr)}_{\text{stringified}}
\;\Bigr)_{\;\underbrace{\ell}_{\text{each label}} \in \underbrace{\text{ordered\_labels}}_{\text{DFS order}}}
"""


# ══════════════════════════════════════════════════════════════════════════
#  6. Styling  (existing + new for morphs / venn / stacks)
# ══════════════════════════════════════════════════════════════════════════
_STYLE = r"""
* { box-sizing: border-box; }
body { font-family: system-ui, sans-serif; margin: 0; color: #222;
       display: grid; grid-template-columns: 260px 1fr; min-height: 100vh; }
body.no-sidebar { grid-template-columns: 0 1fr; }
main { padding: 1.5rem 2rem; min-width: 0; }
h1 { margin-top: 0; }
h2 { border-bottom: 2px solid #333; margin-top: 3rem; padding-bottom: .25rem;
     cursor: pointer; user-select: none; }
h2::before { content: "▼ "; color: #06c; font-size: .8em; }
section.collapsed h2::before { content: "▶ "; }
section.collapsed > *:not(h2) { display: none; }
h3 { color: #555; margin-top: 1.5rem; }
h4 { color: #666; margin: .75rem 0 .25rem; font-weight: 600; }
pre { background: #f4f4f4; padding: .75rem; overflow: auto;
      font-size: 12px; line-height: 1.35; border-radius: 4px; }
code { background: #eee; padding: 1px 4px; border-radius: 3px; font-size: 90%; }
table { border-collapse: collapse; font-size: 12px; margin: .5rem 0; }
th, td { border: 1px solid #bbb; padding: 3px 6px; vertical-align: top;
         max-width: 260px; }
th { background: #eee; }
.empty { color: #bbb; font-style: italic; }
.plot { border: 1px solid #ddd; padding: .5rem; background: white;
        overflow: auto; }

/* Top bar */
.topbar { position: sticky; top: 0; background: #fff; padding: .5rem 0 .35rem;
          border-bottom: 1px solid #ddd; z-index: 20; margin-bottom: 1rem; }
.topbar nav a { margin-right: .5rem; font-size: 12px; text-decoration: none;
                color: #06c; }
.controls { display: flex; align-items: center; gap: .75rem; font-size: 12px;
            flex-wrap: wrap; padding-top: .35rem; color: #555; }
.controls label { display: flex; align-items: center; gap: .25rem; }
.controls input[type=range] { width: 130px; }
.controls kbd { border: 1px solid #bbb; border-bottom-width: 2px;
                border-radius: 3px; padding: 0 4px; font-family: ui-monospace;
                font-size: 11px; background: #fafafa; }

/* Sidebar */
aside.sidebar { border-right: 1px solid #ddd; background: #fafcff;
                position: sticky; top: 0; height: 100vh; overflow-y: auto;
                padding: 1rem .75rem; }
aside.sidebar h3 { margin-top: 0; color: #036; }
.glossary-list { list-style: none; padding: 0; margin: 0; font-size: 12px; }
.glossary-list li { padding: .35rem .4rem; border-radius: 4px;
                    display: grid; grid-template-columns: 42px 1fr;
                    gap: .4rem; cursor: pointer; margin-bottom: .1rem; }
.glossary-list li:hover, .glossary-list li.active { background: #e8f0ff; }
.glossary-sym { color: #036; font-weight: bold; }
.glossary-short { font-weight: 600; color: #333; }
.glossary-long { color: #666; font-size: 11px; line-height: 1.35;
                 display: none; margin-top: .15rem; }
.glossary-list li.active .glossary-long,
.glossary-list li:hover .glossary-long { display: block; }
body.no-sidebar aside.sidebar { display: none; }

/* Stages */
.stage { border-left: 3px solid #06c; padding-left: 1rem; margin-top: 2rem; }
.abstract { background: #f0f7ff; border: 1px solid #cde; padding: .75rem 1rem;
            border-radius: 4px; margin: .5rem 0; }
.abstract h4 { margin-top: 0; color: #036; }
.concrete { background: #f7fff0; border: 1px solid #cec;
            padding: .75rem 1rem; border-radius: 4px; margin: .5rem 0; }
.concrete h4 { margin-top: 0; color: #263; }
.trace { background: #fffaf0; border: 1px solid #edc;
         padding: .75rem 1rem; border-radius: 4px; margin: .5rem 0; }
.trace h4 { margin-top: 0; color: #630; }
.morph { background: #fbf5ff; border: 1px solid #dcd; padding: .75rem 1rem;
         border-radius: 4px; margin: .5rem 0; }
.morph h4 { margin-top: 0; color: #518; }

/* Two-column morph layout */
.morph-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem;
              align-items: start; }
@media (max-width: 1100px) { .morph-grid { grid-template-columns: 1fr; } }
.morph-panel { border: 1px solid #ddd; background: #fff; padding: .5rem;
               border-radius: 4px; }
.morph-panel h5 { margin: 0 0 .25rem; color: #518; font-size: 12px;
                  text-transform: uppercase; letter-spacing: .05em; }

/* Equations */
.math.display { display: block; margin: .5rem 0; padding: .35rem .5rem;
                background: #fff; border-left: 3px solid #cde;
                overflow-x: auto; }
.math.display:hover { background: #fafcff; }

/* Steppers */
.stepper { display: flex; gap: .5rem; align-items: center;
           margin: .5rem 0; flex-wrap: wrap; }
.stepper input[type=range] { flex: 1; min-width: 200px; }
.stepper button { padding: .25rem .75rem; cursor: pointer; }
.stepper .step-label { font-family: ui-monospace; font-size: 12px;
                       min-width: 6em; cursor: pointer;
                       padding: 2px 6px; border-radius: 3px; background: #eef; }
.stepper .step-label:hover { background: #dde; }
.stepper .step-label input { width: 4em; font: inherit; }
.progress { height: 4px; background: #eee; border-radius: 2px; margin: .25rem 0;
            position: relative; overflow: hidden; }
.progress > div { height: 100%; background: linear-gradient(90deg,#4a90ff,#06c);
                  width: 0; transition: width .15s ease-out; }
.stepper-block { border: 1px solid transparent; border-radius: 4px;
                 padding: .25rem; transition: border-color .2s; }
.stepper-block.active { border-color: #06c; box-shadow: 0 0 0 2px #cde; }
.event-box { border: 1px solid #ccc; padding: .5rem; background: white;
             font-family: ui-monospace, monospace; font-size: 12px;
             min-height: 6rem; white-space: pre-wrap; border-radius: 4px; }
.event-index { font-weight: bold; color: #06c; }
.badge { display: inline-block; padding: 1px 6px; border-radius: 8px;
         font-size: 10px; margin-right: 4px; }
.b-new  { background: #cfc; color: #060; }
.b-old  { background: #eee; color: #666; }
.b-warn { background: #fdd; color: #800; }
.b-ok   { background: #cdf; color: #036; }
.highlight-col { background: #fff3a0 !important; }

/* Graph highlight overlay hooks */
.plot svg g.node, .plot svg g.edge {
  transition: opacity .18s ease-out, filter .18s;
}
.plot.dim-others g.node.dim, .plot.dim-others g.edge.dim { opacity: .15; }
.plot g.node.pulse ellipse, .plot g.node.pulse polygon,
.plot g.node.pulse path { filter: drop-shadow(0 0 6px #f0a); stroke: #f0a; stroke-width: 3; }
.plot g.edge.pulse path  { stroke: #f0a; stroke-width: 3; }
.plot g.edge.pulse polygon { fill: #f0a; stroke: #f0a; }
.plot g.node.newly-seen ellipse, .plot g.node.newly-seen polygon,
.plot g.node.newly-seen path { stroke: #0a0; stroke-width: 3; }

/* Ancestor stack viz */
.stack-viz { display: flex; flex-direction: column-reverse; gap: 2px;
             min-height: 5rem; padding: .5rem; background: #f6f6ff;
             border: 1px dashed #99c; border-radius: 4px; }
.stack-frame { background: linear-gradient(90deg,#dde,#ccf); border-radius: 3px;
               padding: 3px 8px; font-family: ui-monospace; font-size: 12px;
               animation: pop-in .15s ease-out; }
@keyframes pop-in { from { transform: translateY(6px); opacity: 0; }
                    to   { transform: none; opacity: 1; } }

/* col_offset O(ℓ) morph */
.offset-map { font-family: ui-monospace; font-size: 12px; padding: .5rem;
              background: #fff8e0; border: 1px dashed #dc9; border-radius: 4px; }
.offset-row { display: flex; gap: .25rem; align-items: center; padding: 2px 0;
              flex-wrap: wrap; }
.offset-label { width: 100px; font-weight: bold; color: #630; }
.offset-slot  { display: inline-block; min-width: 28px; height: 22px;
                border: 1px solid #ba7; text-align: center; line-height: 20px;
                background: #ffe; border-radius: 3px;
                transition: background .2s, transform .2s; }
.offset-slot.new { background: #ffd44a; transform: scale(1.15); }

/* Columns strip (Stage 7) */
.col-strip { display: flex; gap: 1px; overflow-x: auto; padding: .5rem;
             background: #f4f4f4; border-radius: 4px; min-height: 60px; }
.col-strip .col { min-width: 60px; padding: .25rem; text-align: center;
                  border-radius: 3px; background: #fff; border: 1px solid #ccc;
                  font-size: 11px; transition: background .2s, transform .2s; }
.col-strip .col.added  { background: #d4ffd4; transform: translateY(-4px); }
.col-strip .col .lbl { font-weight: bold; color: #06c; }
.col-strip .col .prop { color: #666; font-family: ui-monospace; }

/* Row cells grid (Stage 8) */
.row-grid { display: grid; gap: 2px; padding: .5rem; background: #f4f4f4;
            border-radius: 4px; overflow-x: auto; }
.row-grid .cell { padding: 2px 4px; background: #fff; border: 1px solid #ccc;
                  font-size: 11px; font-family: ui-monospace;
                  min-width: 60px; text-align: center; border-radius: 3px;
                  transition: background .25s, transform .25s; }
.row-grid .cell.empty { color: #bbb; }
.row-grid .cell.just-placed { background: #ffe58a; transform: scale(1.08); }
.row-grid .cell .lbl { display: block; font-size: 9px; color: #06c;
                       text-transform: uppercase; }

/* Sort reorder (Stage 10) */
.sort-reorder { display: flex; flex-direction: column; gap: 3px;
                padding: .5rem; background: #f0f8ff; border-radius: 4px; }
.sort-reorder .sort-row { display: flex; gap: .5rem; align-items: center;
                          padding: 3px 6px; background: #fff; border-radius: 3px;
                          border: 1px solid #ccc; font-family: ui-monospace;
                          font-size: 11px; transition: transform .3s, background .3s; }
.sort-reorder .sort-row.active { background: #ffe58a; border-color: #d90; }
.sort-reorder .sort-key { font-weight: bold; color: #06c; min-width: 15em; }

/* Venn diagram styling */
.venn-wrap { display: grid; grid-template-columns: minmax(200px,340px) 1fr;
             gap: 1rem; align-items: start; }
@media (max-width: 900px) { .venn-wrap { grid-template-columns: 1fr; } }
.venn-svg { background: #fff; border: 1px solid #ddd; border-radius: 4px; }
.venn-svg circle { fill-opacity: .35; stroke-width: 2; cursor: pointer;
                   transition: fill-opacity .2s; }
.venn-svg circle:hover { fill-opacity: .55; }
.venn-svg .region { cursor: pointer; transition: opacity .2s; }
.venn-svg .region:hover { opacity: .6; }
.venn-svg .venn-lbl { font: 12px system-ui; font-weight: bold; }
.venn-svg .venn-count { font: 11px ui-monospace; fill: #333; }
.venn-legend { font-size: 12px; }
.venn-legend .item { display: flex; align-items: center; gap: .5rem;
                     margin: .15rem 0; cursor: pointer; }
.venn-legend .item .swatch { width: 14px; height: 14px; border-radius: 3px;
                             border: 1px solid #444; }
.venn-legend .members { font-family: ui-monospace; font-size: 11px;
                        color: #555; margin-left: 1.5rem;
                        max-height: 6rem; overflow: auto; }
.venn-note { font-size: 11px; color: #900; margin-top: .5rem; font-style: italic; }
"""


# ══════════════════════════════════════════════════════════════════════════
#  7. JavaScript — everything from before + morph engines + venn engine
# ══════════════════════════════════════════════════════════════════════════
_JS = r"""
// ── Math rendering ─────────────────────────────────────────────────────
function renderMath(root){
  (root || document).querySelectorAll('.math').forEach(el => {
    if (el.dataset.rendered) return;
    try {
      temml.render(el.textContent, el,
        {displayMode: el.classList.contains('display')});
      el.dataset.rendered = '1';
    } catch(e) { el.textContent = '[math err] ' + e.message; }
  });
}
window.addEventListener('DOMContentLoaded', () => renderMath());

// ── Global playback speed ──────────────────────────────────────────────
let PLAY_MS = 350;
const speedInput = () => document.getElementById('speed');

// ── Active stepper tracking ────────────────────────────────────────────
let ACTIVE_STEPPER = null;
function setActive(rootEl){
  document.querySelectorAll('.stepper-block').forEach(
    el => el.classList.remove('active'));
  if (rootEl) rootEl.classList.add('active');
  ACTIVE_STEPPER = rootEl;
}

// ── Generic stepper factory ────────────────────────────────────────────
function mkStepper(rootId, events, renderFn, sideEffects) {
  const root = document.getElementById(rootId);
  if (!root || !events.length) return;
  const box    = root.querySelector('.event-box');
  const slider = root.querySelector('input[type=range]');
  const label  = root.querySelector('.step-label');
  const progress = root.querySelector('.progress > div');
  slider.max = events.length - 1;

  function show(i) {
    i = Math.max(0, Math.min(events.length - 1, i|0));
    slider.value = i;
    label.textContent = `Step ${i+1} / ${events.length}`;
    progress.style.width = ((i+1)/events.length*100).toFixed(2) + '%';
    box.innerHTML = renderFn(events[i], i, events);
    if (sideEffects) sideEffects(events[i], i, events);
    setActive(root);
  }
  root._show = show;
  root._len  = events.length;

  slider.addEventListener('input', e => show(+e.target.value));
  root.querySelector('.step-prev').addEventListener('click',
    () => show(+slider.value - 1));
  root.querySelector('.step-next').addEventListener('click',
    () => show(+slider.value + 1));

  let timer = null;
  const playBtn = root.querySelector('.step-play');
  playBtn.addEventListener('click', () => {
    if (timer) { clearInterval(timer); timer = null;
                 playBtn.textContent = '▶ play'; return; }
    playBtn.textContent = '⏸ pause';
    timer = setInterval(() => {
      let i = +slider.value;
      if (i >= events.length - 1) { clearInterval(timer); timer = null;
                                    playBtn.textContent='▶ play'; return; }
      show(i + 1);
    }, PLAY_MS);
  });

  label.addEventListener('click', () => {
    if (label.querySelector('input')) return;
    const cur = +slider.value + 1;
    label.innerHTML = `Step <input type="number" min="1"
      max="${events.length}" value="${cur}"> / ${events.length}`;
    const inp = label.querySelector('input');
    inp.focus(); inp.select();
    const commit = () => show((+inp.value - 1) || 0);
    inp.addEventListener('blur', commit);
    inp.addEventListener('keydown', e => {
      if (e.key === 'Enter') { commit(); e.preventDefault(); }
      if (e.key === 'Escape') show(+slider.value);
    });
  });

  root.addEventListener('mouseenter', () => setActive(root));
  show(0);
}

// ── Keyboard shortcuts ─────────────────────────────────────────────────
document.addEventListener('keydown', e => {
  if (e.target.matches('input, textarea')) return;
  if (!ACTIVE_STEPPER) {
    ACTIVE_STEPPER = document.querySelector('.stepper-block');
    if (!ACTIVE_STEPPER) return;
  }
  const s = ACTIVE_STEPPER;
  const cur = +s.querySelector('input[type=range]').value;
  if (e.key === 'ArrowRight') { s._show(cur + 1); e.preventDefault(); }
  else if (e.key === 'ArrowLeft')  { s._show(cur - 1); e.preventDefault(); }
  else if (e.key === ' ') { s.querySelector('.step-play').click();
                            e.preventDefault(); }
  else if (e.key === 'Home') { s._show(0); e.preventDefault(); }
  else if (e.key === 'End')  { s._show(s._len - 1); e.preventDefault(); }
});

// ── Collapsibles + sidebar + speed ─────────────────────────────────────
window.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('section.stage > h2').forEach(h => {
    h.addEventListener('click', () =>
      h.parentElement.classList.toggle('collapsed'));
  });
  document.querySelectorAll('.glossary-list li').forEach(li => {
    li.addEventListener('click', () => li.classList.toggle('active'));
  });
  const tog = document.getElementById('toggle-sidebar');
  if (tog) tog.addEventListener('click',
    () => document.body.classList.toggle('no-sidebar'));
  const s = speedInput();
  if (s) {
    const out = document.getElementById('speed-val');
    const upd = () => { PLAY_MS = +s.value;
                        if (out) out.textContent = PLAY_MS + 'ms'; };
    s.addEventListener('input', upd); upd();
  }
});

// ── Utilities ──────────────────────────────────────────────────────────
function esc(s){ return String(s).replace(/[&<>]/g,
  c=>({'&':'&amp;','<':'&lt;','>':'&gt;'})[c]); }
function badge(cls, txt){ return `<span class="badge ${cls}">${esc(txt)}</span>`; }

// ── Event renderers ────────────────────────────────────────────────────
function renderSchemaEvt(e){
  return `<span class="event-index">Record ${e.record}, elem ${e.pos}</span> `
    + badge(e.kind==='NODE'?'b-ok':'b-warn', e.kind)
    + badge(e.new?'b-new':'b-old', e.new?'NEW':'seen')
    + `\nvalue: <b>${esc(e.value)}</b>\n\n`
    + `𝓝 (known node types) = [${e.nodes_snapshot.map(esc).join(', ')}]\n`
    + `𝓡 (known relations)  = [\n  ${e.rels_snapshot.map(esc).join(',\n  ')}\n]`;
}
function renderTreeEvt(e){
  const cls = e.action==='cross_type_cycle_hit'?'b-warn':
              e.action==='enter'?'b-ok':'b-old';
  let body = badge(cls, e.action) + '\n\n';
  for (const [k,v] of Object.entries(e)){
    if (k==='action') continue;
    body += `${k.padEnd(24)}= ${JSON.stringify(v)}\n`;
  }
  return body;
}
function renderColEvt(e){
  let body = badge('b-ok', e.action)
    + `  node=<b>${esc(e.node)}</b>  depth=${e.depth}\n`;
  body += `path                     = ${JSON.stringify(e.path||[])}\n`;
  body += `i  (running col-index)   : ${e.idx_before} → ${e.idx_after}\n`;
  body += `P(ℓ) props               = ${JSON.stringify(e.props||[])}\n`;
  if (e.offset_assigned!==undefined)
    body += `O(ℓ) offset assigned     = ${e.offset_assigned}\n`;
  body += `O (col_offset snapshot)  = ${JSON.stringify(e.col_offset_snapshot||{})}\n`;
  body += `ordered_labels           = ${JSON.stringify(e.ordered_labels_snapshot||[])}`;
  return body;
}
function renderRowEvt(ev){
  let s = `<span class="event-index">Record ${ev.record}</span>\n\n`;
  s += `── micro-events (${ev.events.length}) ──\n`;
  ev.events.forEach((m, k) => {
    s += `\n[${k+1}] ${badge('b-old', m.kind)}\n`;
    for (const [key,val] of Object.entries(m)){
      if (key==='kind' || key==='cells_snapshot' || key==='highlighted_cols') continue;
      s += `    ${key.padEnd(16)}= ${JSON.stringify(val)}\n`;
    }
  });
  s += `\n── final cells (indexed) ──\n`;
  ev.final_cells.forEach((c,k)=>{
    const v = c.value===null ? '·' : c.value;
    s += `  [${String(k).padStart(2)}] ${(c.nodeType||'∅').padEnd(12)} = ${esc(String(v))}\n`;
  });
  if (ev.overflow.length) s += `\noverflow → sameTypeNodes: ${JSON.stringify(ev.overflow)}`;
  return s;
}
function renderSortEvt(e){
  let s = `<span class="event-index">Row ${e.row_index}</span>\n`;
  s += `composite key = ${JSON.stringify(e.key)}\n\n── per-label breakdown ──\n`;
  e.detail.forEach(d => {
    s += `  ${d.label.padEnd(14)} O(ℓ)_0=${String(d.offset).padEnd(4)}`
       + `  nodeId=${JSON.stringify(d.node_id||null)}`
       + `  value=${JSON.stringify(d.value||null)}\n`;
  });
  return s;
}

// ══════════════════════════════════════════════════════════════════════
//  GRAPH HIGHLIGHTING  (raw and topology SVGs)
// ══════════════════════════════════════════════════════════════════════
function clearHighlights(plotId){
  const p = document.getElementById(plotId);
  if (!p) return;
  p.classList.remove('dim-others');
  p.querySelectorAll('g.node, g.edge').forEach(g => {
    g.classList.remove('dim','pulse','newly-seen');
  });
}
function highlightGraphByTitle(plotId, titlePredicate, opts) {
  const p = document.getElementById(plotId);
  if (!p) return;
  opts = opts || {};
  p.classList.add('dim-others');
  p.querySelectorAll('g.node, g.edge').forEach(g => {
    const titleEl = g.querySelector('title');
    const t = titleEl ? titleEl.textContent : '';
    if (titlePredicate(t, g)) {
      g.classList.remove('dim');
      if (opts.pulse) g.classList.add('pulse');
      if (opts.newlySeen) g.classList.add('newly-seen');
    } else {
      g.classList.add('dim');
    }
  });
}

// ══════════════════════════════════════════════════════════════════════
//  ANCESTOR STACK VIZ
// ══════════════════════════════════════════════════════════════════════
function renderStack(divId, frames){
  const el = document.getElementById(divId);
  if (!el) return;
  el.innerHTML = (frames||[]).map(f =>
    `<div class="stack-frame">${esc(f)}</div>`).join('');
}

// ══════════════════════════════════════════════════════════════════════
//  COL_OFFSET (O) MAP MORPH
// ══════════════════════════════════════════════════════════════════════
function renderOffsetMap(divId, snapshot, newlyAssignedLabel){
  const el = document.getElementById(divId);
  if (!el) return;
  const rows = Object.entries(snapshot || {}).map(([lbl, arr]) => {
    const slots = arr.map((v, i) => {
      const isNew = (lbl === newlyAssignedLabel && i === arr.length - 1);
      return `<span class="offset-slot ${isNew?'new':''}">${v}</span>`;
    }).join('');
    return `<div class="offset-row">
      <span class="offset-label">O(${esc(lbl)}) =</span>
      [${slots}]
    </div>`;
  }).join('');
  el.innerHTML = rows || '<em style="color:#999">(empty)</em>';
}

// ══════════════════════════════════════════════════════════════════════
//  COLUMN STRIP MORPH (Stage 7)
// ══════════════════════════════════════════════════════════════════════
function renderColStrip(divId, columns, addedCols){
  const el = document.getElementById(divId);
  if (!el) return;
  const addedSet = new Set((addedCols||[]).map(
    c => `${c.nodeType}::${c.property}`));
  el.innerHTML = (columns||[]).map((c, i) => {
    const key = `${c.nodeType}::${c.property}`;
    const cls = addedSet.has(key) ? 'col added' : 'col';
    return `<div class="${cls}"><span class="lbl">${esc(c.nodeType)}</span>
      <span class="prop">${esc(c.property)}</span>
      <span style="color:#999">[${i}]</span></div>`;
  }).join('') || '<em style="color:#999">(no columns yet)</em>';
}

// ══════════════════════════════════════════════════════════════════════
//  ROW CELL GRID MORPH (Stage 8)
// ══════════════════════════════════════════════════════════════════════
function renderRowGrid(divId, cells, highlighted, allColumns){
  const el = document.getElementById(divId);
  if (!el) return;
  const hi = new Set(highlighted || []);
  const w = Math.max(1, cells.length);
  el.style.gridTemplateColumns = `repeat(${w}, minmax(60px, 1fr))`;
  el.innerHTML = cells.map((c, i) => {
    const lbl = (allColumns[i] && allColumns[i].nodeType) || c.nodeType || '';
    const empty = (c.value === null && c.nodeId === null);
    const cls = ['cell', empty?'empty':'', hi.has(i)?'just-placed':''].join(' ');
    return `<div class="${cls}"><span class="lbl">${esc(lbl)}</span>
      ${empty ? '·' : esc(String(c.value))}</div>`;
  }).join('');
}

// ══════════════════════════════════════════════════════════════════════
//  SORT REORDER ANIMATION (Stage 10)
// ══════════════════════════════════════════════════════════════════════
function renderSortReorder(divId, sortEvents, curIdx){
  const el = document.getElementById(divId);
  if (!el) return;
  el.innerHTML = sortEvents.map((e, i) => {
    const cls = (i === curIdx) ? 'sort-row active' : 'sort-row';
    return `<div class="${cls}">
      <span class="sort-key">${esc(JSON.stringify(e.key))}</span>
      <span>row ${e.row_index}</span>
    </div>`;
  }).join('');
  // Scroll active row into view
  const active = el.querySelector('.sort-row.active');
  if (active) active.scrollIntoView({block: 'nearest', behavior: 'smooth'});
}

// ══════════════════════════════════════════════════════════════════════
//  VENN DIAGRAM RENDERER  (2 or 3-way; SVG; interactive legend & regions)
// ══════════════════════════════════════════════════════════════════════
const VENN_COLORS = ['#4a90ff', '#e07050', '#50c078'];
function subsetOf(a, others){
  const os = others.map(o => new Set(o));
  return a.filter(x => os.every(o => o.has(x)));
}
function renderVenn(divId, spec){
  const el = document.getElementById(divId);
  if (!el || !spec) return;
  const names = Object.keys(spec.sets);
  if (names.length === 0) { el.innerHTML = '<em>(empty)</em>'; return; }
  const sets = names.map(n => (spec.sets[n] || []));
  const setObjs = sets.map(s => new Set(s));

  // Compute all 2^N region contents
  const regions = {};
  const all = new Set();
  sets.forEach(s => s.forEach(x => all.add(x)));
  all.forEach(x => {
    const key = names.map((_, i) => setObjs[i].has(x) ? '1' : '0').join('');
    (regions[key] = regions[key] || []).push(x);
  });

  // Layout for 2 or 3 circles
  const W = 320, H = 260;
  let circles;
  if (names.length === 1) {
    circles = [{cx: W/2, cy: H/2, r: 80, lx: W/2, ly: 30}];
  } else if (names.length === 2) {
    circles = [
      {cx: W/2 - 55, cy: H/2, r: 85, lx: W/2 - 120, ly: 35},
      {cx: W/2 + 55, cy: H/2, r: 85, lx: W/2 + 120, ly: 35},
    ];
  } else {
    // 3-way: equilateral
    const R = 78, cx = W/2, cy = H/2 + 12, d = 52;
    circles = [
      {cx: cx,       cy: cy - d,       r: R, lx: cx,       ly: 20},
      {cx: cx - d*0.87, cy: cy + d/2,  r: R, lx: cx - 130, ly: H - 12},
      {cx: cx + d*0.87, cy: cy + d/2,  r: R, lx: cx + 130, ly: H - 12},
    ];
  }

  // Where to place count labels for each region
  const labelPos = {
    '1':    {x: circles[0].cx - 50, y: circles[0].cy},
    '10':   {x: circles[0].cx - 55, y: circles[0].cy},
    '01':   {x: circles[1].cx + 55, y: circles[1].cy},
    '11':   {x: (circles[0].cx + circles[1].cx)/2, y: circles[0].cy},
    '100':  {x: circles[0].cx,      y: circles[0].cy - 30},
    '010':  {x: circles[1].cx - 25, y: circles[1].cy + 15},
    '001':  {x: circles[2].cx + 25, y: circles[2].cy + 15},
    '110':  {x: (circles[0].cx + circles[1].cx)/2 - 15,
             y: (circles[0].cy + circles[1].cy)/2 - 5},
    '101':  {x: (circles[0].cx + circles[2].cx)/2 + 15,
             y: (circles[0].cy + circles[2].cy)/2 - 5},
    '011':  {x: (circles[1].cx + circles[2].cx)/2,
             y: (circles[1].cy + circles[2].cy)/2 + 15},
    '111':  {x: (circles[0].cx + circles[1].cx + circles[2].cx)/3,
             y: (circles[0].cy + circles[1].cy + circles[2].cy)/3 + 5},
  };

  let svg = `<svg class="venn-svg" viewBox="0 0 ${W} ${H}" width="${W}" height="${H}">`;
  circles.forEach((c, i) => {
    svg += `<circle cx="${c.cx}" cy="${c.cy}" r="${c.r}"
              fill="${VENN_COLORS[i]}" stroke="${VENN_COLORS[i]}"
              data-set-idx="${i}"/>`;
  });
  circles.forEach((c, i) => {
    svg += `<text class="venn-lbl" text-anchor="middle"
             x="${c.lx}" y="${c.ly}" fill="${VENN_COLORS[i]}">
             ${esc(names[i])} (${sets[i].length})</text>`;
  });
  Object.entries(regions).forEach(([key, members]) => {
    const p = labelPos[key];
    if (!p || !members.length) return;
    svg += `<text class="venn-count" text-anchor="middle"
             x="${p.x}" y="${p.y}">${members.length}</text>`;
  });
  svg += '</svg>';

  // Legend / region breakdown
  let legend = `<div class="venn-legend"><h5>${esc(spec.title||'')}</h5>`;
  names.forEach((n, i) => {
    legend += `<div class="item" data-set-idx="${i}">
      <span class="swatch" style="background:${VENN_COLORS[i]}"></span>
      <b>${esc(n)}</b> · ${sets[i].length} member(s)</div>
      <div class="members">${sets[i].map(esc).join(', ') || '<em>(empty)</em>'}</div>`;
  });
  legend += `<div style="margin-top:.75rem"><b>Region breakdown</b></div>`;
  Object.entries(regions).forEach(([key, members]) => {
    const label = key.split('').map((b, i) =>
      b === '1' ? names[i] : `¬${names[i]}`).join(' ∩ ');
    legend += `<div style="font-family:ui-monospace;font-size:11px;
                  margin:2px 0;color:#333">
      <b>${esc(label)}</b> = ${members.length}: [${members.map(esc).join(', ')}]</div>`;
  });
  if (spec.note) legend += `<div class="venn-note">${esc(spec.note)}</div>`;
  legend += '</div>';

  el.innerHTML = `<div class="venn-wrap">${svg}${legend}</div>`;
}

// ══════════════════════════════════════════════════════════════════════
//  Graphviz rendering + venn init
// ══════════════════════════════════════════════════════════════════════
window.addEventListener('DOMContentLoaded', () => {
  Viz.instance().then(viz => {
    for (const [id, src] of Object.entries(window.__DOTS__ || {})){
      const target = document.getElementById(id);
      if (target) target.appendChild(viz.renderSVGElement(src));
    }
  });
  // Render every venn from __VENNS__
  Object.entries(window.__VENNS__ || {}).forEach(([divId, spec]) => {
    if (divId.startsWith('_')) return;
    renderVenn(divId, spec);
  });
  // Fork venns (variable count)
  const forkParent = document.getElementById('fork-venns');
  const forks = (window.__VENNS__ && window.__VENNS__._fork_venns) || [];
  if (forkParent && forks.length) {
    forks.forEach((f, i) => {
      const div = document.createElement('div');
      div.id = 'fork-venn-' + i;
      div.style.marginBottom = '1.5rem';
      forkParent.appendChild(div);
      renderVenn(div.id, f);
    });
  } else if (forkParent) {
    forkParent.innerHTML = '<em style="color:#999">' +
      '(no forks in this topology — nothing to plot)</em>';
  }
});
"""


# ══════════════════════════════════════════════════════════════════════════
#  8. HTML template — extended with morph panels + venn slots
# ══════════════════════════════════════════════════════════════════════════
_PAGE = """<!doctype html>
<html><head>
<meta charset="utf-8"><title>read_as_table — deep debug view</title>
<script src="https://cdn.jsdelivr.net/npm/@viz-js/viz@3.2.4/lib/viz-standalone.js"></script>
<script src="https://cdn.jsdelivr.net/npm/temml@0.10.29/dist/temml.min.js"></script>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/temml@0.10.29/dist/Temml-Local.css">
<style>{style}</style>
</head><body>

<aside class="sidebar">
  <h3>Variables (click to pin)</h3>
  <p style="font-size:11px;color:#666;">
    Every equation below uses <em>only</em> the symbols in this list. Hover
    or click any entry to see its long-form definition.
  </p>
  {glossary_html}
</aside>

<main>

<div class="topbar">
  <h1 style="margin:0 0 .25rem">read_as_table — deep pipeline inspector</h1>
  <div style="font-size:12px;color:#555">
    Query: <code>{query}</code> · records: <b>{n_records}</b> ·
    node types: <b>{n_types}</b> · relations: <b>{n_rels}</b> ·
    columns: <b>{n_cols}</b> · rows: <b>{n_rows}</b>
  </div>
  <nav style="margin-top:.25rem;">
    <a href="#glossary">Vars</a>
    <a href="#s0">0 raw</a>
    <a href="#s1">1 schema</a>
    <a href="#s2">2 node graph</a>
    <a href="#s3">3 roots</a>
    <a href="#s4">4 tree</a>
    <a href="#s5">5 expand</a>
    <a href="#s6">6 props</a>
    <a href="#s7">7 columns</a>
    <a href="#s8">8 cells</a>
    <a href="#s9">9 overflow</a>
    <a href="#s10">10 sort</a>
    <a href="#s11">11 final</a>
  </nav>
  <div class="controls">
    <button id="toggle-sidebar" title="Hide/show variables sidebar">☰ vars</button>
    <label>speed
      <input type="range" id="speed" min="80" max="1500" step="10" value="350">
      <span id="speed-val" style="min-width:4em;display:inline-block"></span>
    </label>
    <span>keys:
      <kbd>◀</kbd><kbd>▶</kbd> step ·
      <kbd>space</kbd> play/pause ·
      <kbd>Home</kbd>/<kbd>End</kbd> jump ·
      click <code>Step N / M</code> to type ·
      click <b>h2</b> to collapse
    </span>
  </div>
</div>

<section class="stage" id="glossary">
<h2>Variables (canonical definitions)</h2>
<div class="abstract"><h4>Why this section exists</h4>
Every equation in this document is written using the symbols below —
and only those symbols. Hover any row on the left sidebar for a longer
description; click to pin it open.
</div>
</section>

<!-- ══════════ STAGE 0 ══════════ -->
<section class="stage" id="s0">
<h2>Stage 0 — Raw Neo4j records (input)</h2>
<div class="abstract"><h4>Abstract</h4>
<div class="math display">{eq_s0_d}</div>
<div class="math display">{eq_s0_r}</div>
<div class="math display">{eq_s0_e}</div>
</div>
<div class="concrete"><h4>Concrete graph</h4>
<div class="plot" id="raw"></div>
<details><summary>DOT source</summary><pre>{raw_dot}</pre></details>
</div>
</section>

<!-- ══════════ STAGE 1 ══════════ -->
<section class="stage" id="s1">
<h2>Stage 1 — Schema extraction</h2>
<div class="abstract"><h4>Abstract</h4>
Fold the record stream into 𝓝 and 𝓡:
<div class="math display">{eq_s1_n}</div>
<div class="math display">{eq_s1_r}</div>
</div>
<div class="concrete"><h4>Concrete</h4>
<b>𝓝</b> = <code>{node_types}</code><br/>
<b>𝓡</b> = <pre>{relations_json}</pre>
</div>

<div class="morph"><h4>🔺 Venn — where does each label appear?</h4>
<p style="font-size:12px;color:#666;margin:.25rem 0">
Labels appearing as nodes vs as relationship source/target — if a label
sits only in the "as source of rel" region it means we never observed a
concrete node of that type in the returned rows.</p>
<div id="venn-s1-endpoints"></div>
</div>

<div class="trace"><h4>Trace (per element) — synced with the raw graph above</h4>
<div id="schema-stepper" class="stepper-block">
 <div class="stepper">
   <button class="step-prev">◀</button>
   <input type="range" min="0" value="0">
   <button class="step-next">▶</button>
   <button class="step-play">▶ play</button>
   <span class="step-label"></span>
 </div>
 <div class="progress"><div></div></div>
 <div class="event-box"></div>
</div>
<p style="font-size:11px;color:#666">
  💡 As you step, the raw graph <b>lights up</b> the exact element being
  consumed — new nodes flash <span style="color:#0a0">green</span>,
  new relations pulse <span style="color:#f0a">pink</span>.
</p>
</div>
</section>

<!-- ══════════ STAGE 2 ══════════ -->
<section class="stage" id="s2">
<h2>Stage 2 — TopologyNode graph</h2>
<div class="abstract"><h4>Abstract</h4>
Materialise 𝓝 as vertices; for each (a, ℓ, b) ∈ 𝓡:
<div class="math display">{eq_s2_add}</div>
</div>

<div class="morph"><h4>🔺 Venn — structural roles</h4>
<p style="font-size:12px;color:#666;margin:.25rem 0">
A single label can be simultaneously ROOT, LEAF and FORK. Intersections
show which nodes wear multiple hats.</p>
<div id="venn-s2-roles"></div>
</div>

<div class="concrete"><h4>Concrete node graph — raw records mapped to abstracted vertices</h4>
<div class="morph-grid">
  <div class="morph-panel"><h5>Before: raw graph (Stage 0)</h5>
    <div class="plot" style="max-height:340px"><small style="color:#999">
    see Stage 0 above ↑</small></div></div>
  <div class="morph-panel"><h5>After: TopologyNode graph (deduplicated by label)</h5>
    <div class="plot" id="nodegraph"></div></div>
</div>
<table><thead><tr><th>label</th><th>out-degree</th><th>in-degree</th><th>roles</th></tr></thead>
<tbody>{node_table_rows}</tbody></table>
</div>
</section>

<!-- ══════════ STAGE 3 ══════════ -->
<section class="stage" id="s3">
<h2>Stage 3 — Root detection</h2>
<div class="abstract"><h4>Abstract</h4>
<div class="math display">{eq_s3_root}</div>
<div class="math display">{eq_s3_fallback}</div>
</div>
<div class="concrete"><h4>Concrete</h4>
Roots detected: <code>{roots_list}</code><br/>
Fallback used:  <code>{fallback_used}</code><br/>
Longest path from any root: <code>{longest_path}</code>
</div>
</section>

<!-- ══════════ STAGE 4 ══════════ -->
<section class="stage" id="s4"><!-- (continuing Stage 4) -->
<h2>Stage 4 — Tree build (<code>_build_tree</code>, DFS with ancestor set)</h2>
<div class="abstract"><h4>Abstract</h4>
Recursive descent from each root. Split the current node's children into two
disjoint sets by comparing labels:
<div class="math display">{eq_s4_split}</div>
Then apply the rule:
<div class="math display">{eq_s4_build}</div>
<b>Caveat:</b> the current implementation mutates a <em>shared</em>
ancestor set <code>A</code> across sibling branches — the diamond
Venn below tells you at a glance whether this bug will bite for
the current dataset.
</div>

<div class="morph"><h4>🔺 Venn — diamond detection (one Venn per fork)</h4>
<p style="font-size:12px;color:#666;margin:.25rem 0">
For every FORK in the topology we plot the set of descendant labels
reachable through each of its branches. <b>Any non-empty intersection
means a diamond exists</b> — the same label is reachable via multiple
paths, which triggers the shared-ancestor-set bug in
<code>_build_tree</code>.</p>
<div id="fork-venns"></div>
</div>

<div class="concrete"><h4>Concrete tree — before / after morph</h4>
<div class="morph-grid">
  <div class="morph-panel"><h5>Before: TopologyNode graph (Stage 2)</h5>
    <div class="plot" id="topo-before-clone"></div></div>
  <div class="morph-panel"><h5>After: TopologyTree forest (this stage)</h5>
    <div class="plot" id="topo"></div></div>
</div>
<details open><summary>ASCII rendering</summary><pre>{ascii_tree}</pre></details>
<details><summary>JSON metadata</summary><pre>{topology_meta_json}</pre></details>
</div>

<div class="trace"><h4>Trace (DFS step-by-step) — with live ancestor stack</h4>
<div id="tree-stepper" class="stepper-block">
 <div class="stepper">
   <button class="step-prev">◀</button>
   <input type="range" min="0" value="0">
   <button class="step-next">▶</button>
   <button class="step-play">▶ play</button>
   <span class="step-label"></span>
 </div>
 <div class="progress"><div></div></div>
 <div class="morph-grid">
   <div class="event-box"></div>
   <div class="morph-panel">
     <h5>Ancestor set A (DFS stack — bottom = root)</h5>
     <div id="tree-stack" class="stack-viz"></div>
     <p style="font-size:11px;color:#666;margin:.35rem 0 0">
       Watch <em>A grow</em> on <code>ancestor_push</code> and
       <em>shrink</em> on <code>ancestor_pop</code>. When a
       <code>cross_type_cycle_hit</code> fires, the node being visited
       is already somewhere in this stack.
     </p>
     <h5 style="margin-top:.75rem">Corresponding node in the graph</h5>
     <p style="font-size:11px;color:#666;margin:.25rem 0 0">
       The TopologyNode graph on the right ↗ lights up the node
       currently being processed — try stepping through.
     </p>
   </div>
 </div>
</div>
</div>
</section>

<!-- ══════════ STAGE 5 ══════════ -->
<section class="stage" id="s5">
<h2>Stage 5 — Same-type expansion (<code>_expand_same_type</code>)</h2>
<div class="abstract"><h4>Abstract</h4>
For every tree node <code>t</code> flagged <code>SAME_TYPE</code>, splice a
linear chain of <code>k = SAME_TYPE_DEPTH</code> deep-copied clones below it:
<div class="math display">{eq_s5_chain}</div>
Roles are patched along the chain: intermediate clones gain
<code>CHAIN</code>, the terminal clone gains <code>LEAF</code>, and the
original loses <code>LEAF</code> if it had it.
</div>
<div class="concrete"><h4>Concrete</h4>
<code>k = SAME_TYPE_DEPTH = {same_type_depth}</code><br/>
Same-type nodes found: <code>{same_type_labels}</code>
</div>
</section>

<!-- ══════════ STAGE 6 ══════════ -->
<section class="stage" id="s6">
<h2>Stage 6 — Property discovery (<code>_discover_properties</code>)</h2>
<div class="abstract"><h4>Abstract</h4>
For each label ℓ ∈ 𝓝 record the property names of the first instance
seen (insertion order preserved):
<div class="math display">{eq_s6_p}</div>
</div>

<div class="morph"><h4>🔺 Venn — property overlap between node types</h4>
<p style="font-size:12px;color:#666;margin:.25rem 0">
Highlights property names that show up in multiple node types (e.g. every
type has an <code>id</code> or <code>name</code>). Any intersection means
the same column-header string will appear in different type-blocks — not
a bug, but useful for understanding join keys.</p>
<div id="venn-s6-props"></div>
</div>

<div class="concrete"><h4>Concrete P</h4>
<pre>{props_by_type_json}</pre>
</div>
</section>

<!-- ══════════ STAGE 7 ══════════ -->
<section class="stage" id="s7">
<h2>Stage 7 — Column layout (<code>_build_columns_from_trees</code>)</h2>
<div class="abstract"><h4>Abstract</h4>
DFS over the (possibly same-type-expanded) forest with a running column index
<code>i</code>. On visiting a tree node <code>t</code> with label
<code>ℓ = t.label</code> append its offset, push new column descriptors, and
advance the cursor:
<div class="math display">{eq_s7_append}</div>
Total width:
<div class="math display">{eq_s7_width}</div>
</div>

<div class="concrete"><h4>Concrete (final state)</h4>
<pre>O (col_offset)  = {col_offset}
ordered_labels  = {ordered_labels}
W (total cols)  = {n_cols}</pre>
{columns_table}
</div>

<div class="trace"><h4>Trace — data structures morph as we DFS the tree</h4>
<div id="col-stepper" class="stepper-block">
 <div class="stepper">
   <button class="step-prev">◀</button>
   <input type="range" min="0" value="0">
   <button class="step-next">▶</button>
   <button class="step-play">▶ play</button>
   <span class="step-label"></span>
 </div>
 <div class="progress"><div></div></div>
 <div class="morph-grid">
   <div class="event-box"></div>
   <div class="morph-panel">
     <h5>Data-structures being built</h5>
     <h5 style="color:#630;margin-top:.5rem">O(ℓ) — column-offsets map</h5>
     <div id="col-offset-map" class="offset-map"></div>
     <h5 style="color:#630;margin-top:.5rem">columns[] — the growing list</h5>
     <div id="col-strip" class="col-strip"></div>
     <p style="font-size:11px;color:#666;margin:.25rem 0 0">
       💡 The tree graph above ↑ lights up the tree-node being visited.
       Newly-added columns flash <span style="color:#0a0">green</span>,
       and the offset slot that was just written flashes
       <span style="color:#d90">yellow</span>.
     </p>
   </div>
 </div>
</div>
</div>
</section>

<!-- ══════════ STAGE 8 ══════════ -->
<section class="stage" id="s8">
<h2>Stage 8 — Cell placement (per record)</h2>
<div class="abstract"><h4>Abstract</h4>
For each record <code>r ∈ 𝓓</code> allocate an empty row of width <code>W</code>
and a per-record counter <code>S</code> initialised to zero. For each node
<code>n ∈ r</code> with label <code>ℓ</code>:
<div class="math display">{eq_s8_counter}</div>
<div class="math display">{eq_s8_place}</div>
When we've seen this label more times than there are layers, the extra
instance overflows into <code>row.sameTypeNodes</code>:
<div class="math display">{eq_s8_overflow}</div>
</div>

<div class="morph"><h4>🔺 Venn — label participation across the whole run</h4>
<p style="font-size:12px;color:#666;margin:.25rem 0">
Labels declared by the schema vs labels that actually got a cell placed
vs labels that overflowed into <code>sameTypeNodes</code>. If a label
appears only in "in schema" and never in "placed in row", the query
returned type-info but no instance of that type.</p>
<div id="venn-s8-labels"></div>
</div>

<div class="trace"><h4>Trace — watch cells fill in as micro-events fire</h4>
<div id="row-stepper" class="stepper-block">
 <div class="stepper">
   <button class="step-prev">◀</button>
   <input type="range" min="0" value="0">
   <button class="step-next">▶</button>
   <button class="step-play">▶ play</button>
   <span class="step-label"></span>
 </div>
 <div class="progress"><div></div></div>
 <div class="event-box"></div>
 <div class="morph-panel" style="margin-top:.5rem">
   <h5>Row cells (grid morph — auto-scrolls with sub-step selector)</h5>
   <div class="stepper" style="margin:.25rem 0">
     <label style="font-size:11px">micro-step within record:</label>
     <button id="row-micro-prev">◀</button>
     <input type="range" id="row-micro" min="0" value="0">
     <button id="row-micro-next">▶</button>
     <span id="row-micro-label" style="font-family:ui-monospace;font-size:11px"></span>
   </div>
   <div id="row-grid" class="row-grid"></div>
   <p style="font-size:11px;color:#666;margin:.25rem 0 0">
     Yellow cells were just written; empty cells stay pale.
     Use the micro-slider to step through <em>within</em> one record.
   </p>
 </div>
</div>
</div>
</section>

<!-- ══════════ STAGE 9 ══════════ -->
<section class="stage" id="s9">
<h2>Stage 9 — Overflow (<code>sameTypeNodes</code>)</h2>
<div class="abstract"><h4>Abstract</h4>
Occurrences beyond the expanded depth <code>|O(ℓ)|</code> are stashed as
<code>row.sameTypeNodes : List[List[Cell]]</code> so nothing is lost when
a real data path is deeper than <code>k = SAME_TYPE_DEPTH</code>.
</div>
<div class="concrete"><h4>Overflow occurrences in this run</h4>
<pre>{overflow_json}</pre>
</div>
</section>

<!-- ══════════ STAGE 10 ══════════ -->
<section class="stage" id="s10">
<h2>Stage 10 — Grouping sort key (<code>_grouping_sort_key2</code>)</h2>
<div class="abstract"><h4>Abstract</h4>
Each row gets a composite key derived from the <em>root-most</em>
occurrence of each label — the first offset stored in <code>O(ℓ)</code>:
<div class="math display">{eq_s10_key}</div>
</div>

<div class="morph"><h4>🔺 Venn — which rows contain which label?</h4>
<p style="font-size:12px;color:#666;margin:.25rem 0">
The row-indices that contain a non-null cell for each of the first
three ordered labels. Rows in the intersection are the fully-populated
"join" rows; rows in a lone lobe are records that only carried one type.</p>
<div id="venn-s10-rows"></div>
</div>

<div class="trace"><h4>Trace — watch rows migrate to sorted order</h4>
<div id="sort-stepper" class="stepper-block">
 <div class="stepper">
   <button class="step-prev">◀</button>
   <input type="range" min="0" value="0">
   <button class="step-next">▶</button>
   <button class="step-play">▶ play</button>
   <span class="step-label"></span>
 </div>
 <div class="progress"><div></div></div>
 <div class="morph-grid">
   <div class="event-box"></div>
   <div class="morph-panel">
     <h5>Sorted order (active row highlighted)</h5>
     <div id="sort-reorder" class="sort-reorder"></div>
   </div>
 </div>
</div>
</section>

<!-- ══════════ STAGE 11 ══════════ -->
<section class="stage" id="s11">
<h2>Stage 11 — Final output</h2>
<h3>Rows before grouping sort ({n_rows} rows)</h3>
{rows_unsorted_table}
<h3>Rows after grouping sort</h3>
{rows_sorted_table}
</section>

</main>

<script>
window.__DOTS__ = {{
  raw:                {raw_dot_json},
  nodegraph:          {nodegraph_dot_json},
  "topo-before-clone":{nodegraph_dot_json},
  topo:               {topo_dot_json}
}};
window.__VENNS__ = {venns_json};
window.__ALL_COLUMNS__ = {columns_json};

const SCHEMA_EVTS = {schema_evts_json};
const TREE_EVTS   = {tree_evts_json};
const COL_EVTS    = {col_evts_json};
const ROW_EVTS    = {row_evts_json};
const SORT_EVTS   = {sort_evts_json};

{js}

// ── Side-effect hooks that link steppers to visual morph panels ────────
function schemaSideEffects(e){{
  clearHighlights('raw');
  if (e.kind === 'NODE' && e.highlight_raw_id) {{
    highlightGraphByTitle('raw',
      t => t === e.highlight_raw_id,
      {{pulse: true, newlySeen: e.new}});
  }} else if (e.kind === 'REL') {{
    // Highlight both endpoint nodes and the edge
    const rawId = e.highlight_edge; // rawrel::TYPE::A::B
    highlightGraphByTitle('raw', (t, g) => {{
      // titles of edges look like "A->B"; of nodes look like the element_id
      if (g.classList.contains('edge')) {{
        // Match any edge whose title contains both endpoint element_ids —
        // we don't have them here, so instead we match by rel type via label
        return true;  // dim only when we can't be selective
      }}
      return e.highlight_nodes.some(lbl => t.includes(lbl));
    }}, {{pulse: true}});
  }}
}}
function treeSideEffects(e){{
  renderStack('tree-stack', e.stack_snapshot || e.path || []);
  clearHighlights('nodegraph');
  const target = e.node || e.to || e.from;
  if (target) {{
    highlightGraphByTitle('nodegraph', t => t === target,
      {{pulse: e.action === 'enter' || e.action === 'cross_type_cycle_hit'}});
  }}
}}
function colSideEffects(e){{
  // Highlight the tree-node being visited
  clearHighlights('topo');
  if (e.node) {{
    highlightGraphByTitle('topo', t => t.includes(e.node), {{pulse: true}});
  }}
  // Morph the O(ℓ) map
  renderOffsetMap('col-offset-map',
    e.col_offset_snapshot || {{}},
    e.offset_assigned !== undefined ? e.node : null);
  // Morph the columns strip
  renderColStrip('col-strip', e.columns_so_far || [], e.added_columns || []);
}}

// ── Row micro-stepper (Stage 8) ────────────────────────────────────────
let ROW_CUR_RECORD = 0;
let ROW_CUR_MICRO  = 0;
function rowRenderMicro(){{
  const rec = ROW_EVTS[ROW_CUR_RECORD];
  if (!rec) return;
  const microMax = rec.events.length - 1;
  const mi = Math.max(0, Math.min(microMax, ROW_CUR_MICRO));
  const microSlider = document.getElementById('row-micro');
  const microLabel  = document.getElementById('row-micro-label');
  if (microSlider) {{ microSlider.max = Math.max(0, microMax); microSlider.value = mi; }}
  if (microLabel)  microLabel.textContent =
    `record ${{ROW_CUR_RECORD}} · micro ${{mi+1}}/${{rec.events.length}}`;
  const ev = rec.events[mi];
  if (!ev) return;
  const cells = ev.cells_snapshot ||
    (mi > 0 ? rec.events[mi-1].cells_snapshot : null) ||
    rec.final_cells;
  renderRowGrid('row-grid', cells,
    ev.highlighted_cols || [], window.__ALL_COLUMNS__ || []);
}}
function rowSideEffects(ev, i){{
  ROW_CUR_RECORD = i;
  ROW_CUR_MICRO  = 0;
  rowRenderMicro();
}}

function sortSideEffects(e, i){{
  renderSortReorder('sort-reorder', SORT_EVTS, i);
}}

window.addEventListener('DOMContentLoaded', () => {{
  if (SCHEMA_EVTS.length) mkStepper('schema-stepper', SCHEMA_EVTS,
                                    renderSchemaEvt, schemaSideEffects);
  if (TREE_EVTS.length)   mkStepper('tree-stepper',   TREE_EVTS,
                                    renderTreeEvt,   treeSideEffects);
  if (COL_EVTS.length)    mkStepper('col-stepper',    COL_EVTS,
                                    renderColEvt,    colSideEffects);
  if (ROW_EVTS.length)    mkStepper('row-stepper',    ROW_EVTS,
                                    renderRowEvt,    rowSideEffects);
  if (SORT_EVTS.length)   mkStepper('sort-stepper',   SORT_EVTS,
                                    renderSortEvt,   sortSideEffects);

  // Wire up the row micro-stepper
  const microSlider = document.getElementById('row-micro');
  if (microSlider) {{
    microSlider.addEventListener('input', e => {{
      ROW_CUR_MICRO = +e.target.value; rowRenderMicro();
    }});
    document.getElementById('row-micro-prev').addEventListener('click',
      () => {{ ROW_CUR_MICRO--; rowRenderMicro(); }});
    document.getElementById('row-micro-next').addEventListener('click',
      () => {{ ROW_CUR_MICRO++; rowRenderMicro(); }});
  }}
}});
</script>
</body></html>
"""


# ══════════════════════════════════════════════════════════════════════════
#  9. Small HTML helpers
# ══════════════════════════════════════════════════════════════════════════
def _row_table(columns: list[dict], rows: list[dict]) -> str:
    header = "".join(
        f"<th>{html.escape(c['nodeType'])}<br/>"
        f"<small>{html.escape(c['property'])}</small></th>"
        for c in columns
    )
    body = []
    for r in rows:
        tds = []
        for cell in r["cells"]:
            if cell.get("value") is None and cell.get("nodeId") is None:
                tds.append('<td class="empty">·</td>')
            else:
                tds.append(f"<td>{html.escape(str(cell.get('value')))}</td>")
        body.append("<tr>" + "".join(tds) + "</tr>")
    return (f"<table><thead><tr>{header}</tr></thead>"
            f"<tbody>{''.join(body)}</tbody></table>")


def _columns_table(columns: list[dict]) -> str:
    rows = "".join(
        f"<tr><td>{i}</td><td>{html.escape(c['nodeType'])}</td>"
        f"<td>{html.escape(c['property'])}</td>"
        f"<td>{c.get('depth', '')}</td></tr>"
        for i, c in enumerate(columns)
    )
    return ("<table><thead><tr><th>#</th><th>nodeType</th>"
            "<th>property</th><th>depth</th></tr></thead>"
            f"<tbody>{rows}</tbody></table>")


# ══════════════════════════════════════════════════════════════════════════
# 10. Utilities
# ══════════════════════════════════════════════════════════════════════════
def _walk_forest(trees):
    """Yield every TopologyTree in a forest (pre-order, cycle-safe by id)."""
    seen: set[int] = set()
    stack = list(trees)
    while stack:
        t = stack.pop()
        if id(t) in seen:
            continue
        seen.add(id(t))
        yield t
        stack.extend(t.children)


# ══════════════════════════════════════════════════════════════════════════
# 11. Public entry point
# ══════════════════════════════════════════════════════════════════════════
def render_debug_view(data: list["Record"], params: "ReadRequest",
                      mode: str = "1") -> Response:
    if not data:
        return Response("<p>No data.</p>", mimetype="text/html")

    # ── Run the real pipeline (mirrors topological_rec_to_json) ──
    top_translator = TopologyTranslator(data)
    trees = top_translator.get_topology_tree()
    props_by_type = _discover_properties(data)

    # ── Instrumented replays ──
    schema_evts = _trace_schema_extraction(data)
    tree_evts = _trace_tree_build(top_translator)
    col_evts, columns, col_offset, total_cols, ordered_labels = \
        _trace_column_build(trees, props_by_type)
    row_evts, rows = _trace_row_build(
        data, columns, col_offset, props_by_type, total_cols
    )
    rows_unsorted = [dict(r) for r in rows]
    rows_sorted = sorted(
        rows, key=lambda r: _grouping_sort_key2(r, ordered_labels, col_offset)
    )
    sort_evts = _trace_sort_key(rows_sorted, ordered_labels, col_offset)

    # ── DOTs ──
    raw_dot = _raw_records_to_dot(data)
    topo_dot = _topology_to_dot(trees) if trees else "digraph{}"
    nodegraph_dot = _node_graph_to_dot(top_translator.top)

    # ── Venn diagrams ──
    venns = _compute_all_venns(
        data, top_translator, trees, props_by_type,
        col_offset, ordered_labels, rows,
    )

    # ── Node graph tabular summary ──
    node_rows = "".join(
        f"<tr><td>{html.escape(n.node_lbl)}</td>"
        f"<td>{len(n.connected_to)}</td>"
        f"<td>{n.incoming_con_n}</td>"
        f"<td>{', '.join(sorted(r.name for r in n.get_classification()))}</td></tr>"
        for n in top_translator.top
    )

    roots = top_translator.roots
    fallback_used = not bool(roots) and bool(top_translator.top)

    same_type_labels = sorted({
        t.node_label
        for t in _walk_forest(trees)
        if t.same_type_info is not None
    })

    # Collect overflow rows
    overflow_rows = []
    for i, r in enumerate(rows):
        if "sameTypeNodes" in r:
            overflow_rows.append({"row": i, "overflow": r["sameTypeNodes"]})

    # ── Render the page ──
    page = _PAGE.format(
        style=_STYLE,
        js=_JS,
        glossary_html=_glossary_html(),
        query=html.escape(str(params)),
        n_records=len(data),
        n_types=len(top_translator.top),
        n_rels=len(top_translator.relations),
        n_cols=total_cols,
        n_rows=len(rows_sorted),

        # DOTs
        raw_dot=html.escape(raw_dot),
        raw_dot_json=json.dumps(raw_dot),
        nodegraph_dot_json=json.dumps(nodegraph_dot),
        topo_dot_json=json.dumps(topo_dot),

        # Venns + columns array (needed for row-grid labels)
        venns_json=json.dumps(venns),
        columns_json=json.dumps(columns),

        # Schema
        node_types=sorted({n.node_lbl for n in top_translator.top}),
        relations_json=html.escape(json.dumps([
            {"label": r.label, "from": r.from_node_type, "to": r.to_node_type}
            for r in top_translator.relations
        ], indent=2)),
        node_table_rows=node_rows,

        # Roots
        roots_list=[r.node_lbl for r in roots] if roots else "(none)",
        fallback_used=fallback_used,
        longest_path=(
            max(get_longest_path(r) for r in (roots or top_translator.top[:1]))
            if top_translator.top else 0
        ),

        # Tree
        ascii_tree=html.escape(_capture_ascii_tree(trees)),
        topology_meta_json=html.escape(
            json.dumps([_topology_tree_to_dict(t) for t in trees], indent=2)
        ),

        # Same-type
        same_type_depth=SAME_TYPE_DEPTH,
        same_type_labels=same_type_labels or "(none)",

        # Props / columns
        props_by_type_json=html.escape(json.dumps(props_by_type, indent=2)),
        col_offset=col_offset,
        ordered_labels=ordered_labels,
        columns_table=_columns_table(columns),

        # Overflow
        overflow_json=html.escape(json.dumps(overflow_rows, indent=2)) or "(none)",

        # Final tables
        rows_unsorted_table=_row_table(columns, rows_unsorted),
        rows_sorted_table=_row_table(columns, rows_sorted),

        # Trace event streams
        schema_evts_json=json.dumps(schema_evts),
        tree_evts_json=json.dumps(tree_evts),
        col_evts_json=json.dumps(col_evts),
        row_evts_json=json.dumps(row_evts),
        sort_evts_json=json.dumps(sort_evts),

        # Equations (LaTeX injected verbatim)
        eq_s0_d=EQ_S0_D,
        eq_s0_r=EQ_S0_R,
        eq_s0_e=EQ_S0_E,
        eq_s1_n=EQ_S1_N,
        eq_s1_r=EQ_S1_R,
        eq_s2_add=EQ_S2_ADD,
        eq_s3_root=EQ_S3_ROOT,
        eq_s3_fallback=EQ_S3_FALLBACK,
        eq_s4_split=EQ_S4_SPLIT,
        eq_s4_build=EQ_S4_BUILD,
        eq_s5_chain=EQ_S5_CHAIN,
        eq_s6_p=EQ_S6_P,
        eq_s7_append=EQ_S7_APPEND,
        eq_s7_width=EQ_S7_WIDTH,
        eq_s8_counter=EQ_S8_COUNTER,
        eq_s8_place=EQ_S8_PLACE,
        eq_s8_overflow=EQ_S8_OVERFLOW,
        eq_s10_key=EQ_S10_KEY,
    )
    return Response(page, mimetype="text/html")
