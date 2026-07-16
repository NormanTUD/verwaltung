# debug_view.py
from __future__ import annotations
from typing import TYPE_CHECKING
import html
import json
import logging
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


# ═══════════════════════════════════════════════════════════════════
#  1. DOT generators (unchanged)
# ═══════════════════════════════════════════════════════════════════
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
                    f'  "{nid}" [label=<<b>{html.escape(label)}</b>'
                    f'<br/><font point-size="9">{html.escape(props)}</font>>];'
                )
            elif isinstance(element, Relationship):
                a = element.nodes[0].element_id
                b = element.nodes[1].element_id
                key = (a, element.type, b)
                if key in seen_edges:
                    continue
                seen_edges.add(key)
                lines.append(f'  "{a}" -> "{b}" [label="{html.escape(element.type)}"];')
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
            f'  {nid} [label=<<b>{html.escape(t.node_label)}</b>'
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
            f'  "{n.node_lbl}" [label=<<b>{html.escape(n.node_lbl)}</b>'
            f'<br/><font point-size="9">{html.escape(roles)}<br/>'
            f'in={n.incoming_con_n} out={len(n.connected_to)}</font>>,'
            f' style=filled, fillcolor="{fill}"];'
        )
    for n in top:
        for child, rel in n.connected_to:
            style = "dashed,color=orange" if child is n else "solid"
            lines.append(
                f'  "{n.node_lbl}" -> "{child.node_lbl}" '
                f'[label="{html.escape(rel.label)}", style={style}];'
            )
    lines.append('}')
    return "\n".join(lines)


def _capture_ascii_tree(trees: list[TopologyTree]) -> str:
    import io, contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        _cli_render_tree(trees)
    return buf.getvalue() or "(empty)"


# ═══════════════════════════════════════════════════════════════════
#  2. Instrumented pipeline replays (unchanged)
# ═══════════════════════════════════════════════════════════════════
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
            "roles": [r.name for r in node.get_classification()],
            "same_type_relations": [r.label for _, r in same_type],
            "cross_type_children": [c.node_lbl for c, _ in cross_type],
        })

        if node.node_lbl in ancestors:
            events.append({
                "action": "cross_type_cycle_hit",
                "node": node.node_lbl,
                "ancestors": sorted(ancestors), "path": list(path),
                "verdict": "CROSS_TYPE — stop recursion, no children",
            })
            return

        if same_type:
            events.append({
                "action": "same_type_loop_recorded",
                "node": node.node_lbl,
                "relations": [r.label for _, r in same_type],
            })

        ancestors.add(node.node_lbl)
        events.append({
            "action": "ancestor_push", "node": node.node_lbl,
            "ancestors": sorted(ancestors),
        })

        for child, rel in cross_type:
            events.append({
                "action": "descend", "from": node.node_lbl,
                "to": child.node_lbl, "via": rel.label,
                "ancestors": sorted(ancestors),
            })
            build(child, ancestors, path + [node.node_lbl])

        ancestors.discard(node.node_lbl)
        events.append({
            "action": "ancestor_pop", "node": node.node_lbl,
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
                           "depth": depth, "idx": idx})
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
        if props:
            col_offset.setdefault(label, []).append(idx)
            event["offset_assigned"] = idx
            for prop in props:
                columns.append({"nodeType": label, "property": prop, "depth": depth})
            idx += len(props)
        event["idx_after"] = idx
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
                                   "seen_counts": dict(seen_counts)})
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
                                   "seen_counts": dict(seen_counts)})
                    continue
                offset = offsets[occ]
                placed = []
                for i, prop in enumerate(props):
                    v = element.get(prop)
                    cells[offset + i] = {"nodeId": element.element_id,
                                         "nodeType": label, "value": v}
                    placed.append({"col": offset + i, "prop": prop, "value": v})
                events.append({"kind": "place", "label": label,
                               "occurrence": occ, "offset": offset,
                               "placed": placed,
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
                               "type": element.type})

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


# ═══════════════════════════════════════════════════════════════════
#  3. Variable glossary — single source of truth for every symbol
# ═══════════════════════════════════════════════════════════════════
#  Each entry: (id, LaTeX symbol, one-line meaning, longer explanation)
#  Every equation below uses these symbols consistently.
# ═══════════════════════════════════════════════════════════════════
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
    """Sidebar-friendly definition list of every symbol."""
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


# ═══════════════════════════════════════════════════════════════════
#  4. Equations — every variable wrapped in \underbrace{symbol}{meaning}
#     (plain LaTeX, single braces — never routed through .format on
#      their content, only injected via named placeholders.)
# ═══════════════════════════════════════════════════════════════════

# Stage 0 — Raw input
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

# Stage 1 — Schema extraction
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

# Stage 2 — TopologyNode graph
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

# Stage 3 — Root detection
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
\underbrace{\arg\max}_{\text{pick the biggest}}_{\;\underbrace{v}_{\text{over all vertices}} \in \underbrace{\mathcal{N}}_{\text{node graph}}}
\;\;\underbrace{|v.C|}_{\text{out-degree}}
"""

# Stage 4 — Tree build
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

# Stage 5 — Same-type expansion
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

# Stage 6 — Properties
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

# Stage 7 — Column layout
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

# Stage 8 — Cell placement
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

# Stage 10 — Sort key
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


# ═══════════════════════════════════════════════════════════════════
#  5. Styling
# ═══════════════════════════════════════════════════════════════════
_STYLE = """
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

/* Sticky top bar */
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

/* Sidebar (glossary) */
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

/* Stage banner blocks */
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
"""

# ═══════════════════════════════════════════════════════════════════
#  6. JavaScript (with all new interactive features)
# ═══════════════════════════════════════════════════════════════════
_JS = r"""
// ── Temml auto-render for elements with class .math ────────────
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

// ── Global playback speed ──────────────────────────────────────
let PLAY_MS = 350;
const speedInput = () => document.getElementById('speed');

// ── Track "active" stepper (the last one interacted with, or the
//    one nearest the top of the viewport) for global kbd shortcuts.
// ──────────────────────────────────────────────────────────────
let ACTIVE_STEPPER = null;
function setActive(rootEl){
  document.querySelectorAll('.stepper-block').forEach(
    el => el.classList.remove('active'));
  if (rootEl) rootEl.classList.add('active');
  ACTIVE_STEPPER = rootEl;
}

// ── Generic stepper factory ────────────────────────────────────
function mkStepper(rootId, events, renderFn) {
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

  // Click step-label to jump directly to a step number
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

// ── Global keyboard shortcuts ──────────────────────────────────
document.addEventListener('keydown', e => {
  if (e.target.matches('input, textarea')) return;
  if (!ACTIVE_STEPPER) {
    // Fallback: first stepper on the page
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

// ── Collapsible stage headings ─────────────────────────────────
window.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('section.stage > h2').forEach(h => {
    h.addEventListener('click', () =>
      h.parentElement.classList.toggle('collapsed'));
  });
});

// ── Glossary sidebar interactions ──────────────────────────────
window.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.glossary-list li').forEach(li => {
    li.addEventListener('click', () => {
      li.classList.toggle('active');
    });
  });
  const tog = document.getElementById('toggle-sidebar');
  if (tog) tog.addEventListener('click',
    () => document.body.classList.toggle('no-sidebar'));
});

// ── Playback speed control ────────────────────────────────────
window.addEventListener('DOMContentLoaded', () => {
  const s = speedInput();
  if (!s) return;
  const out = document.getElementById('speed-val');
  const upd = () => {
    PLAY_MS = +s.value;
    if (out) out.textContent = PLAY_MS + 'ms';
  };
  s.addEventListener('input', upd);
  upd();
});

// ── Helpers ────────────────────────────────────────────────────
function esc(s){ return String(s).replace(/[&<>]/g,
  c=>({'&':'&amp;','<':'&lt;','>':'&gt;'})[c]); }
function badge(cls, txt){ return `<span class="badge ${cls}">${esc(txt)}</span>`; }

// ── Per-trace renderers ────────────────────────────────────────
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
      if (key==='kind' || key==='cells_snapshot') continue;
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

// ── Graphviz rendering ─────────────────────────────────────────
window.addEventListener('DOMContentLoaded', () => {
  Viz.instance().then(viz => {
    for (const [id, src] of Object.entries(window.__DOTS__ || {})){
      const target = document.getElementById(id);
      if (target) target.appendChild(viz.renderSVGElement(src));
    }
  });
});
"""

# ═══════════════════════════════════════════════════════════════════
#  7. HTML template
#     — Note: only *named* placeholders like {style}, {eq_s0_d}, …
#       are consumed by .format(); everything else uses single braces.
# ═══════════════════════════════════════════════════════════════════
_PAGE = """<!doctype html>
<html><head>
<meta charset="utf-8"><title>read_as_table — deep debug view</title>
<script src="https://cdn.jsdelivr.net/npm/@viz-js/viz@3.2.4/lib/viz-standalone.js"></script>
<script src="https://cdn.jsdelivr.net/npm/temml@0.10.29/dist/temml.min.js"></script>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/temml@0.10.29/dist/Temml-Local.css">
<style>{style}</style>
</head><body>

<!-- ══════════ sidebar: variable glossary ══════════ -->
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
      click <code>Step N / M</code> to type a step ·
      click <b>h2</b> to collapse a stage
    </span>
  </div>
</div>

<!-- ══════════ variable-glossary anchor (mobile / body copy) ══════════ -->
<section class="stage" id="glossary">
<h2>Variables (canonical definitions)</h2>
<div class="abstract"><h4>Why this section exists</h4>
Every equation in this document is written using the symbols below —
and only those symbols. Hover any row on the left sidebar for a longer
description; click to pin it open. The <em>same LaTeX symbol appears
in the sidebar and in the equations</em>, so you can visually match them.
Every equation additionally uses <code>\\underbrace{{symbol}}{{meaning}}</code>
under every variable so you never need to scroll back here — but this
section stays as the single source of truth.
</div>
</section>

<!-- ══════════ STAGE 0 ══════════ -->
<section class="stage" id="s0">
<h2>Stage 0 — Raw Neo4j records (input)</h2>
<div class="abstract"><h4>Abstract</h4>
Input is a multiset of records, each a heterogeneous tuple of nodes and rels:
<div class="math display">{eq_s0_d}</div>
<div class="math display">{eq_s0_r}</div>
<div class="math display">{eq_s0_e}</div>
A node carries <code>(id, labels, properties)</code>; a relationship carries
<code>(type, start, end)</code>.
</div>
<div class="concrete"><h4>Concrete graph</h4>
<div class="plot" id="raw"></div>
<details><summary>DOT source</summary><pre>{raw_dot}</pre></details>
</div>
</section>

<!-- ══════════ STAGE 1 ══════════ -->
<section class="stage" id="s1">
<h2>Stage 1 — Schema extraction (<code>extract_node_types_and_relations</code>)</h2>
<div class="abstract"><h4>Abstract</h4>
Fold the record stream into two sets — the set of node labels and the set
of abstract relations. Each element is inspected exactly once; duplicates
collapse via set semantics.
<div class="math display">{eq_s1_n}</div>
<div class="math display">{eq_s1_r}</div>
</div>
<div class="concrete"><h4>Concrete</h4>
<b>𝓝</b> = <code>{node_types}</code><br/>
<b>𝓡</b> = <pre>{relations_json}</pre>
</div>
<div class="trace"><h4>Trace (per element)</h4>
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
</div>
</section>

<!-- ══════════ STAGE 2 ══════════ -->
<section class="stage" id="s2">
<h2>Stage 2 — TopologyNode graph (<code>topology_detector</code>)</h2>
<div class="abstract"><h4>Abstract</h4>
Materialise 𝓝 as vertex objects, then for each abstract relation
<code>(a, ℓ, b) ∈ 𝓡</code> execute:
<div class="math display">{eq_s2_add}</div>
Finally sort vertices descending by out-degree — used only as the fallback
root heuristic in Stage&nbsp;3.
</div>
<div class="concrete"><h4>Concrete node graph</h4>
<div class="plot" id="nodegraph"></div>
<table><thead><tr><th>label</th><th>out-degree</th><th>in-degree</th><th>roles</th></tr></thead>
<tbody>{node_table_rows}</tbody></table>
</div>
</section>

<!-- ══════════ STAGE 3 ══════════ -->
<section class="stage" id="s3">
<h2>Stage 3 — Root detection (<code>TopologyNode.is_root</code>)</h2>
<div class="abstract"><h4>Abstract</h4>
A node is a root iff <em>every</em> incoming edge is a self-loop:
<div class="math display">{eq_s3_root}</div>
The pure case <code>in-degree = 0</code> falls out automatically (0 = 0).
If nothing qualifies (fully cyclic graph) the fallback picks the node with
maximum out-degree:
<div class="math display">{eq_s3_fallback}</div>
</div>
<div class="concrete"><h4>Concrete</h4>
Roots detected: <code>{roots_list}</code><br/>
Fallback used:  <code>{fallback_used}</code><br/>
Longest path from any root: <code>{longest_path}</code>
</div>
</section>

<!-- ══════════ STAGE 4 ══════════ -->
<section class="stage" id="s4">
<h2>Stage 4 — Tree build (<code>_build_tree</code>, DFS with ancestor set)</h2>
<div class="abstract"><h4>Abstract</h4>
Recursive descent from each root. Split the current node's children into two
disjoint sets by comparing labels:
<div class="math display">{eq_s4_split}</div>
Then apply the rule:
<div class="math display">{eq_s4_build}</div>
<b>Caveat (see code-review notes):</b> the current implementation mutates a
<em>shared</em> ancestor set <code>A</code> across sibling recursion branches.
If two fork branches converge on the same descendant (diamond), the second
branch may see it as an ancestor even though it isn't on the current path.
The trace below makes this visible — watch the <code>ancestors</code> field.
</div>
<div class="concrete"><h4>Concrete tree</h4>
<div class="plot" id="topo"></div>
<details open><summary>ASCII</summary><pre>{ascii_tree}</pre></details>
<details><summary>JSON metadata</summary><pre>{topology_meta_json}</pre></details>
</div>
<div class="trace"><h4>Trace (DFS step-by-step)</h4>
<div id="tree-stepper" class="stepper-block">
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
original loses <code>LEAF</code> if it had it. This produces one
extra column-block per layer in Stage&nbsp;7.
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
<div class="concrete"><h4>Concrete</h4>
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
Cloned same-type layers make <code>|O(ℓ)| &gt; 1</code>; that is what
supports the multi-layer overflow logic in Stage 8.
</div>
<div class="concrete"><h4>Concrete</h4>
<pre>O (col_offset)  = {col_offset}
ordered_labels  = {ordered_labels}
W (total cols)  = {n_cols}</pre>
{columns_table}
</div>
<div class="trace"><h4>Trace (per DFS visit)</h4>
<div id="col-stepper" class="stepper-block">
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
<div class="trace"><h4>Trace (per record)</h4>
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
Rows are sorted lexicographically by this tuple, so records sharing a
root parent group together while empty slots tie-break to <code>ε&nbsp;=&nbsp;""</code>.
</div>
<div class="trace"><h4>Trace (per row)</h4>
<div id="sort-stepper" class="stepper-block">
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
  raw:       {raw_dot_json},
  nodegraph: {nodegraph_dot_json},
  topo:      {topo_dot_json}
}};
const SCHEMA_EVTS = {schema_evts_json};
const TREE_EVTS   = {tree_evts_json};
const COL_EVTS    = {col_evts_json};
const ROW_EVTS    = {row_evts_json};
const SORT_EVTS   = {sort_evts_json};
{js}
window.addEventListener('DOMContentLoaded', () => {{
  if (SCHEMA_EVTS.length) mkStepper('schema-stepper', SCHEMA_EVTS, renderSchemaEvt);
  if (TREE_EVTS.length)   mkStepper('tree-stepper',   TREE_EVTS,   renderTreeEvt);
  if (COL_EVTS.length)    mkStepper('col-stepper',    COL_EVTS,    renderColEvt);
  if (ROW_EVTS.length)    mkStepper('row-stepper',    ROW_EVTS,    renderRowEvt);
  if (SORT_EVTS.length)   mkStepper('sort-stepper',   SORT_EVTS,   renderSortEvt);
}});
</script>
</body></html>
"""


# ═══════════════════════════════════════════════════════════════════
#  8. Small HTML helpers (unchanged)
# ═══════════════════════════════════════════════════════════════════
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


# ═══════════════════════════════════════════════════════════════════
#  9. Public entry point
# ═══════════════════════════════════════════════════════════════════
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

    # collect overflow rows
    overflow_rows = []
    for i, r in enumerate(rows):
        if "sameTypeNodes" in r:
            overflow_rows.append({"row": i, "overflow": r["sameTypeNodes"]})

    # ── Render the page ──
    # Note: every LaTeX equation is passed as a *named* placeholder so its
    # own single-brace syntax is never re-parsed by str.format().
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

        # Trace event streams (as JSON, embedded into the page's <script>)
        schema_evts_json=json.dumps(schema_evts),
        tree_evts_json=json.dumps(tree_evts),
        col_evts_json=json.dumps(col_evts),
        row_evts_json=json.dumps(row_evts),
        sort_evts_json=json.dumps(sort_evts),

        # ── Equations (LaTeX with \underbrace, injected verbatim) ──
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


# ═══════════════════════════════════════════════════════════════════
# 10. Utilities
# ═══════════════════════════════════════════════════════════════════
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
