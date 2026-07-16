from __future__ import annotations
from typing import TYPE_CHECKING
import html
import json
import logging

if TYPE_CHECKING:
    from neo4j import Record
    from api.neo4j_interface import ReadRequest

from flask import Response
from neo4j.graph import Node, Relationship

from api.read_as_table.helpers import extract_node_label
from api.read_as_table.topology_detector import (
    TopologyTranslator,
    TopologyTree,
    CycleType,
)
from api.read_as_table.topology_helpers import (
    _build_columns_from_trees,
    _discover_properties,
    _grouping_sort_key2,
    _topology_tree_to_dict,
)
# Reuse the ASCII renderer you already wrote for the CLI
from api.read_as_table.topology_cli import render_tree as _cli_render_tree

log = logging.getLogger("[API] read_as_table.debug_view")


# ────────────────────────────────────────────────────────────────
#  DOT generators (Graphviz source)
# ────────────────────────────────────────────────────────────────
def _raw_records_to_dot(data: list["Record"]) -> str:
    """Render the raw Neo4j records as a graph.  Nodes keyed by element_id."""
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
    """Render the derived TopologyTree(s)."""
    lines = ['digraph topology {', '  rankdir=TB; node [shape=box];']
    counter = {"i": 0}

    def walk(t: TopologyTree, parent_id: str | None, edge_label: str | None):
        counter["i"] += 1
        nid = f"n{counter['i']}"

        # colour by cycle type / roles
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
            elabel = html.escape(edge_label or "")
            lines.append(f'  {parent_id} -> {nid} [label="{elabel}"];')

        # same-type loop hint
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


# ────────────────────────────────────────────────────────────────
#  ASCII tree (captured from your existing CLI helper)
# ────────────────────────────────────────────────────────────────
def _capture_ascii_tree(trees: list[TopologyTree]) -> str:
    import io, contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        _cli_render_tree(trees)
    return buf.getvalue() or "(empty)"


# ────────────────────────────────────────────────────────────────
#  Row builder — mirror of topological_rec_to_json but returns
#  intermediate artifacts for inspection.
# ────────────────────────────────────────────────────────────────
def _build_all_stages(data: list["Record"]):
    top_translator = TopologyTranslator(data)
    trees = top_translator.get_topology_tree()

    props_by_type = _discover_properties(data)
    columns, col_offset, total_cols, ordered_labels = _build_columns_from_trees(
        trees, props_by_type
    )

    empty_cell = {"nodeId": None, "nodeType": None, "value": None}
    rows: list[dict] = []

    for record in data:
        cells = [dict(empty_cell) for _ in range(total_cols)]
        relations: list[dict] = []
        seen_counts: dict[str, int] = {}

        for element in record:
            if isinstance(element, Node):
                label = list(element.labels)[0]
                if label not in col_offset:
                    continue
                occ = seen_counts.get(label, 0)
                seen_counts[label] = occ + 1
                offsets = col_offset[label]
                props = props_by_type.get(label, [])
                if occ >= len(offsets):
                    continue
                offset = offsets[occ]
                for i, prop in enumerate(props):
                    cells[offset + i] = {
                        "nodeId": element.element_id,
                        "nodeType": label,
                        "value": element.get(prop),
                    }
            elif isinstance(element, Relationship):
                relations.append({
                    "fromId": element.nodes[0].element_id,
                    "relation": element.type,
                    "toId": element.nodes[1].element_id,
                })
        rows.append({"cells": cells, "relations": relations})

    rows_unsorted = [dict(r) for r in rows]
    rows_sorted = sorted(
        rows,
        key=lambda r: _grouping_sort_key2(r, ordered_labels, col_offset),
    )

    return {
        "trees": trees,
        "node_types": sorted({n.node_lbl for n in top_translator.top}),
        "relations": [
            {"label": r.label, "from": r.from_node_type, "to": r.to_node_type}
            for r in top_translator.relations
        ],
        "props_by_type": props_by_type,
        "columns": columns,
        "col_offset": col_offset,
        "ordered_labels": ordered_labels,
        "rows_unsorted": rows_unsorted,
        "rows_sorted": rows_sorted,
    }


# ────────────────────────────────────────────────────────────────
#  HTML rendering
# ────────────────────────────────────────────────────────────────
_PAGE = """<!doctype html>
<html><head><meta charset="utf-8"><title>read_as_table — debug view</title>
<script src="https://cdn.jsdelivr.net/npm/@viz-js/viz@3.2.4/lib/viz-standalone.js"></script>
<style>
 body {{ font-family: system-ui, sans-serif; margin: 1.5rem; }}
 h2 {{ border-bottom: 1px solid #ccc; margin-top: 2.5rem; }}
 pre {{ background: #f4f4f4; padding: .75rem; overflow: auto;
        font-size: 12px; line-height: 1.35; }}
 table {{ border-collapse: collapse; font-size: 12px; }}
 th, td {{ border: 1px solid #bbb; padding: 3px 6px; vertical-align: top;
          max-width: 240px; }}
 th {{ background: #eee; }}
 .empty {{ color: #bbb; font-style: italic; }}
 .plot {{ border: 1px solid #ddd; padding: .5rem; }}
 nav a {{ margin-right: .75rem; }}
</style></head>
<body>
<h1>read_as_table — pipeline inspector</h1>
<p>Query: <code>{query}</code></p>
<nav>
 <a href="#stage0">0. Raw records</a>
 <a href="#stage1">1. Detected topology</a>
 <a href="#stage2">2. Topology tree</a>
 <a href="#stage3">3. Column layout</a>
 <a href="#stage4">4. Rows (pre-sort)</a>
 <a href="#stage5">5. Final table</a>
</nav>

<h2 id="stage0">0. Raw Neo4j records</h2>
<div class="plot" id="raw"></div>
<details><summary>DOT source</summary><pre>{raw_dot}</pre></details>

<h2 id="stage1">1. Detected node types &amp; relations</h2>
<pre>{topology_summary}</pre>

<h2 id="stage2">2. TopologyTree</h2>
<div class="plot" id="topo"></div>
<details open><summary>ASCII</summary><pre>{ascii_tree}</pre></details>
<details><summary>JSON metadata</summary><pre>{topology_meta_json}</pre></details>

<h2 id="stage3">3. Column layout ({n_cols} columns)</h2>
<pre>col_offset = {col_offset}
ordered_labels = {ordered_labels}
props_by_type = {props_by_type}</pre>
{columns_table}

<h2 id="stage4">4. Rows before grouping sort ({n_rows} rows)</h2>
{rows_unsorted_table}

<h2 id="stage5">5. Final rows after grouping sort</h2>
{rows_sorted_table}

<script>
Viz.instance().then(viz => {{
  document.getElementById('raw').appendChild(
    viz.renderSVGElement({raw_dot_json}));
  document.getElementById('topo').appendChild(
    viz.renderSVGElement({topo_dot_json}));
}});
</script>
</body></html>"""


def _row_table(columns: list[dict], rows: list[dict]) -> str:
    header = "".join(
        f"<th>{html.escape(c['nodeType'])}<br/>"
        f"<small>{html.escape(c['property'])}</small></th>"
        for c in columns
    )
    body_rows = []
    for r in rows:
        tds = []
        for cell in r["cells"]:
            if cell.get("value") is None and cell.get("nodeId") is None:
                tds.append('<td class="empty">·</td>')
            else:
                tds.append(f"<td>{html.escape(str(cell.get('value')))}</td>")
        body_rows.append("<tr>" + "".join(tds) + "</tr>")
    return f"<table><thead><tr>{header}</tr></thead><tbody>{''.join(body_rows)}</tbody></table>"


def _columns_table(columns: list[dict]) -> str:
    rows = "".join(
        f"<tr><td>{i}</td><td>{html.escape(c['nodeType'])}</td>"
        f"<td>{html.escape(c['property'])}</td>"
        f"<td>{c.get('depth', '')}</td></tr>"
        for i, c in enumerate(columns)
    )
    return (
        "<table><thead><tr><th>#</th><th>nodeType</th>"
        "<th>property</th><th>depth</th></tr></thead>"
        f"<tbody>{rows}</tbody></table>"
    )


def render_debug_view(data: list["Record"], params: "ReadRequest",
                      mode: str = "1") -> Response:
    """Return a self-contained HTML page inspecting every pipeline stage."""

    if not data:
        return Response("<p>No data.</p>", mimetype="text/html")

    stages = _build_all_stages(data)
    trees = stages["trees"]

    raw_dot = _raw_records_to_dot(data)
    topo_dot = _topology_to_dot(trees)

    topology_summary = (
        f"node types: {stages['node_types']}\n"
        f"relations : {json.dumps(stages['relations'], indent=2)}"
    )

    page = _PAGE.format(
        query=html.escape(str(params)),
        raw_dot=html.escape(raw_dot),
        raw_dot_json=json.dumps(raw_dot),
        topo_dot_json=json.dumps(topo_dot),
        topology_summary=html.escape(topology_summary),
        ascii_tree=html.escape(_capture_ascii_tree(trees)),
        topology_meta_json=html.escape(
            json.dumps([_topology_tree_to_dict(t) for t in trees], indent=2)
        ),
        n_cols=len(stages["columns"]),
        col_offset=stages["col_offset"],
        ordered_labels=stages["ordered_labels"],
        props_by_type=stages["props_by_type"],
        columns_table=_columns_table(stages["columns"]),
        n_rows=len(stages["rows_sorted"]),
        rows_unsorted_table=_row_table(stages["columns"], stages["rows_unsorted"]),
        rows_sorted_table=_row_table(stages["columns"], stages["rows_sorted"]),
    )
    return Response(page, mimetype="text/html")
