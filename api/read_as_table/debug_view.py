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
#  1. DOT generators (kept from the previous version)
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
    """Raw TopologyNode graph — pre-tree, shows every edge including cycles."""
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
#  2. Instrumented replays of every pipeline step
# ═══════════════════════════════════════════════════════════════════
def _trace_schema_extraction(data: list["Record"]) -> list[dict]:
    """Mirrors extract_node_types_and_relations, one event per element."""
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
    """Replay _build_tree recording ancestor set and control-flow branches."""
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
    """Replay _build_columns_from_trees with per-visit state snapshots."""
    events: list[dict] = []
    columns: list[dict] = []
    col_offset: dict[str, list[int]] = {}
    ordered_labels: list[str] = []
    idx = 0
    visited_ids: set[int] = set()

    def traverse(node: TopologyTree, depth: int, path: list[str]):
        nonlocal idx
        if id(node) in visited_ids:
            events.append({
                "action": "skip_visited", "node": node.node_label,
                "depth": depth, "idx": idx,
            })
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
    """Per-record trace of cell placement (mirrors topological_rec_to_json)."""
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
                    events.append({
                        "kind": "skip_unknown_label", "label": label,
                        "seen_counts": dict(seen_counts),
                    })
                    continue
                occ = seen_counts.get(label, 0)
                seen_counts[label] = occ + 1
                offsets = col_offset[label]
                props = props_by_type.get(label, [])
                if occ >= len(offsets):
                    overflow = [
                        {"nodeId": element.element_id,
                         "nodeType": label, "value": element.get(p)}
                        for p in props
                    ]
                    same_type_extras.append(overflow)
                    events.append({
                        "kind": "overflow_to_sameTypeNodes",
                        "label": label, "occurrence": occ,
                        "available_layers": len(offsets),
                        "overflow": overflow,
                        "seen_counts": dict(seen_counts),
                    })
                    continue
                offset = offsets[occ]
                placed = []
                for i, prop in enumerate(props):
                    v = element.get(prop)
                    cells[offset + i] = {
                        "nodeId": element.element_id,
                        "nodeType": label, "value": v,
                    }
                    placed.append({"col": offset + i, "prop": prop, "value": v})
                events.append({
                    "kind": "place", "label": label,
                    "occurrence": occ, "offset": offset,
                    "placed": placed,
                    "seen_counts": dict(seen_counts),
                    "cells_snapshot": [dict(c) for c in cells],
                })
            elif isinstance(element, Relationship):
                a, b = element.nodes[0], element.nodes[1]
                if not a or not b:
                    events.append({"kind": "rel_missing_node"})
                    continue
                relations.append({
                    "fromId": a.element_id, "relation": element.type,
                    "toId": b.element_id,
                })
                events.append({
                    "kind": "relation",
                    "from_label": extract_node_label(a),
                    "to_label": extract_node_label(b),
                    "type": element.type,
                })

        row = {"cells": cells, "relations": relations}
        if same_type_extras:
            row["sameTypeNodes"] = same_type_extras
        rows.append(row)
        all_traces.append({
            "record": ri, "events": events,
            "final_cells": cells, "final_relations": relations,
            "seen_counts_end": seen_counts,
            "overflow": same_type_extras,
        })

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
            detail.append({
                "label": label, "offset": first_offset,
                "value": cell.get("value"), "node_id": cell.get("nodeId"),
                "key_part": k,
            })
        events.append({
            "row_index": i, "key": key_parts, "detail": detail,
        })
    return events


# ═══════════════════════════════════════════════════════════════════
#  3. HTML rendering
# ═══════════════════════════════════════════════════════════════════
_STYLE = """
* { box-sizing: border-box; }
body { font-family: system-ui, sans-serif; margin: 1.5rem; color: #222; }
h1 { margin-top: 0; }
h2 { border-bottom: 2px solid #333; margin-top: 3rem; padding-bottom: .25rem; }
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
.plot { border: 1px solid #ddd; padding: .5rem; background: white; }
nav { position: sticky; top: 0; background: #fff; padding: .5rem 0;
      border-bottom: 1px solid #ddd; z-index: 10; }
nav a { margin-right: .5rem; font-size: 12px; text-decoration: none;
        color: #06c; }
.stage { border-left: 3px solid #06c; padding-left: 1rem; margin-top: 2rem; }
.abstract { background: #f0f7ff; border: 1px solid #cde; padding: .75rem;
            border-radius: 4px; margin: .5rem 0; }
.abstract h4 { margin-top: 0; color: #036; }
.concrete { background: #f7fff0; border: 1px solid #cec;
            padding: .75rem; border-radius: 4px; margin: .5rem 0; }
.concrete h4 { margin-top: 0; color: #263; }
.trace { background: #fffaf0; border: 1px solid #edc;
         padding: .75rem; border-radius: 4px; margin: .5rem 0; }
.trace h4 { margin-top: 0; color: #630; }
.stepper { display: flex; gap: .5rem; align-items: center;
           margin: .5rem 0; flex-wrap: wrap; }
.stepper input[type=range] { flex: 1; min-width: 200px; }
.stepper button { padding: .25rem .75rem; cursor: pointer; }
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

_JS = r"""
// ── Temml auto-render for elements with class .math ────────────
window.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.math').forEach(el => {
    try { temml.render(el.textContent, el, {displayMode: el.classList.contains('display')}); }
    catch(e) { el.textContent = '[math err] ' + e.message; }
  });
});

// ── Generic stepper factory ────────────────────────────────────
function mkStepper(rootId, events, renderFn) {
  const root = document.getElementById(rootId);
  const box  = root.querySelector('.event-box');
  const slider = root.querySelector('input[type=range]');
  const label = root.querySelector('.step-label');
  slider.max = events.length - 1;
  const show = i => {
    label.textContent = `Step ${i+1} / ${events.length}`;
    box.innerHTML = renderFn(events[i], i, events);
  };
  slider.addEventListener('input', e => show(+e.target.value));
  root.querySelector('.step-prev').addEventListener('click',
    () => { slider.value = Math.max(0, +slider.value - 1); show(+slider.value); });
  root.querySelector('.step-next').addEventListener('click',
    () => { slider.value = Math.min(events.length-1, +slider.value + 1); show(+slider.value); });
  root.querySelector('.step-play').addEventListener('click', () => {
    let i = +slider.value;
    const timer = setInterval(() => {
      if (i >= events.length - 1) { clearInterval(timer); return; }
      i++; slider.value = i; show(i);
    }, 350);
  });
  show(0);
}

function esc(s){ return String(s).replace(/[&<>]/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;'})[c]); }
function badge(cls, txt){ return `<span class="badge ${cls}">${esc(txt)}</span>`; }

// ── Per-trace renderers ────────────────────────────────────────
function renderSchemaEvt(e){
  return `<span class="event-index">Record ${e.record}, elem ${e.pos}</span> `
    + badge(e.kind==='NODE'?'b-ok':'b-warn', e.kind) + badge(e.new?'b-new':'b-old', e.new?'NEW':'seen')
    + `\nvalue: <b>${esc(e.value)}</b>\n\n`
    + `known node types  = [${e.nodes_snapshot.map(esc).join(', ')}]\n`
    + `known relations   = [\n  ${e.rels_snapshot.map(esc).join(',\n  ')}\n]`;
}

function renderTreeEvt(e){
  const l = ['enter','descend','ancestor_push','ancestor_pop',
             'cross_type_cycle_hit','same_type_loop_recorded'];
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
  let body = badge('b-ok', e.action) + `  node=<b>${esc(e.node)}</b>  depth=${e.depth}\n`;
  body += `path                     = ${JSON.stringify(e.path||[])}\n`;
  body += `idx (running col-index)  : ${e.idx_before} → ${e.idx_after}\n`;
  body += `props                    = ${JSON.stringify(e.props||[])}\n`;
  if (e.offset_assigned!==undefined)
    body += `offset assigned          = ${e.offset_assigned}\n`;
  body += `col_offset (state)       = ${JSON.stringify(e.col_offset_snapshot||{})}\n`;
  body += `ordered_labels (state)   = ${JSON.stringify(e.ordered_labels_snapshot||[])}`;
  return body;
}

function renderRowEvt(ev, i, all){
  // ev is a *row-level* trace (not a single event). Show each micro-event inline.
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
    s += `  ${d.label.padEnd(14)} offset=${String(d.offset).padEnd(4)}`
       + `  nodeId=${JSON.stringify(d.node_id||null)}  value=${JSON.stringify(d.value||null)}\n`;
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

_PAGE = """<!doctype html>
<html><head>
<meta charset="utf-8"><title>read_as_table — deep debug view</title>
<script src="https://cdn.jsdelivr.net/npm/@viz-js/viz@3.2.4/lib/viz-standalone.js"></script>
<script src="https://cdn.jsdelivr.net/npm/temml@0.10.29/dist/temml.min.js"></script>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/temml@0.10.29/dist/Temml-Local.css">
<style>{style}</style>
</head><body>

<h1>read_as_table — deep pipeline inspector</h1>
<p>Query: <code>{query}</code>  ·  records: <b>{n_records}</b>  ·
   node types: <b>{n_types}</b>  ·  relations: <b>{n_rels}</b>  ·
   final columns: <b>{n_cols}</b>  ·  final rows: <b>{n_rows}</b></p>

<nav>
 <a href="#s0">0 raw</a>
 <a href="#s1">1 schema extract</a>
 <a href="#s2">2 node graph</a>
 <a href="#s3">3 roots</a>
 <a href="#s4">4 tree build</a>
 <a href="#s5">5 same-type expand</a>
 <a href="#s6">6 props</a>
 <a href="#s7">7 columns</a>
 <a href="#s8">8 cells</a>
 <a href="#s9">9 overflow</a>
 <a href="#s10">10 sort key</a>
 <a href="#s11">11 final</a>
</nav>

<!-- ═══ STAGE 0 ═══ -->
<section class="stage" id="s0">
<h2>Stage 0 — Raw Neo4j records (input)</h2>
<div class="abstract"><h4>Abstract</h4>
Input is a multiset of records, each a heterogeneous tuple of nodes and relationships:
<div class="math display">
D = \\{{ r_1, r_2, \\dots, r_n \\}}, \\quad
r_i = (e_{{i,1}}, e_{{i,2}}, \\dots), \\quad
e_{{i,j}} \\in \\text{{Node}} \\cup \\text{{Rel}}
</div>
Where a node carries <span class="math">(\\text{{id}}, \\text{{labels}}, \\text{{properties}})</span>
and a relationship carries <span class="math">(\\text{{type}}, \\text{{start}}, \\text{{end}})</span>.
</div>
<div class="concrete"><h4>Concrete graph</h4>
<div class="plot" id="raw"></div>
<details><summary>DOT source</summary><pre>{raw_dot}</pre></details>
</div>
</section>

<!-- ═══ STAGE 1 ═══ -->
<section class="stage" id="s1">
<h2>Stage 1 — Schema extraction  (<code>extract_node_types_and_relations</code>)</h2>
<div class="abstract"><h4>Abstract</h4>
Fold the record stream into two sets:
<div class="math display">
N = \\bigcup_{{r \\in D}} \\{{ \\text{{label}}(n) : n \\in r \\cap \\text{{Node}} \\}}
</div>
<div class="math display">
R = \\bigcup_{{r \\in D}} \\{{ (\\text{{label}}(n_1),\\, \\rho.\\text{{type}},\\, \\text{{label}}(n_2))
                              : \\rho \\in r \\cap \\text{{Rel}},\\ \\rho = (n_1 \\to n_2) \\}}
</div>
Each element is looked at exactly once; duplicates are collapsed via set semantics.
</div>
<div class="concrete"><h4>Concrete</h4>
<b>N</b> = <code>{node_types}</code><br/>
<b>R</b> = <pre>{relations_json}</pre>
</div>
<div class="trace"><h4>Trace (per element)</h4>
<div id="schema-stepper">
 <div class="stepper">
   <button class="step-prev">◀</button>
   <input type="range" min="0" value="0">
   <button class="step-next">▶</button>
   <button class="step-play">▶ play</button>
   <span class="step-label"></span>
 </div>
 <div class="event-box"></div>
</div>
</div>
</section>

<!-- ═══ STAGE 2 ═══ -->
<section class="stage" id="s2">
<h2>Stage 2 — TopologyNode graph  (<code>topology_detector</code>)</h2>
<div class="abstract"><h4>Abstract</h4>
Materialise <span class="math">N</span> as vertex objects, then for each relation
<span class="math">(a, \\ell, b) \\in R</span> execute:
<div class="math display">
\\begin{{aligned}}
&amp;\\text{{node}}[a].\\text{{connected\\_to}}\\ \\mathrel{{+}}{{=}}\\ (\\text{{node}}[b], \\ell)\\\\
&amp;\\text{{node}}[b].\\text{{incoming\\_con\\_n}}\\ \\mathrel{{+}}{{=}}\\ 1
\\end{{aligned}}
</div>
Then sort descending by out-degree (used only as a fallback root heuristic).
</div>
<div class="concrete"><h4>Concrete node graph</h4>
<div class="plot" id="nodegraph"></div>
<table><thead><tr><th>label</th><th>out-degree</th><th>in-degree</th><th>roles</th></tr></thead>
<tbody>{node_table_rows}</tbody></table>
</div>
</section>

<!-- ═══ STAGE 3 ═══ -->
<section class="stage" id="s3">
<h2>Stage 3 — Root detection  (<code>TopologyNode.is_root</code>)</h2>
<div class="abstract"><h4>Abstract</h4>
A node is a root iff <em>every</em> incoming edge is a self-loop:
<div class="math display">
\\text{{is\\_root}}(n) \\iff |\\{{c : (c,\\cdot) \\in n.\\text{{connected\\_to}},\\ c = n\\}}|
                              = n.\\text{{incoming\\_con\\_n}}
</div>
Which includes the pure case <span class="math">n.\\text{{incoming\\_con\\_n}}=0</span>.
When the entire graph is cyclic, the fallback picks the node with maximum out-degree
(<span class="math">\\arg\\max_{{n \\in N}} |n.\\text{{connected\\_to}}|</span>).
</div>
<div class="concrete"><h4>Concrete</h4>
Roots detected: <code>{roots_list}</code><br/>
Fallback used: <code>{fallback_used}</code><br/>
Longest path from any root: <code>{longest_path}</code>
</div>
</section>

<!-- ═══ STAGE 4 ═══ -->
<section class="stage" id="s4">
<h2>Stage 4 — Tree build  (<code>_build_tree</code>, DFS + ancestor set)</h2>
<div class="abstract"><h4>Abstract</h4>
Recursive descent from each root. Maintain an <em>ancestor set</em>
<span class="math">A \\subseteq N</span> along the active path. For a node
<span class="math">n</span> with children <span class="math">C = n.\\text{{connected\\_to}}</span>
split into:
<div class="math display">
C_{{\\text{{same}}}} = \\{{(c,\\ell) \\in C : c = n\\}}, \\quad
C_{{\\text{{cross}}}} = C \\setminus C_{{\\text{{same}}}}
</div>
Recursion rules:
<div class="math display">
\\text{{build}}(n, A) = \\begin{{cases}}
  \\text{{CROSS\\_TYPE stub}} &amp; \\text{{if }} n \\in A \\\\
  \\text{{node}}\\bigl(n,\\ \\{{\\text{{build}}(c, A \\cup \\{{n\\}}) : (c,\\cdot) \\in C_{{\\text{{cross}}}}\\}}\\bigr) &amp; \\text{{otherwise}}
\\end{{cases}}
</div>
<b>Caveat (see the code review notes):</b> the current implementation mutates a
<em>shared</em> <span class="math">A</span> across sibling recursion branches, so
if two fork branches converge on the same descendant (diamond), the second branch
may see it as an ancestor even though it isn't on the current path.
The trace below makes this visible: watch the <code>ancestors</code> field.
</div>
<div class="concrete"><h4>Concrete tree</h4>
<div class="plot" id="topo"></div>
<details open><summary>ASCII</summary><pre>{ascii_tree}</pre></details>
<details><summary>JSON metadata</summary><pre>{topology_meta_json}</pre></details>
</div>
<div class="trace"><h4>Trace (DFS step-by-step)</h4>
<div id="tree-stepper">
 <div class="stepper">
   <button class="step-prev">◀</button>
   <input type="range" min="0" value="0">
   <button class="step-next">▶</button>
   <button class="step-play">▶ play</button>
   <span class="step-label"></span>
 </div>
 <div class="event-box"></div>
</div>
</div>
</section>

<!-- ═══ STAGE 5 ═══ -->
<section class="stage" id="s5">
<h2>Stage 5 — Same-type expansion  (<code>_expand_same_type</code>)</h2>
<div class="abstract"><h4>Abstract</h4>
For every tree node <span class="math">t</span> flagged
<span class="math">\\text{{SAME\\_TYPE}}</span>, splice a linear chain of
<span class="math">k = \\text{{SAME\\_TYPE\\_DEPTH}}</span> deep-copied clones
below it:
<div class="math display">
t \\xrightarrow{{\\text{{clone}}}} t' \\xrightarrow{{\\text{{clone}}}} t'' \\xrightarrow{{\\text{{clone}}}} \\dots \\ (k\\text{{ layers}})
</div>
Roles are patched along the chain: intermediate nodes gain
<span class="math">\\text{{CHAIN}}</span>, the terminal clone gains
<span class="math">\\text{{LEAF}}</span>, and the original loses
<span class="math">\\text{{LEAF}}</span> if it had it. This produces one column
block per layer in stage 7.
</div>
<div class="concrete"><h4>Concrete</h4>
SAME_TYPE_DEPTH = <code>{same_type_depth}</code><br/>
Same-type nodes found: <code>{same_type_labels}</code>
</div>
</section>

<!-- ═══ STAGE 6 ═══ -->
<section class="stage" id="s6">
<h2>Stage 6 — Property discovery  (<code>_discover_properties</code>)</h2>
<div class="abstract"><h4>Abstract</h4>
For each label <span class="math">\\ell \\in N</span>, record the property names of
the first-seen instance (order preserved):
<div class="math display">
P : N \\to \\text{{List}}[\\text{{String}}], \\quad
P(\\ell) = \\text{{keys}}\\bigl(\\text{{first}}\\{{n \\in D : \\text{{label}}(n)=\\ell\\}}\\bigr)
</div>
</div>
<div class="concrete"><h4>Concrete</h4>
<pre>{props_by_type_json}</pre>
</div>
</section>

<!-- ═══ STAGE 7 ═══ -->
<section class="stage" id="s7">
<h2>Stage 7 — Column layout  (<code>_build_columns_from_trees</code>)</h2>
<div class="abstract"><h4>Abstract</h4>
DFS over the (possibly same-type-expanded) forest. Maintain a running column index
<span class="math">i</span> and, per label, a list of <em>occurrence offsets</em>
<span class="math">O(\\ell) \\in \\mathbb{{N}}^*</span>. On visiting a tree node
<span class="math">t</span> with <span class="math">\\ell = t.\\text{{label}}</span>:
<div class="math display">
\\begin{{aligned}}
&amp;O(\\ell) \\mathrel{{+}}{{=}} [i] \\\\
&amp;\\text{{columns}} \\mathrel{{+}}{{=}} [(\\ell, p, \\text{{depth}}) : p \\in P(\\ell)] \\\\
&amp;i \\mathrel{{+}}{{=}} |P(\\ell)|
\\end{{aligned}}
</div>
Total width:
<span class="math">|\\text{{columns}}| = \\sum_{{\\ell \\in N}} |O(\\ell)| \\cdot |P(\\ell)|</span>.
Cloned same-type layers make <span class="math">|O(\\ell)|>1</span>; that is what
supports the multi-layer overflow logic in stage 8.
</div>
<div class="concrete"><h4>Concrete</h4>
<pre>col_offset     = {col_offset}
ordered_labels = {ordered_labels}
total columns  = {n_cols}</pre>
{columns_table}
</div>
<div class="trace"><h4>Trace (per DFS visit)</h4>
<div id="col-stepper">
 <div class="stepper">
   <button class="step-prev">◀</button>
   <input type="range" min="0" value="0">
   <button class="step-next">▶</button>
   <button class="step-play">▶ play</button>
   <span class="step-label"></span>
 </div>
 <div class="event-box"></div>
</div>
</div>
</section>

<!-- ═══ STAGE 8 ═══ -->
<section class="stage" id="s8">
<h2>Stage 8 — Cell placement  (per record)</h2>
<div class="abstract"><h4>Abstract</h4>
For each record <span class="math">r \\in D</span>, allocate an empty row of width
<span class="math">|\\text{{columns}}|</span> and a per-record counter
<span class="math">S : N \\to \\mathbb{{N}}</span> (all zero). For each node
<span class="math">n \\in r</span> with label
<span class="math">\\ell=\\text{{label}}(n)</span>:
<div class="math display">
k = S(\\ell), \\quad S(\\ell) \\mathrel{{+}}{{=}} 1
</div>
<div class="math display">
\\text{{row.cells}}\\bigl[O(\\ell)_k + j\\bigr] = \\bigl(n.\\text{{id}},\\ \\ell,\\ n.P(\\ell)_j\\bigr)
\\quad \\text{{for }} j = 0, \\dots, |P(\\ell)|-1
</div>
When <span class="math">k \\geq |O(\\ell)|</span> the extra instance overflows into
<code>row.sameTypeNodes</code> (stage 9).
</div>
<div class="trace"><h4>Trace (per record)</h4>
<div id="row-stepper">
 <div class="stepper">
   <button class="step-prev">◀</button>
   <input type="range" min="0" value="0">
   <button class="step-next">▶</button>
   <button class="step-play">▶ play</button>
   <span class="step-label"></span>
 </div>
 <div class="event-box"></div>
</div>
</div>
</section>

<!-- ═══ STAGE 9 ═══ -->
<section class="stage" id="s9">
<h2>Stage 9 — Overflow  (<code>sameTypeNodes</code>)</h2>
<div class="abstract"><h4>Abstract</h4>
Overflowing occurrences of a label beyond the expanded depth
<span class="math">|O(\\ell)|</span> are stashed as
<span class="math">\\text{{row.sameTypeNodes}} \\in \\text{{List}}[\\text{{List}}[\\text{{Cell}}]]</span>
so nothing is lost when a real data path is deeper than
<span class="math">\\text{{SAME\\_TYPE\\_DEPTH}}</span>.
</div>
<div class="concrete"><h4>Overflow occurrences in this run</h4>
<pre>{overflow_json}</pre>
</div>
</section>

<!-- ═══ STAGE 10 ═══ -->
<section class="stage" id="s10">
<h2>Stage 10 — Grouping sort key  (<code>_grouping_sort_key2</code>)</h2>
<div class="abstract"><h4>Abstract</h4>
Each row is assigned a composite key derived from the <em>root-most</em>
occurrence of each label — that is, the first offset stored in
<span class="math">O(\\ell)</span>:
<div class="math display">
\\text{{key}}(r) = \\bigl(\\ \\text{{str}}(r.\\text{{cells}}[O(\\ell)_0].\\text{{nodeId}} \\lor \\varepsilon)
                       \\bigr)_{{\\ell \\in \\text{{ordered\\_labels}}}}
</div>
Rows are sorted lexicographically by this tuple, so records sharing a root parent
group together while empty slots (missing label in the record) tie-break to
<span class="math">\\varepsilon = \\text{{&quot;&quot;}}</span>.
</div>
<div class="trace"><h4>Trace (per row)</h4>
<div id="sort-stepper">
 <div class="stepper">
   <button class="step-prev">◀</button>
   <input type="range" min="0" value="0">
   <button class="step-next">▶</button>
   <button class="step-play">▶ play</button>
   <span class="step-label"></span>
 </div>
 <div class="event-box"></div>
</div>
</div>
</section>

<!-- ═══ STAGE 11 ═══ -->
<section class="stage" id="s11">
<h2>Stage 11 — Final output</h2>
<h3>Rows before grouping sort ({n_rows} rows)</h3>
{rows_unsorted_table}
<h3>Rows after grouping sort</h3>
{rows_sorted_table}
</section>

<script>
window.__DOTS__ = {{
  raw:       {raw_dot_json},
  nodegraph: {nodegraph_dot_json},
  topo:      {topo_dot_json},
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
#  4. Small HTML helpers
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
#  5. Public entry point
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

    # collect overflow
    overflow_rows = []
    for i, r in enumerate(rows):
        if "sameTypeNodes" in r:
            overflow_rows.append({"row": i, "overflow": r["sameTypeNodes"]})

    page = _PAGE.format(
        style=_STYLE,
        js=_JS,
        query=html.escape(str(params)),
        n_records=len(data),
        n_types=len(top_translator.top),
        n_rels=len(top_translator.relations),
        n_cols=total_cols,
        n_rows=len(rows_sorted),
        raw_dot=html.escape(raw_dot),
        raw_dot_json=json.dumps(raw_dot),
        nodegraph_dot_json=json.dumps(nodegraph_dot),
        topo_dot_json=json.dumps(topo_dot),
        node_types=sorted({n.node_lbl for n in top_translator.top}),
        relations_json=html.escape(json.dumps([
            {"label": r.label, "from": r.from_node_type, "to": r.to_node_type}
            for r in top_translator.relations
        ], indent=2)),
        node_table_rows=node_rows,
        roots_list=[r.node_lbl for r in roots] if roots else "(none)",
        fallback_used=fallback_used,
        longest_path=(
            max(get_longest_path(r) for r in (roots or top_translator.top[:1]))
            if top_translator.top else 0
        ),
        ascii_tree=html.escape(_capture_ascii_tree(trees)),
        topology_meta_json=html.escape(
            json.dumps([_topology_tree_to_dict(t) for t in trees], indent=2)
        ),
        same_type_depth=SAME_TYPE_DEPTH,
        same_type_labels=same_type_labels or "(none)",
        props_by_type_json=html.escape(json.dumps(props_by_type, indent=2)),
        col_offset=col_offset,
        ordered_labels=ordered_labels,
        columns_table=_columns_table(columns),
        overflow_json=html.escape(json.dumps(overflow_rows, indent=2)) or "(none)",
        rows_unsorted_table=_row_table(columns, rows_unsorted),
        rows_sorted_table=_row_table(columns, rows_sorted),
        schema_evts_json=json.dumps(schema_evts),
        tree_evts_json=json.dumps(tree_evts),
        col_evts_json=json.dumps(col_evts),
        row_evts_json=json.dumps(row_evts),
        sort_evts_json=json.dumps(sort_evts),
    )
    return Response(page, mimetype="text/html")


def _walk_forest(trees):
    """Yield every TopologyTree in a forest (pre-order)."""
    stack = list(trees)
    while stack:
        t = stack.pop()
        yield t
        stack.extend(t.children)
