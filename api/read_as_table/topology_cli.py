#!/usr/bin/env python3
"""
CLI wrapper for topology_detector.py

Lets you specify nodes and relationships on the command line (or interactively)
and inspect the resulting topology tree + table layout — no Neo4j connection needed.

Usage:
  # Interactive mode
  python topology_cli.py

  # Inline mode
  python topology_cli.py \
      --nodes Person,Company,City \
      --rels  "Person WORKS_AT Company" \
              "Person LIVES_IN City"    \
              "Company LOCATED_IN City"

  # Same-type (self-referencing) loop
  python topology_cli.py --nodes Employee --rels "Employee MANAGES Employee"
"""
from __future__ import annotations

import argparse
import logging
import sys

from api.read_as_table.topology_detector import (
    AbstractRelation,
    CycleType,
    TopologyNode,
    TopologyTranslator,
    TopologyTree,
    get_longest_path,
)

log = logging.getLogger("[TopologyCLI]")


# ──────────────────────────────────────────────
#  Factory: build a TopologyTranslator without
#  Neo4j Records
# ──────────────────────────────────────────────
def build_translator(
    node_labels: set[str],
    relations: set[AbstractRelation],
) -> TopologyTranslator:
    """
    Construct a fully initialised TopologyTranslator by injecting
    raw node labels and abstract relations — bypassing the Neo4j
    Record parsing in __init__ / extract_node_types_and_relations.
    """
    # Allocate without calling __init__ (avoids the Record dependency)
    translator = object.__new__(TopologyTranslator)
    translator.log = log

    # ── replicate topology_detector() logic ──
    nodes: dict[str, TopologyNode] = {n: TopologyNode(n) for n in node_labels}

    for r in relations:
        from_node = nodes[r.from_node_type]
        to_node = nodes[r.to_node_type]
        from_node.connected_to.append((to_node, r))
        to_node.incoming_con_n += 1

    translator.top = sorted(
        nodes.values(), key=lambda nd: -len(nd.connected_to)
    )
    translator.relations = relations
    return translator


# ──────────────────────────────────────────────
#  Parsing helpers
# ──────────────────────────────────────────────
def parse_relation(text: str) -> AbstractRelation:
    """Parse ``'FromNode REL_TYPE ToNode'`` into an AbstractRelation."""
    parts = text.strip().split()
    if len(parts) != 3:
        raise ValueError(
            f"Bad relation format: '{text}'. "
            f"Expected: 'FromNode REL_TYPE ToNode'"
        )
    return AbstractRelation(label=parts[1], from_node_type=parts[0], to_node_type=parts[2])


# ──────────────────────────────────────────────
#  Display: tree view (box-drawing)
# ──────────────────────────────────────────────
def render_tree(trees: list[TopologyTree], indent: int = 0, is_last_stack: list[bool] | None = None):
    """Pretty-print the topology tree with box-drawing characters."""
    if is_last_stack is None:
        is_last_stack = []

    for i, tree in enumerate(trees):
        is_last = i == len(trees) - 1

        # Build the prefix from the ancestor stack
        prefix = ""
        for parent_is_last in is_last_stack:
            prefix += "    " if parent_is_last else "│   "
        connector = "└── " if is_last else "├── "

        # Relation from parent
        rel_tag = ""
        if tree.relation_from_parent:
            rel_tag = f"--[{tree.relation_from_parent.label}]--> "

        # Roles
        roles = ", ".join(sorted(r.name for r in tree.roles))

        # Cycle annotation
        cycle_tag = ""
        if tree.cycle_type == CycleType.SAME_TYPE and tree.same_type_info:
            via = ", ".join(r.label for r in tree.same_type_info.relations)
            cycle_tag = f"  ⟳ SAME_TYPE via [{via}]"
        elif tree.cycle_type == CycleType.CROSS_TYPE:
            cycle_tag = "  ↩ CROSS_TYPE_LOOP"

        print(f"{prefix}{connector}{rel_tag}[{tree.node_label}] ({roles}){cycle_tag}")

        render_tree(tree.children, indent + 1, is_last_stack + [is_last])


# ──────────────────────────────────────────────
#  Display: flattened table (root-to-leaf paths)
# ──────────────────────────────────────────────
def _flatten(trees: list[TopologyTree], path: list[str], rows: list[list[str]]):
    for tree in trees:
        rel_prefix = f"--[{tree.relation_from_parent.label}]-->" if tree.relation_from_parent else ""

        suffix = ""
        if tree.cycle_type == CycleType.SAME_TYPE and tree.same_type_info:
            via = ",".join(r.label for r in tree.same_type_info.relations)
            suffix = f" ⟳{via}"
        elif tree.cycle_type == CycleType.CROSS_TYPE:
            suffix = " ↩LOOP"

        cell = f"{rel_prefix}{tree.node_label}{suffix}"
        current = path + [cell]

        if not tree.children:
            rows.append(current)
        else:
            _flatten(tree.children, current, rows)


def render_table(trees: list[TopologyTree]):
    """Print a table whose rows are root→leaf paths and columns are depth levels."""
    rows: list[list[str]] = []
    _flatten(trees, [], rows)

    if not rows:
        print("  (empty topology)")
        return

    max_depth = max(len(r) for r in rows)
    headers = [f"Level {i}" for i in range(max_depth)]

    # Column widths
    widths = [len(h) for h in headers]
    for row in rows:
        for col, cell in enumerate(row):
            widths[col] = max(widths[col], len(cell))

    def fmt_row(cells):
        padded = []
        for col in range(max_depth):
            val = cells[col] if col < len(cells) else ""
            padded.append(f" {val:<{widths[col]}} ")
        return "|" + "|".join(padded) + "|"

    sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"

    print(sep)
    print(fmt_row(headers))
    print(sep)
    for row in rows:
        print(fmt_row(row))
    print(sep)


# ──────────────────────────────────────────────
#  Display: node summary
# ──────────────────────────────────────────────
def render_summary(translator: TopologyTranslator):
    print("── Node Summary ──")
    for node in translator.top:
        print(f"  {node}")

    roots = translator.roots
    print(f"\n  Root(s): {[r.node_lbl for r in roots] if roots else '(none — possible full cycle)'}")

    if translator.top:
        longest = get_longest_path(translator.top[0])
        print(f"  Longest path: {longest}")
    print()


# ──────────────────────────────────────────────
#  Core runner
# ──────────────────────────────────────────────
def run(node_labels: set[str], relations: set[AbstractRelation]):
    translator = build_translator(node_labels, relations)

    render_summary(translator)

    trees = translator.get_topology_tree()

    print("── Topology Tree ──")
    if trees:
        render_tree(trees)
    else:
        print("  (no tree)")
    print()

    print("── Table Layout (each row = root ➜ leaf path) ──")
    if trees:
        render_table(trees)
    else:
        print("  (no table)")
    print()


# ──────────────────────────────────────────────
#  Interactive mode
# ──────────────────────────────────────────────
def interactive():
    print("=" * 60)
    print("  Topology Translator — Interactive CLI")
    print("=" * 60)
    print()

    # -- nodes --
    print("Enter node labels (comma-separated):")
    print("  e.g.  Person, Company, City")
    raw = input("> ").strip()
    if not raw:
        print("No nodes given. Bye.")
        return
    node_labels = {tok.strip() for tok in raw.split(",") if tok.strip()}
    print(f"  ✓ Nodes: {sorted(node_labels)}\n")

    # -- relations --
    print("Enter relationships, one per line  (blank line to finish):")
    print("  Format:  FromNode REL_TYPE ToNode")
    print("  e.g.     Person WORKS_AT Company\n")

    relations: set[AbstractRelation] = set()
    while True:
        line = input("rel> ").strip()
        if not line:
            break
        try:
            rel = parse_relation(line)
            for lbl in (rel.from_node_type, rel.to_node_type):
                if lbl not in node_labels:
                    print(f"  ⚠  '{lbl}' was not declared — adding automatically.")
                    node_labels.add(lbl)
            relations.add(rel)
            print(f"  ✓ ({rel.from_node_type})-[{rel.label}]->({rel.to_node_type})")
        except ValueError as exc:
            print(f"  ✗ {exc}")

    print(f"\n{'=' * 60}")
    print(f"  Building topology: {len(node_labels)} node(s), {len(relations)} relationship(s)")
    print(f"{'=' * 60}\n")

    run(node_labels, relations)


# ──────────────────────────────────────────────
#  CLI entry point
# ──────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="CLI wrapper for the Neo4j Topology Translator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  # interactive
  %(prog)s

  # inline
  %(prog)s -n Person,Company,City \\
           -r "Person WORKS_AT Company" \\
              "Person LIVES_IN City"    \\
              "Company LOCATED_IN City"

  # self-referencing loop
  %(prog)s -n Employee -r "Employee MANAGES Employee"
""",
    )
    parser.add_argument(
        "-n", "--nodes",
        help="Comma-separated node labels, e.g. 'Person,Company,City'",
    )
    parser.add_argument(
        "-r", "--rels",
        nargs="*",
        help="Relationships as 'From REL_TYPE To' strings",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    # No flags → interactive
    if args.nodes is None and args.rels is None:
        interactive()
        return

    node_labels: set[str] = set()
    if args.nodes:
        node_labels = {t.strip() for t in args.nodes.split(",") if t.strip()}

    relations: set[AbstractRelation] = set()
    if args.rels:
        for r in args.rels:
            rel = parse_relation(r)
            relations.add(rel)
            node_labels.add(rel.from_node_type)
            node_labels.add(rel.to_node_type)

    if not node_labels:
        print("Error: no nodes provided.", file=sys.stderr)
        sys.exit(1)

    run(node_labels, relations)


if __name__ == "__main__":
    main()
