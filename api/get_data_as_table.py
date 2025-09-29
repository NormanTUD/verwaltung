import json
from flask import Blueprint, request, jsonify

def create_get_data_bp(graph):
    bp = Blueprint("get_data_bp", __name__)

    class GraphAPI:
        def __init__(self, driver):
            self.driver = driver

        def _node_to_dict(self, node):
            return {
                "id": getattr(node, "identity", None),
                "labels": list(getattr(node, "labels", [])),
                "props": dict(node) if dict(node) else {},
            }

        def _rel_to_dict(self, rel):
            return {
                "fromId": getattr(rel.start_node, "identity", None),
                "toId": getattr(rel.end_node, "identity", None),
                "type": type(rel).__name__,
            }

        def fetch_nodes(self, label, limit=None, where=None):
            base_query = f"MATCH (n:`{label}`)"
            if where:
                base_query += f" WHERE {where}"
            base_query += " RETURN n"
            if limit:
                base_query += f" LIMIT {limit}"
            try:
                records = self.driver.run(base_query).data()
            except Exception as e:
                raise RuntimeError(f"Neo4j-Fehler bei fetch_nodes({label}): {e}") from e
            result = []
            for r in records:
                n = r.get("n")
                if n is None:
                    continue
                node_dict = self._node_to_dict(n)
                node_dict["props"] = node_dict.get("props") or {}
                result.append(node_dict)
            return result

        def fetch_paths(self, labels, max_depth, limit=None, where=None, rel_filter=None):
            depth_str = f"*..{max_depth}" if max_depth >= 0 else "*"
            rel_match = f"[r:{'|'.join(rel_filter)}{depth_str}]" if rel_filter else f"[{depth_str}]"

            cypher = f"""
                MATCH p=(start)-{rel_match}->(end)
                WHERE ANY(n IN nodes(p) WHERE ANY(l IN labels(n) WHERE l IN $labels))
            """

            if where:
                # Filter nur auf relevante Labels anwenden
                label_conditions = []
                for lbl in labels:
                    label_conditions.append(f"ANY(n IN nodes(p) WHERE '{lbl}' IN labels(n) AND ({where}))")
                cypher += " AND (" + " OR ".join(label_conditions) + ")"

            cypher += " RETURN p"
            if limit:
                cypher += f" LIMIT {limit}"

            try:
                records = self.driver.run(cypher, labels=labels).data()
            except Exception as e:
                raise RuntimeError(f"Neo4j-Fehler bei fetch_paths: {e}") from e

            paths = []
            for r in records:
                p = r["p"]
                nodes = [self._node_to_dict(n) for n in p.nodes]
                rels = [
                    self._rel_to_dict(rel)
                    for rel in p.relationships
                    if not rel_filter or type(rel).__name__ in rel_filter
                ]
                paths.append({"nodes": nodes, "rels": rels})
            return paths

    graph_api = GraphAPI(graph)

    @bp.route("/get_data_as_table", methods=["GET"])
    def get_data_as_table():
        try:
            params = parse_request_params(request)
            buckets = build_buckets(graph_api, params)
            columns = extract_table_columns(buckets)
            rows = assemble_table_rows(buckets, columns)
            return jsonify({"columns": columns, "rows": rows})
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 500

    def build_buckets(graph_api, params):
        selected_labels, main_label, max_depth, limit, filter_labels, where, rel_filter = params
        paths = graph_api.fetch_paths(selected_labels, max_depth, limit, where, rel_filter)
        if paths:
            buckets = extract_nodes_from_paths(paths, main_label, selected_labels, filter_labels)
        else:
            buckets = {}
            for node in graph_api.fetch_nodes(main_label, limit, where):
                add_node_to_bucket(buckets, node, main_label)
        return ensure_all_labels_present(graph_api, buckets, selected_labels, filter_labels, limit, where)

    def ensure_all_labels_present(graph_api, buckets, selected_labels, filter_labels, limit, where):
        existing_labels = {lbl for bucket in buckets.values() for lbl in bucket.get("nodes", {})}
        required_labels = [lbl for lbl in selected_labels if not filter_labels or lbl in filter_labels]
        for lbl in [l for l in required_labels if l not in existing_labels]:
            for node in graph_api.fetch_nodes(lbl, limit, where):
                add_node_to_bucket(buckets, node, lbl)
        return buckets

    def assemble_table_rows(buckets, columns):
        """
        Build rows from buckets.

        Dynamic rules (no hardcoded label names):
        - By default produce a single row per bucket (per main node),
            picking for each label the node with minimal min_dist.
        - If some label L in the bucket has multiple nodes AND those
            L-nodes have distinct related nodes for other labels (i.e. each
            Bestellung links to its own Shipment), then expand rows
            by L (one row per L-node). To avoid explosion we choose
            a single pivot label to expand (the one with the most nodes).
        """
        rows = []

        for bucket in buckets.values():
            nodes_by_label = bucket.get("nodes", {})  # label -> {nodeid: {props, min_dist}}
            relations = bucket.get("relations", [])

            # detect labels that have >1 node
            candidate_multi = {label for label, nodes in nodes_by_label.items() if len(nodes) > 1}

            # determine whether a candidate label should actually trigger an expansion:
            # expansion is warranted if nodes of that label have differing mappings to nodes of other labels.
            def build_relation_index():
                # map from node id -> set of directly related node ids (from bucket['relations'])
                rel_index = {}
                for rel in relations:
                    a, b = rel.get("fromId"), rel.get("toId")
                    if a is not None:
                        rel_index.setdefault(a, set()).add(b)
                    if b is not None:
                        rel_index.setdefault(b, set()).add(a)
                return rel_index

            rel_index = build_relation_index()

            def related_nodes_of_type(node_id, target_label):
                """Return set of node ids of `target_label` that are related to node_id (and exist in bucket)."""
                related = rel_index.get(node_id, set())
                # intersect with nodes_by_label[target_label] keys
                targets = nodes_by_label.get(target_label, {})
                return set(nid for nid in related if nid in targets)

            expansion_candidates = set()
            for L in candidate_multi:
                # for label L, check whether L-nodes have distinct relations to any other label
                L_node_ids = list(nodes_by_label[L].keys())
                for other_label in nodes_by_label.keys():
                    if other_label == L:
                        continue
                    # build mapping Lnode -> set of related other_label nodes
                    mapping = [frozenset(related_nodes_of_type(nid, other_label)) for nid in L_node_ids]
                    # if mapping differs across L nodes and at least one mapping is non-empty, expand
                    if any(m for m in mapping) and (len(set(mapping)) > 1):
                        expansion_candidates.add(L)
                        break  # no need to check other labels for this L

            if expansion_candidates:
                # pick a single pivot to expand by (the label with the most nodes) to keep things bounded
                pivot_label = max(expansion_candidates, key=lambda l: len(nodes_by_label.get(l, {})))
                pivot_items = list(nodes_by_label.get(pivot_label, {}).items())  # [(nodeid, data), ...]

                # for each pivot node, create one row and try to attach related nodes for other labels
                for pivot_node_id, pivot_node_data in pivot_items:
                    cells = []
                    for col in columns:
                        label = col["nodeType"]
                        prop = col["property"]
                        node_map = nodes_by_label.get(label, {})

                        if not node_map:
                            cells.append({"value": None, "nodeId": None, "nodeType": label})
                            continue

                        if label == pivot_label:
                            # use the pivot node
                            node_id = pivot_node_id
                            node_data = pivot_node_data
                        else:
                            # first try to find nodes of this label directly related to the pivot node
                            related = related_nodes_of_type(pivot_node_id, label)
                            if related:
                                # if multiple related, choose the one with minimal min_dist
                                best_id = min(related, key=lambda nid: node_map[nid].get("min_dist", 1e9))
                                node_id = best_id
                                node_data = node_map[node_id]
                            else:
                                # fallback: choose the best node by min_dist (conservative)
                                node_id, node_data = min(node_map.items(), key=lambda x: x[1].get("min_dist", 1e9))

                        value = node_data.get("props", {}).get(prop) if prop is not None else None
                        cells.append({"value": value, "nodeId": node_id, "nodeType": label})

                    rows.append({"cells": cells, "relations": relations})
            else:
                # no expansion needed: single row per bucket, pick best node per label by min_dist
                cells = []
                for col in columns:
                    label = col["nodeType"]
                    prop = col["property"]
                    node_map = nodes_by_label.get(label, {})
                    if not node_map:
                        cells.append({"value": None, "nodeId": None, "nodeType": label})
                        continue
                    node_id, node_data = min(node_map.items(), key=lambda x: x[1].get("min_dist", 1e9))
                    value = node_data.get("props", {}).get(prop) if prop is not None else None
                    cells.append({"value": value, "nodeId": node_id, "nodeType": label})
                rows.append({"cells": cells, "relations": relations})

        return rows

    def extract_table_columns(buckets):
        if not buckets:
            return []
        label_property_pairs = {
            (label, prop)
            for bucket in buckets.values()
            for label, node_map in bucket.get("nodes", {}).items()
            for node_data in node_map.values()
            for prop in node_data.get("props", {})
        }
        if not label_property_pairs:
            return []
        return [
            {"nodeType": label, "property": prop}
            for label, prop in sorted(label_property_pairs, key=lambda x: (x[0], str(x[1])))
        ]

    def qb_to_cypher(qb):
        if not qb.get("valid"):
            return None

        def parse_rule(rule):
            field = rule["field"]
            op = rule["operator"]
            value = rule.get("value")
            field_name = field.split('.')[-1]

            # Helper für string comparison case-insensitive
            def ci_value(v):
                if isinstance(v, str):
                    return f"'{v.lower()}'"

                return str(v)

            if op in {"equal", "not_equal", "less", "less_or_equal", "greater", "greater_or_equal"}:
                op_map = {
                    "equal": "=",
                    "not_equal": "<>",
                    "less": "<",
                    "less_or_equal": "<=",
                    "greater": ">",
                    "greater_or_equal": ">="
                }
                cypher_op = op_map[op]
                # nur Strings case-insensitive machen
                if isinstance(value, str):
                    return f"TOLOWER(n.`{field_name}`) {cypher_op} {ci_value(value)}"

                return f"n.`{field_name}` {cypher_op} {ci_value(value)}"

            elif op == "contains":
                return f"TOLOWER(n.`{field_name}`) CONTAINS '{value.lower()}'"
            elif op == "begins_with":
                return f"TOLOWER(n.`{field_name}`) STARTS WITH '{value.lower()}'"
            elif op == "ends_with":
                return f"TOLOWER(n.`{field_name}`) ENDS WITH '{value.lower()}'"

            elif op == "not_contains":
                return f"NOT TOLOWER(n.`{field_name}`) CONTAINS '{value.lower()}'"
            elif op == "not_begins_with":
                return f"NOT TOLOWER(n.`{field_name}`) STARTS WITH '{value.lower()}'"
            elif op == "not_ends_with":
                return f"NOT TOLOWER(n.`{field_name}`) ENDS WITH '{value.lower()}'"

            elif op == "in":
                if not isinstance(value, (list, tuple)):
                    raise ValueError("Operator 'in' requires a list of values")
                value_list = ', '.join(f"'{v.lower()}'" for v in value)
                return f"TOLOWER(n.`{field_name}`) IN [{value_list}]"
            elif op == "not_in":
                if not isinstance(value, (list, tuple)):
                    raise ValueError("Operator 'not_in' requires a list of values")
                value_list = ', '.join(f"'{v.lower()}'" for v in value)
                return f"NOT TOLOWER(n.`{field_name}`) IN [{value_list}]"

            elif op == "is_empty":
                return f"n.`{field_name}` = ''"
            elif op == "is_not_empty":
                return f"n.`{field_name}` <> ''"

            elif op == "is_null":
                return f"n.`{field_name}` IS NULL"
            elif op == "is_not_null":
                return f"n.`{field_name}` IS NOT NULL"

            raise ValueError(f"Unsupported operator: {op}")

        if "rules" in qb and qb["rules"]:
            parsed_rules = [parse_rule(r) for r in qb["rules"]]
            condition = qb.get("condition", "AND").upper()
            return f" {condition} ".join(parsed_rules)

        return None

    def parse_request_params(req):
        nodes_param = req.args.get("nodes")
        if not nodes_param:
            raise ValueError("Parameter 'nodes' required")
        selected_labels = [n.strip() for n in nodes_param.split(",") if n.strip()]
        main_label = selected_labels[0]

        max_depth = int(req.args.get("maxDepth", max(3, len(selected_labels))))
        limit_raw = req.args.get("limit")
        limit = int(limit_raw) if limit_raw else None

        filter_labels_raw = req.args.get("filterLabels")
        filter_labels = [l.strip() for l in filter_labels_raw.split(",")] if filter_labels_raw else None

        relationships_raw = req.args.get("relationships")
        rel_filter = [r.strip() for r in relationships_raw.split(",")] if relationships_raw else None

        qb_raw = req.args.get("qb")
        where = None
        if qb_raw and qb_raw.lower() != "null":
            qb_json = json.loads(qb_raw)
            if qb_json:  # prüfen, dass es nicht None ist
                where = qb_to_cypher(qb_json)

        # allow manual where override
        manual_where = req.args.get("where")
        if manual_where:
            where = manual_where

        return selected_labels, main_label, max_depth, limit, filter_labels, where, rel_filter

    def extract_nodes_from_paths(paths, main_label, selected_labels, filter_labels=None):
        buckets = {}
        for p in paths:
            nodes, rels = p["nodes"], p["rels"]
            for idx, node in enumerate(nodes):
                if main_label in node["labels"]:
                    bucket = buckets.setdefault(node["id"], {"nodes": {}, "adjacent": set(), "relations": []})
                    add_path_nodes_to_bucket(bucket, nodes, idx, selected_labels, filter_labels)
                    add_path_relations_to_bucket(bucket, node["id"], rels)
        return buckets

    def add_path_nodes_to_bucket(bucket, node_list, main_index, selected_labels, filter_labels=None):
        for idx, node in enumerate(node_list):
            node_id = node["id"]
            if node_id is None:
                continue
            labels = [l for l in node["labels"] if l in selected_labels]
            labels = [l for l in labels if not filter_labels or l in filter_labels]
            for label in labels:
                store_or_update_node(bucket.setdefault("nodes", {}), node_id, label, node["props"], abs(idx - main_index))

    def add_path_relations_to_bucket(bucket, main_id, relationships):
        for rel in relationships:
            from_id, to_id = rel["fromId"], rel["toId"]
            if from_id is None or to_id is None:
                continue
            add_relation(bucket, main_id, from_id, to_id, rel["type"])

    def add_relation(bucket, main_id, from_id, to_id, relation_name):
        rel = {"fromId": from_id, "toId": to_id, "relation": relation_name}
        if rel not in bucket.get("relations", []):
            bucket["relations"].append(rel)
        if from_id == main_id:
            bucket["adjacent"].add(to_id)
        if to_id == main_id:
            bucket["adjacent"].add(from_id)

    def build_cells_for_bucket(bucket, columns):
        return [build_cell(bucket, col) for col in columns]

    def build_cell(bucket, col):
        node_candidates = bucket.get("nodes", {}).get(col["nodeType"], {})
        chosen = select_best_node(node_candidates, bucket.get("adjacent", set()))
        value = None
        if chosen:
            props = chosen[1].get("props", {})
            if col["property"] is not None:
                value = props.get(col["property"])
        return {"value": value, "nodeId": chosen[0] if chosen else None}

    def select_best_node(nodes_map, adjacent_nodes):
        if not nodes_map:
            return None
        # Alle Nodes verwenden, Adjacent optional
        candidates = {nid: data for nid, data in nodes_map.items() if nid in adjacent_nodes} or nodes_map
        return min(candidates.items(), key=lambda x: x[1].get("min_dist", 1e9))

    def store_or_update_node(bucket_nodes, node_id, label, props, distance):
        node_map = bucket_nodes.setdefault(label, {})
        existing = node_map.get(node_id)
        if not existing or distance < existing.get("min_dist", 1e9):
            node_map[node_id] = {"props": props, "min_dist": distance}

    def add_node_to_bucket(buckets, node, label):
        node_id = node["id"]
        if node_id is None:
            return
        bucket = buckets.setdefault(node_id, {"nodes": {}, "adjacent": set(), "relations": []})
        # Node korrekt speichern
        bucket["nodes"].setdefault(label, {})[node_id] = {"props": node.get("props", {}), "min_dist": 0}

    return bp
