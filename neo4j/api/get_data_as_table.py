from flask import Blueprint, request, jsonify


def create_get_data_bp(graph):
    bp = Blueprint("get_data_bp", __name__)

    @bp.route("/get_data_as_table", methods=["GET"])
    def get_data_as_table():
        try:
            log_api_start()
            params = parse_request_params(request)
            path_results = query_paths_from_graph(graph, params[0], params[2], params[3])
            buckets = build_node_buckets(graph, path_results, params)
            columns = extract_table_columns(buckets)
            rows = assemble_table_rows(buckets, columns)
            return jsonify({"columns": columns, "rows": rows})
        except Exception as e:
            return handle_api_error(e)

    # === Logging & Fehlerbehandlung ===

    def log_api_start():
        print("\n=== API get_data_as_table gestartet ===")

    def handle_api_error(e):
        print(f"!!! Fehler aufgetreten: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

    # === Hauptlogik ===

    def build_node_buckets(graph, results, params):
        selected_labels, main_label, max_depth, limit, filter_labels = params
        if results:
            buckets = extract_nodes_from_paths(results, main_label, selected_labels, filter_labels)
        else:
            buckets = {}
            fetch_single_nodes(graph, buckets, main_label, limit)
        return ensure_all_labels_present(graph, buckets, selected_labels, filter_labels, limit)

    def ensure_all_labels_present(graph, buckets, selected_labels, filter_labels, limit):
        existing_labels = {lbl for bucket in buckets.values() for lbl in bucket.get("nodes", {})}
        required_labels = [lbl for lbl in selected_labels if not filter_labels or lbl in filter_labels]
        for lbl in [l for l in required_labels if l not in existing_labels]:
            fetch_single_nodes(graph, buckets, lbl, limit)
        return buckets

    # === Tabellendaten bauen ===

    def assemble_table_rows(buckets, columns):
        return [
            {"cells": build_cells_for_bucket(bucket, columns), "relations": bucket.get("relations", [])}
            for bucket in buckets.values()
        ]

    def extract_table_columns(buckets):
        label_property_pairs = {
            (label, prop)
            for bucket in buckets.values()
            for label, node_map in bucket.get("nodes", {}).items()
            for node_data in node_map.values()
            for prop in node_data.get("props", {})
        }
        return [{"nodeType": label, "property": prop} for label, prop in sorted(label_property_pairs)]

    # === Request-Parameter parsen ===

    def parse_request_params(req):
        nodes_param = req.args.get("nodes")
        if not nodes_param:
            raise ValueError("Parameter 'nodes' erforderlich")
        selected_labels = [n.strip() for n in nodes_param.split(",") if n.strip()]
        main_label = selected_labels[0]
        max_depth = int(req.args.get("maxDepth", 3))
        limit = int(req.args["limit"]) if req.args.get("limit") else None
        filter_labels = (
            [l.strip() for l in req.args["filterLabels"].split(",")] if req.args.get("filterLabels") else None
        )
        return selected_labels, main_label, max_depth, limit, filter_labels

    # === Neo4j-Abfragen ===

    def fetch_single_nodes(graph, buckets, label, limit=None):
        query = f"MATCH (n:`{label}`) RETURN n" + (f" LIMIT {limit}" if limit else "")
        for record in graph.run(query).data():
            add_node_to_bucket(buckets, label, record.get("n"))

    def add_node_to_bucket(buckets, label, node):
        if node is None:
            return
        node_id = getattr(node, "identity", None)
        if node_id is None:  # explizit, nicht "if not node_id"
            return
        bucket = buckets.setdefault(node_id, {"nodes": {}, "adjacent": set(), "relations": []})
        bucket["nodes"].setdefault(label, {})[node_id] = {"props": dict(node), "min_dist": 0}

    def query_paths_from_graph(graph, selected_labels, max_depth, limit=None):
        cypher = f"""
        MATCH p=(start)-[*..{max_depth}]->(end)
        WHERE ANY(n IN nodes(p) WHERE ANY(l IN labels(n) WHERE l IN $labels))
        RETURN p
        """ + (f" LIMIT {limit}" if limit else "")
        return graph.run(cypher, labels=selected_labels).data()

    # === Pfad-Verarbeitung ===

    def extract_nodes_from_paths(results, main_label, selected_labels, filter_labels=None):
        buckets = {}
        for record in results:
            path = record["p"]
            for idx, node in enumerate(path.nodes):
                if main_label in node.labels:
                    bucket = buckets.setdefault(node.identity, {"nodes": {}, "adjacent": set(), "relations": []})
                    add_path_nodes_to_bucket(bucket, path.nodes, idx, selected_labels, filter_labels)
                    add_path_relations_to_bucket(bucket, node.identity, path.relationships)
        return buckets

    def add_path_nodes_to_bucket(bucket, node_list, main_index, selected_labels, filter_labels=None):
        for idx, node in enumerate(node_list):
            node_id = get_node_id(node)
            if not node_id:
                continue
            labels = filter_labels_for_node(node, selected_labels, filter_labels)
            for label in labels:
                store_or_update_node(bucket.setdefault("nodes", {}), node_id, label, dict(node), abs(idx - main_index))

    def add_path_relations_to_bucket(bucket, main_id, relationships):
        for rel in relationships:
            from_id, to_id = getattr(rel.start_node, "identity", None), getattr(rel.end_node, "identity", None)
            if not from_id or not to_id:
                continue
            add_relation(bucket, main_id, from_id, to_id, type(rel).__name__)

    def add_relation(bucket, main_id, from_id, to_id, relation_name):
        rel = {"fromId": from_id, "toId": to_id, "relation": relation_name}
        if rel not in bucket.get("relations", []):
            bucket["relations"].append(rel)
        if from_id == main_id:
            bucket["adjacent"].add(to_id)
        if to_id == main_id:
            bucket["adjacent"].add(from_id)

    # === Zellen für Tabelle ===

    def build_cells_for_bucket(bucket, columns):
        return [build_cell(bucket, col) for col in columns]

    def build_cell(bucket, col):
        node_candidates = bucket.get("nodes", {}).get(col["nodeType"], {})
        chosen = select_best_node(node_candidates, bucket.get("adjacent", set()))
        value = chosen[1].get("props", {}).get(col["property"]) if chosen else None
        return {"value": value, "nodeId": chosen[0] if chosen else None}

    def select_best_node(nodes_map, adjacent_nodes):
        if not nodes_map:
            return None
        candidates = {nid: data for nid, data in nodes_map.items() if nid in adjacent_nodes}
        return min(
            candidates.items() if candidates else nodes_map.items(),
            key=lambda x: x[1].get("min_dist", 1e9),
        )

    # === Hilfsfunktionen ===

    def store_or_update_node(bucket_nodes, node_id, label, props, distance):
        node_map = bucket_nodes.setdefault(label, {})
        existing = node_map.get(node_id)
        if not existing or distance < existing.get("min_dist", 1e9):
            node_map[node_id] = {"props": props, "min_dist": distance}

    def filter_labels_for_node(node, selected_labels, filter_labels=None):
        labels = [l for l in getattr(node, "labels", []) if l in selected_labels]
        return [l for l in labels if not filter_labels or l in filter_labels]

    def get_node_id(node):
        return getattr(node, "identity", None)

    return bp
