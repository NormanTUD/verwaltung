from flask import Blueprint, request, jsonify


def create_get_data_bp(graph):
    bp = Blueprint("get_data_bp", __name__)

    @bp.route("/get_data_as_table", methods=["GET"])
    def get_data_as_table():
        try:
            log_start()
            params = parse_request_params(request)
            results = run_cypher_paths(graph, params[0], params[2], params[3])
            main_nodes = handle_results(graph, results, params)
            columns = determine_columns(main_nodes)
            rows = build_rows(main_nodes, columns)
            return jsonify({"columns": columns, "rows": rows})
        except Exception as e:
            return handle_error(e)

    # === Helper Functions (Tiny, <= 5 lines each) ===

    def log_start():
        print("\n=== API get_data_as_table gestartet ===")

    def handle_error(e):
        print(f"!!! Fehler aufgetreten: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

    def handle_results(graph, results, params):
        selected_labels, main_label, max_depth, limit, filter_labels = params
        if results:
            nodes = process_paths(results, main_label, selected_labels, filter_labels)
        else:
            nodes = {}
            collect_single_nodes(graph, nodes, main_label, limit)
        return ensure_missing_labels(graph, nodes, selected_labels, filter_labels, limit)

    def ensure_missing_labels(graph, main_nodes, selected, filters, limit):
        existing = {lbl for b in main_nodes.values() for lbl in b.get("nodes", {})}
        candidates = [lbl for lbl in selected if (not filters or lbl in filters)]
        for lbl in [l for l in candidates if l not in existing]:
            collect_single_nodes(graph, main_nodes, lbl, limit)
        return main_nodes

    def build_rows(main_nodes, columns):
        return [{"cells": build_cells_for_bucket(b, columns), "relations": b.get("relations", [])} for b in main_nodes.values()]

    def determine_columns(main_nodes):
        pairs = {(l, p) for b in main_nodes.values() for l, m in b.get("nodes", {}).items() for i in m.values() for p in i.get("props", {})}
        return [{"nodeType": l, "property": p} for l, p in sorted(pairs)]

    def parse_request_params(req):
        nodes = req.args.get("nodes")
        if not nodes:
            raise ValueError("Parameter 'nodes' erforderlich")
        selected = [n.strip() for n in nodes.split(",") if n.strip()]
        main_label = selected[0]
        max_depth = int(req.args.get("maxDepth", 3))
        limit = int(req.args["limit"]) if req.args.get("limit") else None
        filters = [l.strip() for l in req.args["filterLabels"].split(",")] if req.args.get("filterLabels") else None
        return selected, main_label, max_depth, limit, filters

    def collect_single_nodes(graph, main_nodes, label, limit=None):
        query = f"MATCH (n:`{label}`) RETURN n" + (f" LIMIT {limit}" if limit else "")
        for r in graph.run(query).data():
            add_single_node(main_nodes, label, r.get("n"))

    def add_single_node(main_nodes, label, n):
        if n is None:
            return
        mid = getattr(n, "identity", None)
        if mid is None:   # <-- entscheidend: nicht `if not mid`
            return
        b = main_nodes.setdefault(mid, {"nodes": {}, "adjacent": set(), "relations": []})
        b["nodes"].setdefault(label, {})[mid] = {"props": dict(n), "min_dist": 0}

    def process_paths(results, main_label, selected_labels, filter_labels=None):
        nodes = {}
        for r in results:
            path = r["p"]
            for i, n in enumerate(path.nodes):
                if main_label in n.labels:
                    b = nodes.setdefault(n.identity, {"nodes": {}, "adjacent": set(), "relations": []})
                    collect_nodes_for_bucket(b, path.nodes, i, selected_labels, filter_labels)
                    collect_relations_for_bucket(b, n.identity, path.relationships)
        return nodes

    def run_cypher_paths(graph, selected_labels, max_depth, limit=None):
        q = f"""
        MATCH p=(start)-[*..{max_depth}]->(end)
        WHERE ANY(n IN nodes(p) WHERE ANY(l IN labels(n) WHERE l IN $labels))
        RETURN p
        """ + (f" LIMIT {limit}" if limit else "")
        return graph.run(q, labels=selected_labels).data()

    def collect_nodes_for_bucket(bucket, node_list, main_index, selected_labels, filter_labels=None):
        for idx, node in enumerate(node_list):
            node_id = fn_get_node_id(node)
            if not node_id:
                continue
            labels = fn_filter_labels(node, selected_labels, filter_labels)
            for l in labels:
                fn_store_or_update_node(bucket.setdefault("nodes", {}), node_id, l, dict(node), abs(idx - main_index))

    def collect_relations_for_bucket(bucket, main_id, relationships):
        for rel in relationships:
            f, t = getattr(rel.start_node, "identity", None), getattr(rel.end_node, "identity", None)
            if not f or not t:
                continue
            add_relation(bucket, main_id, f, t, type(rel).__name__)

    def add_relation(bucket, main_id, f, t, name):
        rel = {"fromId": f, "toId": t, "relation": name}
        if rel not in bucket.get("relations", []):
            bucket["relations"].append(rel)
        if f == main_id:
            bucket["adjacent"].add(t)
        if t == main_id:
            bucket["adjacent"].add(f)

    def build_cells_for_bucket(bucket, columns):
        return [pick_cell(bucket, c) for c in columns]

    def pick_cell(bucket, col):
        nodes = bucket.get("nodes", {}).get(col["nodeType"], {})
        chosen = pick_node(nodes, bucket.get("adjacent", set()))
        val = chosen[1].get("props", {}).get(col["property"]) if chosen else None
        return {"value": val, "nodeId": chosen[0] if chosen else None}

    def pick_node(nodes_map, adj):
        if not nodes_map:
            return None
        candidates = {nid: i for nid, i in nodes_map.items() if nid in adj}
        return min(candidates.items() if candidates else nodes_map.items(), key=lambda x: x[1].get("min_dist", 1e9))

    def fn_store_or_update_node(bucket_nodes, node_id, label, props, dist):
        m = bucket_nodes.setdefault(label, {})
        e = m.get(node_id)
        if not e or dist < e.get("min_dist", 1e9):
            m[node_id] = {"props": props, "min_dist": dist}

    def fn_filter_labels(node, selected, filters=None):
        labs = [l for l in getattr(node, "labels", []) if l in selected]
        return [l for l in labs if not filters or l in filters]

    def fn_get_node_id(node):
        return getattr(node, "identity", None)

    return bp
