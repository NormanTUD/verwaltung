from flask import Blueprint, request, jsonify

def create_get_data_bp(graph):
    bp = Blueprint("get_data_bp", __name__)

    class GraphAPI:
        def __init__(self, driver):
            self.driver = driver

        def fetch_nodes(self, label, limit=None, where=None):
            where_clause = f"WHERE {where}" if where else ""
            query = f"MATCH (n:`{label}`) {where_clause} RETURN n" + (f" LIMIT {limit}" if limit else "")
            try:
                records = self.driver.run(query).data()
            except Exception as e:
                raise RuntimeError(f"Neo4j-Fehler bei fetch_nodes({label}): {e}")
            result = []
            for r in records:
                n = r.get("n")
                if n is None:
                    continue
                node_dict = self._node_to_dict(n)
                if node_dict["props"] is None:
                    node_dict["props"] = {}
                result.append(node_dict)
            return result

        def fetch_paths(self, labels, max_depth, limit=None, where=None, rel_filter=None):
            if max_depth < 0:
                depth_str = "*"
            else:
                depth_str = f"*..{max_depth}"

            rel_match = "" if not rel_filter else f"[r:{'|'.join(rel_filter)}{depth_str}]"
            normal_match = f"[{depth_str}]"

            cypher = f"""
            MATCH p=(start)-{rel_match if rel_filter else normal_match}->(end)
            WHERE ANY(n IN nodes(p) WHERE ANY(l IN labels(n) WHERE l IN $labels))
            """
            if where:
                cypher += f" AND {where}"
            cypher += f" RETURN p" + (f" LIMIT {limit}" if limit else "")

            try:
                records = self.driver.run(cypher, labels=labels).data()
            except Exception as e:
                raise RuntimeError(f"Neo4j-Fehler bei fetch_paths: {e}")

            paths = []
            for r in records:
                p = r["p"]
                nodes = [self._node_to_dict(n) for n in p.nodes]
                rels = [self._rel_to_dict(rel) for rel in p.relationships if not rel_filter or type(rel).__name__ in rel_filter]
                paths.append({"nodes": nodes, "rels": rels})
            return paths

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
        return [
            {"cells": build_cells_for_bucket(bucket, columns), "relations": bucket.get("relations", [])}
            for bucket in buckets.values()
        ]

    def extract_table_columns(buckets, selected_labels=None):
        label_property_pairs = {
            (label, prop)
            for bucket in buckets.values()
            for label, node_map in bucket.get("nodes", {}).items()
            for node_data in node_map.values()
            for prop in node_data.get("props", {})
        }
        if not label_property_pairs and selected_labels:
            label_property_pairs = {(label, None) for label in selected_labels}
        return [
            {"nodeType": label, "property": prop}
            for label, prop in sorted(label_property_pairs, key=lambda x: (x[0], str(x[1])))
        ]

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
        where = req.args.get("where")  # optional
        rel_filter = [r.strip() for r in req.args["relationships"].split(",")] if req.args.get("relationships") else None
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
            if not node_id:
                continue
            labels = [l for l in node["labels"] if l in selected_labels]
            labels = [l for l in labels if not filter_labels or l in filter_labels]
            for label in labels:
                store_or_update_node(bucket.setdefault("nodes", {}), node_id, label, node["props"], abs(idx - main_index))

    def add_path_relations_to_bucket(bucket, main_id, relationships):
        for rel in relationships:
            from_id, to_id = rel["fromId"], rel["toId"]
            if not from_id or not to_id:
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
        candidates = {nid: data for nid, data in nodes_map.items() if nid in adjacent_nodes}
        return min(
            candidates.items() if candidates else nodes_map.items(),
            key=lambda x: x[1].get("min_dist", 1e9),
        )

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
        bucket["nodes"].setdefault(label, {})[node_id] = {"props": node["props"], "min_dist": 0}

    return bp
