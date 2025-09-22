from flask import Blueprint, request, jsonify

get_data_bp = Blueprint('get_data_bp', __name__)

def create_get_data_bp(graph):
    bp = Blueprint('get_data_bp', __name__)

    @bp.route('/get_data_as_table', methods=['GET'])
    def get_data_as_table():
        try:
            print("\n=== API get_data_as_table gestartet ===")

            print("-> Parsing Request-Parameter ...")
            selected_labels, main_label, max_depth, limit, filter_labels = parse_request_params(request)
            print(f"   -> Selected labels: {selected_labels}")
            print(f"   -> Main label (Pivot): {main_label}")
            print(f"   -> Max depth: {max_depth}")
            print(f"   -> Limit: {limit}")
            print(f"   -> Filter labels: {filter_labels}")

            print("-> Starte Cypher-Abfrage für Pfade ...")
            results = run_cypher_paths(graph, selected_labels, max_depth, limit)
            print(f"   -> Anzahl Pfade erhalten: {len(results)}")

            main_nodes = {}
            if results:
                main_nodes = process_paths(results, main_label, selected_labels, filter_labels)
            else:
                collect_single_nodes(graph, main_nodes, main_label, limit)

            existing_labels = {lbl for bucket in main_nodes.values() for lbl in bucket.get("nodes", {}).keys()}

            candidate_labels = [lbl for lbl in selected_labels if (not filter_labels or lbl in filter_labels)]
            missing_labels = [lbl for lbl in candidate_labels if lbl not in existing_labels]

            for lbl in missing_labels:
                collect_single_nodes(graph, main_nodes, lbl, limit)

            print("-> Bestimme Spalten (Columns) basierend auf gesammelten Nodes ...")
            columns = determine_columns(main_nodes)
            print(f"   -> Columns (sortiert): {columns}")

            print("-> Baue Rows für die Ausgabe ...")
            rows = build_rows(main_nodes, columns)
            print(f"   -> Anzahl Rows erstellt: {len(rows)}")

            print("-> JSON-Antwort wird erstellt und zurückgegeben ...")
            return jsonify({"columns": columns, "rows": rows})

        except Exception as e:
            print(f"!!! Fehler aufgetreten: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500

    def build_rows(main_nodes, columns):
        rows = []
        for main_id, bucket in main_nodes.items():
            cells = build_cells_for_bucket(bucket, columns)
            rows.append({"cells": cells, "relations": bucket.get("relations", [])})
        return rows

    def determine_columns(main_nodes):
        columns_set = set()
        for bucket in main_nodes.values():
            for label, nodes_map in bucket.get("nodes", {}).items():
                for info in nodes_map.values():
                    for prop in info.get("props", {}):
                        columns_set.add((label, prop))
        return [{"nodeType": lbl, "property": prop} for lbl, prop in sorted(columns_set, key=lambda x: (x[0], x[1]))]

    def parse_request_params(request):
        node_csv = request.args.get('nodes')
        if not node_csv:
            raise ValueError("Parameter 'nodes' erforderlich")
        selected_labels = [n.strip() for n in node_csv.split(',') if n.strip()]
        if not selected_labels:
            raise ValueError("No labels parsed")
        main_label = selected_labels[0]

        max_depth = int(request.args.get('maxDepth', 3))
        limit = request.args.get('limit')
        limit = int(limit) if limit else None

        filter_labels_csv = request.args.get('filterLabels')
        filter_labels = [_l.strip() for _l in filter_labels_csv.split(',')] if filter_labels_csv else None

        return selected_labels, main_label, max_depth, limit, filter_labels

    def collect_single_nodes(graph, main_nodes, label, limit=None):
        try:
            print(f"Hole einzelne Nodes für Label: {label}")
            cypher_nodes = f"MATCH (n:`{label}`) RETURN n"
            if limit:
                cypher_nodes += f" LIMIT {limit}"
            node_results = graph.run(cypher_nodes).data()
            print(f"Einzelne Nodes erhalten: {len(node_results)}")

            for r in node_results:
                n = r.get('n')
                if n is None:
                    continue

                main_id = getattr(n, "identity", None)
                if main_id is None:
                    continue

                if main_id not in main_nodes:
                    # Neuer Bucket für jeden Node, auch wenn kein Pivot existiert
                    main_nodes[main_id] = {"nodes": {}, "adjacent": set(), "relations": []}

                bucket = main_nodes[main_id]
                props = dict(n)
                label_map = bucket.setdefault("nodes", {}).setdefault(label, {})
                label_map[main_id] = {"props": props, "min_dist": 0}
                print(f"  -> Einzelnode gespeichert: {main_id} mit props {list(props.keys())}")

        except Exception as e:
            print(f"Fehler beim Sammeln einzelner Nodes: {e}")

    def process_paths(results, main_label, selected_labels, filter_labels=None):
        """
        Verarbeitet alle Pfade, sammelt Nodes und Relations pro main_node.

        Args:
            results (list): Liste der Pfad-Ergebnisse, jedes Element enthält 'p' als Path-Objekt.
            main_label (str): Label, das als Hauptknoten identifiziert wird.
            selected_labels (list/set): Labels, die für Nodes berücksichtigt werden.
            filter_labels (list/set, optional): Optional weitere Filterlabels für Nodes.

        Returns:
            dict: main_nodes Dictionary mit gesammelten Nodes, Adjacent-IDs und Relations.
        """
        main_nodes = {}

        for path_idx, r in enumerate(results):
            path = r['p']
            print(f"\n--- Verarbeitung Pfad {path_idx+1}/{len(results)} ---")
            node_list = list(path.nodes)
            print("Pfad-Nodes (id: labels):", [(n.identity, list(n.labels)) for n in node_list])

            # Finde main nodes in diesem Pfad
            main_positions = [(i, n) for i, n in enumerate(node_list) if main_label in n.labels]
            if not main_positions:
                print("  Kein main_label in diesem Pfad gefunden -> überspringe")
                continue
            print("  Main positions in path:", [(i, n.identity) for i, n in main_positions])

            for main_index, main_node in main_positions:
                main_id = main_node.identity
                if main_id not in main_nodes:
                    main_nodes[main_id] = {"nodes": {}, "adjacent": set(), "relations": []}
                    print(f"  Neuer main_node bucket: {main_id}")

                bucket = main_nodes[main_id]

                # Nodes sammeln
                collect_nodes_for_bucket(bucket, node_list, main_index, selected_labels, filter_labels)

                # Relations sammeln
                collect_relations_for_bucket(bucket, main_id, path.relationships)

        return main_nodes

    def run_cypher_paths(graph, selected_labels, max_depth, limit=None):
        cypher_query = f"""
        MATCH p=(start)-[*..{max_depth}]->(end)
        WHERE ANY(n IN nodes(p) WHERE ANY(l IN labels(n) WHERE l IN $labels))
        RETURN p
        """
        if limit:
            cypher_query += f" LIMIT {limit}"
        print("Cypher Query:", cypher_query.strip())
        return graph.run(cypher_query, labels=selected_labels).data()

    def collect_nodes_for_bucket(bucket, node_list, main_index, selected_labels, filter_labels=None):
        """
        Sammelt Nodes in einem Bucket und aktualisiert min_dist, falls nötig.

        Args:
            bucket (dict): Bucket mit bucket["nodes"] als dict.
            node_list (iterable): Liste der Nodes (z.B. aus Neo4j).
            main_index (int): Index des Hauptknotens.
            selected_labels (set/list): Labels, die berücksichtigt werden sollen.
            filter_labels (set/list, optional): Zusätzliche Filterlabels.
        """
        try:
            bucket_nodes = bucket.setdefault("nodes", {})
            fn_debug_collect("Starting bucket collection", {"main_index": main_index, "selected_labels": selected_labels, "filter_labels": filter_labels})

            for idx, node in enumerate(node_list):
                fn_debug_collect("Processing node index", idx)

                node_id = fn_get_node_id(node)
                if node_id is None:
                    fn_debug_collect("Skipping invalid node", node)
                    continue

                node_labels = fn_filter_labels(node, selected_labels, filter_labels)
                if not node_labels:
                    fn_debug_collect("No matching labels, skipping node", node_id)
                    continue

                dist = fn_calculate_distance(idx, main_index)
                props = dict(node)  # Annahme: Node ist dict-like
                fn_debug_collect("Node props", props)

                for label in node_labels:
                    fn_store_or_update_node(bucket_nodes, node_id, label, props, dist)

            fn_debug_collect("Finished bucket collection", bucket_nodes)

        except Exception as e:
            print(f"Fehler beim Sammeln von Nodes für bucket: {e}")

    def collect_relations_for_bucket(bucket, main_id, relationships):
        """
        Sammelt die Relations und Adjacent-Nodes für einen Bucket.

        Args:
            bucket (dict): Ein einzelner Bucket, enthält 'relations' (list) und 'adjacent' (set).
            main_id (int/str): Die ID des Hauptknotens.
            relationships (iterable): Iterable von Relationship-Objekten (z.B. path.relationships).

        Modifiziert:
            bucket["relations"]: Fügt neue Relations hinzu, falls sie noch nicht existieren.
            bucket["adjacent"]: Fügt IDs von benachbarten Nodes hinzu.
        """
        try:
            for rel in relationships:
                from_id = getattr(rel.start_node, 'identity', None)
                to_id = getattr(rel.end_node, 'identity', None)
                if from_id is None or to_id is None:
                    continue  # Ungültige Relation überspringen

                rel_dict = {"fromId": from_id, "toId": to_id, "relation": type(rel).__name__}

                if rel_dict not in bucket.get("relations", []):
                    bucket.setdefault("relations", []).append(rel_dict)

                bucket.setdefault("adjacent", set())
                if from_id == main_id:
                    bucket["adjacent"].add(to_id)
                if to_id == main_id:
                    bucket["adjacent"].add(from_id)

        except Exception as e:
            print(f"Fehler beim Sammeln der Relations für bucket {main_id}: {e}")

    def build_cells_for_bucket(bucket, columns):
        """
        Erzeugt die Zellen für einen einzelnen Bucket.

        Args:
            bucket (dict): Ein einzelner Bucket aus main_nodes.
            columns (list): Liste der Spaltendefinitionen, jede mit "nodeType" und "property".

        Returns:
            list: Liste von Zellen im Format {"value": ..., "nodeId": ...}
        """
        cells = []
        try:
            for col in columns:
                label = col.get("nodeType")
                prop = col.get("property")
                nodes_map = bucket.get("nodes", {}).get(label, {})
                chosen_node_id = None
                chosen_info = None

                if nodes_map:
                    adjacent_candidates = {nid: info for nid, info in nodes_map.items() if nid in bucket.get("adjacent", {})}
                    if adjacent_candidates:
                        chosen_node_id, chosen_info = min(
                            adjacent_candidates.items(), key=lambda x: x[1].get("min_dist", float('inf'))
                        )
                    else:
                        chosen_node_id, chosen_info = min(
                            nodes_map.items(), key=lambda x: x[1].get("min_dist", float('inf'))
                        )

                if chosen_info:
                    val = chosen_info.get("props", {}).get(prop)
                    cells.append({"value": val if val is not None else None, "nodeId": chosen_node_id})
                else:
                    cells.append({"value": None, "nodeId": None})

        except Exception as e:
            print(f"Fehler beim Erstellen der Zellen für bucket: {e}")
            # Optional: fallback, leere Zellen erzeugen
            cells = [{"value": None, "nodeId": None} for _ in columns]

        return cells

    def fn_store_or_update_node(bucket_nodes, node_id, label, props, dist):
        label_map = bucket_nodes.setdefault(label, {})
        existing = label_map.get(node_id)

        if existing is None:
            label_map[node_id] = {"props": props, "min_dist": dist}
            fn_debug_collect("Storing new node", {"node_id": node_id, "label": label, "dist": dist})
        else:
            if dist < existing.get("min_dist", float('inf')):
                existing["min_dist"] = dist
                existing["props"] = props
                fn_debug_collect("Updating node", {"node_id": node_id, "label": label, "dist": dist})
        return

    def fn_calculate_distance(idx, main_index):
        dist = abs(idx - main_index)
        fn_debug_collect(f"Distance for index {idx}", dist)
        return dist

    def fn_filter_labels(node, selected_labels, filter_labels=None):
        node_labels = [label for label in getattr(node, "labels", []) if label in selected_labels]
        if filter_labels:
            node_labels = [label for label in node_labels if label in filter_labels]
        fn_debug_collect("Filtered labels", node_labels)
        return node_labels

    def fn_get_node_id(node):
        node_id = getattr(node, "identity", None)
        fn_debug_collect("Node ID", node_id)
        return node_id

    def fn_debug_collect(label, data):
        print(f"DEBUG [collect_nodes]: {label}: {data}")

    return bp
