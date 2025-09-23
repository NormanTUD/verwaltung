from flask import Blueprint, request, jsonify

def create_add_row_bp(graph):
    bp = Blueprint("add_row", __name__)

    @bp.route('/add_row', methods=['POST'])
    def add_row():
        data = request.get_json(silent=True)
        if not data:
            return jsonify({"status": "error", "message": "Request-Body leer"}), 400

        label = data.get("label")
        properties = data.get("properties", {})
        if not label:
            return jsonify({"status": "error", "message": "'label' fehlt"}), 400

        try:
            # Nur gültige Property-Namen
            safe_properties = {k: v for k, v in properties.items() if k.isidentifier()}

            # Node erstellen
            query = f"CREATE (n:`{label}`) SET n += $props RETURN ID(n) AS id"
            result = graph.run(query, props=safe_properties).data()
            if not result:
                return jsonify({"status": "error", "message": "Node konnte nicht erstellt werden"}), 500

            new_id = result[0]["id"]

            # Vorhandene Relationship-Patterns dynamisch ermitteln
            rel_patterns = graph.run(
                "MATCH (a)-[r]->(b) "
                "RETURN DISTINCT labels(a) AS from_labels, labels(b) AS to_labels, type(r) AS rel_type"
            ).data()

            for pattern in rel_patterns:
                from_labels = pattern["from_labels"]
                to_labels = pattern["to_labels"]
                rel_type = pattern["rel_type"]

                # Wenn neuer Node auf der From-Seite ist
                if label in from_labels:
                    for to_label in to_labels:
                        target_nodes = graph.run(
                            f"""
                            MATCH (t:`{to_label}`)
                            WHERE NOT EXISTS {{
                                MATCH (:{label})-[r:`{rel_type}`]->(t)
                            }}
                            RETURN ID(t) AS tid
                            """
                        ).data()
                        for t in target_nodes:
                            graph.run(
                                "MATCH (a),(b) WHERE ID(a)=$aid AND ID(b)=$bid "
                                f"CREATE (a)-[r:`{rel_type}`]->(b)",
                                aid=new_id, bid=t["tid"]
                            )

                # Wenn neuer Node auf der To-Seite ist
                elif label in to_labels:
                    for from_label in from_labels:
                        target_nodes = graph.run(
                            f"""
                            MATCH (f:`{from_label}`)
                            WHERE NOT EXISTS {{
                                MATCH (f)-[r:`{rel_type}`]->(:`{label}`)
                            }}
                            RETURN ID(f) AS fid
                            """
                        ).data()
                        for f in target_nodes:
                            graph.run(
                                "MATCH (a),(b) WHERE ID(a)=$aid AND ID(b)=$bid "
                                f"CREATE (a)-[r:`{rel_type}`]->(b)",
                                aid=f["fid"], bid=new_id
                            )

            return jsonify({"status": "success", "id": new_id, "message": f"Node '{label}' erstellt"})

        except Exception as e:
            print(f"Fehler: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500

    return bp
