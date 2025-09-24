from flask import Blueprint, request, jsonify

def create_add_relationship_bp(graph):
    bp = Blueprint("add_relationship", __name__)

    @bp.route('/add_relationship', methods=['POST'])
    def add_relationship():
        """
        Fügt eine Relationship zwischen zwei Nodes hinzu.
        JSON:
        {
            "start_id": 123,
            "end_id": 456,
            "type": "WOHNT_IN",
            "props": { "key": "value" }  # optional
        }
        """
        data = request.get_json(silent=True)
        if not data:
            return jsonify({"status": "error", "message": "Request-Body leer"}), 400

        start_id = data.get("start_id")
        end_id = data.get("end_id")
        rel_type = data.get("type")
        props = data.get("props", {})

        if not start_id or not end_id or not rel_type:
            return jsonify({
                "status": "error",
                "message": "start_id, end_id und type müssen angegeben werden."
            }), 400

        # Property-Namen validieren
        safe_props = {k: v for k, v in props.items() if k.isidentifier()}

        try:
            query = f"""
                MATCH (a),(b)
                WHERE ID(a)=$start AND ID(b)=$end
                CREATE (a)-[r:`{rel_type}`]->(b)
                SET r += $props
                RETURN ID(r) AS id
            """
            result = graph.run(query, start=start_id, end=end_id, props=safe_props).data()
            if not result:
                return jsonify({"status": "error", "message": "Relationship konnte nicht erstellt werden"}), 500

            return jsonify({"status": "success", "id": result[0]["id"], "message": f"Relationship '{rel_type}' erstellt."})

        except Exception as e:
            print(f"Fehler beim Erstellen der Relationship: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500

    return bp
