from flask import Blueprint, request, jsonify

def create_add_row_bp(graph):
    bp = Blueprint("add_row", __name__)

    @bp.route('/add_row', methods=['POST'])
    def add_row():
        """Fügt einen neuen Node mit Label und optionalen Properties hinzu."""
        data = request.get_json(silent=True)

        if not data:
            return jsonify({"status": "error", "message": "Request-Body ist leer oder hat ein ungültiges Format."}), 400

        label = data.get("label")
        properties = data.get("properties", {})

        if not label:
            return jsonify({"status": "error", "message": "Fehlende Daten: 'label' muss angegeben sein."}), 400

        if not graph:
            return jsonify({"status": "error", "message": "Datenbank nicht verbunden."}), 500

        try:
            # Property-Namen validieren
            safe_properties = {}
            for key, value in properties.items():
                if not key.isidentifier():
                    return jsonify({"status": "error", "message": f"Ungültiger Property-Name: {key}"}), 400
                safe_properties[key] = value

            # Cypher Query aufbauen
            query = f"""
                CREATE (n:`{label}`)
                SET n += $props
                RETURN ID(n) AS id
            """

            result = graph.run(query, props=safe_properties).data()
            if not result:
                return jsonify({"status": "error", "message": "Node konnte nicht erstellt werden."}), 500

            new_id = result[0]["id"]

            return jsonify({
                "status": "success",
                "message": f"Neuer Node mit Label '{label}' und ID {new_id} hinzugefügt.",
                "id": new_id
            })

        except Exception as e:
            print(f"Fehler beim Hinzufügen des Nodes: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500

    return bp
