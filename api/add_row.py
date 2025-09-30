from flask import Blueprint, request, jsonify
from oasis_helper import conditional_login_required

def create_add_row_bp(graph):
    bp = Blueprint("add_row", __name__)

    @bp.route('/add_row', methods=['POST'])
    @conditional_login_required
    def add_row():
        """
        Erstellt einen neuen Node eines gegebenen Labels mit Properties.
        Relationships werden nicht automatisch erstellt!
        """
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

            # ✅ Keine automatische Erstellung von Relationships mehr
            return jsonify({"status": "success", "id": new_id, "message": f"Node '{label}' erstellt"})

        except Exception as e:
            print(f"Fehler beim Erstellen des Nodes: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500

    return bp
