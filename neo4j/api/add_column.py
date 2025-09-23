from flask import Blueprint, request, jsonify

def create_add_column_bp(graph):
    bp = Blueprint("add_column", __name__)

    @bp.route('/add_column', methods=['POST'])
    def add_column():
        """Fügt allen Nodes eines bestimmten Labels eine neue Property hinzu (Standardwert = "")."""
        data = request.get_json(silent=True)

        if not data:
            return jsonify({"status": "error", "message": "Request-Body ist leer oder hat ein ungültiges Format."}), 400

        column_name = data.get("column")
        label = data.get("label")

        if not column_name or not label:
            return jsonify({"status": "error", "message": "Fehlende Daten: 'column' und 'label' müssen angegeben sein."}), 400

        if not graph:
            return jsonify({"status": "error", "message": "Datenbank nicht verbunden."}), 500

        try:
            # ⚡️ Wichtig: Property-Namen können nicht direkt als Parameter in Cypher verwendet werden.
            # Deshalb setzen wir ihn sicher via f-String (nur wenn wir geprüft haben, dass es ein valider Name ist).
            if not column_name.isidentifier():
                return jsonify({"status": "error", "message": f"Ungültiger Spaltenname: {column_name}"}), 400

            query = f"""
                MATCH (n:`{label}`)
                SET n.{column_name} = COALESCE(n.{column_name}, "")
            """

            graph.run(query)

            return jsonify({"status": "success", "message": f"Neue Spalte '{column_name}' für alle Nodes vom Typ '{label}' hinzugefügt (Default '')."})
        except Exception as e:
            print(f"Fehler beim Hinzufügen der Spalte: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500

    return bp
