from flask import Blueprint, request, jsonify

def create_update_nodes_bp(graph):
    bp = Blueprint("update_nodes", __name__)

    @bp.route('/update_nodes', methods=['PUT'])
    def update_nodes():
        data = request.get_json(silent=True)

        if not data:
            return jsonify({"status": "error", "message": "Request-Body ist leer oder hat ein ungültiges Format."}), 400

        node_ids = data.get('ids', [])
        property_name = data.get('property')
        new_value = data.get('value')

        if not all([node_ids, property_name is not None, new_value is not None]):
            return jsonify({"status": "error", "message": "Fehlende Daten im Request."}), 400

        if not graph:
            return jsonify({"status": "error", "message": "Datenbank nicht verbunden."}), 500

        try:
            # Dynamische Erstellung der Cypher-Abfrage
            query = f"""
                UNWIND $ids AS id
                MATCH (n) WHERE ID(n) = id
                SET n.{property_name} = $value
            """

            # Führe die Abfrage aus
            graph.run(query, ids=node_ids, value=new_value)

            return jsonify({"status": "success", "message": f"{len(node_ids)} Nodes wurden aktualisiert."})
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 500

    return bp
