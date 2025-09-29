from flask import Blueprint, request, jsonify

def create_update_node_bp(graph):
    bp = Blueprint("update_node", __name__)

    @bp.route('/update_node/<int:node_id>', methods=['PUT'])
    def update_node(node_id):
        data = request.get_json()
        property_name = data.get('property')
        new_value = data.get('value')

        if not graph:
            return jsonify({"status": "error", "message": "Datenbank nicht verbunden."}), 500

        # Abfrage, um den Node zu finden und das Property zu aktualisieren
        query = f"""
            MATCH (n) WHERE ID(n) = {node_id}
            SET n.{property_name} = $new_value
            RETURN n
        """
        try:
            graph.run(query, new_value=new_value)
            return jsonify({"status": "success", "message": f"Node {node_id} wurde aktualisiert."})
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 500

    return bp
