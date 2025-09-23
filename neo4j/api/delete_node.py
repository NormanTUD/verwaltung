import logging
from flask import Blueprint, request, jsonify

def create_delete_node_bp(graph):
    bp = Blueprint("delete_node", __name__)

    @bp.route('/api/delete_node/<int:node_id>', methods=['DELETE'])
    def delete_node(node_id):
        """Löscht einen Node und seine Beziehungen aus der Datenbank."""
        if not graph:
            return jsonify({"status": "error", "message": "Datenbank nicht verbunden."}), 500

        query = f"""
            MATCH (n) WHERE ID(n) = {node_id}
            DETACH DELETE n
        """
        try:
            graph.run(query)
            return jsonify({"status": "success", "message": f"Node mit ID {node_id} und alle Beziehungen wurden gelöscht."})
        except Exception as e:
            print(f"Fehler beim Löschen des Nodes: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500

    return bp
