from flask import Blueprint, request, jsonify
from oasis_helper import conditional_login_required

def create_delete_nodes_bp(graph):
    bp = Blueprint("delete_nodes", __name__)

    @bp.route('/delete_nodes', methods=['DELETE'])
    @conditional_login_required
    def delete_nodes():
        """Löscht mehrere Nodes und ihre Beziehungen aus der Datenbank."""
        if not graph:
            return jsonify({"status": "error", "message": "Datenbank nicht verbunden."}), 500

        # Check if 'ids' is in the URL query string
        ids_param = request.args.get('ids')

        if not ids_param:
            return jsonify({"status": "error", "message": "Fehlende 'ids' im URL-Parameter."}), 400

        try:
            # Split the string by comma and convert each part to an integer
            node_ids = []
            if ids_param is not None:
                for id_str in ids_param.split(','):
                    id_str = id_str.strip()
                    if id_str == "":
                        continue
                    try:
                        node_ids.append(int(id_str))
                    except ValueError:
                        # Ungültiger Eintrag wird übersprungen oder man könnte hier loggen
                        continue
        except ValueError:
            return jsonify({"status": "error", "message": "Ungültiges Format für 'ids'. Erwarte eine durch Kommas getrennte Liste von Zahlen."}), 400

        if not node_ids:
            return jsonify({"status": "success", "message": "Keine Nodes zum Löschen angegeben."})

        query = """
            UNWIND $ids AS id
            MATCH (n) WHERE ID(n) = id
            DETACH DELETE n
        """
        try:
            graph.run(query, ids=node_ids)
            return jsonify({"status": "success", "message": f"{len(node_ids)} Nodes und alle Beziehungen wurden gelöscht."})
        except Exception as e:
            print(f"Fehler beim Löschen der Nodes: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500

    return bp
