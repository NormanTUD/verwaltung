from flask import Blueprint, jsonify

def create_delete_all_bp(graph):
    bp = Blueprint("delete_all", __name__)

    # TODO!!! DELETE AGAIN!!!
    @bp.route('/delete_all')
    def delete_all():
        """
        Löscht alle Nodes und Relationships in der Datenbank.
        Kann ohne Body oder Parameter aufgerufen werden.
        """
        try:
            graph.run("MATCH (n) DETACH DELETE n")
            return jsonify({"status": "success", "message": "Alle Knoten und Beziehungen wurden gelöscht"})
        except Exception as e:
            print("EXCEPTION:", str(e))
            return jsonify({"status": "error", "message": str(e)}), 500

    return bp
