import logging
from flask import Blueprint, request, jsonify

def create_dump_database_bp(graph):
    bp = Blueprint("dump_database", __name__)

    @bp.route("/dump_database")
    def api_dump_database():
        try:
            print("Start API", "Dumping database")

            query_nodes = """
                MATCH (n)
                RETURN id(n) AS id, labels(n) AS labels, properties(n) AS props
            """
            query_rels = """
                MATCH (a)-[r]->(b)
                RETURN id(r) AS id, type(r) AS type,
                       id(a) AS start_id, id(b) AS end_id,
                       properties(r) AS props
            """

            nodes = graph.run(query_nodes).data()
            rels = graph.run(query_rels).data()

            dump = {
                "nodes": nodes,
                "relationships": rels
            }

            print("Finished API", f"Dumped {len(nodes)} nodes and {len(rels)} relationships")
            return jsonify(dump)

        except Exception as e:
            logging.error(f"Error dumping database: {e}", exc_info=True)
            return jsonify({"status": "error", "message": str(e)}), 500

    return bp
