from flask import Blueprint, jsonify

def create_labels_bp(graph):
    bp = Blueprint("labels_bp", __name__)

    class GraphAPI:
        def __init__(self, driver):
            self.driver = driver

        def fetch_labels(self):
            try:
                records = self.driver.run("""
                    MATCH (n)
                    WITH DISTINCT labels(n) AS lbls
                    UNWIND lbls AS lbl
                    RETURN DISTINCT lbl ORDER BY lbl
                """).data()
                return [r["lbl"] for r in records]
            except Exception as e:
                raise RuntimeError(f"Neo4j error fetching labels: {e}")

    graph_api = GraphAPI(graph)

    @bp.route("/labels", methods=["GET"])
    def get_labels():
        try:
            labels = graph_api.fetch_labels()
            return jsonify(labels)
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 500

    return bp
