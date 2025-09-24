from flask import Blueprint, jsonify

def create_labels_bp(graph):
    bp = Blueprint("labels_bp", __name__)

    class GraphAPI:
        def __init__(self, driver):
            self.driver = driver

        def fetch_labels(self):
            try:
                # probiere system call
                records = self.driver.run("CALL db.labels()").data()
                labels = [r["label"] for r in records if "label" in r]
                if labels:
                    return sorted(set(labels))
            except Exception:
                # fallback: aus existierenden nodes ziehen
                try:
                    records = self.driver.run("MATCH (n) RETURN DISTINCT labels(n) AS lbls").data()
                    labels = []
                    for r in records:
                        lbls = r.get("lbls") or []
                        labels.extend(lbls)
                    return sorted(set(labels))
                except Exception as e2:
                    raise RuntimeError(f"Neo4j error fetching labels: {e2}")

            return []

    graph_api = GraphAPI(graph)

    @bp.route("/labels", methods=["GET"])
    def get_labels():
        try:
            labels = graph_api.fetch_labels()
            return jsonify(labels)
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 500

    return bp
