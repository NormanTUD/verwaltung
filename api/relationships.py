from flask import Blueprint, jsonify
from oasis_helper import conditional_login_required

def create_relationships_bp(graph):
    bp = Blueprint("relationships_bp", __name__)

    class GraphAPI:
        def __init__(self, driver):
            self.driver = driver

        def fetch_relationships(self):
            try:
                # probiere system call
                records = self.driver.run("CALL db.relationshipTypes()").data()
                rels = [r["relationshipType"] for r in records if "relationshipType" in r]
                if rels:
                    return sorted(set(rels))
            except Exception:
                # fallback: aus existierenden relationships ziehen
                try:
                    query = """
                    MATCH ()-[r]->()
                    RETURN DISTINCT type(r) AS rel_type
                    """
                    records = self.driver.run(query).data()
                    rels = [r["rel_type"] for r in records if r.get("rel_type")]
                    return sorted(set(rels))
                except Exception as e:
                    raise RuntimeError(f"Neo4j error fetching relationships: {e}") from e

            return []

    graph_api = GraphAPI(graph)

    @bp.route("/relationships", methods=["GET"])
    @conditional_login_required
    def get_relationships():
        try:
            rels = graph_api.fetch_relationships()
            return jsonify(rels)
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 500

    return bp
