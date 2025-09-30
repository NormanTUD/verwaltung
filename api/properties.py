from flask import Blueprint, jsonify, request
from oasis_helper import conditional_login_required

def create_properties_bp(graph):
    bp = Blueprint("properties_bp", __name__)

    class GraphAPI:
        def __init__(self, driver):
            self.driver = driver

        def fetch_properties(self, label):
            if not label:
                raise ValueError("Missing label parameter")

            try:
                # Mit APOC: liefert uns saubere Typen
                query = f"""
                MATCH (n:{label})
                UNWIND keys(n) AS key
                RETURN DISTINCT key, apoc.meta.type(n[key]) AS type
                ORDER BY key
                """
                records = self.driver.run(query).data()
                return [{"property": r["key"], "type": r["type"]} for r in records]

            except Exception:
                # Fallback: ohne APOC, wir schätzen Typen
                query = f"""
                MATCH (n:{label})
                UNWIND keys(n) AS key
                RETURN DISTINCT key, head(collect(n[key])) AS sample
                ORDER BY key
                """
                records = self.driver.run(query).data()
                props = []
                for r in records:
                    val = r.get("sample")
                    if isinstance(val, bool):
                        t = "Boolean"
                    elif isinstance(val, int):
                        t = "Integer"
                    elif isinstance(val, float):
                        t = "Float"
                    elif isinstance(val, str):
                        t = "String"
                    elif isinstance(val, list):
                        t = "List"
                    else:
                        t = "Unknown"
                    props.append({"property": r["key"], "type": t})
                return props

    graph_api = GraphAPI(graph)

    @bp.route("/properties", methods=["GET"])
    @conditional_login_required
    def get_properties():
        try:
            label = request.args.get("label")
            props = graph_api.fetch_properties(label)
            return jsonify(props)
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 500

    return bp
