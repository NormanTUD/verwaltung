from flask import Blueprint, jsonify, request, current_app

bp = Blueprint("api", __name__, template_folder="../../templates", static_folder="../../static")

@bp.route("/", methods=["GET"])
def index():
    return jsonify({
        "info": "Simple API: GET /api/node/<id>  PATCH /api/node/<id> (json body with properties)  POST /api/query (cypher)"
    })

@bp.route("/node/<node_id>", methods=["GET"])
def get_node(node_id: int):
    neo = current_app.neo
    try:
        res = neo.run_cypher("MATCH (n) WHERE elementId(n)=$id RETURN properties(n) as props, elementId(n) as id, labels(n) as labels", {"id": node_id})
        if not res:
            return jsonify({"error": "not found"}), 404
        return jsonify(res[0])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@bp.route("/node/<node_id>", methods=["PATCH"])
def patch_node(node_id: int):
    data = request.get_json() or {}
    if not isinstance(data, dict):
        return jsonify({"error": "invalid body, must be json object of properties"}), 400
    neo = current_app.neo
    try:
        # set properties; we allow null to remove property if value is null
        set_parts = []
        params = {"id": node_id}
        for i, (k, v) in enumerate(data.items()):
            key = f"k{i}"
            params[key] = v
            set_parts.append(f"n.{k} = ${key}")
        set_cypher = "SET " + ", ".join(set_parts) if set_parts else ""
        cypher = f"MATCH (n) WHERE elementId(n)=$id {set_cypher} RETURN properties(n) as props, elementId(n) as id"
        res = neo.run_cypher(cypher, params)
        return jsonify(res[0] if res else {}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@bp.route("/query", methods=["POST"])
def run_query():
    data = request.get_json() or {}
    cypher = data.get("cypher")
    if not cypher:
        return jsonify({"error": "no cypher provided"}), 400
    neo = current_app.neo
    try:
        res = neo.run_cypher(cypher)
        return jsonify({"result": res})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@bp.route("/api/node/<node_id>", methods=["PATCH"])
def update_node(node_id):
    neo = current_app.neo
    data = request.get_json()
    try:
        set_str = ", ".join(f"n.{k} = $val_{k}" for k in data.keys())
        params = {f"val_{k}": v for k,v in data.items()}
        params["node_id"] = node_id
        neo.run_cypher(f"MATCH (n) WHERE elementId(n)=$node_id SET {set_str}", params)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 400
