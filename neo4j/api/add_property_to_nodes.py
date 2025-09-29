from flask import Blueprint, request, jsonify

def create_add_property_to_nodes_bp(graph):
    bp = Blueprint("add_property_to_nodes", __name__)

    @bp.route("/add_property_to_nodes", methods=["POST"])
    def add_property_to_nodes():
        if not graph:
            return jsonify({"error": "No database connection"}), 500

        data = request.get_json(force=True)

        label = data.get("label")
        property_name = data.get("property")
        value = data.get("value", None)
        return_nodes = data.get("return_nodes", False)

        if not label or not isinstance(label, str) or not label.isidentifier():
            return jsonify({"error": f"Invalid label name: {label}"}), 400
        if not property_name or not isinstance(property_name, str) or not property_name.isidentifier():
            return jsonify({"error": f"Invalid property name: {property_name}"}), 400

        query = f"""
            MATCH (n:`{label}`)
            WHERE n.{property_name} IS NULL
            SET n.{property_name} = $value
            RETURN id(n) AS id
        """

        try:
            result = graph.run(query, value=value).data()
            updated_ids = [r["id"] for r in result]
            response = {"updated": len(updated_ids)}
            if return_nodes:
                response["nodes"] = updated_ids
            return jsonify(response)
        except Exception as e:
            print(f"Error adding property: {e}", exc_info=True)
            return (
                jsonify({
                    "error": str(e),
                    "query": query,
                    "params": {"value": value}
                }),
                500,
            )

    return bp
