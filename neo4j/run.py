from app import create_app

app = create_app()

@app.route("/all_nodes", methods=["GET"])
def all_nodes():
    """
    Listet alle Nodes in der Neo4j DB auf, gruppiert nach Label.
    """
    from flask import render_template, current_app

    neo = current_app.neo

    try:
        # 1. Alle Labels ermitteln
        labels = [row["label"] for row in neo.run_cypher("CALL db.labels() YIELD label RETURN label")]

        all_data = {}

        # 2. Für jedes Label alle Nodes abrufen
        for label in labels:
            cypher = f"MATCH (n:{label}) RETURN n, elementId(n) AS node_id LIMIT 100"
            raw_results = neo.run_cypher(cypher)

            nodes = []
            for row in raw_results:
                node = dict(row["n"]) if "n" in row else {}
                node["__node_id"] = row.get("node_id")
                nodes.append(node)

            all_data[label] = nodes

    except Exception as e:
        return f"Fehler beim Abrufen der Nodes: {e}", 500

    return render_template("all_nodes.html", all_nodes=all_data)

if __name__ == "__main__":
    app.run(debug=True, port=5000)
