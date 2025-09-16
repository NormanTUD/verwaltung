import os
from flask import Flask, jsonify, request, render_template
from neo4j import GraphDatabase
import pandas as pd

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret")

# Initialisiere den Neo4j-Treiber
uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
user = os.getenv("NEO4J_USER", "neo4j")
password = os.getenv("NEO4J_PASS", "test1234")
driver = GraphDatabase.driver(uri, auth=(user, password))

@app.route("/")
def index():
    """Rendert die Haupt-HTML-Seite."""
    return render_template("index.html")

@app.route("/api/data", methods=["GET"])
def get_data():
    """Holt Daten aus der Datenbank basierend auf einem Start-Label und gibt sie als JSON-Tabelle zurück."""
    start_label = request.args.get("start_label", "Person")
    
    query = (
        f"MATCH (start_node:{start_label}) OPTIONAL MATCH (start_node)-[r*1..3]->(end_node) "
        "WHERE end_node IS NOT NULL "
        "RETURN start_node, COLLECT(DISTINCT end_node) as connected_nodes"
    )
    
    results = []
    with driver.session() as session:
        result = session.run(query)
        for record in result:
            row = {}
            # Start-Node-Attribute hinzufügen
            start_node_data = record["start_node"]
            for key, value in start_node_data.items():
                start_node_label = list(start_node_data.labels)[0]
                row[f"{start_node_label}_{key}"] = value

            # Verbundene Knoten-Attribute hinzufügen
            for node in record["connected_nodes"]:
                for key, value in node.items():
                    node_label = list(node.labels)[0]
                    row[f"{node_label}_{key}"] = value
            
            results.append(row)

    df = pd.DataFrame(results).fillna("") # Fülle fehlende Werte mit leeren Strings auf
    json_response = df.to_dict(orient="records")
    
    return jsonify(json_response)

@app.route("/api/add", methods=["POST"])
def add_data():
    """Fügt einen neuen Knoten mit Eigenschaften hinzu."""
    data = request.json
    node_label = data.get("node_label")
    properties = data.get("properties")

    # Stelle sicher, dass node_label und properties vorhanden sind
    if not node_label or not properties:
        return jsonify({"error": "Fehlende Node-Label oder Eigenschaften"}), 400

    props_str = ", ".join([f"{k}: '{v}'" for k, v in properties.items()])
    query = f"CREATE (n:{node_label} {{{props_str}}}) RETURN n"
    
    with driver.session() as session:
        try:
            session.run(query)
            return jsonify({"message": f"Knoten des Typs '{node_label}' erfolgreich hinzugefügt."}), 201
        except Exception as e:
            return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
