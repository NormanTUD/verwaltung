import os
from flask import Flask, jsonify, request, render_template
from neo4j import GraphDatabase
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

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

@app.route("/api/labels", methods=["GET"])
def get_labels():
    """Holt alle verfügbaren Node-Labels aus der Datenbank."""
    query = "CALL db.labels()"
    labels = []
    with driver.session() as session:
        result = session.run(query)
        labels = [record["label"] for record in result]
    return jsonify(labels)

@app.route("/api/data", methods=["GET"])
def get_data():
    """Holt Daten aus der Datenbank basierend auf einem oder mehreren Start-Labels."""
    start_labels = request.args.getlist("start_label")
    
    if not start_labels:
        return jsonify({"error": "Mindestens ein Start-Label muss angegeben werden"}), 400
    
    # Korrekte Cypher-Syntax für Label-Kombination
    label_string = ":`" + "`:`".join(start_labels) + "`"
    
    query = (
        f"MATCH (start_node{label_string}) OPTIONAL MATCH (start_node)-[r*1..3]->(end_node) "
        "WHERE end_node IS NOT NULL "
        "RETURN start_node, COLLECT(DISTINCT end_node) as connected_nodes"
    )
    
    results = []
    with driver.session() as session:
        result = session.run(query)
        for record in result:
            row = {}
            
            start_node_data = record["start_node"]
            start_node_label = list(start_node_data.labels)[0]
            for key, value in start_node_data.items():
                row[f"{start_node_label}_{key}"] = value

            for node in record["connected_nodes"]:
                if node.labels:
                    node_label = list(node.labels)[0]
                    for key, value in node.items():
                        row[f"{node_label}_{key}"] = value
            
            results.append(row)

    df = pd.DataFrame(results).fillna("")
    json_response = df.to_dict(orient="records")
    
    return jsonify(json_response)


@app.route("/api/add", methods=["POST"])
def add_data():
    """Fügt einen neuen Knoten mit Eigenschaften hinzu."""
    data = request.json
    node_label = data.get("node_label")
    properties = data.get("properties")

    if not node_label or not properties:
        return jsonify({"error": "Fehlende Node-Label oder Eigenschaften"}), 400

    props_str = ", ".join([f"{k}: '{v}'" for k, v in properties.items()])
    query = f"CREATE (n:`{node_label}` {{{props_str}}}) RETURN n"
    
    with driver.session() as session:
        try:
            session.run(query)
            return jsonify({"message": f"Knoten des Typs '{node_label}' erfolgreich hinzugefügt."}), 201
        except Exception as e:
            return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
