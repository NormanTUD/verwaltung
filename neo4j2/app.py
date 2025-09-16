import os
from flask import Flask, jsonify, request, render_template
from neo4j import GraphDatabase
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret")

uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
user = os.getenv("NEO4J_USER", "neo4j")
password = os.getenv("NEO4J_PASS", "test1234")
driver = GraphDatabase.driver(uri, auth=(user, password))

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/labels", methods=["GET"])
def get_labels():
    labels_query = "CALL db.labels()"
    rel_types_query = "CALL db.relationshipTypes()"
    
    with driver.session() as session:
        labels_result = session.run(labels_query)
        labels = [record["label"] for record in labels_result]

        rel_types_result = session.run(rel_types_query)
        rel_types = [record["relationshipType"] for record in rel_types_result]
        
    return jsonify({
        "labels": labels,
        "relationshipTypes": rel_types
    })

@app.route("/api/data", methods=["GET"])
def get_data():
    start_labels = request.args.getlist("start_label")
    
    if not start_labels:
        return jsonify({"error": "Mindestens ein Start-Label muss angegeben werden"}), 400
    
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
            
            # Use the node's internal ID for direct linking
            row[f"{start_node_label}_id"] = start_node_data.id
            for key, value in start_node_data.items():
                row[f"{start_node_label}_{key}"] = value

            for node in record["connected_nodes"]:
                if node.labels:
                    node_label = list(node.labels)[0]
                    # Use the connected node's ID
                    row[f"{node_label}_id"] = node.id
                    for key, value in node.items():
                        row[f"{node_label}_{key}"] = value
            
            results.append(row)

    df = pd.DataFrame(results).fillna("")
    json_response = df.to_dict(orient="records")
    
    return jsonify(json_response)

@app.route("/api/add", methods=["POST"])
def add_data():
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

@app.route("/api/relate", methods=["POST"])
def relate_nodes():
    data = request.json
    source_id = data.get("source_id")
    target_label = data.get("target_label")
    target_prop = data.get("target_prop")
    target_val = data.get("target_val")
    rel_type = data.get("rel_type")

    if not all([source_id, target_label, target_prop, target_val, rel_type]):
        return jsonify({"error": "Alle Felder müssen ausgefüllt werden."}), 400

    query = (
        f"MATCH (a), (b:`{target_label}`) "
        "WHERE ID(a) = $source_id AND b.`{target_prop}` = $target_val "
        f"MERGE (a)-[:`{rel_type}`]->(b)"
    )

    with driver.session() as session:
        try:
            session.run(query, source_id=source_id, target_prop=target_prop, target_val=target_val)
            return jsonify({"message": f"Beziehung erfolgreich erstellt: Node {source_id} -> '{rel_type}' -> '{target_label}' mit {target_prop}:'{target_val}'"}), 201
        except Exception as e:
            return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
