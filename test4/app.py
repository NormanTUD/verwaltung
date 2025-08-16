from flask import Flask, render_template, request, jsonify
from neo4j import GraphDatabase

app = Flask(__name__)
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "test"))

def run_cypher(query, params=None):
    with driver.session() as session:
        return [dict(r) for r in session.run(query, params or {})]

# Node-Typ erstellen
@app.route('/create_node_type', methods=['POST'])
def create_node_type():
    data = request.json
    name = data['name']
    props = data['properties']  # {"vorname":"str", "alter":"int"}
    # Wir speichern den Node-Type in Neo4j als Metadaten-Knoten
    run_cypher("MERGE (t:NodeType {name:$name}) SET t.props=$props", {"name": name, "props": props})
    return jsonify(success=True)

# Node erstellen
@app.route('/create_node', methods=['POST'])
def create_node():
    data = request.json
    label = data['label']
    props = data['properties']
    prop_str = ", ".join([f"{k}: ${k}" for k in props.keys()])
    cypher = f"CREATE (n:{label} {{ {prop_str} }}) RETURN n"
    run_cypher(cypher, props)
    return jsonify(success=True)

# Alle Nodes eines Typs
@app.route('/get_nodes/<label>')
def get_nodes(label):
    rows = run_cypher(f"MATCH (n:{label}) RETURN n")
    return jsonify(rows)

# Property update
@app.route('/update_node', methods=['POST'])
def update_node():
    data = request.json
    label = data['label']
    node_id = data['id']
    prop = data['property']
    value = data['value']
    run_cypher(f"""
        MATCH (n:{label})
        WHERE id(n) = $id
        SET n.{prop} = $value
    """, {"id": node_id, "value": value})
    return jsonify(success=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
