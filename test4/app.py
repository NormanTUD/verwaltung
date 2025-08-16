from flask import Flask, render_template, jsonify, request
from neo4j import GraphDatabase

app = Flask(__name__)
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "test123"))

@app.route("/")
def index():
    return render_template("index.html")

# Node-Editor für einen Type
@app.route("/nodes/<node_type>")
def node_editor(node_type):
    return render_template("node_editor.html", node_type=node_type)

# API: Nodes abrufen
@app.route("/api/nodes/<node_type>")
def get_nodes(node_type):
    with driver.session() as session:
        result = session.run(f"MATCH (n:{node_type}) RETURN n")
        nodes = [record["n"] for record in result]
        return jsonify(nodes)

# API: Node aktualisieren
@app.route("/api/nodes/<node_type>/<node_id>", methods=["POST"])
def update_node(node_type, node_id):
    data = request.json
    with driver.session() as session:
        set_stmt = ", ".join([f"n.{k}=$props.{k}" for k in data.keys()])
        session.run(f"MATCH (n:{node_type} {{id:$id}}) SET {set_stmt}", props=data, id=node_id)
    return jsonify({"status": "ok"})

def get_nodes(node_type):
    with driver.session() as session:
        result = session.run(f"MATCH (n:{node_type}) RETURN id(n) as id, n")
        nodes = []
        for record in result:
            nodes.append({
                "id": record["id"],
                "properties": dict(record["n"])
            })
        return nodes

def update_node(node_type, node_id, data):
    with driver.session() as session:
        set_str = ", ".join([f"n.{k} = $props.{k}" for k in data.keys()])
        session.run(f"MATCH (n:{node_type}) WHERE id(n)=$id SET {set_str}", id=int(node_id), props=data)

@app.route("/api/nodes/<node_type>")
def api_get_nodes(node_type):
    nodes = get_nodes(node_type)
    return jsonify(nodes)

@app.route("/api/nodes/<node_type>/<int:node_id>", methods=["POST"])
def api_update_node(node_type, node_id):
    data = request.json
    update_node(node_type, node_id, data)
    return jsonify({"status": "ok"})

@app.route("/node_types", methods=["GET", "POST"])
def node_types():
    if request.method == "POST":
        node_type_name = request.form.get("node_type_name")
        if node_type_name:
            with driver.session() as session:
                # Dummy-Node anlegen (damit Label existiert)
                session.run(f"CREATE (n:{node_type_name}) RETURN id(n)")
        return render_template("node_types.html", message=f"Node-Type '{node_type_name}' erstellt")
    return render_template("node_types.html", message="")

if __name__ == "__main__":
    app.run(debug=True)

