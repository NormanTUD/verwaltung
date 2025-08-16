from flask import Flask, render_template, jsonify, request
from neo4j import GraphDatabase

app = Flask(__name__)
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "test123"))

def run_cypher(query, params=None):
    with driver.session() as session:
        result = session.run(query, params or {})
        return [dict(r) for r in result]

# --- Node Types ---
@app.route("/node_types", methods=["GET", "POST"])
def node_types():
    message = ""
    if request.method == "POST":
        node_type_name = request.form.get("node_type_name")
        if node_type_name:
            with driver.session() as session:
                # Dummy-Node anlegen, damit Label existiert
                session.run(f"CREATE (n:{node_type_name}) RETURN id(n)")
            message = f"Node-Type '{node_type_name}' erstellt"
    return render_template("node_types.html", message=message)

@app.route('/')
def index():
    # Alle Personen + ihre Properties anzeigen
    rows = run_cypher("MATCH (p:Person) RETURN p LIMIT 100")
    properties = set()
    for r in rows:
        properties.update(r['p'].keys())
    return render_template("index.html", rows=rows, columns=list(properties))

# --- Node Editor Page ---
@app.route("/nodes/<node_type>")
def node_editor(node_type):
    return render_template("node_editor.html", node_type=node_type)

# --- API: Nodes abrufen ---
def get_nodes(node_type):
    with driver.session() as session:
        result = session.run(f"MATCH (n:{node_type}) RETURN id(n) AS id, n")
        nodes = []
        for record in result:
            nodes.append({
                "id": record["id"],
                "properties": dict(record["n"])
            })
        return nodes

@app.route("/api/nodes/<node_type>")
def api_get_nodes(node_type):
    nodes = get_nodes(node_type)
    return jsonify(nodes)

# --- API: Node aktualisieren ---
def update_node(node_type, node_id, data):
    with driver.session() as session:
        set_str = ", ".join([f"n.{k} = $props.{k}" for k in data.keys()])
        session.run(
            f"MATCH (n:{node_type}) WHERE id(n) = $id SET {set_str}",
            id=int(node_id),
            props=data
        )

@app.route("/api/nodes/<node_type>/<int:node_id>", methods=["POST"])
def api_update_node(node_type, node_id):
    data = request.json
    update_node(node_type, node_id, data)
    return jsonify({"status": "ok"})

# --- Node erstellen ---
@app.route("/api/nodes/<node_type>", methods=["POST"])
def api_create_node(node_type):
    data = request.json or {}
    with driver.session() as session:
        result = session.run(f"CREATE (n:{node_type} $props) RETURN id(n) AS id", props=data)
        node_id = result.single()["id"]
    return jsonify({"status": "ok", "id": node_id})

if __name__ == "__main__":
    app.run(debug=True)
