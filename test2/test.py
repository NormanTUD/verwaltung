# app.py
from flask import Flask, request, jsonify, render_template
from arango import ArangoClient
import os

app = Flask(__name__)

# --- ArangoDB Setup ---
ARANGO_URL = os.environ.get("ARANGO_URL", "http://localhost:8529")
ARANGO_DB = os.environ.get("ARANGO_DB", "test_db")
ARANGO_USER = os.environ.get("ARANGO_USER", "root")
ARANGO_PASS = os.environ.get("ARANGO_PASS", "")

client = ArangoClient(hosts=ARANGO_URL)
sys_db = client.db("_system", username=ARANGO_USER, password=ARANGO_PASS)

# Ensure DB exists
if not sys_db.has_database(ARANGO_DB):
    sys_db.create_database(ARANGO_DB)

db = client.db(ARANGO_DB, username=ARANGO_USER, password=ARANGO_PASS)

# Ensure collections exist
if not db.has_collection("entities"):
    db.create_collection("entities")

# --------------------
# Entity Manager
# --------------------
@app.route("/api/entities", methods=["GET", "POST", "DELETE"])
def api_entities():
    if request.method == "GET":
        return jsonify(list(db.collection("entities").all()))
    
    data = request.json
    if request.method == "POST":
        if "name" not in data or "fields" not in data:
            return jsonify({"error": "name and fields required"}), 400
        
        # Create entity meta
        entities = db.collection("entities")
        if entities.has(data["name"]):
            entities.update({"_key": data["name"], "fields": data["fields"]})
        else:
            entities.insert({"_key": data["name"], "fields": data["fields"]})
        
        # Ensure data collection exists
        coll_name = f"data_{data['name']}"
        if not db.has_collection(coll_name):
            db.create_collection(coll_name)
        
        return jsonify({"status": "ok"})
    
    if request.method == "DELETE":
        name = request.args.get("name")
        if not name:
            return jsonify({"error": "name required"}), 400
        entities = db.collection("entities")
        if entities.has(name):
            entities.delete(name)
        coll_name = f"data_{name}"
        if db.has_collection(coll_name):
            db.collection(coll_name).delete()
        return jsonify({"status": "deleted"})

# --------------------
# Wizard / Data Entry
# --------------------
@app.route("/api/data/<entity>", methods=["GET", "POST"])
def api_data(entity):
    coll_name = f"data_{entity}"
    if not db.has_collection(coll_name):
        return jsonify({"error": "entity not found"}), 404
    coll = db.collection(coll_name)
    
    if request.method == "POST":
        payload = request.json
        coll.insert(payload)
        return jsonify({"status": "ok"})
    
    # GET: return all data
    return jsonify(list(coll.all()))

# --------------------
# Frontend Routes
# --------------------
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/wizard/<entity>")
def wizard(entity):
    return render_template("wizard.html", entity=entity)

@app.route("/viewer/<entity>")
def viewer(entity):
    return render_template("viewer.html", entity=entity)

# --------------------
# Run
# --------------------
if __name__ == "__main__":
    app.run(debug=True)
