import os
import uuid
import json
from flask import Flask, render_template, request, redirect, url_for, jsonify
from couchdb import Server

app = Flask(__name__)

# Verwendung der Service-Namen aus Docker Compose für die Verbindung
couchdb_host = os.environ.get('COUCHDB_HOST', "http://couchdb:5984/")
couchdb_user = os.environ.get('COUCHDB_USER', 'admin')
couchdb_password = os.environ.get('COUCHDB_PASSWORD', 'password')

# Aufbau der vollständigen URL für die Verbindung
server_url = f"http://{couchdb_user}:{couchdb_password}@{couchdb_host.replace('http://', '').strip('/')}"
server = None

# Retry-Schleife für die Verbindung mit CouchDB, um "Cannot assign requested address" zu verhindern
retries = 10
while retries > 0:
    try:
        server = Server(server_url)
        print("Successfully connected to CouchDB.")
        break
    except Exception as e:
        print(f"Connection to CouchDB failed. Retrying in 5 seconds... ({e})")
        time.sleep(5)
        retries -= 1

if server is None:
    print("Failed to connect to CouchDB after multiple retries. Exiting.")
    exit(1)

# Datenbank initialisieren
db_name = "mein-projekt-db"
if db_name not in server:
    db = server.create(db_name)
else:
    db = server[db_name]

@app.route('/')
def index():
    docs = [doc.doc for doc in db.view('_all_docs', include_docs=True)]
    return render_template('index.html', docs=docs)

@app.route('/create', methods=['POST'])
def create_document():
    data = request.get_json()
    new_doc = {
        "_id": str(uuid.uuid4()),
        **data
    }
    db.save(new_doc)
    return jsonify(success=True)

@app.route('/update/<doc_id>', methods=['PUT'])
def update_document(doc_id):
    try:
        doc = db[doc_id]
        data = request.get_json()
        for key, value in data.items():
            doc[key] = value
        db.save(doc)
        return jsonify(success=True)
    except:
        return jsonify(success=False, error="Dokument nicht gefunden")

@app.route('/delete/<doc_id>', methods=['DELETE'])
def delete_document(doc_id):
    try:
        doc = db[doc_id]
        db.delete(doc)
        return jsonify(success=True)
    except:
        return jsonify(success=False, error="Dokument nicht gefunden")

# Dynamische Spaltenerweiterung (nicht trivial)
# Da CouchDB schemalos ist, musst du diese Logik im Frontend und Backend selbst verwalten.
# Ein Ansatz wäre, alle Dokumente zu durchlaufen und alle Schlüssel zu sammeln, um die Spaltennamen zu erhalten.

@app.route('/add_column', methods=['POST'])
def add_column():
    data = request.get_json()
    new_column_name = data.get('column_name')
    if not new_column_name:
        return jsonify(success=False, error="Spaltenname fehlt."), 400

    docs = [doc.doc for doc in db.view('_all_docs', include_docs=True)]

    for doc in docs:
        # Die neue Spalte nur hinzufügen, wenn sie nicht existiert
        if new_column_name not in doc:
            doc[new_column_name] = ""
            db.save(doc)

    return jsonify(success=True)

if __name__ == '__main__':
    app.run(debug=True)
