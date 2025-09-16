import uuid
import json
from flask import Flask, render_template, request, redirect, url_for, jsonify
from couchdb import Server

app = Flask(__name__)

# Verbindung zur CouchDB
server = Server("http://admin:password@localhost:5984/")
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

if __name__ == '__main__':
    app.run(debug=True)
