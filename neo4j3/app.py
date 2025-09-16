import os
import csv
import io
import json
from flask import Flask, request, jsonify, render_template, session, redirect, url_for
from py2neo import Graph, Node, Relationship
from dotenv import load_dotenv

# Lade Umgebungsvariablen aus der .env-Datei
load_dotenv()

# Konfiguration und Initialisierung
app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY')

# Neo4j-Verbindung
try:
    graph = Graph(os.getenv('NEO4J_URI'), auth=(os.getenv('NEO4J_USER'), os.getenv('NEO4J_PASS')))
    graph.run("MATCH (n) RETURN n LIMIT 1")
    print("Verbindung zu Neo4j erfolgreich hergestellt!")
except Exception as e:
    print(f"Fehler bei der Verbindung zu Neo4j: {e}")
    graph = None

def get_node_by_id(node_id):
    """Hilfsfunktion, um einen Node anhand seiner ID zu finden."""
    query = f"MATCH (n) WHERE ID(n) = {node_id} RETURN n"
    return graph.run(query).data()[0]['n']

def get_all_nodes_and_relationships():
    """Holt alle Node-Typen und Relationship-Typen aus der Datenbank."""
    node_labels = graph.run("CALL db.labels()").data()
    relationship_types = graph.run("CALL db.relationshipTypes()").data()
    return {
        "labels": [label['label'] for label in node_labels],
        "types": [rel['relationshipType'] for rel in relationship_types]
    }

@app.route('/')
def index():
    return render_template('import.html')

@app.route('/upload', methods=['POST'])
def upload_data():
    """Verarbeitet den CSV/TSV-Upload und zeigt die Header für die Zuordnung an."""
    if 'data' not in request.form:
        return "Keine Daten hochgeladen", 400
    
    data = request.form['data']
    session['raw_data'] = data
    
    try:
        f = io.StringIO(data)
        dialect = csv.Sniffer().sniff(f.read(1024))
        f.seek(0)
        reader = csv.reader(f, dialect)
        headers = next(reader)
        
        # Lege Header in der Session ab
        session['headers'] = headers
        
        return render_template('mapping.html', headers=headers)
    except csv.Error as e:
        return f"Fehler beim Parsen der Daten: {e}", 400

@app.route('/save_mapping', methods=['POST'])
def save_mapping():
    """Speichert die zugeordneten Daten in der Neo4j-Datenbank."""
    mapping_data = request.get_json()
    
    if 'raw_data' not in session or not graph:
        return jsonify({"status": "error", "message": "Sitzungsdaten fehlen oder Datenbank nicht verbunden."}), 500

    raw_data = session.pop('raw_data')
    
    f = io.StringIO(raw_data)
    dialect = csv.Sniffer().sniff(f.read(1024))
    f.seek(0)
    reader = csv.DictReader(f, dialect=dialect)

    tx = graph.begin()
    try:
        # Hier findet die eigentliche, komplexe Logik statt
        # Wir müssen jeden Datensatz durchgehen und die Nodes/Relationships erstellen
        for row in reader:
            # Nodes erstellen und Properties hinzufügen
            nodes_to_create = {}
            for node_type, fields in mapping_data.get('nodes', {}).items():
                props = {field: row.get(field) for field in fields if row.get(field)}
                if props:
                    # Identifier finden, um Nodes zu mergen (nicht duplizieren)
                    identifier_fields = mapping_data['nodes'][node_type]
                    identifier_props = {field: row.get(field) for field in identifier_fields}
                    
                    cypher_query = f"MERGE (n:{node_type} {json.dumps(identifier_props).replace('\"', '')}) ON CREATE SET n = $props RETURN n"
                    result = graph.run(cypher_query, props=props).data()
                    if result:
                        nodes_to_create[node_type] = result[0]['n']
            
            # Relationships erstellen
            for rel_data in mapping_data.get('relationships', []):
                from_node_type = rel_data['from']
                to_node_type = rel_data['to']
                rel_type = rel_data['type']
                
                if from_node_type in nodes_to_create and to_node_type in nodes_to_create:
                    from_node = nodes_to_create[from_node_type]
                    to_node = nodes_to_create[to_node_type]
                    rel = Relationship(from_node, rel_type, to_node)
                    tx.create(rel)

        tx.commit()
        return jsonify({"status": "success", "message": "Daten erfolgreich in Neo4j importiert."})
    except Exception as e:
        tx.rollback()
        print(f"Fehler beim Speichern in der DB: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/overview')
def overview():
    """Zeigt die Übersichtsseite mit allen Node-Typen an."""
    if not graph:
        return "Datenbank nicht verbunden.", 500
    db_info = get_all_nodes_and_relationships()
    return render_template('overview.html', db_info=db_info)

@app.route('/api/query_data', methods=['POST'])
def query_data():
    """Führt eine dynamische Abfrage basierend auf den vom Benutzer ausgewählten Node-Typen aus."""
    selected_labels = request.get_json().get('selectedLabels', [])
    
    if not selected_labels:
        return jsonify({"status": "error", "message": "Keine Node-Labels ausgewählt."}), 400
    
    match_statements = []
    return_variables = []
    
    for label in selected_labels:
        var = f"{label.lower()}"
        match_statements.append(f"({var}:{label})")
        return_variables.append(var)
    
    match_clause = ", ".join(match_statements)
    return_clause = ", ".join(return_variables)
    
    # Vereinfachte Query für die Aggregation
    query = f"""
    MATCH {match_clause}
    RETURN {return_clause} LIMIT 100
    """
    
    try:
        results = graph.run(query).data()
        
        # Daten für das Frontend vorbereiten
        formatted_results = []
        for record in results:
            item = {}
            for key, node in record.items():
                item[key] = {
                    'id': node.identity,
                    'properties': dict(node)
                }
            formatted_results.append(item)
            
        return jsonify(formatted_results)
    except Exception as e:
        print(f"Fehler bei der Abfrage: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/update_node/<int:node_id>', methods=['PUT'])
def update_node(node_id):
    """Aktualisiert einen Node in der Datenbank anhand seiner ID."""
    data = request.get_json()
    property_name = data.get('property')
    new_value = data.get('value')
    
    if not graph:
        return jsonify({"status": "error", "message": "Datenbank nicht verbunden."}), 500

    query = f"""
        MATCH (n) WHERE ID(n) = {node_id}
        SET n.{property_name} = $new_value
        RETURN n
    """
    try:
        graph.run(query, new_value=new_value)
        return jsonify({"status": "success", "message": "Wert aktualisiert."})
    except Exception as e:
        print(f"Fehler beim Aktualisieren des Nodes: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/delete_node/<int:node_id>', methods=['DELETE'])
def delete_node(node_id):
    """Löscht einen Node und seine Beziehungen aus der Datenbank."""
    if not graph:
        return jsonify({"status": "error", "message": "Datenbank nicht verbunden."}), 500

    query = f"""
        MATCH (n) WHERE ID(n) = {node_id}
        DETACH DELETE n
    """
    try:
        graph.run(query)
        return jsonify({"status": "success", "message": f"Node mit ID {node_id} und alle Beziehungen wurden gelöscht."})
    except Exception as e:
        print(f"Fehler beim Löschen des Nodes: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
