import os
import csv
import io
import json
from flask import Flask, request, jsonify, render_template, session, redirect, url_for
from py2neo import Graph, Relationship
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
        for row in reader:
            nodes_to_create = {}
            for node_type, fields in mapping_data.get('nodes', {}).items():
                identifier_props = {field: row.get(field) for field in fields if row.get(field)}
                
                if not identifier_props:
                    continue
                
                identifier_str = ', '.join([f"{k}: '{v}'" for k, v in identifier_props.items()])
                
                cypher_query = f"""
                MERGE (n:{node_type} {{{identifier_str}}})
                ON CREATE SET n = $props
                RETURN n
                """
                
                all_props = {field: row.get(field) for field in fields if row.get(field)}
                
                result = graph.run(cypher_query, props=all_props).data()
                if result:
                    nodes_to_create[node_type] = result[0]['n']
            
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


import itertools

@app.route('/api/query_data', methods=['POST'])
def query_data():
    """
    Führt eine dynamische Abfrage basierend auf den vom Benutzer ausgewählten
    Node-Typen aus, die alle ausgewählten Nodes und ihre Beziehungen anzeigt.
    """
    selected_labels = request.get_json().get('selectedLabels', [])
    
    if not selected_labels:
        return jsonify({"status": "error", "message": "Bitte wählen Sie mindestens einen Node-Typ aus."}), 400

    # Sortiere die Labels, um eine konsistente Reihenfolge für die RETURN-Klausel zu gewährleisten
    selected_labels.sort()
    
    return_vars = [f"{label.lower()} AS {label.lower()}" for label in selected_labels]
    
    query_parts = []
    
    # 1. Base-Query: Finde alle Nodes, die einem der ausgewählten Typen entsprechen
    for label in selected_labels:
        query = f"MATCH ({label.lower()}:{label}) RETURN {label.lower()} AS {label.lower()}"
        
        # Füge null-Werte für alle anderen Spalten hinzu
        for other_label in selected_labels:
            if other_label != label:
                query += f", null AS {other_label.lower()}"
        query_parts.append(query)

    # 2. Relationship-Queries: Finde alle Beziehungen zwischen den Paaren der ausgewählten Typen
    # Verwende itertools, um alle eindeutigen Paare zu erstellen (z.B. Person-Ort, Person-Buch)
    for label1, label2 in itertools.combinations(selected_labels, 2):
        # Erstelle Variablen
        var1 = label1.lower()
        var2 = label2.lower()

        # Erstelle eine MATCH-Klausel für die Beziehung
        match_clause = f"MATCH ({var1}:{label1})-[]-({var2}:{label2})"
        
        # Erstelle eine RETURN-Klausel mit den passenden Variablen und null-Werten
        return_clause = []
        for label in selected_labels:
            var = label.lower()
            if var == var1 or var == var2:
                return_clause.append(f"{var} AS {var}")
            else:
                return_clause.append(f"null AS {var}")
        
        query_parts.append(f"{match_clause} RETURN {', '.join(return_clause)}")

    # Kombiniere alle Teile mit UNION
    cypher_query = " UNION ALL ".join(query_parts) + " LIMIT 100"

    try:
        results = graph.run(cypher_query).data()
        
        formatted_results = []
        for record in results:
            item = {}
            for key, node in record.items():
                if node:
                    item[key] = {
                        'id': node.identity,
                        'labels': list(node.labels),
                        'properties': dict(node)
                    }
                else:
                    item[key] = None
            formatted_results.append(item)
        
        return jsonify(formatted_results)
    except Exception as e:
        print(f"Fehler bei der Abfrage: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/update_node/<int:node_id>', methods=['PUT'])
def update_node(node_id):
    data = request.get_json()
    property_name = data.get('property')
    new_value = data.get('value')
    
    if not graph:
        return jsonify({"status": "error", "message": "Datenbank nicht verbunden."}), 500

    # Abfrage, um den Node zu finden und das Property zu aktualisieren
    query = f"""
        MATCH (n) WHERE ID(n) = {node_id}
        SET n.{property_name} = $new_value
        RETURN n
    """
    try:
        graph.run(query, new_value=new_value)
        return jsonify({"status": "success", "message": f"Node {node_id} wurde aktualisiert."})
    except Exception as e:
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

@app.route('/api/update_nodes', methods=['PUT'])
def update_nodes():
    data = request.get_json(silent=True)

    if not data:
        return jsonify({"status": "error", "message": "Request-Body ist leer oder hat ein ungültiges Format."}), 400

    node_ids = data.get('ids', [])
    property_name = data.get('property')
    new_value = data.get('value')
    
    if not all([node_ids, property_name is not None, new_value is not None]):
        return jsonify({"status": "error", "message": "Fehlende Daten im Request."}), 400

    if not graph:
        return jsonify({"status": "error", "message": "Datenbank nicht verbunden."}), 500

    try:
        # Dynamische Erstellung der Cypher-Abfrage
        query = f"""
            UNWIND $ids AS id
            MATCH (n) WHERE ID(n) = id
            SET n.{property_name} = $value
        """
        
        # Führe die Abfrage aus
        graph.run(query, ids=node_ids, value=new_value)
        
        return jsonify({"status": "success", "message": f"{len(node_ids)} Nodes wurden aktualisiert."})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/delete_nodes', methods=['DELETE'])
def delete_nodes():
    """Löscht mehrere Nodes und ihre Beziehungen aus der Datenbank."""
    if not graph:
        return jsonify({"status": "error", "message": "Datenbank nicht verbunden."}), 500
    
    # Check if 'ids' is in the URL query string
    ids_param = request.args.get('ids')
    
    if not ids_param:
        return jsonify({"status": "error", "message": "Fehlende 'ids' im URL-Parameter."}), 400
    
    try:
        # Split the string by comma and convert each part to an integer
        node_ids = [int(id_str.strip()) for id_str in ids_param.split(',')]
    except ValueError:
        return jsonify({"status": "error", "message": "Ungültiges Format für 'ids'. Erwarte eine durch Kommas getrennte Liste von Zahlen."}), 400
        
    if not node_ids:
        return jsonify({"status": "success", "message": "Keine Nodes zum Löschen angegeben."})
        
    query = """
        UNWIND $ids AS id
        MATCH (n) WHERE ID(n) = id
        DETACH DELETE n
    """
    try:
        graph.run(query, ids=node_ids)
        return jsonify({"status": "success", "message": f"{len(node_ids)} Nodes und alle Beziehungen wurden gelöscht."})
    except Exception as e:
        print(f"Fehler beim Löschen der Nodes: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
