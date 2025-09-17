import os
import csv
import io
import json
import inspect
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

def serialize_properties(props):
    """
    Ensures all property values are JSON-serializable.
    Converts functions and other non-standard objects to strings.
    """
    clean_props = {}
    for key, value in props.items():
        # Check if the value is a function or method.
        if inspect.isfunction(value) or inspect.ismethod(value):
            clean_props[key] = f"FUNCTION_OBJECT: {value.__name__}"
        # If it's a list or dict, recursively clean it.
        elif isinstance(value, (list, dict)):
            clean_props[key] = serialize_properties(value)
        # Otherwise, if it's not a primitive type, convert it to a string.
        elif not isinstance(value, (str, int, float, bool, type(None))):
            clean_props[key] = str(value)
        # If all checks pass, it's a safe value.
        else:
            clean_props[key] = value
    return clean_props

def serialize_value(value):
    """Recursively serializes a value, handling functions and complex types."""
    if inspect.isfunction(value) or inspect.ismethod(value):
        return f"FUNCTION_OBJECT: {value.__name__}"
    if isinstance(value, (str, int, float, bool, type(None))):
        return value
    if isinstance(value, list):
        return [serialize_value(item) for item in value]
    if isinstance(value, dict):
        return {k: serialize_value(v) for k, v in value.items()}
    # Catch any other complex objects and convert them to a string.
    return str(value)

@app.route('/')
def index():
    return render_template('import.html')

@app.route('/graph')
def show_graph():
    return render_template('graph.html')

def serialize_entity(entity):
    """
    Serializes a Neo4j Node or Relationship object to a dictionary.
    """
    data = {'id': entity.id, 'properties': dict(entity)}
    if isinstance(entity, Node):
        data['label'] = next(iter(entity.labels), None)
    elif isinstance(entity, Relationship):
        data['type'] = entity.type
        data['source'] = entity.start_node.id
        data['target'] = entity.end_node.id
    return data



@app.route('/api/graph-data')
def get_graph_data():
    if graph is None:
        return jsonify({"error": "Neo4j connection not available"}), 500

    query = """
    MATCH (n)
    OPTIONAL MATCH (n)-[r]->(m)
    RETURN n, m, r, ID(n) AS n_id, ID(m) AS m_id, ID(r) AS r_id
    """

    try:
        result = graph.run(query)

        nodes = {}
        links = []
        seen_links = set()

        for record in result:
            n = record['n']
            r = record['r']
            m = record['m']
            n_id = record['n_id']

            if n_id not in nodes:
                nodes[n_id] = {
                    'id': n_id,
                    'label': next(iter(n.labels), None),
                    'properties': dict(n)
                }

            if r is not None and m is not None:
                m_id = record['m_id']
                r_id = record['r_id']

                if m_id not in nodes:
                    nodes[m_id] = {
                        'id': m_id,
                        'label': next(iter(m.labels), None),
                        'properties': dict(m)
                    }

                if r_id not in seen_links:
                    rel_type = r.type
                    # Safely get the type string, even if it's a function object
                    if callable(rel_type):
                        rel_type = rel_type.__name__
                        
                    links.append({
                        'source': n_id,
                        'target': m_id,
                        'type': rel_type,
                        'properties': dict(r)
                    })
                    seen_links.add(r_id)

        return jsonify({'nodes': list(nodes.values()), 'links': links})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

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
        print("Fehler: Sitzungsdaten fehlen oder Datenbank nicht verbunden.")
        return jsonify({"status": "error", "message": "Sitzungsdaten fehlen oder Datenbank nicht verbunden."}), 500

    raw_data = session.pop('raw_data')
    
    f = io.StringIO(raw_data)
    try:
        # csv.Sniffer() muss ausreichend Daten zum Analysieren haben
        dialect = csv.Sniffer().sniff(f.read(1024))
        f.seek(0)
        reader = csv.DictReader(f, dialect=dialect)
    except csv.Error as e:
        print(f"Fehler beim Analysieren der CSV-Daten: {e}")
        return jsonify({"status": "error", "message": f"Fehler beim Analysieren der CSV-Daten: {str(e)}"}), 400

    tx = graph.begin()
    try:
        nodes_to_create = {}
        for i, row in enumerate(reader):
            print(f"\n--- Bearbeite Zeile {i+1} ---")
            
            # MERGE-Vorgänge für Knoten
            for node_type, fields in mapping_data.get('nodes', {}).items():
                all_props = {field: row.get(field) for field in fields if row.get(field)}
                
                if not all_props:
                    print(f"  ❌ Keine Daten für den Knoten-Typ '{node_type}' in dieser Zeile. Überspringe.")
                    continue
                
                # Der erste Property wird als eindeutiger Identifikator verwendet.
                identifier_key, identifier_value = next(iter(all_props.items()))
                
                print(f"  ➡️ Versuche, einen Knoten vom Typ '{node_type}' zu mergen.")
                print(f"     Identifikator: '{identifier_key}' = '{identifier_value}'")
                print(f"     Alle Properties: {all_props}")

                # Cypher-Query für MERGE.
                # Wir mergen auf den Identifikator und setzen alle Properties ON CREATE.
                cypher_query = f"""
                MERGE (n:{node_type} {{{identifier_key}: $identifier_value}})
                ON CREATE SET n = $all_props
                RETURN n
                """
                
                params = {
                    "identifier_value": identifier_value,
                    "all_props": all_props
                }
                
                result = graph.run(cypher_query, **params).data()
                
                if result:
                    # Der py2neo-Treiber gibt das Node-Objekt direkt zurück,
                    # also speichern wir es, um es für Relationen zu nutzen.
                    nodes_to_create[node_type] = result[0]['n']
                    print(f"  ✅ Knoten '{node_type}' wurde erfolgreich gemerged (entweder erstellt oder gefunden).")
                else:
                    print(f"  ⚠️ Warnung: Der MERGE-Vorgang für '{node_type}' hat nichts zurückgegeben.")

            # Erstellung von Beziehungen
            for rel_data in mapping_data.get('relationships', []):
                from_node_type = rel_data['from']
                to_node_type = rel_data['to']
                rel_type = rel_data['type']
                
                print(f"  ➡️ Versuche, eine Beziehung '{rel_type}' zu erstellen.")

                if from_node_type in nodes_to_create and to_node_type in nodes_to_create:
                    from_node = nodes_to_create[from_node_type]
                    to_node = nodes_to_create[to_node_type]
                    
                    # Überprüfe, ob die Beziehung bereits existiert, um Duplikate zu vermeiden
                    match_rel_query = f"""
                    MATCH (a:{from_node_type} {{`{from_node_type}`.id: $from_id}})
                    MATCH (b:{to_node_type} {{`{to_node_type}`.id: $to_id}})
                    MERGE (a)-[r:{rel_type}]->(b)
                    """
                    
                    # Hier brauchst du die IDs der Knoten, um die Beziehung korrekt zu mergen
                    from_id = from_node.identity
                    to_id = to_node.identity
                    
                    # In diesem Beispiel verwenden wir die internen IDs, um die Knoten zu finden.
                    # Dies ist aber nicht die beste Praxis. Besser wäre es,
                    # die Identifikatoren aus der CSV zu verwenden.
                    # Aber dein Code mergt die Knoten ja bereits anhand der Identifier,
                    # also ist es einfacher, das mit den Nodes zu machen,
                    # die wir uns gerade gemerkt haben
                    
                    rel_exists_query = f"""
                    MATCH (from_n:{from_node_type})
                    WHERE id(from_n) = {from_node.identity}
                    MATCH (to_n:{to_node_type})
                    WHERE id(to_n) = {to_node.identity}
                    MERGE (from_n)-[rel:{rel_type}]->(to_n)
                    """
                    
                    graph.run(rel_exists_query)
                    print(f"  ✅ Beziehung '{rel_type}' zwischen '{from_node_type}' und '{to_node_type}' erstellt.")
                else:
                    print(f"  ❌ Konnte die Beziehung nicht erstellen, da einer oder beide Knoten fehlen: '{from_node_type}' (vorhanden: {from_node_type in nodes_to_create}), '{to_node_type}' (vorhanden: {to_node_type in nodes_to_create}).")

        tx.commit()
        print("\nGesamtvorgang erfolgreich: Daten wurden in die Neo4j-Datenbank importiert.")
        return jsonify({"status": "success", "message": "Daten erfolgreich in Neo4j importiert."})
    except Exception as e:
        tx.rollback()
        print(f"\n❌ Fehler beim Speichern in der DB: {e}")
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
    Führt eine dynamische Abfrage aus, die alle ausgewählten Knoten und ihre
    optionalen Beziehungen zueinander anzeigt, indem sie die Ergebnisse pro
    zentralem Hauptknoten aggregiert.
    """
    selected_labels = request.get_json().get('selectedLabels', [])
    
    if not selected_labels:
        return jsonify({"status": "error", "message": "Bitte wählen Sie mindestens einen Node-Typ aus."}), 400

    # Bestimme den zentralen Ankerknoten
    # Wir nehmen "person" als Anker, da es der einzige Knoten ist, der sich mit allen
    # anderen ausgewählten Knoten verbinden kann.
    main_label = 'person'
    main_var = 'person'

    # Prüfen, ob der zentrale Knoten überhaupt ausgewählt wurde.
    if main_label not in selected_labels:
        # Fallback: Wenn 'person' nicht ausgewählt ist, dann ist das Datenmodell anders.
        # Hier können wir auf den vorherigen, generischen Ansatz zurückfallen.
        selected_labels.sort()
        main_label = selected_labels[0]
        main_var = main_label.lower()
    
    # Basis-MATCH-Klausel für den Haupt-Label
    cypher_query = f"MATCH ({main_var}:{main_label})"

    # OPTIONAL MATCH für alle anderen ausgewählten Labels
    other_labels = [label for label in selected_labels if label != main_label]
    for other_label in other_labels:
        other_var = other_label.lower()
        cypher_query += f" OPTIONAL MATCH ({main_var})-[]-({other_var}:{other_label})"

    # RETURN-Klausel: Sammle alle verbundenen Knoten in Listen
    return_clause = [f"{main_var}"]
    for other_label in other_labels:
        return_clause.append(f"collect(DISTINCT {other_label.lower()}) AS {other_label.lower()}_list")
        
    cypher_query += f" RETURN {', '.join(return_clause)} LIMIT 100"

    print("Generierte Cypher-Abfrage:")
    print(cypher_query)
    
    try:
        results = graph.run(cypher_query).data()
        
        formatted_results = []
        if not results:
            return jsonify([])

        for record in results:
            item = {}
            main_node = record.get(main_var)
            if main_node:
                item[main_var] = {
                    'id': main_node.identity,
                    'labels': list(main_node.labels),
                    'properties': dict(main_node)
                }

            for other_label in other_labels:
                list_name = f"{other_label.lower()}_list"
                node_list = record.get(list_name, [])
                
                if node_list:
                    related_node = node_list[0]
                    item[other_label.lower()] = {
                        'id': related_node.identity,
                        'labels': list(related_node.labels),
                        'properties': dict(related_node)
                    }
                else:
                    item[other_label.lower()] = None
            
            if any(item.values()):
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
