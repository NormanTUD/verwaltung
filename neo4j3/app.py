import os
import csv
import io
import json
import inspect
from flask import Flask, request, jsonify, render_template, session, redirect, url_for
from py2neo import Graph, Relationship
import time
from dotenv import load_dotenv
import itertools

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

# Definiere den Dateipfad
SAVED_QUERIES_FILE = 'saved_queries.json'

def load_saved_queries():
    """Läd die gespeicherten Abfragen aus der Datei."""
    if not os.path.exists(SAVED_QUERIES_FILE):
        return []
    with open(SAVED_QUERIES_FILE, 'r') as f:
        return json.load(f)

def save_queries_to_file(queries):
    """Speichert die Abfragen in der Datei."""
    with open(SAVED_QUERIES_FILE, 'w') as f:
        json.dump(queries, f, indent=4)

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
        node_ids = []
        if ids_param is not None:
            for id_str in ids_param.split(','):
                id_str = id_str.strip()
                if id_str == "":
                    continue
                try:
                    node_ids.append(int(id_str))
                except ValueError:
                    # Ungültiger Eintrag wird übersprungen oder man könnte hier loggen
                    continue
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


@app.route('/api/save_query', methods=['POST'])
def save_query():
    """Speichert eine Abfrage mit einem Namen."""
    try:
        data = request.json
        name = data.get('name')
        labels = data.get('selectedLabels')
        if not name or not labels:
            return jsonify({'status': 'error', 'message': 'Name und Labels sind erforderlich.'}), 400

        queries = load_saved_queries()
        
        # Prüfe, ob die Abfrage bereits existiert
        if any(q['name'] == name for q in queries):
            return jsonify({'status': 'error', 'message': f'Abfrage mit dem Namen "{name}" existiert bereits.'}), 409
            
        queries.append({'name': name, 'labels': labels})
        save_queries_to_file(queries)
        return jsonify({'status': 'success', 'message': 'Abfrage erfolgreich gespeichert.'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/get_saved_queries')
def get_saved_queries():
    """Gibt alle gespeicherten Abfragen zurück."""
    try:
        queries = load_saved_queries()
        return jsonify(queries)
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

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

@app.route('/get_rel_types', methods=['GET'])
def get_rel_types():
    """Gibt eine Liste aller existierenden Relationship-Typen in der DB zurück."""
    try:
        # Führe eine Cypher-Abfrage aus, um alle eindeutigen Relationship-Typen zu finden
        query = "MATCH ()-[r]->() RETURN DISTINCT type(r) AS type"
        result = graph.run(query).data()
        types = [d['type'] for d in result]
        return jsonify(types)
    except Exception as e:
        print(f"Fehler beim Abrufen der Relationship-Typen: {e}")
        return jsonify([]), 500


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
                node_var = safe_var_name(node_type)      # safe für Cypher-Variablen
                node_label = f"`{node_type}`"            # Label immer escapen

                all_props = {}
                for field_map in fields:
                    original_name = field_map['original']
                    renamed_name = field_map['renamed']
                    value = row.get(original_name)
                    if value:
                        all_props[renamed_name] = value

                if not all_props:
                    print(f"  ❌ Keine Daten für den Knoten-Typ '{node_type}' in dieser Zeile. Überspringe.")
                    continue
                
                identifier_key, identifier_value = next(iter(all_props.items()))
                
                print(f"  ➡️ Versuche, einen Knoten vom Typ '{node_type}' zu mergen.")
                print(f"     Identifikator: '{identifier_key}' = '{identifier_value}'")
                print(f"     Alle Properties: {all_props}")

                cypher_query = f"""
                MERGE ({node_var}:{node_label} {{`{identifier_key}`: $identifier_value}})
                ON CREATE SET {node_var} = $all_props
                RETURN {node_var}
                """
                
                params = {
                    "identifier_value": identifier_value,
                    "all_props": all_props
                }
                
                result = graph.run(cypher_query, **params).data()
                
                if result:
                    nodes_to_create[node_type] = result[0][node_var]
                    print(f"  ✅ Knoten '{node_type}' wurde erfolgreich gemerged (entweder erstellt oder gefunden).")
                else:
                    print(f"  ⚠️ Warnung: Der MERGE-Vorgang für '{node_type}' hat nichts zurückgegeben.")

            # Erstellung von Beziehungen
            for rel_data in mapping_data.get('relationships', []):
                from_node_type = rel_data['from']
                to_node_type = rel_data['to']
                rel_type = rel_data['type']
                
                clean_rel_type = rel_type.replace(' ', '_').upper()
                rel_label = f"`{clean_rel_type}`"

                from_label = f"`{from_node_type}`"
                to_label = f"`{to_node_type}`"
                from_var = safe_var_name(from_node_type)
                to_var = safe_var_name(to_node_type)

                print(f"  ➡️ Versuche, eine Beziehung '{rel_type}' zu erstellen.")

                if from_node_type in nodes_to_create and to_node_type in nodes_to_create:
                    from_node = nodes_to_create[from_node_type]
                    to_node = nodes_to_create[to_node_type]
                    
                    rel_query = f"""
                    MATCH ({from_var}:{from_label}) WHERE id({from_var}) = {from_node.identity}
                    MATCH ({to_var}:{to_label}) WHERE id({to_var}) = {to_node.identity}
                    MERGE ({from_var})-[rel:{rel_label}]->({to_var})
                    """
                    
                    graph.run(rel_query)
                    print(f"  ✅ Beziehung '{clean_rel_type}' zwischen '{from_node_type}' und '{to_node_type}' erstellt.")
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
        # Fehler-Meldung ins Template geben
        return render_template('overview.html', db_info=None, error="Datenbank nicht verbunden."), 500
    
    db_info = get_all_nodes_and_relationships()
    return render_template('overview.html', db_info=db_info, error=None)

def safe_var_name(label):
    # Ersetzt alle nicht-alphanumerischen Zeichen durch "_"
    return "".join(ch if ch.isalnum() else "_" for ch in label.lower())


@app.route('/api/query_data', methods=['POST'])
def query_data():
    start_time = time.time()
    print("🚀 API-Anfrage erhalten: /api/query_data")

    try:
        data = request.get_json()
        if not data:
            print("🚨 Fehler: Ungültiges JSON-Format oder leerer Body")
            return jsonify({"status": "error", "message": "Ungültiges JSON-Format oder leerer Body"}), 400

        selected_labels = data.get('selectedLabels', [])
    except Exception as e:
        print(f"🚨 Fehler beim Laden der JSON-Daten: {e}")
        return jsonify({"status": "error", "message": "Ungültiges JSON-Format"}), 400

    print(f"🏷️ Empfangene Labels: {selected_labels}")

    if not selected_labels:
        print("🚨 Fehler: Keine Node-Typen ausgewählt")
        return jsonify({"status": "error", "message": "Bitte wählen Sie mindestens einen Node-Typ aus."}), 400

    # ⭐ Sonderfall: nur ein Label
    if len(selected_labels) == 1:
        single_label = selected_labels[0]
        single_label_escaped = f"`{single_label}`"  # Label escapen
        cypher_query = f"MATCH (n:{single_label_escaped}) RETURN n LIMIT 100"
        print("📊 Generierte Cypher-Abfrage (Einzelfall):")
        print(cypher_query)

        try:
            results = graph.run(cypher_query).data()
            formatted_results = []
            for record in results:
                node = record.get('n')
                if node:
                    formatted_results.append({
                        'id': node.identity,
                        'labels': list(node.labels),
                        'properties': dict(node)
                    })
            end_time = time.time()
            duration = end_time - start_time
            print(f"✅ Abfrage erfolgreich. {len(formatted_results)} Knoten gefunden in {duration:.2f} Sekunden.")
            return jsonify(formatted_results)
        except Exception as e:
            print(f"🚨 Fehler bei der Einzelfall-Abfrage: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500

    # ⭐ Mehrere Labels → kombinierte Abfrage mit Relationen
    main_label = selected_labels[0]
    main_label_escaped = f"`{main_label}`"
    main_var = safe_var_name(main_label)

    cypher_parts = [f"MATCH ({main_var}:{main_label_escaped})"]
    return_parts = [f"{main_var} AS {main_var}"]

    # Liste aller Relationen
    rel_vars = []

    for other_label in selected_labels[1:]:
        other_label_escaped = f"`{other_label}`"
        other_var = safe_var_name(other_label)
        rel_var = f"rel_{main_var}_{other_var}"
        cypher_parts.append(f"OPTIONAL MATCH ({main_var})-[{rel_var}]-({other_var}:{other_label_escaped})")
        return_parts.append(f"{other_var} AS {other_var}")
        return_parts.append(f"{rel_var} AS {rel_var}")
        rel_vars.append(rel_var)

    cypher_query = " ".join(cypher_parts) + " RETURN " + ", ".join(return_parts) + " LIMIT 100"

    print("📊 Generierte Cypher-Abfrage (Mehrfachfall inkl. Relationen):")
    print(cypher_query)

    try:
        results = graph.run(cypher_query).data()
        print(f"📥 Rohdaten aus Neo4j (erste 2 Zeilen): {results[:2]} ...")
    except Exception as e:
        print(f"🚨 Fehler bei der Abfrage: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

    if not results:
        print("🤷‍♂️ Keine Ergebnisse gefunden.")
        return jsonify([])

    formatted_results = []
    for record in results:
        row = {}
        relationships = []

        # Knoten
        for label in selected_labels:
            var = safe_var_name(label)
            node = record.get(var)
            if node:
                row[label] = {
                    'id': node.identity,
                    'labels': list(node.labels),
                    'properties': dict(node)
                }
            else:
                row[label] = None

        # Relationen
        for rel_var in rel_vars:
            rel = record.get(rel_var)
            if rel is not None:
                try:
                    relationships.append({
                        'from': rel.start_node.identity,
                        'to': rel.end_node.identity,
                        'type': rel.__class__.__name__,
                        'properties': dict(rel)
                    })
                except Exception as e:
                    print(f"⚠️ Fehler beim Auslesen von Relation {rel_var}: {e}")

        row['relationships'] = relationships
        formatted_results.append(row)

    end_time = time.time()
    duration = end_time - start_time
    print(f"✅ Abfrage erfolgreich. {len(formatted_results)} Zeilen gefunden in {duration:.2f} Sekunden.")
    return jsonify(formatted_results)

if __name__ == '__main__':
    app.run(debug=True)

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

if __name__ == '__main__':
    app.run(debug=True)
