import time
import sys
import os
import csv
import io
import json
import inspect
import functools
from flask import Flask, request, jsonify, render_template, session
from py2neo import Graph
from dotenv import load_dotenv
import logging

from rich.console import Console

console = Console()

def test_if_deleted_db(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            before_count = 0
            if graph:
                try:
                    res = graph.run("MATCH (n) RETURN count(n) as c").data()
                    before_count = res[0]["c"] if res else 0
                except Exception as e:
                    console.print(f"[red]!!!!!!!!!!!!!!! Fehler beim Vor-Check: {e} !!!!!!!!!!!!!!![/red]")

            result = func(*args, **kwargs)

            after_count = 0
            if graph:
                try:
                    res = graph.run("MATCH (n) RETURN count(n) as c").data()
                    after_count = res[0]["c"] if res else 0
                except Exception as e:
                    console.print(f"[red]!!!!!!!!!!!!!!! Fehler beim Nach-Check: {e} !!!!!!!!!!!!!!![/red]")

            if before_count > 0 and after_count == 0:
                console.print(
                    f"[bold red]\n\n"
                    f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n"
                    f"!!!!!!!!!!!!!!!  DATEN GELÖSCHT von Funktion {func.__name__} !!!!!!!!!!!!!!!\n"
                    f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n[/bold red]"
                )
            elif after_count < before_count:
                console.print(
                    f"[yellow]\n\n"
                    f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n"
                    f"!!!! WARNUNG: Funktion {func.__name__} hat Knoten entfernt "
                    f"({before_count} → {after_count}) !!!!\n"
                    f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n\n[/yellow]"
                )

            return result
        except Exception as e:
            console.print(f"[red]!!!!!!!!!!!!!!! Fehler im Decorator test_if_deleted_db: {e} !!!!!!!!!!!!!!![/red]")
            raise
    return wrapper

# Lade Umgebungsvariablen aus der .env-Datei
load_dotenv()

# Konfiguration und Initialisierung
app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'replace_this_with_a_long_random_secret') # TODO: Remove hardcoded

try:
    # Neo4j-Verbindung
    graph = None
    for attempt in range(15):  # max 15 Versuche
        try:
            graph = Graph(
                os.getenv("NEO4J_URI", "bolt://localhost:7687"),
                auth=(
                    os.getenv("NEO4J_USER", "neo4j"),
                    os.getenv("NEO4J_PASS", "testTEST12345678")
                )
            )
            graph.run("RETURN 1")  # Testabfrage
            print("Neo4j ist bereit!")
            break
        except Exception as e:
            print(f"[{attempt+1}/15] Neo4j nicht bereit, warte 2 Sekunden... ({e})")
            time.sleep(2)

    if graph is None:
        print("Neo4j konnte nicht erreicht werden!")
        sys.exit(1)
except KeyboardInterrupt:
    print("You pressed CTRL-C")
    sys.exit(0)

# Definiere den Dateipfad
SAVED_QUERIES_FILE = 'saved_queries.json'

def load_saved_queries():
    """Läd die gespeicherten Abfragen aus der Datei."""
    if not os.path.exists(SAVED_QUERIES_FILE):
        return []
    with open(SAVED_QUERIES_FILE, encoding="utf-8", mode='r') as f:
        return json.load(f)

def save_queries_to_file(queries):
    """Speichert die Abfragen in der Datei."""
    with open(SAVED_QUERIES_FILE, encoding="utf-8", mode='w') as f:
        json.dump(queries, f, indent=4)

# TODO!!! DELETE AGAIN!!!
@app.route('/api/delete_all')
@test_if_deleted_db
def delete_all():
    """
    Löscht alle Nodes und Relationships in der Datenbank.
    Kann ohne Body oder Parameter aufgerufen werden.
    """
    try:
        graph.run("MATCH (n) DETACH DELETE n")
        return jsonify({"status": "success", "message": "Alle Knoten und Beziehungen wurden gelöscht"})
    except Exception as e:
        print("EXCEPTION:", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/delete_nodes', methods=['DELETE'])
@test_if_deleted_db
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

@app.route('/api/add_row', methods=['POST'])
@test_if_deleted_db
def add_row():
    """Fügt einen neuen Node mit Label und optionalen Properties hinzu."""
    data = request.get_json(silent=True)

    if not data:
        return jsonify({"status": "error", "message": "Request-Body ist leer oder hat ein ungültiges Format."}), 400

    label = data.get("label")
    properties = data.get("properties", {})

    if not label:
        return jsonify({"status": "error", "message": "Fehlende Daten: 'label' muss angegeben sein."}), 400

    if not graph:
        return jsonify({"status": "error", "message": "Datenbank nicht verbunden."}), 500

    try:
        # Property-Namen validieren
        safe_properties = {}
        for key, value in properties.items():
            if not key.isidentifier():
                return jsonify({"status": "error", "message": f"Ungültiger Property-Name: {key}"}), 400
            safe_properties[key] = value

        # Cypher Query aufbauen
        query = f"""
            CREATE (n:`{label}`)
            SET n += $props
            RETURN ID(n) AS id
        """

        result = graph.run(query, props=safe_properties).data()
        if not result:
            return jsonify({"status": "error", "message": "Node konnte nicht erstellt werden."}), 500

        new_id = result[0]["id"]

        return jsonify({
            "status": "success",
            "message": f"Neuer Node mit Label '{label}' und ID {new_id} hinzugefügt.",
            "id": new_id
        })

    except Exception as e:
        print(f"Fehler beim Hinzufügen des Nodes: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/update_nodes', methods=['PUT'])
@test_if_deleted_db
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

@app.route('/api/add_column', methods=['POST'])
@test_if_deleted_db
def add_column():
    """Fügt allen Nodes eines bestimmten Labels eine neue Property hinzu (Standardwert = "")."""
    data = request.get_json(silent=True)

    if not data:
        return jsonify({"status": "error", "message": "Request-Body ist leer oder hat ein ungültiges Format."}), 400

    column_name = data.get("column")
    label = data.get("label")

    if not column_name or not label:
        return jsonify({"status": "error", "message": "Fehlende Daten: 'column' und 'label' müssen angegeben sein."}), 400

    if not graph:
        return jsonify({"status": "error", "message": "Datenbank nicht verbunden."}), 500

    try:
        # ⚡️ Wichtig: Property-Namen können nicht direkt als Parameter in Cypher verwendet werden.
        # Deshalb setzen wir ihn sicher via f-String (nur wenn wir geprüft haben, dass es ein valider Name ist).
        if not column_name.isidentifier():
            return jsonify({"status": "error", "message": f"Ungültiger Spaltenname: {column_name}"}), 400

        query = f"""
            MATCH (n:`{label}`)
            SET n.{column_name} = COALESCE(n.{column_name}, "")
        """

        graph.run(query)

        return jsonify({"status": "success", "message": f"Neue Spalte '{column_name}' für alle Nodes vom Typ '{label}' hinzugefügt (Default '')."})
    except Exception as e:
        print(f"Fehler beim Hinzufügen der Spalte: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/save_query', methods=['POST'])
@test_if_deleted_db
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
@test_if_deleted_db
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
@test_if_deleted_db
def index():
    return render_template('import.html')

@app.route('/graph')
@test_if_deleted_db
def show_graph():
    return render_template('graph.html')

@app.route('/api/graph-data')
@test_if_deleted_db
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
                    rel_type = type(r)  # <- Hier liegt der Trick
                    if not isinstance(rel_type, str):
                        rel_type = r.__class__.__name__

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
@test_if_deleted_db
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
@test_if_deleted_db
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
@test_if_deleted_db
def save_mapping():
    """Hauptfunktion: speichert die zugeordneten Daten in Neo4j."""
    mapping_data = request.get_json()

    if 'raw_data' not in session or not graph:
        print("Fehler: Sitzungsdaten fehlen oder Datenbank nicht verbunden.")
        return jsonify({"status": "error", "message": "Sitzungsdaten fehlen oder Datenbank nicht verbunden."}), 500

    reader = parse_csv_from_session()
    if reader is None:
        return jsonify({"status": "error", "message": "Fehler beim Analysieren der CSV-Daten."}), 400

    tx = graph.begin()
    try:
        for i, row in enumerate(reader):
            print(f"\n--- Bearbeite Zeile {i+1} ---")
            nodes_created = process_row(tx, row, mapping_data)

        graph.commit(tx)
        print("\nGesamtvorgang erfolgreich: Daten wurden in die Neo4j-Datenbank importiert.")
        return jsonify({"status": "success", "message": "Daten erfolgreich in Neo4j importiert."})
    except Exception as e:
        tx.rollback()
        print(f"\n❌ Fehler beim Speichern in der DB: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


def parse_csv_from_session():
    """Liest die CSV-Daten aus der Session und gibt einen DictReader zurück."""
    raw_data = session.pop('raw_data')
    f = io.StringIO(raw_data)
    try:
        dialect = csv.Sniffer().sniff(f.read(1024))
        f.seek(0)
        reader = csv.DictReader(f, dialect=dialect)
        return reader
    except csv.Error as e:
        print(f"Fehler beim Analysieren der CSV-Daten: {e}")
        return None


def process_row(tx, row, mapping_data):
    """Verarbeitet eine Zeile: Knoten mergen und Beziehungen erstellen."""
    nodes_created = {}

    # Knoten erstellen/mergen
    for node_type, fields in mapping_data.get('nodes', {}).items():
        node = merge_node(tx, node_type, fields, row)
        if node:
            nodes_created[node_type] = node

    # Beziehungen erstellen
    for rel_data in mapping_data.get('relationships', []):
        create_relationship(tx, rel_data['from'], rel_data['to'], rel_data['type'], nodes_created)

    return nodes_created


def merge_node(tx, node_type, fields, row):
    """Merged einen Knoten vom Typ node_type mit gegebenen Properties."""
    node_var = safe_var_name(node_type)
    node_label = f"`{node_type}`"

    all_props = {}
    for field_map in fields:
        original_name = field_map['original']
        renamed_name = field_map['renamed']
        value = row.get(original_name)
        if value:
            all_props[renamed_name] = value

    if not all_props:
        print(f"  ❌ Keine Daten für den Knoten-Typ '{node_type}' in dieser Zeile. Überspringe.")
        return None

    identifier_key, identifier_value = next(iter(all_props.items()))
    print(f"  ➡️ Versuche, einen Knoten vom Typ '{node_type}' zu mergen.")
    print(f"     Identifikator: '{identifier_key}' = '{identifier_value}'")
    print(f"     Alle Properties: {all_props}")

    cypher_query = f"""
    MERGE ({node_var}:{node_label} {{`{identifier_key}`: $identifier_value}})
    ON CREATE SET {node_var} = $all_props
    RETURN {node_var}
    """

    params = {"identifier_value": identifier_value, "all_props": all_props}
    result = graph.run(cypher_query, **params).data()

    if result:
        print(f"  ✅ Knoten '{node_type}' erfolgreich gemerged.")
        return result[0][node_var]
    else:
        print(f"  ⚠️ MERGE-Vorgang für '{node_type}' hat nichts zurückgegeben.")
        return None


def create_relationship(tx, from_node_type, to_node_type, rel_type, nodes_created):
    """Erstellt eine Beziehung zwischen zwei vorhandenen Knoten."""
    clean_rel_type = rel_type.replace(' ', '_').upper()
    rel_label = f"`{clean_rel_type}`"

    from_var = safe_var_name(from_node_type)
    to_var = safe_var_name(to_node_type)

    print(f"  ➡️ Versuche, eine Beziehung '{rel_type}' zu erstellen.")

    if from_node_type in nodes_created and to_node_type in nodes_created:
        from_node = nodes_created[from_node_type]
        to_node = nodes_created[to_node_type]

        rel_query = f"""
        MATCH ({from_var}:`{from_node_type}`) WHERE id({from_var}) = {from_node.identity}
        MATCH ({to_var}:`{to_node_type}`) WHERE id({to_var}) = {to_node.identity}
        MERGE ({from_var})-[rel:{rel_label}]->({to_var})
        """
        graph.run(rel_query)
        print(f"  ✅ Beziehung '{clean_rel_type}' zwischen '{from_node_type}' und '{to_node_type}' erstellt.")
    else:
        print(f"  ❌ Beziehung konnte nicht erstellt werden, Knoten fehlen: '{from_node_type}' (vorhanden: {from_node_type in nodes_created}), '{to_node_type}' (vorhanden: {to_node_type in nodes_created}).")

@app.route('/overview')
@test_if_deleted_db
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

# -------------------------------
# Helper Functions
# -------------------------------

def parse_request_json(req_json):
    """JSON auslesen und default values setzen"""
    print("🔍 parse_request_json: Start")
    print(f"📥 Eingehendes JSON: {req_json}")

    if not req_json:
        raise ValueError("Ungültiges JSON-Format oder leerer Body")
    selected_labels = req_json.get('selectedLabels', [])
    max_depth = req_json.get('maxDepth', 3)
    limit = req_json.get('limit', 200)

    print(f"✅ Geparste Werte: selected_labels={selected_labels}, max_depth={max_depth}, limit={limit}")

    if not selected_labels:
        raise ValueError("Bitte wählen Sie mindestens einen Node-Typ aus.")

    return selected_labels, max_depth, limit

def generate_cypher_query(max_depth):
    """Dynamische Pfad-Abfrage für Neo4j generieren"""
    print("🔍 generate_cypher_query: Start")
    query = f"""
    MATCH p=(start)-[*..{max_depth}]->(end)
    WHERE ANY(n IN nodes(p) WHERE ANY(l IN labels(n) WHERE l IN $labels))
    RETURN p LIMIT $limit
    """
    print(f"📄 Generierte Cypher-Abfrage:\n{query}")
    return query

# ===========================
# Backend: Flask / Neo4j
# ===========================
def fn_debug_print(label, data):
    print(f"DEBUG: {label}: {data}")

def fn_validate_request_body(data):
    fn_debug_print("Validating request body", data)
    if not data:
        return False, "Request-Body leer oder ungültig. property und value erforderlich."
    if "property" not in data:
        return False, "property fehlt im Request."
    if "value" not in data:
        return False, "value fehlt im Request."
    if not str(data["property"]).isidentifier():
        return False, f"Ungültiger Property-Name: {data['property']}"
    return True, None

def fn_parse_request_data(data):
    fn_debug_print("Parsing request data", data)
    prop_name = data.get("property")
    value = data.get("value")
    connect_ids = data.get("connectTo", [])
    relation_data = data.get("relation", None)
    return prop_name, value, connect_ids, relation_data

def fn_determine_node_label(relation_data):
    node_label = "Node"
    if relation_data and "targetLabel" in relation_data:
        node_label = relation_data["targetLabel"]
    fn_debug_print("Determined node label", node_label)
    return node_label

def fn_create_node(node_label, prop_name, value):
    query = f"CREATE (n:{node_label}) SET n.{prop_name}=$value RETURN ID(n) AS id"
    fn_debug_print("Node creation query", query)
    result = graph.run(query, value=value).data()
    fn_debug_print("Node creation result", result)
    if not result:
        raise Exception("Node konnte nicht erstellt werden.")
    new_node_id = result[0]["id"]
    fn_debug_print("New node ID", new_node_id)
    return new_node_id

def fn_clean_connect_ids(connect_ids):
    cleaned = [int(i) for i in connect_ids if isinstance(i, (int, float))]
    cleaned = list(set(cleaned))
    fn_debug_print("Cleaned connect IDs", cleaned)
    return cleaned

def fn_create_relationships(new_node_id, connect_ids_clean, relation_data):
    if not connect_ids_clean or not relation_data:
        fn_debug_print("No relationships to create", {"connect_ids_clean": connect_ids_clean, "relation_data": relation_data})
        return

    rel_type = relation_data.get("relation", "CONNECTED_TO")
    direction = relation_data.get("direction", "from_new_to_existing")
    fn_debug_print("Relationship info", {"rel_type": rel_type, "direction": direction})

    for other_id in connect_ids_clean:
        from_id, to_id = (new_node_id, other_id) if direction == "from_new_to_existing" else (other_id, new_node_id)
        query_rel = f"""
            MATCH (n),(m)
            WHERE ID(n) = $from_id AND ID(m) = $to_id
            MERGE (n)-[:{rel_type}]->(m)
        """
        fn_debug_print("Relationship creation query", f"{from_id} -[{rel_type}]-> {to_id}")
        graph.run(query_rel, from_id=from_id, to_id=to_id)
        fn_debug_print("Relationship created", f"{from_id} -[{rel_type}]-> {to_id}")

# ===============================
# Flask Route
# ===============================

@app.route('/api/create_node', methods=['POST'])
def api_create_node():
    try:
        data = request.get_json(silent=True)
        fn_debug_print("Incoming request data", data)

        # Validate request
        is_valid, error_msg = fn_validate_request_body(data)
        if not is_valid:
            return jsonify({"status": "error", "message": error_msg}), 400

        # Parse request data
        prop_name, value, connect_ids, relation_data = fn_parse_request_data(data)

        # Determine node label
        node_label = fn_determine_node_label(relation_data)

        # Create node
        new_node_id = fn_create_node(node_label, prop_name, value)

        # Clean connect IDs
        connect_ids_clean = fn_clean_connect_ids(connect_ids)

        # Create relationships
        fn_create_relationships(new_node_id, connect_ids_clean, relation_data)

        fn_debug_print("Node creation process completed", new_node_id)
        return jsonify({
            "status": "success",
            "message": f"Neuer Node erstellt mit ID {new_node_id}",
            "newNodeId": new_node_id
        })

    except Exception as e:
        fn_debug_print("Exception in api_create_node", e)
        return jsonify({"status": "error", "message": str(e)}), 500

def run_query(graph, query, labels, limit):
    """Neo4j-Abfrage ausführen und Ergebnis zurückgeben"""
    print("🔍 run_query: Start")
    print(f"📊 Parameter: labels={labels}, limit={limit}")
    try:
        results = graph.run(query, labels=labels, limit=limit).data()
    except Exception as e:
        print(f"❌ Fehler bei der Neo4j-Abfrage: {e}")
    print(f"✅ Abfrage erfolgreich, {len(results)} Ergebnisse erhalten")
    return results

def collect_labels(path_results, selected_labels):
    """Alle Labels aus den Pfaden sammeln, die ausgewählt wurden"""
    print("🔍 collect_labels: Start")
    all_labels = set()
    for idx, r in enumerate(path_results):
        print(f"  🛣️ Pfad {idx+1}: {len(r['p'].nodes)} Nodes")
        for n in r['p'].nodes:
            filtered_labels = [_l for _l in n.labels if _l in selected_labels]
            if filtered_labels:
                print(f"    Node {n.identity} Labels gefiltert: {filtered_labels}")
            all_labels.update(filtered_labels)
    all_labels_list = list(all_labels)
    print(f"✅ Alle gesammelten Labels: {all_labels_list}")
    return all_labels_list

def build_table_results(path_results, selected_labels, all_labels):
    """Tabellarische Ergebnisse aus Pfaden aufbereiten"""
    print("🔍 build_table_results: Start")
    table_results = []

    for path_idx, r in enumerate(path_results):
        print(f"  🛤️ Bearbeite Pfad {path_idx+1}/{len(path_results)}")
        path = r['p']
        row = {label: [] for label in all_labels}
        row['relationships'] = []

        # Nodes einfügen
        for n in path.nodes:
            filtered_labels = [_l for _l in n.labels if _l in selected_labels]
            for label in filtered_labels:
                node_info = {'id': n.identity, 'properties': dict(n)}
                print(f"    Node hinzufügen: Label={label}, ID={n.identity}, Properties={node_info['properties']}")
                row[label].append(node_info)

        # Beziehungen einfügen
        for rel in path.relationships:
            rel_info = {
                'from': rel.start_node.identity,
                'to': rel.end_node.identity,
                'type': type(rel).__name__,
                'properties': dict(rel)
            }
            print(f"    Beziehung hinzufügen: {rel_info}")
            row['relationships'].append(rel_info)

        # Listen mit nur einem Element zu Dictionary konvertieren
        for label in all_labels:
            if len(row[label]) == 1:
                print(f"    Label {label} hat nur 1 Node, konvertiere zu Dict")
                row[label] = row[label][0]
            elif len(row[label]) == 0:
                print(f"    Label {label} hat 0 Nodes, setze auf None")
                row[label] = None

        table_results.append(row)
        print(f"  ✅ Pfad {path_idx+1} verarbeitet")

    print(f"✅ build_table_results: Fertig, {len(table_results)} Zeilen erstellt")
    return table_results

@app.route('/api/update_node/<int:node_id>', methods=['PUT'])
@test_if_deleted_db
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
@test_if_deleted_db
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

@app.route('/api/get_data_as_table', methods=['GET'])
def get_data_as_table():
    try:
        print("\n=== API get_data_as_table gestartet ===")

        # Parameter parsen
        print("-> Parsing Request-Parameter ...")
        selected_labels, main_label, max_depth, limit, filter_labels = parse_request_params(request)
        print(f"   -> Selected labels: {selected_labels}")
        print(f"   -> Main label (Pivot): {main_label}")
        print(f"   -> Max depth: {max_depth}")
        print(f"   -> Limit: {limit}")
        print(f"   -> Filter labels: {filter_labels}")

        # Cypher-Abfrage starten
        print("-> Starte Cypher-Abfrage für Pfade ...")
        results = run_cypher_paths(graph, selected_labels, max_depth, limit)
        print(f"   -> Anzahl Pfade erhalten: {len(results)}")

        # Haupt-Sammlung initialisieren
        main_nodes = {}
        if results:
            print("-> Verarbeite Pfade und sammle Nodes & Relations ...")
            main_nodes = process_paths(results, main_label, selected_labels, filter_labels)
            print(f"   -> Fertig mit Pfad-Verarbeitung, Haupt-Buckets: {list(main_nodes.keys())}")
        else:
            print("-> Keine Pfade gefunden, hole einzelne Nodes ...")
            collect_single_nodes(graph, main_nodes, main_label, limit)
            print(f"   -> Fertig mit Single-Node-Verarbeitung, Haupt-Buckets: {list(main_nodes.keys())}")

        for lbl in selected_labels:
            if lbl != main_label:
                collect_single_nodes(graph, main_nodes, lbl, limit)

        # Columns bestimmen
        print("-> Bestimme Spalten (Columns) basierend auf gesammelten Nodes ...")
        columns = determine_columns(main_nodes)
        print(f"   -> Columns (sortiert): {columns}")

        # Rows bauen
        print("-> Baue Rows für die Ausgabe ...")
        rows = build_rows(main_nodes, columns)
        print(f"   -> Anzahl Rows erstellt: {len(rows)}")

        # Fertige Tabelle zurückgeben
        print("-> JSON-Antwort wird erstellt und zurückgegeben ...")
        return jsonify({"columns": columns, "rows": rows})

    except Exception as e:
        print(f"!!! Fehler aufgetreten: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# ------------------------
# Hilfsfunktionen
# ------------------------
def parse_request_params(request):
    node_csv = request.args.get('nodes')
    if not node_csv:
        raise ValueError("Parameter 'nodes' erforderlich")
    selected_labels = [n.strip() for n in node_csv.split(',') if n.strip()]
    if not selected_labels:
        raise ValueError("No labels parsed")
    main_label = selected_labels[0]

    max_depth = int(request.args.get('maxDepth', 3))
    limit = request.args.get('limit')
    limit = int(limit) if limit else None

    filter_labels_csv = request.args.get('filterLabels')
    filter_labels = [_l.strip() for _l in filter_labels_csv.split(',')] if filter_labels_csv else None

    return selected_labels, main_label, max_depth, limit, filter_labels


def run_cypher_paths(graph, selected_labels, max_depth, limit=None):
    cypher_query = f"""
    MATCH p=(start)-[*..{max_depth}]->(end)
    WHERE ANY(n IN nodes(p) WHERE ANY(l IN labels(n) WHERE l IN $labels))
    RETURN p
    """
    if limit:
        cypher_query += f" LIMIT {limit}"
    print("Cypher Query:", cypher_query.strip())
    return graph.run(cypher_query, labels=selected_labels).data()


def determine_columns(main_nodes):
    columns_set = set()
    for bucket in main_nodes.values():
        for label, nodes_map in bucket.get("nodes", {}).items():
            for info in nodes_map.values():
                for prop in info.get("props", {}):
                    columns_set.add((label, prop))
    return [{"nodeType": lbl, "property": prop} for lbl, prop in sorted(columns_set, key=lambda x: (x[0], x[1]))]


def build_rows(main_nodes, columns):
    rows = []
    for main_id, bucket in main_nodes.items():
        cells = build_cells_for_bucket(bucket, columns)
        rows.append({"cells": cells, "relations": bucket.get("relations", [])})
    return rows

def collect_single_nodes(graph, main_nodes, main_label, limit=None):
    """
    Holt einzelne Nodes vom Graphen, falls keine Pfade gefunden wurden.

    Args:
        graph: Neo4j Graph-Objekt.
        main_nodes (dict): Dictionary zum Speichern der Buckets.
        main_label (str): Label der Hauptknoten.
        limit (int, optional): Optional Limit für abgefragte Nodes.

    Modifiziert:
        main_nodes: Fügt Nodes hinzu, initialisiert Buckets, speichert props und min_dist.
    """
    try:
        print("Keine Pfade gefunden -> hole einzelne Nodes")
        cypher_nodes = f"MATCH (n:{main_label}) RETURN n"
        if limit:
            cypher_nodes += f" LIMIT {limit}"

        node_results = graph.run(cypher_nodes).data()
        print(f"Einzelne Nodes erhalten: {len(node_results)}")

        for r in node_results:
            n = r.get('n')
            if n is None:
                continue  # Ungültige Node überspringen

            main_id = getattr(n, "identity", None)
            if main_id is None:
                continue

            if main_id not in main_nodes:
                main_nodes[main_id] = {"nodes": {}, "adjacent": set(), "relations": []}
                print(f"  Neuer main_node bucket (single): {main_id}")

            bucket = main_nodes[main_id]
            props = dict(n)
            label_map = bucket.setdefault("nodes", {}).setdefault(main_label, {})
            label_map[main_id] = {"props": props, "min_dist": 0}
            print(f"  -> Einzelnode gespeichert: {main_id} mit props {list(props.keys())}")

    except Exception as e:
        print(f"Fehler beim Sammeln einzelner Nodes: {e}")

def process_paths(results, main_label, selected_labels, filter_labels=None):
    """
    Verarbeitet alle Pfade, sammelt Nodes und Relations pro main_node.

    Args:
        results (list): Liste der Pfad-Ergebnisse, jedes Element enthält 'p' als Path-Objekt.
        main_label (str): Label, das als Hauptknoten identifiziert wird.
        selected_labels (list/set): Labels, die für Nodes berücksichtigt werden.
        filter_labels (list/set, optional): Optional weitere Filterlabels für Nodes.

    Returns:
        dict: main_nodes Dictionary mit gesammelten Nodes, Adjacent-IDs und Relations.
    """
    main_nodes = {}

    for path_idx, r in enumerate(results):
        path = r['p']
        print(f"\n--- Verarbeitung Pfad {path_idx+1}/{len(results)} ---")
        node_list = list(path.nodes)
        print("Pfad-Nodes (id: labels):", [(n.identity, list(n.labels)) for n in node_list])

        # Finde main nodes in diesem Pfad
        main_positions = [(i, n) for i, n in enumerate(node_list) if main_label in n.labels]
        if not main_positions:
            print("  Kein main_label in diesem Pfad gefunden -> überspringe")
            continue
        print("  Main positions in path:", [(i, n.identity) for i, n in main_positions])

        for main_index, main_node in main_positions:
            main_id = main_node.identity
            if main_id not in main_nodes:
                main_nodes[main_id] = {"nodes": {}, "adjacent": set(), "relations": []}
                print(f"  Neuer main_node bucket: {main_id}")

            bucket = main_nodes[main_id]

            # Nodes sammeln
            collect_nodes_for_bucket(bucket, node_list, main_index, selected_labels, filter_labels)

            # Relations sammeln
            collect_relations_for_bucket(bucket, main_id, path.relationships)

    return main_nodes

def fn_debug_collect(label, data):
    print(f"DEBUG [collect_nodes]: {label}: {data}")


def fn_get_node_id(node):
    node_id = getattr(node, "identity", None)
    fn_debug_collect("Node ID", node_id)
    return node_id


def fn_filter_labels(node, selected_labels, filter_labels=None):
    node_labels = [label for label in getattr(node, "labels", []) if label in selected_labels]
    if filter_labels:
        node_labels = [label for label in node_labels if label in filter_labels]
    fn_debug_collect("Filtered labels", node_labels)
    return node_labels


def fn_calculate_distance(idx, main_index):
    dist = abs(idx - main_index)
    fn_debug_collect(f"Distance for index {idx}", dist)
    return dist


def fn_store_or_update_node(bucket_nodes, node_id, label, props, dist):
    label_map = bucket_nodes.setdefault(label, {})
    existing = label_map.get(node_id)

    if existing is None:
        label_map[node_id] = {"props": props, "min_dist": dist}
        fn_debug_collect("Storing new node", {"node_id": node_id, "label": label, "dist": dist})
    else:
        if dist < existing.get("min_dist", float('inf')):
            existing["min_dist"] = dist
            existing["props"] = props
            fn_debug_collect("Updating node", {"node_id": node_id, "label": label, "dist": dist})
    return


def collect_nodes_for_bucket(bucket, node_list, main_index, selected_labels, filter_labels=None):
    """
    Sammelt Nodes in einem Bucket und aktualisiert min_dist, falls nötig.

    Args:
        bucket (dict): Bucket mit bucket["nodes"] als dict.
        node_list (iterable): Liste der Nodes (z.B. aus Neo4j).
        main_index (int): Index des Hauptknotens.
        selected_labels (set/list): Labels, die berücksichtigt werden sollen.
        filter_labels (set/list, optional): Zusätzliche Filterlabels.
    """
    try:
        bucket_nodes = bucket.setdefault("nodes", {})
        fn_debug_collect("Starting bucket collection", {"main_index": main_index, "selected_labels": selected_labels, "filter_labels": filter_labels})

        for idx, node in enumerate(node_list):
            fn_debug_collect("Processing node index", idx)

            node_id = fn_get_node_id(node)
            if node_id is None:
                fn_debug_collect("Skipping invalid node", node)
                continue

            node_labels = fn_filter_labels(node, selected_labels, filter_labels)
            if not node_labels:
                fn_debug_collect("No matching labels, skipping node", node_id)
                continue

            dist = fn_calculate_distance(idx, main_index)
            props = dict(node)  # Annahme: Node ist dict-like
            fn_debug_collect("Node props", props)

            for label in node_labels:
                fn_store_or_update_node(bucket_nodes, node_id, label, props, dist)

        fn_debug_collect("Finished bucket collection", bucket_nodes)

    except Exception as e:
        print(f"Fehler beim Sammeln von Nodes für bucket: {e}")


def collect_relations_for_bucket(bucket, main_id, relationships):
    """
    Sammelt die Relations und Adjacent-Nodes für einen Bucket.

    Args:
        bucket (dict): Ein einzelner Bucket, enthält 'relations' (list) und 'adjacent' (set).
        main_id (int/str): Die ID des Hauptknotens.
        relationships (iterable): Iterable von Relationship-Objekten (z.B. path.relationships).

    Modifiziert:
        bucket["relations"]: Fügt neue Relations hinzu, falls sie noch nicht existieren.
        bucket["adjacent"]: Fügt IDs von benachbarten Nodes hinzu.
    """
    try:
        for rel in relationships:
            from_id = getattr(rel.start_node, 'identity', None)
            to_id = getattr(rel.end_node, 'identity', None)
            if from_id is None or to_id is None:
                continue  # Ungültige Relation überspringen

            rel_dict = {"fromId": from_id, "toId": to_id, "relation": type(rel).__name__}

            if rel_dict not in bucket.get("relations", []):
                bucket.setdefault("relations", []).append(rel_dict)

            bucket.setdefault("adjacent", set())
            if from_id == main_id:
                bucket["adjacent"].add(to_id)
            if to_id == main_id:
                bucket["adjacent"].add(from_id)

    except Exception as e:
        print(f"Fehler beim Sammeln der Relations für bucket {main_id}: {e}")

def build_cells_for_bucket(bucket, columns):
    """
    Erzeugt die Zellen für einen einzelnen Bucket.

    Args:
        bucket (dict): Ein einzelner Bucket aus main_nodes.
        columns (list): Liste der Spaltendefinitionen, jede mit "nodeType" und "property".

    Returns:
        list: Liste von Zellen im Format {"value": ..., "nodeId": ...}
    """
    cells = []
    try:
        for col in columns:
            label = col.get("nodeType")
            prop = col.get("property")
            nodes_map = bucket.get("nodes", {}).get(label, {})
            chosen_node_id = None
            chosen_info = None

            if nodes_map:
                adjacent_candidates = {nid: info for nid, info in nodes_map.items() if nid in bucket.get("adjacent", {})}
                if adjacent_candidates:
                    chosen_node_id, chosen_info = min(
                        adjacent_candidates.items(), key=lambda x: x[1].get("min_dist", float('inf'))
                    )
                else:
                    chosen_node_id, chosen_info = min(
                        nodes_map.items(), key=lambda x: x[1].get("min_dist", float('inf'))
                    )

            if chosen_info:
                val = chosen_info.get("props", {}).get(prop)
                cells.append({"value": val if val is not None else None, "nodeId": chosen_node_id})
            else:
                cells.append({"value": None, "nodeId": None})

    except Exception as e:
        print(f"Fehler beim Erstellen der Zellen für bucket: {e}")
        # Optional: fallback, leere Zellen erzeugen
        cells = [{"value": None, "nodeId": None} for _ in columns]

    return cells

@app.route("/api/add_property_to_nodes", methods=["POST"])
@test_if_deleted_db
def add_property_to_nodes():
    if not graph:
        return jsonify({"error": "No database connection"}), 500

    data = request.get_json(force=True)

    label = data.get("label")
    property_name = data.get("property")
    value = data.get("value", None)
    return_nodes = data.get("return_nodes", False)

    if not label or not isinstance(label, str) or not label.isidentifier():
        return jsonify({"error": f"Invalid label name: {label}"}), 400
    if not property_name or not isinstance(property_name, str) or not property_name.isidentifier():
        return jsonify({"error": f"Invalid property name: {property_name}"}), 400

    query = f"""
        MATCH (n:`{label}`)
        WHERE n.{property_name} IS NULL
        SET n.{property_name} = $value
        RETURN id(n) AS id
    """

    try:
        result = graph.run(query, value=value).data()
        updated_ids = [r["id"] for r in result]
        response = {"updated": len(updated_ids)}
        if return_nodes:
            response["nodes"] = updated_ids
        return jsonify(response)
    except Exception as e:
        logging.error(f"Error adding property: {e}", exc_info=True)
        return (
            jsonify({
                "error": str(e),
                "query": query,
                "params": {"value": value}
            }),
            500,
        )


def fn_debug(label, data):
    print(f"DEBUG [reset_load]: {label}: {data}")

def fn_clear_database():
    query = """
        MATCH (n)
        DETACH DELETE n
    """
    fn_debug("Clearing database", "Deleting all nodes and relationships")
    graph.run(query)
    fn_debug("Database cleared", "All nodes and relationships removed")

def fn_create_person(vorname, nachname):
    query = """
        CREATE (p:Person {vorname:$vorname, nachname:$nachname})
        RETURN ID(p) AS id
    """
    fn_debug("Creating person", {"vorname": vorname, "nachname": nachname})
    result = graph.run(query, vorname=vorname, nachname=nachname).data()
    person_id = result[0]["id"]
    fn_debug("Created person ID", person_id)
    return person_id

def fn_create_city(stadt):
    query = """
        MERGE (s:Stadt {name:$stadt})
        RETURN ID(s) AS id
    """
    fn_debug("Creating or merging city", stadt)
    result = graph.run(query, stadt=stadt).data()
    city_id = result[0]["id"]
    fn_debug("City ID", city_id)
    return city_id

def fn_create_address(street, plz):
    query = """
        CREATE (o:Ort {straße:$street, plz:$plz})
        RETURN ID(o) AS id
    """
    fn_debug("Creating address", {"straße": street, "plz": plz})
    result = graph.run(query, street=street, plz=plz).data()
    addr_id = result[0]["id"]
    fn_debug("Address ID", addr_id)
    return addr_id

def fn_create_book(title, year):
    query = """
        CREATE (b:Buch {titel:$title, erscheinungsjahr:$year})
        RETURN ID(b) AS id
    """
    fn_debug("Creating book", {"titel": title, "erscheinungsjahr": year})
    result = graph.run(query, title=title, year=year).data()
    book_id = result[0]["id"]
    fn_debug("Book ID", book_id)
    return book_id

def fn_create_relation_has_written(person_vorname, person_nachname, book_title):
    query = """
        MATCH (p:Person {vorname:$vorname, nachname:$nachname})
        MATCH (b:Buch {titel:$title})
        MERGE (p)-[:HAT_GESCHRIEBEN]->(b)
    """
    fn_debug("Creating HAT_GESCHRIEBEN relation", {"person": f"{person_vorname} {person_nachname}", "buch": book_title})
    graph.run(query, vorname=person_vorname, nachname=person_nachname, title=book_title)

def fn_create_relation_lives_in(person_vorname, person_nachname, ort_id):
    query = """
        MATCH (p:Person {vorname:$vorname, nachname:$nachname})
        MATCH (o:Ort) WHERE ID(o)=$ort_id
        MERGE (p)-[:WOHNT_IN]->(o)
    """
    fn_debug("Creating WOHNT_IN relation", {"person": f"{person_vorname} {person_nachname}", "ort_id": ort_id})
    graph.run(query, vorname=person_vorname, nachname=person_nachname, ort_id=ort_id)

def fn_create_relation_located_in(ort_id, stadt_name):
    query = """
        MATCH (o:Ort) WHERE ID(o)=$ort_id
        MATCH (s:Stadt {name:$stadt})
        MERGE (o)-[:LIEGT_IN]->(s)
    """
    fn_debug("Creating LIEGT_IN relation", {"ort_id": ort_id, "stadt": stadt_name})
    graph.run(query, ort_id=ort_id, stadt=stadt_name)

# ===============================
# Flask Route
# ===============================

@app.route('/api/reset_and_load_data')
def api_reset_and_load_data():
    try:
        fn_debug("Start API", "Reset and load data endpoint called")

        # 1. Clear DB
        fn_clear_database()

        # 2. Person & Address Data
        person_data = [
            {"vorname": "Maria", "nachname": "Müller", "straße": "Hauptstraße 1", "stadt": "Berlin", "plz": "10115"},
            {"vorname": "Hans", "nachname": "Schmidt", "straße": "Marktplatz 5", "stadt": "Hamburg", "plz": "20095"},
            {"vorname": "Anna", "nachname": "Fischer", "straße": "Bahnhofsallee 12", "stadt": "München", "plz": "80331"},
            {"vorname": "Bob", "nachname": "Johnson", "straße": "Unbekannt", "stadt": "Unbekannt", "plz": "00000"},
            {"vorname": "Charlie", "nachname": "Brown", "straße": "Unbekannt", "stadt": "Unbekannt", "plz": "00000"}
        ]

        ort_ids = {}
        for person in person_data:
            fn_create_person(person["vorname"], person["nachname"])
            ort_id = fn_create_address(person["straße"], person["plz"])
            ort_ids[f"{person['vorname']}_{person['nachname']}"] = ort_id
            fn_create_relation_lives_in(person["vorname"], person["nachname"], ort_id)
            fn_create_relation_located_in(ort_id, person["stadt"])

        # 3. Books
        book_data = [
            {"titel": "The Cypher Key", "erscheinungsjahr": 2023, "vorname": "Maria", "nachname": "Müller"},
            {"titel": "The Graph Odyssey", "erscheinungsjahr": 2022, "vorname": "Bob", "nachname": "Johnson"},
            {"titel": "Neo's Journey", "erscheinungsjahr": 2024, "vorname": "Charlie", "nachname": "Brown"}
        ]

        for book in book_data:
            fn_create_book(book["titel"], book["erscheinungsjahr"])
            fn_create_relation_has_written(book["vorname"], book["nachname"], book["titel"])

        fn_debug("Finished API", "Database reset and data loaded successfully")
        return jsonify({"status": "success", "message": "Database cleared and data loaded successfully"})

    except Exception as e:
        fn_debug("Exception in reset_and_load_data", e)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/api/dump_database")
def api_dump_database():
    try:
        fn_debug("Start API", "Dumping database")

        query_nodes = """
            MATCH (n)
            RETURN id(n) AS id, labels(n) AS labels, properties(n) AS props
        """
        query_rels = """
            MATCH (a)-[r]->(b)
            RETURN id(r) AS id, type(r) AS type,
                   id(a) AS start_id, id(b) AS end_id,
                   properties(r) AS props
        """

        nodes = graph.run(query_nodes).data()
        rels = graph.run(query_rels).data()

        dump = {
            "nodes": nodes,
            "relationships": rels
        }

        fn_debug("Finished API", f"Dumped {len(nodes)} nodes and {len(rels)} relationships")
        return jsonify(dump)

    except Exception as e:
        logging.error(f"Error dumping database: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == '__main__':
    try:
        app.run(debug=True)
    except (KeyboardInterrupt, OSError):
        print("You pressed CTRL-C")
        sys.exit(0)
