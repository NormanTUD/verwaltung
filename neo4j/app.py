import sys
import csv
import io
import json
from flask import Flask, request, jsonify, render_template, session
from dotenv import load_dotenv
import oasis_helper
from api.get_data_as_table import create_get_data_bp
from api.dump_database import create_dump_database_bp
from api.reset_and_load_data import create_reset_and_load_data_bp
from api.delete_node import create_delete_node_bp
from api.delete_nodes import create_delete_nodes_bp
from api.create_node import create_create_node_bp
from api.add_property_to_nodes import create_add_property_to_nodes_bp
from api.delete_all import create_delete_all_bp
from api.graph_data import create_graph_data_bp
from api.update_node import create_update_node_bp
from api.add_row import create_add_row_bp
from api.add_column import create_add_column_bp
from api.update_nodes import create_update_nodes_bp
from api.save_queries import create_save_queries
from api.add_relationship import create_add_relationship_bp
from api.reset_and_load_complex_data import create_complex_data_bp
from api.labels import create_labels_bp
from api.properties import create_properties_bp
from api.relationships import create_relationships_bp

from index_manager import create_index_bp

load_dotenv()
app = Flask(__name__)
app.secret_key = oasis_helper.load_or_generate_secret_key()

graph = oasis_helper.get_graph_db_connection()

app.config['GRAPH'] = graph

app.register_blueprint(create_get_data_bp(graph), url_prefix='/api')
app.register_blueprint(create_dump_database_bp(graph), url_prefix='/api')
app.register_blueprint(create_reset_and_load_data_bp(graph), url_prefix='/api')
app.register_blueprint(create_delete_node_bp(graph), url_prefix='/api')
app.register_blueprint(create_add_property_to_nodes_bp(graph), url_prefix='/api')
app.register_blueprint(create_delete_nodes_bp(graph), url_prefix='/api')
app.register_blueprint(create_create_node_bp(graph), url_prefix='/api')
app.register_blueprint(create_delete_all_bp(graph), url_prefix='/api')
app.register_blueprint(create_graph_data_bp(graph), url_prefix='/api')
app.register_blueprint(create_update_node_bp(graph), url_prefix='/api')
app.register_blueprint(create_add_row_bp(graph), url_prefix='/api')
app.register_blueprint(create_add_column_bp(graph), url_prefix='/api')
app.register_blueprint(create_save_queries(graph), url_prefix='/api')
app.register_blueprint(create_update_nodes_bp(graph), url_prefix='/api')
app.register_blueprint(create_add_relationship_bp(graph), url_prefix='/api')
app.register_blueprint(create_complex_data_bp(graph), url_prefix='/api')
app.register_blueprint(create_labels_bp(graph), url_prefix='/api')
app.register_blueprint(create_properties_bp(graph), url_prefix='/api')
app.register_blueprint(create_relationships_bp(graph), url_prefix='/api')

app.register_blueprint(create_index_bp(graph), url_prefix='/')

@app.route('/')
def index():
    return render_template('import.html')

@app.route('/graph')
def show_graph():
    return render_template('graph.html')

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
    """Hauptfunktion: speichert die zugeordneten Daten in Neo4j."""
    mapping_data = request.get_json()

    if not graph:
        print("Fehler: Datenbank nicht verbunden.")
        return jsonify({"status": "error", "message": "Datenbank nicht verbunden."}), 500

    if 'raw_data' not in session:
        return jsonify({"status": "error", "message": "raw_data not in session."}), 500

    reader = parse_csv_from_session()
    if reader is None:
        return jsonify({"status": "error", "message": "Fehler beim Analysieren der CSV-Daten."}), 400

    tx = graph.begin()
    try:
        for i, row in enumerate(reader):
            #print(f"\n--- Bearbeite Zeile {i+1} ---")
            process_row(tx, row, mapping_data)

        graph.commit(tx)
        #print("\nGesamtvorgang erfolgreich: Daten wurden in die Neo4j-Datenbank importiert.")
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
        #print(f"  ❌ Keine Daten für den Knoten-Typ '{node_type}' in dieser Zeile. Überspringe.")
        return None

    identifier_key, identifier_value = next(iter(all_props.items()))
    #print(f"  ➡️ Versuche, einen Knoten vom Typ '{node_type}' zu mergen.")
    #print(f"     Identifikator: '{identifier_key}' = '{identifier_value}'")
    #print(f"     Alle Properties: {all_props}")

    cypher_query = f"""
    MERGE ({node_var}:{node_label} {{`{identifier_key}`: $identifier_value}})
    ON CREATE SET {node_var} = $all_props
    RETURN {node_var}
    """

    params = {"identifier_value": identifier_value, "all_props": all_props}
    result = graph.run(cypher_query, **params).data()

    if result:
        #print(f"  ✅ Knoten '{node_type}' erfolgreich gemerged.")
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

    #print(f"  ➡️ Versuche, eine Beziehung '{rel_type}' zu erstellen.")

    if from_node_type in nodes_created and to_node_type in nodes_created:
        from_node = nodes_created[from_node_type]
        to_node = nodes_created[to_node_type]

        rel_query = f"""
        MATCH ({from_var}:`{from_node_type}`) WHERE id({from_var}) = {from_node.identity}
        MATCH ({to_var}:`{to_node_type}`) WHERE id({to_var}) = {to_node.identity}
        MERGE ({from_var})-[rel:{rel_label}]->({to_var})
        """
        graph.run(rel_query)
        #print(f"  ✅ Beziehung '{clean_rel_type}' zwischen '{from_node_type}' und '{to_node_type}' erstellt.")
    #else:
        #print(f"  ❌ Beziehung konnte nicht erstellt werden, Knoten fehlen: '{from_node_type}' (vorhanden: {from_node_type in nodes_created}), '{to_node_type}' (vorhanden: {to_node_type in nodes_created}).")

def get_all_nodes_and_relationships():
    """Holt alle Node-Typen und Relationship-Typen aus der Datenbank."""
    node_labels = graph.run("CALL db.labels()").data()
    relationship_types = graph.run("CALL db.relationshipTypes()").data()
    return {
        "labels": [label['label'] for label in node_labels],
        "types": [rel['relationshipType'] for rel in relationship_types]
    }

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

if __name__ == '__main__':
    try:
        app.run(debug=True)
    except (KeyboardInterrupt, OSError):
        print("You pressed CTRL-C")
        sys.exit(0)
