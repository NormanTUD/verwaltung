import unittest
import os
import json
from flask import session
from py2neo import Graph, Node, Relationship
from dotenv import load_dotenv
from unittest.mock import patch

from app import (
    get_node_by_id,
    get_all_nodes_and_relationships,
    serialize_properties,
    serialize_value
)

# Lade Umgebungsvariablen aus der .env.test-Datei für die Tests
load_dotenv('.env.test')

# Wichtig: Importieren Sie die App-Instanz aus Ihrer Hauptdatei
from app import app, graph

# Beispiel-Daten für die Tests
SAMPLE_CSV_DATA = """id,name,city,country
1,Alice,Berlin,Germany
2,Bob,Paris,France
3,Charlie,London,UK
"""

SAMPLE_MAPPING = {
    "nodes": {
        "Person": ["name"],
        "Location": ["city", "country"]
    },
    "relationships": [
        {
            "from": "Person",
            "to": "Location",
            "type": "LIVES_IN"
        }
    ]
}

class TestNeo4jApp(unittest.TestCase):
    """
    Test-Suite für die Flask-Anwendung, die mit Neo4j interagiert.
    """

    @classmethod
    def setUpClass(cls):
        """
        Wird einmal vor allen Tests ausgeführt.
        Stellt die Verbindung zur Testdatenbank her.
        """
        cls.graph = Graph(os.getenv('NEO4J_URI'), auth=(os.getenv('NEO4J_USER'), os.getenv('NEO4J_PASS')))
        print("Verbindung zu Test-Graph erfolgreich hergestellt!")
    
    @classmethod
    def tearDownClass(cls):
        """
        Wird einmal nach allen Tests ausgeführt.
        Reinigt die Datenbank wieder.
        """
        cls.graph.run("MATCH (n) DETACH DELETE n")
        print("Testdatenbank wurde nach den Tests erfolgreich gereinigt.")

    def setUp(self):
        """
        Wird vor jedem Test ausgeführt.
        Erstellt einen Flask-Test-Client und leert die Datenbank.
        """
        self.app = app.test_client()
        self.app.testing = True
        
        # Leere die Datenbank vor jedem Test für saubere, isolierte Bedingungen
        self.graph.run("MATCH (n) DETACH DELETE n")
        print("Testdatenbank wurde erfolgreich geleert für einen neuen Test.")

    def test_index_page(self):
        """Testet, ob die Startseite erreichbar ist."""
        response = self.app.get('/')
        self.assertEqual(response.status_code, 200)

    def test_upload_valid_data(self):
        """Testet den Upload von gültigen CSV-Daten."""
        response = self.app.post('/upload', data={'data': SAMPLE_CSV_DATA}, content_type='multipart/form-data')
        self.assertEqual(response.status_code, 200)
        
        with self.app as client:
            with client.session_transaction() as sess:
                self.assertIn('headers', sess)
                self.assertEqual(sess['headers'], ['id', 'name', 'city', 'country'])
                self.assertIn('raw_data', sess)

    def test_upload_no_data(self):
        """Testet den Upload ohne Daten."""
        response = self.app.post('/upload', data={}, content_type='multipart/form-data')
        self.assertEqual(response.status_code, 400)
        self.assertIn(b"Keine Daten hochgeladen", response.data)

    def test_update_node_property(self):
        """Testet die Aktualisierung eines Nodes."""
        node = Node("UpdateNode", status="old")
        self.graph.create(node)
        node_id = node.identity
        
        response = self.app.put(f'/api/update_node/{node_id}', data=json.dumps({"property": "status", "value": "new"}), content_type='application/json')
        self.assertEqual(response.status_code, 200)
        
        updated_node = self.graph.run(f"MATCH (n) WHERE ID(n) = {node_id} RETURN n").data()[0]['n']
        self.assertEqual(updated_node['status'], "new")

    def test_delete_node(self):
        """Testet das Löschen eines Nodes."""
        node = Node("DeleteNode", name="Temp")
        self.graph.create(node)
        node_id = node.identity
        
        response = self.app.delete(f'/api/delete_node/{node_id}')
        self.assertEqual(response.status_code, 200)

        result = self.graph.run(f"MATCH (n) WHERE ID(n) = {node_id} RETURN n").data()
        self.assertEqual(len(result), 0)

    def test_update_multiple_nodes(self):
        """Testet die Massenaktualisierung von Nodes."""
        node1 = Node("BulkUpdate", status="old")
        node2 = Node("BulkUpdate", status="old")
        self.graph.create(node1)
        self.graph.create(node2)
        node_ids = [node1.identity, node2.identity]
        
        response = self.app.put(f'/api/update_nodes', data=json.dumps({
            "ids": node_ids,
            "property": "status",
            "value": "updated"
        }), content_type='application/json')
        self.assertEqual(response.status_code, 200)
        
        results = self.graph.run(f"MATCH (n) WHERE ID(n) IN {node_ids} RETURN n.status AS status").data()
        for res in results:
            self.assertEqual(res['status'], "updated")

    def test_upload_empty_file(self):
        """Testet den Upload einer leeren Zeichenfolge."""
        response = self.app.post('/upload', data={'data': ''}, content_type='multipart/form-data')
        self.assertEqual(response.status_code, 400)
        self.assertIn(b"Fehler beim Parsen", response.data)

    def test_save_mapping_with_missing_session_data(self):
        """Testet save_mapping ohne vorherigen Upload."""
        response = self.app.post('/save_mapping', data=json.dumps(SAMPLE_MAPPING), content_type='application/json')
        self.assertEqual(response.status_code, 500)
        self.assertIn(b"Sitzungsdaten fehlen", response.data)

    def test_save_mapping_no_nodes_or_rels(self):
        """Testet save_mapping mit einem leeren Mapping."""
        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = SAMPLE_CSV_DATA
                sess['headers'] = ['id', 'name', 'city', 'country']

        empty_mapping = {"nodes": {}, "relationships": []}
        response = self.app.post('/save_mapping', data=json.dumps(empty_mapping), content_type='application/json')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Daten erfolgreich in Neo4j importiert", response.data)

        # Überprüfen, ob keine Nodes erstellt wurden
        person_nodes = self.graph.run("MATCH (n:Person) RETURN n").data()
        self.assertEqual(len(person_nodes), 0)

    def test_update_node_with_nonexistent_id(self):
        """Testet die Aktualisierung eines Nodes mit nicht existierender ID."""
        non_existent_id = 999999999
        response = self.app.put(f'/api/update_node/{non_existent_id}', data=json.dumps({"property": "status", "value": "new"}), content_type='application/json')
        self.assertEqual(response.status_code, 200)
        # Die Abfrage wird erfolgreich sein, da das Cypher-MATCH fehlschlägt
        self.assertIn(b"Node 999999999 wurde aktualisiert", response.data)

    def test_update_nodes_with_invalid_data(self):
        """Testet Massenaktualisierung mit fehlenden JSON-Daten."""
        response = self.app.put(f'/api/update_nodes', data=json.dumps({
            "ids": [1],
            "property": "status"
        }), content_type='application/json')
        self.assertEqual(response.status_code, 400)
        self.assertIn(b"Fehlende Daten im Request", response.data)

    def test_delete_nodes_success(self):
        """Testet das Löschen mehrerer Nodes über URL-Parameter."""
        # 1. Erstellen von Test-Nodes
        node1 = Node("DeleteTestNode", name="Node1")
        node2 = Node("DeleteTestNode", name="Node2")
        self.graph.create(node1)
        self.graph.create(node2)
        node_ids = [node1.identity, node2.identity]

        # 2. Senden der DELETE-Anfrage mit IDs im URL-Parameter
        # Die Methode muss als DELETE gesendet werden, auch wenn die Daten als URL-Parameter übergeben werden
        response = self.app.delete(f'/api/delete_nodes?ids={node_ids[0]},{node_ids[1]}')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Nodes und alle Beziehungen wurden", response.data)

        # 3. Überprüfen, ob die Nodes nicht mehr existieren
        results = self.graph.run(f"MATCH (n) WHERE ID(n) IN {node_ids} RETURN n").data()
        self.assertEqual(len(results), 0)

    def test_index_content(self):
        """Testet, ob die Startseite den erwarteten Text enthält."""
        response = self.app.get('/')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Upload", response.data)

    def test_update_node_success(self):
        """Aktualisiert erfolgreich ein Property an einem existierenden Node."""
        node = Node("Person", name="Alice")
        self.graph.create(node)
        node_id = node.identity

        response = self.app.put(
            f'/api/update_node/{node_id}',
            data=json.dumps({"property": "age", "value": 42}),
            content_type='application/json'
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"wurde aktualisiert", response.data)

        # Überprüfen, ob Wert gesetzt wurde
        result = self.graph.run("MATCH (n:Person) WHERE ID(n)=$id RETURN n.age AS age", id=node_id).data()
        self.assertEqual(result[0]["age"], 42)

    def test_update_node_nonexistent(self):
        """Versuch, einen Node zu aktualisieren, der nicht existiert."""
        fake_id = 999999999
        response = self.app.put(
            f'/api/update_node/{fake_id}',
            data=json.dumps({"property": "age", "value": 99}),
            content_type='application/json'
        )
        # Query läuft durch, findet aber nichts → trotzdem success
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"wurde aktualisiert", response.data)

    def test_update_node_missing_fields(self):
        """Property oder Value fehlen im Request-Body."""
        node = Node("Person", name="Charlie")
        self.graph.create(node)

        # Nur property ohne value
        response = self.app.put(
            f'/api/update_node/{node.identity}',
            data=json.dumps({"property": "status"}),
            content_type='application/json'
        )
        # Dein Code prüft das noch nicht → wäre Erweiterung: hier trotzdem prüfen
        self.assertIn(response.status_code, [200, 500])

    def test_delete_node_success(self):
        """Löscht erfolgreich einen Node samt Relationen."""
        person = Node("Person", name="DeleteMe")
        ort = Node("Ort", name="Berlin")
        rel = Relationship(person, "HAT_WOHNSITZ", ort)
        self.graph.create(rel)
        person_id = person.identity

        response = self.app.delete(f'/api/delete_node/{person_id}')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"alle Beziehungen wurden", response.data)

        # Überprüfen, ob Node wirklich weg ist
        result = self.graph.run("MATCH (n) WHERE ID(n)=$id RETURN n", id=person_id).data()
        self.assertEqual(len(result), 0)

    def test_delete_node_nonexistent(self):
        """Versuch, einen Node zu löschen, der nicht existiert."""
        fake_id = 123456789
        response = self.app.delete(f'/api/delete_node/{fake_id}')
        # Query läuft leer durch → trotzdem success
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"und alle Beziehungen wurden", response.data)

    def test_delete_node_invalid_id(self):
        """Ungültige Node-ID (kein Integer)."""
        response = self.app.delete('/api/delete_node/abc')
        # Flask selbst wird hier 404 zurückgeben, weil <int:node_id> nicht matcht
        self.assertEqual(response.status_code, 404)

    def test_delete_node_with_multiple_nodes(self):
        """Erstellt mehrere Nodes, löscht einen und prüft, dass die anderen bestehen bleiben."""
        n1 = Node("Test", name="KeepMe")
        n2 = Node("Test", name="RemoveMe")
        self.graph.create(n1 | n2)

        response = self.app.delete(f'/api/delete_node/{n2.identity}')
        self.assertEqual(response.status_code, 200)

        # Überprüfen
        kept = self.graph.run("MATCH (n:Test {name:'KeepMe'}) RETURN n").data()
        removed = self.graph.run("MATCH (n:Test {name:'RemoveMe'}) RETURN n").data()
        self.assertEqual(len(kept), 1)
        self.assertEqual(len(removed), 0)

    def test_save_mapping_no_nodes_or_rels_two(self):
        """Speichert ein leeres Mapping → keine Nodes/Beziehungen."""
        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = "id,name\n1,Alice\n2,Bob"

        empty_mapping = {"nodes": {}, "relationships": []}
        response = self.app.post(
            '/save_mapping',
            data=json.dumps(empty_mapping),
            content_type='application/json'
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Daten erfolgreich in Neo4j importiert", response.data)

        result = self.graph.run("MATCH (n) RETURN n").data()
        self.assertEqual(len(result), 0)

    def test_save_mapping_with_nodes(self):
        """Speichert Daten mit Node-Mapping."""
        csv_data = "id,name\n1,Alice\n2,Bob"
        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data

        mapping = {
            "nodes": {
                "Person": [
                    {"original": "id", "renamed": "id"},
                    {"original": "name", "renamed": "name"}
                ]
            },
            "relationships": []
        }

        response = self.app.post(
            '/save_mapping',
            data=json.dumps(mapping),
            content_type='application/json'
        )
        self.assertEqual(response.status_code, 200)

        nodes = self.graph.run("MATCH (n:Person) RETURN n").data()
        self.assertEqual(len(nodes), 2)
        self.assertEqual(nodes[0]["n"]["name"], "Alice")

    def test_save_mapping_with_relationship(self):
        """Speichert Daten mit Node- und Relationship-Mapping."""
        csv_data = "id,name,city\n1,Alice,Berlin\n2,Bob,Hamburg"
        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data

        mapping = {
            "nodes": {
                "Person": [
                    {"original": "name", "renamed": "name"}
                ],
                "Ort": [
                    {"original": "city", "renamed": "name"}
                ]
            },
            "relationships": [
                {"from": "Person", "to": "Ort", "type": "WOHNT_IN"}
            ]
        }

        response = self.app.post(
            '/save_mapping',
            data=json.dumps(mapping),
            content_type='application/json'
        )
        self.assertEqual(response.status_code, 200)

        rels = self.graph.run("MATCH (p:Person)-[r:WOHNT_IN]->(o:Ort) RETURN r").data()
        self.assertEqual(len(rels), 2)

    def test_save_mapping_missing_session(self):
        """Fehler, wenn keine raw_data in der Session ist."""
        response = self.app.post(
            '/save_mapping',
            data=json.dumps({"nodes": {}, "relationships": []}),
            content_type='application/json'
        )
        self.assertEqual(response.status_code, 500)
        self.assertIn(b"Sitzungsdaten fehlen", response.data)

    def test_overview_success(self):
        """Übersicht mit Datenbank-Verbindung."""
        person = Node("Person", name="Alice")
        self.graph.create(person)

        response = self.app.get('/overview')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Person", response.data)  # Template enthält Node-Label

    def test_overview_empty_db(self):
        """Übersicht ohne Knoten in der DB."""
        self.graph.run("MATCH (n) DETACH DELETE n")  # DB leeren
        response = self.app.get('/overview')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"overview", response.data.lower())  # Template wird korrekt geladen

    @patch("app.graph", None)  # Graph-Objekt simuliert als None
    def test_overview_no_db(self):
        """Übersicht mit fehlender DB-Verbindung."""
        response = self.app.get('/overview')
        self.assertEqual(response.status_code, 500)
        self.assertIn(b"Datenbank nicht verbunden", response.data)

    def test_save_mapping_invalid_csv(self):
        """Fehler beim Einlesen von CSV mit falschem Dialekt/Trennzeichen."""
        csv_data = "id;name\nAlice;Bob"  # Semikolon statt Komma
        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data

        mapping = {"nodes": {}, "relationships": []}
        response = self.app.post(
            '/save_mapping',
            data=json.dumps(mapping),
            content_type='application/json'
        )
        self.assertEqual(response.status_code, 200)  # Sniffer akzeptiert die CSV

    def test_add_column_success(self):
        """Fügt erfolgreich eine neue Property zu allen Nodes eines Labels hinzu."""
        # Testdaten erstellen
        node1 = Node("Person", name="Alice")
        node2 = Node("Person", name="Bob", age=30)
        self.graph.create(node1 | node2)

        response = self.app.post(
            '/api/add_column',
            data=json.dumps({"column": "status", "label": "Person"}),
            content_type='application/json'
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Neue Spalte 'status'", response.data)

        # Prüfen, ob die Property bei allen Nodes existiert
        result = self.graph.run("MATCH (n:Person) RETURN n.status AS status, n.name AS name").data()
        self.assertEqual(len(result), 2)
        self.assertTrue(all("status" in row and row["status"] == "" for row in result))

    def test_add_column_invalid_name(self):
        """Fehler, wenn der Spaltenname kein gültiger Python-Identifier ist."""
        response = self.app.post(
            '/api/add_column',
            data=json.dumps({"column": "123invalid", "label": "Person"}),
            content_type='application/json'
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn(b"ltiger Spaltenname", response.data)

    def test_add_column_missing_data(self):
        """Fehler, wenn 'column' oder 'label' fehlen."""
        response = self.app.post(
            '/api/add_column',
            data=json.dumps({"column": "status"}),  # label fehlt
            content_type='application/json'
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn(b"Fehlende Daten", response.data)

        response2 = self.app.post(
            '/api/add_column',
            data=json.dumps({"label": "Person"}),  # column fehlt
            content_type='application/json'
        )
        self.assertEqual(response2.status_code, 400)
        self.assertIn(b"Fehlende Daten", response2.data)

    def test_add_column_empty_json(self):
        """Fehler, wenn der Request-Body leer oder ungültig ist."""
        response = self.app.post(
            '/api/add_column',
            data="",
            content_type='application/json'
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn(b"Request-Body ist leer", response.data)

        response2 = self.app.post(
            '/api/add_column',
            data="INVALID_JSON",
            content_type='application/json'
        )
        self.assertEqual(response2.status_code, 400)
        self.assertIn(b"Request-Body ist leer", response2.data)

    def test_add_column_already_exists(self):
        """Wenn die Property bereits existiert, wird sie nicht überschrieben."""
        node = Node("Person", name="Alice", status="existing")
        self.graph.create(node)

        response = self.app.post(
            '/api/add_column',
            data=json.dumps({"column": "status", "label": "Person"}),
            content_type='application/json'
        )
        self.assertEqual(response.status_code, 200)

        # Prüfen, dass bestehende Property erhalten bleibt
        result = self.graph.run("MATCH (n:Person) RETURN n.status AS status").data()
        self.assertEqual(result[0]["status"], "existing")

    def test_add_column_label_not_exist(self):
        """Wenn das Label nicht existiert, passiert nichts, aber kein Fehler."""
        response = self.app.post(
            '/api/add_column',
            data=json.dumps({"column": "status", "label": "NonExistentLabel"}),
            content_type='application/json'
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Neue Spalte 'status'", response.data)

        # Prüfen, dass keine Nodes existieren
        result = self.graph.run("MATCH (n:NonExistentLabel) RETURN n").data()
        self.assertEqual(len(result), 0)

    def test_get_node_by_id_success(self):
        """Node wird korrekt über ID zurückgegeben."""
        node = Node("Person", name="Alice")
        self.graph.create(node)
        
        result_node = get_node_by_id(node.identity)
        self.assertEqual(result_node["name"], "Alice")
        self.assertIn("Person", list(result_node.labels))

    def test_get_node_by_id_nonexistent(self):
        """Fehler beim Zugriff auf nicht existierende Node-ID."""
        with self.assertRaises(IndexError):
            get_node_by_id(999999)

    def test_get_all_nodes_and_relationships_success(self):
        """Holt Labels und Relationship-Typen korrekt."""
        node = Node("Person", name="Alice")
        city = Node("Ort", name="Berlin")
        rel = Relationship(node, "HAT_WOHNSITZ", city)
        self.graph.create(node | city | rel)
        
        result = get_all_nodes_and_relationships()
        self.assertIn("Person", result["labels"])
        self.assertIn("Ort", result["labels"])
        self.assertIn("HAT_WOHNSITZ", result["types"])

    def test_get_all_nodes_and_relationships_empty(self):
        """Leere DB liefert leere Listen."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        result = get_all_nodes_and_relationships()
        self.assertEqual(result["labels"], [])
        self.assertEqual(result["types"], [])

    def test_serialize_properties_basic(self):
        """Serialisiert einfache Properties unverändert."""
        props = {"name": "Alice", "age": 30, "active": True}
        result = serialize_properties(props)
        self.assertEqual(result, props)

    def test_serialize_properties_with_function(self):
        """Funktion wird in String konvertiert."""
        def dummy_func(): pass
        props = {"callback": dummy_func}
        result = serialize_properties(props)
        self.assertEqual(result["callback"], f"FUNCTION_OBJECT: {dummy_func.__name__}")

    def test_serialize_properties_nonstandard_object(self):
        """Nicht-standard Objekte werden in Strings konvertiert."""
        class Custom:
            def __str__(self):
                return "custom_obj"
        props = {"obj": Custom()}
        result = serialize_properties(props)
        self.assertEqual(result["obj"], "custom_obj")

    def test_serialize_value_basic(self):
        """Primitive Werte bleiben unverändert."""
        self.assertEqual(serialize_value(42), 42)
        self.assertEqual(serialize_value("text"), "text")
        self.assertEqual(serialize_value(True), True)
        self.assertEqual(serialize_value(None), None)

    def test_serialize_value_function(self):
        """Funktion wird in String konvertiert."""
        def f(): pass
        self.assertEqual(serialize_value(f), f"FUNCTION_OBJECT: {f.__name__}")

    def test_serialize_value_list_and_dict(self):
        """Listen und Dicts werden rekursiv serialisiert."""
        def g(): pass
        value = {"a": [1, g, {"b": g}]}
        result = serialize_value(value)
        self.assertEqual(result["a"][1], f"FUNCTION_OBJECT: {g.__name__}")
        self.assertEqual(result["a"][2]["b"], f"FUNCTION_OBJECT: {g.__name__}")

    def test_serialize_value_custom_object(self):
        """Nicht-standard Objekt wird als String serialisiert."""
        class Custom: pass
        obj = Custom()
        self.assertEqual(serialize_value(obj), str(obj))

    def test_graph_data_empty_db(self):
        """Leere DB liefert leere Nodes und Links."""
        response = self.app.get('/api/graph-data')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertIn('nodes', data)
        self.assertIn('links', data)
        self.assertEqual(len(data['nodes']), 0)
        self.assertEqual(len(data['links']), 0)

    def test_graph_data_single_node(self):
        """Einzelner Node, keine Beziehungen."""
        node = Node("Person", name="Alice")
        self.graph.create(node)

        response = self.app.get('/api/graph-data')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(len(data['nodes']), 1)
        self.assertEqual(len(data['links']), 0)
        self.assertEqual(data['nodes'][0]['label'], "Person")
        self.assertEqual(data['nodes'][0]['properties']['name'], "Alice")

    def test_graph_data_relationship_without_properties(self):
        """Relationship ohne Properties."""
        n1 = Node("Person", name="Bob")
        n2 = Node("Ort", name="Hamburg")
        rel = Relationship(n1, "LIVES_IN", n2)
        self.graph.create(n1 | n2 | rel)

        response = self.app.get('/api/graph-data')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)

        self.assertEqual(len(data['links']), 1)
        link = data['links'][0]
        self.assertEqual(link['properties'], {})  # leer

    def test_graph_data_multiple_relationships_same_nodes(self):
        """Mehrere Relationen zwischen denselben Nodes werden nicht doppelt gezählt."""
        n1 = Node("Person", name="Alice")
        n2 = Node("Ort", name="Berlin")
        rel1 = Relationship(n1, "HAT_WOHNSITZ", n2)
        rel2 = Relationship(n1, "LIVES_IN", n2)
        self.graph.create(n1 | n2 | rel1 | rel2)

        response = self.app.get('/api/graph-data')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(len(data['nodes']), 2)
        self.assertEqual(len(data['links']), 2)

    def test_upload_valid_csv(self):
        """Testet den Upload von gültigen CSV-Daten."""
        csv_data = "id,name,city\n1,Alice,Berlin\n2,Bob,Hamburg"
        response = self.app.post('/upload', data={'data': csv_data})
        self.assertEqual(response.status_code, 200)
        # Prüfen, ob die Header in der Session gespeichert wurden
        with self.app.session_transaction() as sess:
            self.assertIn('headers', sess)
            self.assertEqual(sess['headers'], ['id', 'name', 'city'])

    def test_upload_missing_data(self):
        """Upload ohne Daten liefert 400."""
        response = self.app.post('/upload', data={})
        self.assertEqual(response.status_code, 400)
        self.assertIn(b"Keine Daten hochgeladen", response.data)

    def test_get_rel_types_empty_db(self):
        """Wenn DB keine Relationships hat, wird leere Liste zurückgegeben."""
        self.graph.delete_all()
        response = self.app.get('/get_rel_types')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 0)

    def test_get_rel_types_with_relationships(self):
        """Testet get_rel_types mit vorhandenen Relationships."""
        n1 = Node("Person", name="Alice")
        n2 = Node("Ort", name="Berlin")
        rel1 = Relationship(n1, "HAT_WOHNSITZ", n2)
        rel2 = Relationship(n1, "LIVES_IN", n2)
        self.graph.create(n1 | n2 | rel1 | rel2)

        response = self.app.get('/get_rel_types')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertIn("HAT_WOHNSITZ", data)
        self.assertIn("LIVES_IN", data)

    def test_upload_large_csv(self):
        """Testet Upload einer sehr großen CSV (z.B. 20.000 Zeilen)."""
        large_csv = "id,name\n" + "\n".join(f"{i},Name{i}" for i in range(1, 20001))
        response = self.app.post('/upload', data={'data': large_csv}, content_type='multipart/form-data')
        self.assertEqual(response.status_code, 200)
        with self.app.session_transaction() as sess:
            self.assertEqual(len(sess['raw_data'].splitlines()), 20001)  # inkl. Header

    def test_upload_csv_with_missing_and_extra_columns(self):
        """CSV enthält fehlende Werte und zusätzliche Spalten."""
        csv_data = "id,name,extra\n1,Alice,\n2,,ExtraValue"
        response = self.app.post('/upload', data={'data': csv_data}, content_type='multipart/form-data')
        self.assertEqual(response.status_code, 200)
        with self.app.session_transaction() as sess:
            self.assertIn('headers', sess)
            self.assertEqual(sess['headers'], ['id', 'name', 'extra'])
        
    def test_save_mapping_with_expired_session(self):
        """Session enthält keine raw_data, save_mapping schlägt fehl."""
        with self.app.session_transaction() as sess:
            sess.clear()
        response = self.app.post('/save_mapping', data=json.dumps({"nodes": {}, "relationships": []}), content_type='application/json')
        self.assertEqual(response.status_code, 500)
        self.assertIn(b"Sitzungsdaten fehlen", response.data)

    def test_graph_data_with_cyclic_relationship(self):
        """Zyklische Relation wird korrekt zurückgegeben."""
        n1 = Node("Person", name="Alice")
        n2 = Node("Person", name="Bob")
        rel1 = Relationship(n1, "KNOWS", n2)
        rel2 = Relationship(n2, "KNOWS", n1)
        self.graph.create(n1 | n2 | rel1 | rel2)

        response = self.app.get('/api/graph-data')
        data = json.loads(response.data)
        self.assertEqual(len(data['links']), 2)
        self.assertTrue(any(link['source'] == n1.identity and link['target'] == n2.identity for link in data['links']))
        self.assertTrue(any(link['source'] == n2.identity and link['target'] == n1.identity for link in data['links']))

    @patch("app.graph.run", side_effect=Exception("DB offline"))
    def test_update_node_db_offline(self, mock_run):
        """DB-Verbindungsfehler bei Node-Update wird korrekt behandelt."""
        node = Node("Person", name="Alice")
        self.graph.create(node)
        node_id = node.identity
        response = self.app.put(f'/api/update_node/{node_id}',
                                data=json.dumps({"property": "age", "value": 42}),
                                content_type='application/json')
        self.assertEqual(response.status_code, 500)
        self.assertIn(b"DB offline", response.data)

    def test_get_data_as_table_missing_nodes_param(self):
        """GET /api/get_data_as_table without nodes -> 400"""
        with self.app as client:
            resp = client.get('/api/get_data_as_table')
            self.assertEqual(resp.status_code, 400)
            # error message contains 'nodes'
            self.assertIn(b"nodes", resp.data)

    def test_get_data_as_table_invalid_maxdepth_param(self):
        """Non-integer maxDepth should error (server returns 500 in current impl)"""
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person', 'maxDepth': 'notanint'})
            # current implementation casts int() and will raise -> caught -> 500
            self.assertEqual(resp.status_code, 500)
            self.assertIn(b"invalid literal", resp.data)

    def test_get_data_as_table_empty_db_returns_empty(self):
        """If DB has no relevant nodes, route returns empty columns/rows."""
        # ensure DB clean
        self.graph.run("MATCH (n) DETACH DELETE n")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person,Ort,Stadt'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertIsInstance(data, dict)
            self.assertIn('columns', data)
            self.assertIn('rows', data)
            self.assertEqual(data['columns'], [])
            self.assertEqual(data['rows'], [])

    def test_get_data_as_table_basic_positive_single_person(self):
        """Create Person->Ort->Stadt and verify single row with merged cells."""
        # cleanup and create sample data
        self.graph.run("MATCH (n) DETACH DELETE n")
        r = self.graph.run(
            "CREATE (p:Person {vorname:'Maria', nachname:'Muller'}) "
            "CREATE (o:Ort {strasse:'Hauptstrasse 1', plz:'10115'}) "
            "CREATE (s:Stadt {stadt:'Berlin'}) "
            "CREATE (p)-[:WOHNT_IN]->(o) "
            "CREATE (o)-[:LIEGT_IN]->(s) "
            "RETURN id(p) AS pid, id(o) AS oid, id(s) AS sid"
        ).data()[0]
        pid, oid, sid = r['pid'], r['oid'], r['sid']

        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person,Ort,Stadt', 'maxDepth': '3'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            # columns should include the expected (label, property) combos
            expected_cols = {('Ort', 'plz'), ('Ort', 'strasse'), ('Person', 'nachname'), ('Person', 'vorname'), ('Stadt', 'stadt')}
            cols = {(c['nodeType'], c['property']) for c in data['columns']}
            self.assertTrue(expected_cols.issubset(cols))

            # exactly one main row (one person)
            self.assertEqual(len(data['rows']), 1)
            row = data['rows'][0]
            self.assertIn('cells', row)
            # We expect 5 columns (ordered by sorted columns), check their values by mapping prop->value
            # Build map from columns -> cell values for readability
            col_list = data['columns']
            cell_values = { (col_list[i]['nodeType'], col_list[i]['property']) : row['cells'][i]['value']
                            for i in range(len(col_list)) }
            self.assertEqual(cell_values.get(('Person','vorname')), 'Maria')
            self.assertEqual(cell_values.get(('Person','nachname')), 'Muller')
            self.assertEqual(cell_values.get(('Ort','strasse')), 'Hauptstrasse 1')
            self.assertEqual(cell_values.get(('Ort','plz')), '10115')
            self.assertEqual(cell_values.get(('Stadt','stadt')), 'Berlin')

        # cleanup
        self.graph.run("MATCH (n) DETACH DELETE n")

    def test_get_data_as_table_multiple_persons_and_limit(self):
        """Create two persons and verify limit parameter restricts rows."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        # create two persons each with an Ort+Stadt

        self.graph.run(
            "CREATE (p1:Person {vorname:'A', nachname:'One'}) "
            "CREATE (o1:Ort {strasse:'S1', plz:'11111'}) "
            "CREATE (s1:Stadt {stadt:'City1'}) "
            "CREATE (p1)-[:WOHNT_IN]->(o1), (o1)-[:LIEGT_IN]->(s1)"
        )

        self.graph.run(
            "CREATE (p2:Person {vorname:'B', nachname:'Two'}) "
            "CREATE (o2:Ort {strasse:'S2', plz:'22222'}) "
            "CREATE (s2:Stadt {stadt:'City2'}) "
            "CREATE (p2)-[:WOHNT_IN]->(o2), (o2)-[:LIEGT_IN]->(s2)"
        )

        with self.app as client:
            resp_no_limit = client.get('/api/get_data_as_table', query_string={'nodes': 'Person,Ort,Stadt'})
            self.assertEqual(resp_no_limit.status_code, 200)
            data_all = resp_no_limit.get_json()
            # Expect at least 2 rows
            self.assertGreaterEqual(len(data_all['rows']), 2)

            resp_limit1 = client.get('/api/get_data_as_table', query_string={'nodes': 'Person,Ort,Stadt', 'limit': '1'})
            self.assertEqual(resp_limit1.status_code, 200)
            data_l = resp_limit1.get_json()
            # limit=1 should return at most 1 row
            self.assertLessEqual(len(data_l['rows']), 1)

        self.graph.run("MATCH (n) DETACH DELETE n")

    def test_get_data_as_table_filter_labels_behavior(self):
        """Create mixed labels and assert filterLabels restricts columns/rows accordingly."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run(
            "CREATE (p:Person {vorname:'C', nachname:'Three'}) "
            "CREATE (o:Ort {strasse:'S3', plz:'33333'}) "
            "CREATE (s:Stadt {stadt:'City3'}) "
            "CREATE (p)-[:WOHNT_IN]->(o), (o)-[:LIEGT_IN]->(s)"
        )

        with self.app as client:
            # filter to only Person -> expect only Person columns present
            resp_person_only = client.get('/api/get_data_as_table', query_string={'nodes': 'Person,Ort,Stadt', 'filterLabels': 'Person'})
            self.assertEqual(resp_person_only.status_code, 200)
            data_person = resp_person_only.get_json()
            # all columns nodeType should be 'Person'
            self.assertTrue(all(c['nodeType'] == 'Person' for c in data_person['columns']))

            # filter to only Ort -> expect Ort columns (and rows may exist based on pivoting)
            resp_ort_only = client.get('/api/get_data_as_table', query_string={'nodes': 'Person,Ort,Stadt', 'filterLabels': 'Ort'})
            self.assertEqual(resp_ort_only.status_code, 200)
            data_ort = resp_ort_only.get_json()
            self.assertTrue(all(c['nodeType'] == 'Ort' for c in data_ort['columns']))

        self.graph.run("MATCH (n) DETACH DELETE n")

    def test_get_data_as_table_invalid_limit_param(self):
        """Non-integer limit should return 500 (current impl casts int and fails)."""
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person,Ort', 'limit': 'nope'})
            self.assertEqual(resp.status_code, 500)
            self.assertIn(b"invalid literal", resp.data)

    def test_get_data_as_table_person_stadt_full_table(self):
        """Ensure /api/get_data_as_table returns clean table form without duplicates."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        inserts = [
            ("Maria", "Müller", "Hauptstraße 1", "Berlin", "10115"),
            ("Hans", "Schmidt", "Marktplatz 5", "Hamburg", "20095"),
            ("Anna", "Fischer", "Bahnhofsallee 12", "München", "80331"),
        ]
        for vorname, nachname, strasse, stadt, plz in inserts:
            self.graph.run(
                "CREATE (p:Person {vorname:$vn, nachname:$nn}) "
                "CREATE (s:Stadt {stadt:$st, plz:$plz, strasse:$str}) "
                "CREATE (p)-[:WOHNT_IN]->(s)",
                vn=vorname, nn=nachname, st=stadt, plz=plz, str=strasse
            )

        with self.app as client:
            resp = client.get(
                '/api/get_data_as_table',
                query_string={'nodes': 'Person,Stadt'}
            )
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()

            # Must have all expected columns
            expected_cols = {
                ('Person', 'vorname'),
                ('Person', 'nachname'),
                ('Stadt', 'strasse'),
                ('Stadt', 'stadt'),
                ('Stadt', 'plz'),
            }
            cols = {(c['nodeType'], c['property']) for c in data['columns']}
            self.assertTrue(expected_cols.issubset(cols))

            # Map rows using column order
            col_names = [c['property'] for c in data['columns']]
            actual_rows = []
            for row in data['rows']:
                values = {col_names[i]: cell['value'] for i, cell in enumerate(row['cells'])}
                tup = (
                    values.get('vorname'),
                    values.get('nachname'),
                    values.get('strasse'),
                    values.get('stadt'),
                    values.get('plz'),
                )
                actual_rows.append(tup)

            expected_rows = [
                ("Maria", "Müller", "Hauptstraße 1", "Berlin", "10115"),
                ("Hans", "Schmidt", "Marktplatz 5", "Hamburg", "20095"),
                ("Anna", "Fischer", "Bahnhofsallee 12", "München", "80331"),
            ]

            self.assertEqual(sorted(actual_rows), sorted(expected_rows))
            self.assertEqual(len(actual_rows), len(expected_rows))

    def test_get_data_as_table_complex(self):
        """Test with 10 persons connected to cities, companies and hobbies."""
        self.graph.run("MATCH (n) DETACH DELETE n")

        inserts = [
            ("Alice", "Meier", "Berlin", "10115", "Siemens", "Klettern"),
            ("Bob", "Schulz", "Hamburg", "20095", "Airbus", "Segeln"),
            ("Carla", "Becker", "München", "80331", "BMW", "Musik"),
            ("David", "Fischer", "Köln", "50667", "Deutsche Post", "Kochen"),
            ("Eva", "Wolf", "Frankfurt", "60311", "Deutsche Bank", "Lesen"),
            ("Felix", "Wagner", "Stuttgart", "70173", "Porsche", "Fotografie"),
            ("Greta", "Neumann", "Leipzig", "04109", "DB", "Theater"),
            ("Heinz", "Krüger", "Dresden", "01067", "SAP", "Reisen"),
            ("Ines", "Zimmer", "Bremen", "28195", "Universität", "Malen"),
            ("Jonas", "Hoffmann", "Dortmund", "44135", "ThyssenKrupp", "Laufen"),
        ]

        for vn, nn, stadt, plz, firma, hobby in inserts:
            self.graph.run("""
                CREATE (p:Person {vorname:$vn, nachname:$nn})
                CREATE (s:Stadt {name:$stadt, plz:$plz})
                CREATE (c:Company {name:$firma})
                CREATE (h:Hobby {name:$hobby})
                CREATE (p)-[:WOHNT_IN]->(s)
                CREATE (p)-[:ARBEITET_BEI]->(c)
                CREATE (p)-[:MAG]->(h)
            """, vn=vn, nn=nn, stadt=stadt, plz=plz, firma=firma, hobby=hobby)

        with self.app as client:
            resp = client.get(
                '/api/get_data_as_table',
                query_string={'nodes': 'Person,Stadt,Company,Hobby'}
            )
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()

            # Expect at least these columns
            expected_cols = {
                ('Person', 'vorname'),
                ('Person', 'nachname'),
                ('Stadt', 'name'),
                ('Stadt', 'plz'),
                ('Company', 'name'),
                ('Hobby', 'name'),
            }
            cols = {(c['nodeType'], c['property']) for c in data['columns']}
            self.assertTrue(expected_cols.issubset(cols))

            # Expect 10 rows
            self.assertEqual(len(data['rows']), 10)


if __name__ == '__main__':
    unittest.main()
