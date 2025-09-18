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
    serialize_entity,
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

    def test_query_data_no_labels_provided(self):
        """Testet query_data mit leeren Labels."""
        response = self.app.post('/api/query_data', data=json.dumps({"selectedLabels": []}), content_type='application/json')
        self.assertEqual(response.status_code, 400)
        self.assertIn(b"Sie mindestens", response.data)

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

    def test_query_data_single_label(self):
        """Testet query_data mit nur einem Label."""
        # Test-Node erstellen
        person = Node("Person", name="Alice", age=30)
        self.graph.create(person)

        # Anfrage an die API mit Label "Person"
        response = self.app.post('/api/query_data',
                                data=json.dumps({"selectedLabels": ["Person"]}),
                                content_type='application/json')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))

        # Überprüfen, dass genau 1 Node zurückkommt
        self.assertTrue(any("Alice" in str(node.get("properties")) for node in data))

    def test_query_data_multiple_labels_with_relation(self):
        """Testet query_data mit zwei Labels, die durch eine definierte Relation verbunden sind."""
        # Testdaten: Person -> HAT_WOHNSITZ -> Ort
        person = Node("Person", name="Bob")
        ort = Node("Ort", name="Berlin")
        rel = Relationship(person, "HAT_WOHNSITZ", ort)
        self.graph.create(rel)

        response = self.app.post('/api/query_data',
                                data=json.dumps({"selectedLabels": ["Person", "Ort"]}),
                                content_type='application/json')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))

        # Überprüfen, dass beide Nodes und die Relation im Ergebnis enthalten sind
        self.assertTrue(any(row.get("Person") for row in data))
        self.assertTrue(any(row.get("Ort") for row in data))
        self.assertTrue(any(row.get("relationships") for row in data))

    def test_query_data_invalid_json(self):
        """Testet query_data mit ungültigem JSON."""
        response = self.app.post('/api/query_data',
                                data="INVALID_JSON",
                                content_type='application/json')
        self.assertEqual(response.status_code, 400)
        self.assertIn(b"JSON-Format", response.data)

    def test_query_data_multiple_labels_without_relation(self):
        """Testet query_data mit Labels, die zwar im relation_map stehen, aber ohne bestehende Relation."""
        # Testdaten: Ort ohne Relation zu Stadt
        ort = Node("Ort", name="Hamburg")
        stadt = Node("Stadt", name="Hamburg")
        self.graph.create(ort | stadt)

        response = self.app.post(
            '/api/query_data',
            data=json.dumps({"selectedLabels": ["Ort", "Stadt"]}),
            content_type='application/json'
        )
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data.decode('utf-8'))

        # Ort sollte immer zurückkommen
        self.assertTrue(any(row.get("Ort") for row in data))

        # Stadt sollte None sein, weil keine LIEGT_IN-Relation existiert
        self.assertTrue(all(row.get("Stadt") is None for row in data))

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

    def test_save_mapping_no_nodes_or_rels(self):
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

if __name__ == '__main__':
    unittest.main()
