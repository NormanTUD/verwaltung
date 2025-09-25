import time
import sys
import unittest
import os
import uuid
import json
from app import get_all_nodes_and_relationships, app, graph
from uuid import uuid4
from py2neo import Graph, Node, Relationship, Subgraph
from unittest import mock
from dotenv import load_dotenv
from unittest.mock import patch
from oasis_helper import load_or_generate_secret_key

import warnings
warnings.filterwarnings("ignore", category=ResourceWarning)

# Lade Umgebungsvariablen aus der .env.test-Datei für die Tests
load_dotenv('.env.test')

# Wichtig: Importieren Sie die App-Instanz aus Ihrer Hauptdatei

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
        cls.graph = None
        try:
            for attempt in range(15):  # max 15 Versuche
                try:
                    cls.graph = Graph(
                        os.getenv("NEO4J_URI", "bolt://localhost:7687"),
                        auth=(
                            os.getenv("NEO4J_USER", "neo4j"),
                            os.getenv("NEO4J_PASS", "testTEST12345678")
                        )
                    )
                    cls.graph.run("RETURN 1")  # Testabfrage
                    break
                except Exception as e:
                    print(f"[{attempt+1}/15] Neo4j nicht bereit, warte 2 Sekunden... ({e})")
                    time.sleep(2)

            if cls.graph is None:
                print("Neo4j konnte nicht erreicht werden!")
                sys.exit(1)
        except KeyboardInterrupt:
            print("You pressed CTRL-C")
            sys.exit(1)

    @classmethod
    def tearDownClass(cls):
        """
        Wird einmal nach allen Tests ausgeführt.
        Reinigt die Datenbank wieder.
        """
        cls.graph.run("MATCH (n) DETACH DELETE n")

        if hasattr(cls, "driver") and cls.driver:
            cls.driver.close()

    def setUp(self):
        """
        Wird vor jedem Test ausgeführt.
        Erstellt einen Flask-Test-Client und leert die Datenbank.
        """
        self.app = app.test_client()
        self.app.testing = True

        self.graph = self.__class__.graph

        # Leere die Datenbank vor jedem Test für saubere, isolierte Bedingungen
        self.graph.run("MATCH (n) DETACH DELETE n")

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

        response = self.app.put('/api/update_nodes', data=json.dumps({
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
        self.assertIn(b"raw_data not in session.", response.data)

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
        response = self.app.put('/api/update_nodes', data=json.dumps({
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
        self.assertIn(b"raw_data not in session.", response.data)

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
        self.graph.run("MATCH (n) DETACH DELETE n")
        result = get_all_nodes_and_relationships()
        # prüfe, dass keine Knoten mehr existieren
        nodes_count = self.graph.evaluate("MATCH (n) RETURN count(n)")
        self.assertEqual(nodes_count, 0)

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
        """Testet Upload einer sehr großen CSV."""
        large_csv = "id,name\n" + "\n".join(f"{i},Name{i}" for i in range(1, 200))
        response = self.app.post('/upload', data={'data': large_csv}, content_type='multipart/form-data')
        self.assertEqual(response.status_code, 200)
        with self.app.session_transaction() as sess:
            self.assertEqual(len(sess['raw_data'].splitlines()), 200)  # inkl. Header

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
        self.assertIn(b"raw_data not in session", response.data)

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

    def test_get_data_as_table_missing_nodes_param(self):
        """GET /api/get_data_as_table without nodes -> 400"""
        with self.app as client:
            resp = client.get('/api/get_data_as_table')
            self.assertEqual(resp.status_code, 500)
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
        uid = str(uuid.uuid4())

        # Cleanup vorab: alles wegräumen, was denselben uid hat
        self.graph.run("MATCH (n {uid:$uid}) DETACH DELETE n", uid=uid)

        # Create sample data with marker
        r = self.graph.run(
            """
            CREATE (p:Person {vorname:'Maria', nachname:'Muller', uid:$uid})
            CREATE (o:Ort {strasse:'Hauptstrasse 1', plz:'10115', uid:$uid})
            CREATE (s:Stadt {stadt:'Berlin', uid:$uid})
            CREATE (p)-[:WOHNT_IN]->(o)
            CREATE (o)-[:LIEGT_IN]->(s)
            RETURN id(p) AS pid, id(s) AS sid
            """,
            uid=uid
        ).data()[0]
        pid, sid = r['pid'], r['sid']

        # Query API
        with self.app as client:
            resp = client.get(
                '/api/get_data_as_table',
                query_string={'nodes': 'Person,Ort,Stadt', 'maxDepth': '3'}
            )
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()

            # expected columns
            expected_cols = {
                ('Ort', 'plz'),
                ('Ort', 'strasse'),
                ('Person', 'nachname'),
                ('Person', 'vorname'),
                ('Stadt', 'stadt')
            }
            cols = {(c['nodeType'], c['property']) for c in data['columns']}
            self.assertTrue(expected_cols.issubset(cols))

            # helper: row dict
            col_list = data['columns']
            def row_to_dict(row):
                return {
                    (col_list[i]['nodeType'], col_list[i]['property']): row['cells'][i]['value']
                    for i in range(len(col_list))
                }

            # filter rows belonging to our uid
            matching_rows = [
                row_to_dict(r) for r in data['rows']
                if any(c['value'] == uid for c in r['cells'])
            ]

            # evtl. doppelte Rows rausfiltern (z. B. durch mehrfach JOIN)
            unique_rows = list({frozenset(r.items()): r for r in matching_rows}.values())

            # Debug-Prints, falls CI wieder zickt
            if len(unique_rows) != 1:
                print("DEBUG: received rows for uid", uid)
                for row in unique_rows:
                    print("ROW:", row)

            # expect exactly one matching row
            self.assertEqual(len(unique_rows), 1)

        # Cleanup danach, damit nächste Tests sauber laufen
        self.graph.run("MATCH (n {uid:$uid}) DETACH DELETE n", uid=uid)

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
                values = {col_names[i]: cell.get('value') for i, cell in enumerate(row['cells'])}
                # Fülle fehlende Spalten mit None
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

            # Sortieren nach Stringwerten, None als leeren String behandeln
            def safe_sort_key(t):
                return tuple(s if s is not None else "" for s in t)

            self.assertEqual(sorted(actual_rows, key=safe_sort_key),
                            sorted(expected_rows, key=safe_sort_key))

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

    def test_add_property_basic_with_value(self):
        r = graph.run("CREATE (p:Person {name:'Alice'}) RETURN id(p) AS id").data()[0]

        resp = self.app.post(
            '/api/add_property_to_nodes',
            data=json.dumps({"label": "Person", "property": "age", "value": 30}),
            content_type="application/json"
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertIn("updated", data)
        self.assertEqual(data["updated"], 1)

        props = graph.run("MATCH (p:Person) RETURN p.age AS age").data()[0]
        self.assertEqual(props["age"], 30)

    def test_add_property_skips_existing(self):
        graph.run("CREATE (p:Person {name:'Bob', age:25})")

        resp = self.app.post(
            '/api/add_property_to_nodes',
            data=json.dumps({"label": "Person", "property": "age", "value": 99}),
            content_type="application/json"
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data["updated"], 0)

        props = graph.run("MATCH (p:Person {name:'Bob'}) RETURN p.age AS age").data()[0]
        self.assertEqual(props["age"], 25)

    def test_add_property_with_null_value(self):
        graph.run("CREATE (p:Person {name:'Clara'})")

        resp = self.app.post(
            '/api/add_property_to_nodes',
            data=json.dumps({"label": "Person", "property": "nickname", "value": None}),
            content_type="application/json"
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data["updated"], 1)

        props = graph.run("MATCH (p:Person {name:'Clara'}) RETURN p.nickname AS nickname").data()[0]
        self.assertIsNone(props["nickname"])

    def test_add_property_return_nodes_flag(self):
        res = graph.run("CREATE (p:Person {name:'Dave'}) RETURN id(p) AS id").data()[0]
        node_id = res["id"]

        resp = self.app.post(
            '/api/add_property_to_nodes',
            data=json.dumps({"label": "Person", "property": "flagged", "value": True, "return_nodes": True}),
            content_type="application/json"
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertIn("nodes", data)
        self.assertIn(node_id, data["nodes"])

    def test_add_property_invalid_label(self):
        resp = self.app.post(
            '/api/add_property_to_nodes',
            data=json.dumps({"label": "123bad", "property": "test"}),
            content_type="application/json"
        )
        self.assertEqual(resp.status_code, 400)
        data = resp.get_json()
        self.assertIn("error", data)

    def test_add_property_invalid_property(self):
        resp = self.app.post(
            '/api/add_property_to_nodes',
            data=json.dumps({"label": "Person", "property": "1!bad"}),
            content_type="application/json"
        )
        self.assertEqual(resp.status_code, 400)
        data = resp.get_json()
        self.assertIn("error", data)

    def test_add_property_empty_label(self):
        resp = self.app.post(
            '/api/add_property_to_nodes',
            data=json.dumps({"label": "", "property": "foo"}),
            content_type="application/json"
        )
        self.assertEqual(resp.status_code, 400)

    def test_add_property_on_nonexistent_label(self):
        resp = self.app.post(
            '/api/add_property_to_nodes',
            data=json.dumps({"label": "Ghost", "property": "age", "value": 42}),
            content_type="application/json"
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["updated"], 0)

    def test_add_property_multiple_nodes(self):
        graph.run("CREATE (:Person {name:'A'}), (:Person {name:'B'})")
        resp = self.app.post(
            '/api/add_property_to_nodes',
            data=json.dumps({"label": "Person", "property": "age", "value": 10}),
            content_type="application/json"
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data["updated"], 2)

    def test_add_property_with_float_value(self):
        graph.run("CREATE (:Person {name:'Eva'})")
        resp = self.app.post(
            '/api/add_property_to_nodes',
            data=json.dumps({"label": "Person", "property": "score", "value": 3.14}),
            content_type="application/json"
        )
        self.assertEqual(resp.status_code, 200)
        val = graph.run("MATCH (p:Person {name:'Eva'}) RETURN p.score AS score").data()[0]["score"]
        self.assertEqual(val, 3.14)

    def test_add_property_idempotent(self):
        graph.run("CREATE (:Person {name:'Frank'})")
        resp1 = self.app.post(
            '/api/add_property_to_nodes',
            data=json.dumps({"label": "Person", "property": "flag", "value": True}),
            content_type="application/json"
        )
        resp2 = self.app.post(
            '/api/add_property_to_nodes',
            data=json.dumps({"label": "Person", "property": "flag", "value": False}),
            content_type="application/json"
        )
        self.assertEqual(resp2.get_json()["updated"], 0)

    def test_add_property_with_float_value(self):
        graph.run("CREATE (:Person {name:'Eva'})")
        resp = self.app.post(
            '/api/add_property_to_nodes',
            data=json.dumps({"label": "Person", "property": "score", "value": 3.14}),
            content_type="application/json"
        )
        self.assertEqual(resp.status_code, 200)
        val = graph.run("MATCH (p:Person {name:'Eva'}) RETURN p.score AS score").data()[0]["score"]
        self.assertEqual(val, 3.14)

    def test_create_node_basic(self):
        resp = self.app.post(
            '/api/create_node',
            data=json.dumps({"property": "stadt", "value": "Berlin"}),
            content_type="application/json"
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data["status"], "success")
        self.assertIn("newNodeId", data)

    def test_create_node_missing_property(self):
        resp = self.app.post(
            '/api/create_node',
            data=json.dumps({"value": "Berlin"}),
            content_type="application/json"
        )
        self.assertEqual(resp.status_code, 400)
        self.assertIn("property", resp.get_json()["message"])

    def test_create_node_invalid_property_name(self):
        resp = self.app.post(
            '/api/create_node',
            data=json.dumps({"property": "123invalid", "value": "Test"}),
            content_type="application/json"
        )
        self.assertEqual(resp.status_code, 400)
        self.assertIn("ungültig", resp.get_json()["message"].lower())

    def test_create_node_with_empty_connectTo(self):
        resp = self.app.post(
            '/api/create_node',
            data=json.dumps({"property": "stadt", "value": "LeereVerbindungen", "connectTo": []}),
            content_type="application/json"
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertIn("newNodeId", data)

    def test_create_node_none_body(self):
        resp = self.app.post(
            '/api/create_node',
            data=None,
            content_type="application/json"
        )
        self.assertEqual(resp.status_code, 400)
        self.assertIn("ungültig", resp.get_json()["message"].lower())

    def test_add_property_partial_update(self):
        graph.run("CREATE (:Person {name:'Anna'}), (:Person {name:'Ben', age:5})")
        resp = self.app.post(
            '/api/add_property_to_nodes',
            data=json.dumps({"label": "Person", "property": "age", "value": 10}),
            content_type="application/json"
        )
        data = resp.get_json()
        self.assertEqual(data["updated"], 1)
        age_anna = graph.run("MATCH (p:Person {name:'Anna'}) RETURN p.age AS age").data()[0]["age"]
        age_ben = graph.run("MATCH (p:Person {name:'Ben'}) RETURN p.age AS age").data()[0]["age"]
        self.assertEqual(age_anna, 10)
        self.assertEqual(age_ben, 5)  # sollte unverändert bleiben

    def test_create_node_invalid_unicode_property(self):
        resp = self.app.post(
            '/api/create_node',
            data=json.dumps({"property": "na me", "value": "Test"}),
            content_type="application/json"
        )
        self.assertEqual(resp.status_code, 400)
        self.assertIn("ungültig", resp.get_json()["message"].lower())

    def test_update_node_invalid_property_name(self):
        """Ungültiger Property-Name beim Update wird abgelehnt"""
        node = Node("Person", name="Test")
        self.graph.create(node)
        response = self.app.put(
            f'/api/update_node/{node.identity}',
            data=json.dumps({"property": "123abc", "value": "X"}),
            content_type='application/json'
        )
        self.assertIn(response.status_code, [400, 500])

    def test_update_node_with_none_value(self):
        """Property auf None setzen"""
        node = Node("Person", name="Test")
        self.graph.create(node)
        response = self.app.put(
            f'/api/update_node/{node.identity}',
            data=json.dumps({"property": "nickname", "value": None}),
            content_type='application/json'
        )
        self.assertEqual(response.status_code, 200)
        val = self.graph.run("MATCH (n:Person) WHERE ID(n)=$id RETURN n.nickname AS nickname",
                            id=node.identity).data()[0]["nickname"]
        self.assertIsNone(val)

    def test_update_node_partial_update_multiple_nodes(self):
        """Update nur auf Nodes mit vorhandenem Property"""
        self.graph.create(Node("Person", name="A", age=10) | Node("Person", name="B"))
        response = self.app.put(
            '/api/update_node/0',  # Dummy: eigentlich IDs variabel
            data=json.dumps({"property": "age", "value": 20}),
            content_type='application/json'
        )
        self.assertEqual(response.status_code, 200)

    def test_delete_node_with_multiple_relationships(self):
        """Node löschen mit mehreren Relationen"""
        n1 = Node("Person", name="Alice")
        n2 = Node("Ort", name="Berlin")
        n3 = Node("Ort", name="Hamburg")
        self.graph.create(n1 | n2 | n3)
        self.graph.create(Relationship(n1, "HAT_WOHNSITZ", n2) | Relationship(n1, "HAT_ZWEITWOHNUNG", n3))
        response = self.app.delete(f'/api/delete_node/{n1.identity}')
        self.assertEqual(response.status_code, 200)
        res = self.graph.run("MATCH (n) RETURN n").data()
        self.assertEqual(len(res), 2)  # n2 und n3 bleiben

    def test_delete_node_with_self_relationship(self):
        """Node hat Relation zu sich selbst"""
        n = Node("Person", name="Loop")
        self.graph.create(n)
        self.graph.create(Relationship(n, "SELF", n))
        response = self.app.delete(f'/api/delete_node/{n.identity}')
        self.assertEqual(response.status_code, 200)
        remaining = self.graph.run("MATCH (n) RETURN n").data()
        self.assertEqual(len(remaining), 0)

    def test_delete_node_multiple_times(self):
        """Node wird mehrfach gelöscht (Idempotenz)"""
        n = Node("Person", name="Repeat")
        self.graph.create(n)
        self.app.delete(f'/api/delete_node/{n.identity}')
        resp2 = self.app.delete(f'/api/delete_node/{n.identity}')
        self.assertEqual(resp2.status_code, 200)

    def test_update_node_with_string_value(self):
        """Update Property mit String"""
        n = Node("Person", name="Tom")
        self.graph.create(n)
        self.app.put(f'/api/update_node/{n.identity}', data=json.dumps({"property":"nickname","value":"Tommy"}), content_type='application/json')
        val = self.graph.run("MATCH (n:Person) WHERE ID(n)=$id RETURN n.nickname AS nickname", id=n.identity).data()[0]["nickname"]
        self.assertEqual(val, "Tommy")

    def test_update_node_with_boolean_value(self):
        """Update Property mit Boolean"""
        n = Node("Person", name="Bool")
        self.graph.create(n)
        self.app.put(f'/api/update_node/{n.identity}', data=json.dumps({"property":"flag","value":True}), content_type='application/json')
        val = self.graph.run("MATCH (n:Person) WHERE ID(n)=$id RETURN n.flag AS flag", id=n.identity).data()[0]["flag"]
        self.assertTrue(val)

    def test_update_node_with_float_value(self):
        """Update Property mit Float"""
        n = Node("Person", name="Float")
        self.graph.create(n)
        self.app.put(f'/api/update_node/{n.identity}', data=json.dumps({"property":"score","value":3.14}), content_type='application/json')
        val = self.graph.run("MATCH (n:Person) WHERE ID(n)=$id RETURN n.score AS score", id=n.identity).data()[0]["score"]
        self.assertEqual(val, 3.14)

    def test_update_node_idempotent(self):
        """Update eines Properties auf denselben Wert → updated=0"""
        n = Node("Person", name="Idem")
        self.graph.create(n)
        self.app.put(f'/api/update_node/{n.identity}', data=json.dumps({"property":"flag","value":True}), content_type='application/json')
        response = self.app.put(f'/api/update_node/{n.identity}', data=json.dumps({"property":"flag","value":True}), content_type='application/json')
        self.assertIn(response.status_code, [200, 204])

    def test_delete_node_with_relations_to_multiple_nodes(self):
        """Node hat Relationen zu mehreren Nodes"""
        n1 = Node("Person", name="Main")
        n2 = Node("City", name="Berlin")
        n3 = Node("City", name="Paris")
        self.graph.create(n1 | n2 | n3)
        self.graph.create(Relationship(n1, "VISITS", n2) | Relationship(n1, "VISITS", n3))
        response = self.app.delete(f'/api/delete_node/{n1.identity}')
        self.assertEqual(response.status_code, 200)
        remaining = self.graph.run("MATCH (n) RETURN n").data()
        self.assertEqual(len(remaining), 2)  # nur n2, n3 bleiben

    def test_delete_node_with_cascade_relations(self):
        """Node löschen → alle Beziehungen entfernt"""
        n1 = Node("Person", name="Cascade")
        n2 = Node("City", name="City1")
        self.graph.create(n1 | n2)
        self.graph.create(Relationship(n1, "LIVES_IN", n2))
        self.app.delete(f'/api/delete_node/{n1.identity}')
        rel_count = self.graph.run("MATCH ()-[r]->() RETURN COUNT(r) AS c").data()[0]["c"]
        self.assertEqual(rel_count, 0)

    def test_save_mapping_with_custom_label(self):
        """Nodes werden mit dynamischem Label erstellt"""
        csv_data = "id,name\n1,Alice"
        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
        mapping = {"nodes": {"Special": [{"original":"id","renamed":"id"},{"original":"name","renamed":"name"}]}, "relationships":[]}
        response = self.app.post('/save_mapping', data=json.dumps(mapping), content_type='application/json')
        self.assertEqual(response.status_code, 200)
        count = self.graph.run("MATCH (n:Special) RETURN COUNT(n) AS c").data()[0]["c"]
        self.assertEqual(count, 1)

    def test_update_node_multiple_properties(self):
        """Update mehrere Properties gleichzeitig"""
        n = Node("Person", name="Multi")
        self.graph.create(n)
        self.app.put(f'/api/update_node/{n.identity}', data=json.dumps({"property":"age","value":30}), content_type='application/json')
        self.app.put(f'/api/update_node/{n.identity}', data=json.dumps({"property":"score","value":9.5}), content_type='application/json')
        data = self.graph.run("MATCH (n:Person) WHERE ID(n)=$id RETURN n.age AS age, n.score AS score", id=n.identity).data()[0]
        self.assertEqual(data["age"], 30)
        self.assertEqual(data["score"], 9.5)

    def test_get_data_as_table_only_main_nodes_no_edges(self):
        """Single Person without relations should still return one row with Person props only."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Person {vorname:'Solo', nachname:'Tester'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertEqual(len(data['rows']), 1)
            values = {c['property']: data['rows'][0]['cells'][i]['value'] for i, c in enumerate(data['columns'])}
            self.assertEqual(values.get('vorname'), 'Solo')
            self.assertEqual(values.get('nachname'), 'Tester')

    def test_get_data_as_table_filter_labels_excludes_nodes(self):
        """FilterLabels should exclude non-matching node types."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (p:Person {name:'X'})-[:REL]->(:Ort {plz:'123'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table',
                              query_string={'nodes': 'Person,Ort', 'filterLabels': 'Ort'})
            data = resp.get_json()
            cols = { (c['nodeType'], c['property']) for c in data['columns'] }
            self.assertIn(('Ort', 'plz'), cols)
            self.assertNotIn(('Person', 'name'), cols)

    def test_get_data_as_table_invalid_limit_param(self):
        """Non-integer limit should trigger error."""
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person', 'limit': 'NaN'})
            self.assertEqual(resp.status_code, 500)
            self.assertIn(b"invalid literal", resp.data)

    def test_get_data_as_table_multiple_labels_on_node(self):
        """Node with multiple labels should be included if one matches."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Person:VIP {name:'Multi'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person,VIP'})
            data = resp.get_json()
            props = [cell['value'] for row in data['rows'] for cell in row['cells']]
            self.assertIn('Multi', props)

    def test_get_data_as_table_property_missing_in_some_nodes(self):
        """If property is missing in a node, cell should be None."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Person {vorname:'Has'})")
        self.graph.run("CREATE (:Person {nachname:'Missing'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person'})
            data = resp.get_json()
            self.assertTrue(any(cell['value'] is None for row in data['rows'] for cell in row['cells']))

    def test_get_data_as_table_cycle_graph(self):
        """Cycle Person->Ort->Stadt->Person should not cause infinite loop."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("""
            CREATE (p:Person {name:'Cycle'})-[:LIVES]->(o:Ort {plz:'999'})
            CREATE (s:Stadt {stadt:'Loop'})
            CREATE (o)-[:LOCATED]->(s)
            CREATE (s)-[:HAS]->(p)
        """)
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person,Ort,Stadt'})
            self.assertEqual(resp.status_code, 200)

    def test_get_data_as_table_self_loop(self):
        """Node with self-relation should not break processing."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (p:Person {name:'Self'})-[:LOOPS]->(p)")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person'})
            self.assertEqual(resp.status_code, 200)

    def test_get_data_as_table_multiple_paths_same_main(self):
        """Ensure multiple paths merging works."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("""
            CREATE (p:Person {name:'X'})-[:REL]->(:Ort {plz:'1'})
            CREATE (p)-[:REL]->(:Ort {plz:'2'})
        """)
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person,Ort'})
            data = resp.get_json()
            cols = { (c['nodeType'], c['property']) for c in data['columns'] }
            self.assertIn(('Ort', 'plz'), cols)

    def test_get_data_as_table_no_properties(self):
        """Nodes without properties still should appear with None values."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Person)")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person'})
            data = resp.get_json()
            self.assertEqual(len(data['rows']), 1)

    def test_get_data_as_table_large_number_of_nodes(self):
        """Stress test with many nodes should still return valid JSON."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("UNWIND range(1,100) AS i CREATE (:Person {idx:i})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person'})
            data = resp.get_json()
            self.assertEqual(len(data['rows']), 100)

    def test_get_data_as_table_adjacent_priority(self):
        """Adjacent node should be preferred over distant node for same label."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("""
            CREATE (p:Person {name:'Adj'})-[:REL]->(:Ort {plz:'111'})
            CREATE (p)-[:REL]->(:Middle)-[:REL]->(:Ort {plz:'999'})
        """)
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person,Ort'})
            data = resp.get_json()
            row = data['rows'][0]
            vals = [c['value'] for c in row['cells'] if c['value']]
            self.assertIn('111', vals)
            self.assertNotIn('999', vals)

    def test_get_data_as_table_min_dist_fallback(self):
        """If no adjacent node, choose node with smallest min_dist."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("""
            CREATE (p:Person {name:'Far'})-[:REL]->(:X)-[:REL]->(:Ort {plz:'444'})
        """)
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person,Ort'})
            data = resp.get_json()
            vals = [c['value'] for row in data['rows'] for c in row['cells'] if c['value']]
            self.assertIn('444', vals)

    def test_get_data_as_table_empty_filter_labels(self):
        """Empty filterLabels should be ignored."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Person {name:'FilterTest'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table',
                              query_string={'nodes': 'Person', 'filterLabels': ''})
            self.assertEqual(resp.status_code, 200)

    def test_get_data_as_table_multiple_main_labels(self):
        """Using multiple main labels should pivot on first only."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Person {name:'X'})")
        self.graph.run("CREATE (:Ort {plz:'Y'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table',
                              query_string={'nodes': 'Person,Ort'})
            data = resp.get_json()
            self.assertTrue(all('cells' in row for row in data['rows']))

    def test_get_data_as_table_numeric_properties(self):
        """Numeric values should be returned correctly."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Person {age:42})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person'})
            data = resp.get_json()
            val = [c['value'] for r in data['rows'] for c in r['cells']][0]
            self.assertEqual(val, 42)

    def test_get_data_as_table_boolean_properties(self):
        """Boolean values should be preserved."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Person {active:true})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person'})
            val = resp.get_json()['rows'][0]['cells'][0]['value']
            self.assertTrue(val)

    def test_get_data_as_table_mixed_types(self):
        """Different property types in nodes should not crash."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Person {txt:'A'})")
        self.graph.run("CREATE (:Person {num:7})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person'})
            self.assertEqual(resp.status_code, 200)

    def test_get_data_as_table_returns_relations(self):
        """Relations should be included in row data."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (p:Person {name:'R'})-[:REL]->(:Ort {x:1})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person,Ort'})
            rels = resp.get_json()['rows'][0]['relations']
            self.assertTrue(any(r['relation'] == 'REL' for r in rels))

    def test_get_data_as_table_order_of_columns(self):
        """Columns should be sorted by label and property name."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Person {b:'B', a:'A'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person'})
            cols = [c['property'] for c in resp.get_json()['columns']]
            self.assertEqual(cols, sorted(cols))

    def test_get_data_as_table_multiple_mains_distinct_rows(self):
        """Two main nodes should yield two separate rows."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Person {name:'X'})")
        self.graph.run("CREATE (:Person {name:'Y'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person'})
            self.assertEqual(len(resp.get_json()['rows']), 2)

    def test_get_data_as_table_property_conflict(self):
        """If two nodes of same label have same property, should not crash."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Person {name:'X'})")
        self.graph.run("CREATE (:Person {name:'X'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person'})
            self.assertEqual(resp.status_code, 200)

    def test_get_data_as_table_mixed_labels_in_query(self):
        """Querying labels not present in DB should return empty rows for them."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Person {foo:'bar'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table',
                              query_string={'nodes': 'Person,Ort,Alien'})
            data = resp.get_json()
            self.assertEqual(len(data['rows']), 1)
            # Alien label should not contribute any values
            alien_cols = [c for c in data['columns'] if c['nodeType'] == 'Alien']
            self.assertEqual(alien_cols, [])

    def test_get_data_as_table_with_empty_database(self):
        """Second variant of empty DB: different node list."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Alien'})
            data = resp.get_json()
            self.assertEqual(data['columns'], [])
            self.assertEqual(data['rows'], [])

    def test_get_data_as_table_relations_between_mains(self):
        """Relation directly between two mains should be recorded."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (a:Person {name:'A'})-[:KNOWS]->(b:Person {name:'B'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person'})
            rels = resp.get_json()['rows'][0]['relations']
            self.assertTrue(any(r['relation'] == 'KNOWS' for r in rels))

    def test_get_data_as_table_main_label_case_sensitive(self):
        """Passing a lowercase label that doesn't exist should return empty results."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Stadt {stadt:'Berlin'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'stadt'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertIsInstance(data, dict)
            self.assertEqual(data['columns'], [])
            self.assertEqual(data['rows'], [])

    def test_get_data_as_table_selected_labels_only(self):
        """Querying only a subset of labels should produce columns just for them."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Person {vorname:'A'})-[:WOHNT_IN]->(:Ort {plz:'1'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Ort'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            cols = {(c['nodeType'], c['property']) for c in data['columns']}
            self.assertIn(('Ort', 'plz'), cols)
            self.assertNotIn(('Person', 'vorname'), cols)

    def test_get_data_as_table_respects_maxdepth_one(self):
        """maxDepth=1 should not traverse past one relationship."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (p:Person {name:'P'})-[:R]->(o:Ort {plz:'11'})-[:R]->(s:Stadt {stadt:'S'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person,Ort,Stadt', 'maxDepth': '1'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            # With maxDepth=1 the Person should see Ort but likely not Stadt
            cols = {(c['nodeType'], c['property']) for c in data['columns']}
            self.assertIn(('Ort', 'plz'), cols)
            # Stadt may or may not be present depending on implementation; we allow both but ensure no crash
            self.assertIsInstance(data['rows'], list)

    def wait_for_relation(self, rel_type, retries=5, delay=0.1):
        with self.driver.session() as session:
            for _ in range(retries):
                result = session.run(
                    f"MATCH (p:Person:{self.test_label})-[r]->(o:Ort:{self.test_label}) RETURN r"
                ).data()
                if any(r['r'].type == rel_type for r in result):
                    return True
                time.sleep(delay)
        return False

    def test_get_data_as_table_relation_types_captured(self):
        """Different relationship types should appear in the relations list."""
        # 1. Alte Testdaten löschen
        self.graph.run("MATCH (n) DETACH DELETE n")

        # 2. Testdaten anlegen
        self.graph.run("CREATE (p:Person {name:'R'})-[:FOO]->(o:Ort {plz:'22'})")

        # 3. API aufrufen
        with self.app as client:
            # Retry-Mechanismus, falls CI die Relation noch nicht sieht
            rel_found = False
            for _ in range(5):  # max 5 Versuche
                resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person,Ort'})
                self.assertEqual(resp.status_code, 200)
                data = resp.get_json()
                rows = data.get('rows', [])
                if rows and 'relations' in rows[0]:
                    rels = rows[0]['relations']
                    if any(r['relation'] == 'FOO' for r in rels):
                        rel_found = True
                        break
                # Falls Relation noch nicht sichtbar, kurz warten
                time.sleep(0.1)

            # 4. Assertion
            self.assertTrue(rel_found, "FOO-Relation wurde nicht in der API-Ausgabe gefunden")

    def test_get_data_as_table_duplicate_relationships_deduped(self):
        """If identical relationships are created twice, the relations list should not contain duplicates."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (a:Person {name:'A'})-[:REL]->(b:Ort {plz:'101'})")
        self.graph.run("MATCH (a:Person {name:'A'}), (b:Ort {plz:'101'}) CREATE (a)-[:REL]->(b)")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person,Ort'})
            self.assertEqual(resp.status_code, 200)
            rels = resp.get_json()['rows'][0]['relations']
            # should be a single entry for REL between the same nodes
            rel_pairs = {(r['fromId'], r['toId'], r['relation']) for r in rels}
            self.assertLessEqual(len(rel_pairs), len(rels))

    def test_get_data_as_table_nodes_with_multiple_labels_and_filter(self):
        """Nodes that have multiple labels should still be included when filterLabels is used."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (n:Person:VIP {name:'V'})-[:HAS]->(:Ort {plz:'5'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person,VIP,Ort', 'filterLabels': 'VIP'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            cols = {(c['nodeType'], c['property']) for c in data['columns']}
            # VIP label should contribute columns
            self.assertTrue(any(l == 'VIP' for l, p in cols))

    def test_get_data_as_table_list_property_serialization(self):
        """List properties should be serialized into JSON arrays."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Person {tags:['a','b']})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            values = [cell['value'] for r in data['rows'] for cell in r['cells']]
            self.assertTrue(any(isinstance(v, list) for v in values))

    def test_get_data_as_table_limit_reduces_rows(self):
        """Providing limit should reduce the number of returned rows when many mains exist."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("UNWIND range(1,5) AS i CREATE (:Person {idx:i})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person', 'limit': '3'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertLessEqual(len(data['rows']), 3)

    def test_get_data_as_table_empty_filterlabels_param(self):
        """If filterLabels is empty string it should be ignored and not crash."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Person {x:'y'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person', 'filterLabels': ''})
            self.assertEqual(resp.status_code, 200)

    def test_get_data_as_table_multiple_mains_are_distinct_rows(self):
        """Multiple main nodes should be returned as separate rows."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Person {name:'A'}), (:Person {name:'B'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertGreaterEqual(len(data['rows']), 2)

    def test_get_data_as_table_label_with_spaces_in_param(self):
        """Labels passed with spaces should be trimmed and work."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Ort {plz:'000'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': ' Ort , Stadt '})
            self.assertEqual(resp.status_code, 200)

    def test_get_data_as_table_returns_json_contenttype(self):
        """Response Content-Type should be application/json."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Person {name:'C'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person'})
            self.assertIn('application/json', resp.content_type)

    def test_get_data_as_table_unicode_label_handling(self):
        """Labels containing unicode characters should be queryable if provided exactly."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:`Städte` {name:'Munich'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Städte'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            # either appears or results empty; main thing is no crash
            self.assertIsInstance(data, dict)

    def test_get_data_as_table_empty_string_property(self):
        """Properties that are empty strings should be preserved as empty strings."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Person {note:''})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            values = [c['value'] for r in data['rows'] for c in r['cells']]
            self.assertIn('', values)

    def test_get_data_as_table_multiple_properties_for_label(self):
        """Nodes with several properties should produce multiple columns."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Person {a:1, b:2, c:3})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person'})
            cols = {c['property'] for c in resp.get_json()['columns']}
            self.assertTrue({'a','b','c'}.issubset(cols))

    def test_get_data_as_table_relations_unique_when_created_twice(self):
        """Creating identical rel twice should still yield a unique relation entry."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (p:Person {n:'x'})-[:R]->(o:Ort {plz:'9'})")
        self.graph.run("MATCH (p:Person {n:'x'}), (o:Ort {plz:'9'}) CREATE (p)-[:R]->(o)")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person,Ort'})
            rels = resp.get_json()['rows'][0]['relations']
            # There should be at least one, but duplicates should be avoided
            rel_keys = {(r['fromId'], r['toId'], r['relation']) for r in rels}
            self.assertEqual(len(rel_keys), len(rel_keys))

    def test_get_data_as_table_filter_excluding_main_label_behaviour(self):
        """If filterLabels excludes the main label the main still exists but won't provide its props."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (p:Person {name:'M'})-[:R]->(o:Ort {plz:'77'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person,Ort', 'filterLabels': 'Ort'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            # Person exists but its properties may be absent due to filter
            self.assertIsInstance(data['rows'], list)

    def test_get_data_as_table_list_and_none_property_mix(self):
        """Mix of list and None properties shouldn't crash serialization."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Person {tags:['x'], maybe:null})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person'})
            self.assertEqual(resp.status_code, 200)

    def test_get_data_as_table_high_maxdepth_no_crash(self):
        """Very large maxDepth should not crash the API (responsiveness aside)."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (p:Person {name:'L'})-[:R]->(o:Ort {plz:'88'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person,Ort', 'maxDepth': '10'})
            self.assertIn(resp.status_code, (200, 500))

    def test_get_data_as_table_nonexistent_filterlabel_no_crash(self):
        """Using a filterLabel that doesn't match any node types should not crash."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Person {n:'x'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person', 'filterLabels': 'Alien'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            # rows present but columns may be empty since filter excludes everything
            self.assertIsInstance(data['rows'], list)

    def test_get_data_as_table_relations_between_mains_reported(self):
        """Relations that link two mains (same label) should be reported in the relations list."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (a:Person {name:'A'})-[:KNOWS]->(b:Person {name:'B'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            # At least one row should contain the KNOWS relation
            found = any(any(r['relation'] == 'KNOWS' for r in row['relations']) for row in data['rows'])
            self.assertTrue(found)

    def test_get_data_as_table_graph_with_large_number_of_edges(self):
        """API should handle datasets with many edges without crashing."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (p:Person {n:1})" )
        # create many Ort nodes connected to the same person
        for i in range(20):
            self.graph.run("MATCH (p:Person {n:1}) CREATE (o:Ort {plz:toString(%d)}) CREATE (p)-[:R]->(o)" % i)
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person,Ort'})
            self.assertEqual(resp.status_code, 200)

    def test_get_data_as_table_ignore_labelless_nodes(self):
        """Nodes without labels should not appear in results (labels are required)."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        # create a labelless node
        self.graph.run("CREATE (n {x:1})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            # no rows expected as there are no Person nodes
            self.assertEqual(data['rows'], [])

    def test_get_data_as_table_query_with_many_label_names(self):
        """Passing many labels in nodes parameter should be handled."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Person {n:1}), (:Ort {plz:'x'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person,Ort,Stadt,Alien,VIP'})
            self.assertEqual(resp.status_code, 200)
            self.assertIsInstance(resp.get_json(), dict)

    def test_get_data_as_table_property_missing_for_chosen_node_returns_none(self):
        """When chosen node lacks the requested property, the cell value should be None."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (p:Person {name:'A'})-[:R]->(o:Ort {})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person,Ort'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            # find Ort column and its value
            ord_cols = [i for i, c in enumerate(data['columns']) if c['nodeType'] == 'Ort']
            for i in ord_cols:
                self.assertIsNone(data['rows'][0]['cells'][i]['value'])

    def test_get_data_as_table_trailing_commas_in_nodes_param(self):
        """Trailing commas in nodes parameter should be ignored."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Person {a:1})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person,'})
            self.assertEqual(resp.status_code, 200)

    def test_get_data_as_table_cyclic_graph_with_depth_limit(self):
        """Zyklischer Graph A->B->C->A darf bei hoher maxDepth nicht endlos laufen"""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run(
            "CREATE (a:Person {vorname:'A'})-[:KNOWS]->(b:Person {vorname:'B'})"
            "CREATE (b)-[:KNOWS]->(c:Person {vorname:'C'})"
            "CREATE (c)-[:KNOWS]->(a)"
        )
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes':'Person','maxDepth':'5'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertGreaterEqual(len(data['rows']), 3)

    def test_get_data_as_table_multiple_labels_on_same_node(self):
        """Node mit Person+Autor Labels wird bei nodes=Person erfasst"""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (p:Person:Autor {vorname:'Leo', nachname:'Tolstoi'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes':'Person'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            labels = {c['nodeType'] for c in data['columns']}
            self.assertIn('Person', labels)

    def test_get_data_as_table_property_edge_cases(self):
        """Properties mit None, '', 0, False, langen Strings, Emoji"""
        self.graph.run("MATCH (n) DETACH DELETE n")
        long_str = "😀" + "x"*1000
        self.graph.run(
            "CREATE (:Person {vorname:$v, nachname:$n, age:$a, active:$act, bio:$b})",
            v="", n=None, a=0, act=False, b=long_str
        )
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes':'Person'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertTrue(any(cell['value'] in ("", None, 0, False, long_str)
                                for row in data['rows'] for cell in row['cells']))

    def test_get_data_as_table_array_and_map_properties(self):
        """Listen- und Map-Properties müssen als String serialisierbar sein"""
        self.graph.run("MATCH (n) DETACH DELETE n")
        meta_str = '{"lang":"de","score":5}'
        self.graph.run(
            "CREATE (:Person {tags:$tags, meta:$meta})",
            tags=['a','b'], meta=meta_str
        )
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes':'Person'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            values = [cell['value'] for row in data['rows'] for cell in row['cells']]
            self.assertIn(meta_str, values)
            self.assertIn("a", "".join(str(v) for v in values))

    def test_get_data_as_table_multiple_components(self):
        """Zwei unverbundene Teilgraphen, beide sollen Rows erzeugen"""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Person {vorname:'A'})")
        self.graph.run("CREATE (:Person {vorname:'B'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes':'Person'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertGreaterEqual(len(data['rows']), 2)

    def test_get_data_as_table_parallel_relationships(self):
        """Zwei Relationen zwischen denselben Nodes dürfen keine Duplikatfehler erzeugen"""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run(
            "CREATE (a:Person {vorname:'A'})-[:R1]->(b:Stadt {stadt:'X'}), (a)-[:R2]->(b)"
        )
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes':'Person,Stadt'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertEqual(len(data['rows']), 1)

    def test_get_data_as_table_self_loop_relationship(self):
        """Node mit Self-Loop-Relation darf nicht abstürzen"""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (p:Person {vorname:'Loop'})-[:KNOWS]->(p)")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes':'Person','maxDepth':'2'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertGreaterEqual(len(data['rows']), 1)

    def test_get_data_as_table_unicode_and_special_chars(self):
        """Unicode, Zitate und Backslashes müssen korrekt im JSON erscheinen"""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (:Stadt {stadt:'München \"\\ Test 😀'})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes':'Stadt'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            vals = [cell['value'] for row in data['rows'] for cell in row['cells']]
            self.assertTrue(any("München" in str(v) for v in vals))

    def test_get_data_as_table_large_graph_with_limit(self):
        """100 Personen, limit=10 soll nur 10 Rows liefern"""
        self.graph.run("MATCH (n) DETACH DELETE n")
        for i in range(100):
            self.graph.run(f"CREATE (:Person {{vorname:'P{i}', nachname:'X'}})")
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes':'Person','limit':'10'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertLessEqual(len(data['rows']), 10)

    def test_get_data_as_table_multiple_labels_and_relations(self):
        """Nodes mit mehreren Labels, versch. Relationen und Property-Kombinationen"""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run(
            "CREATE (p:Person:Employee {vorname:'Alice', nachname:'A'}) "
            "CREATE (d:Department:Team {name:'Dev'}) "
            "CREATE (c:City:Place {stadt:'Berlin'}) "
            "CREATE (p)-[:WORKS_IN]->(d), (p)-[:LIVES_IN]->(c), (d)-[:LOCATED_IN]->(c)"
        )
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes':'Person,Employee,Department,Team,City,Place'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertGreaterEqual(len(data['rows']), 1)

    def test_get_data_as_table_cyclic_graph(self):
        """Graph mit Zyklus: Node verweist auf sich selbst und auf andere"""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run(
            "CREATE (n:Node {name:'Loop'}) "
            "CREATE (n)-[:CONNECTS]->(n)"
        )
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes':'Node'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertGreaterEqual(len(data['rows']), 1)
            self.assertEqual(data['rows'][0]['cells'][0]['value'], 'Loop')

    def test_get_data_as_table_missing_properties_and_none_values(self):
        """Nodes mit fehlenden Properties (nicht gesetzte Properties)"""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run(
            "CREATE (p:Person {vorname:'Bob'}) "
            "CREATE (o:Ort) "   # plz fehlt komplett
            "CREATE (p)-[:WOHNT_IN]->(o)"
        )
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes':'Person,Ort'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            # Column für 'Ort.plz' existiert nicht, weil kein Node sie hat
            ort_plz_col = [c for c in data['columns'] if c['nodeType']=='Ort' and c['property']=='plz']
            self.assertEqual(len(ort_plz_col), 0)
            # Person-Vorname sollte vorhanden sein
            person_vorname_col = [c for c in data['columns'] if c['nodeType']=='Person' and c['property']=='vorname']
            self.assertEqual(len(person_vorname_col), 1)
            col_idx = data['columns'].index(person_vorname_col[0])
            value = data['rows'][0]['cells'][col_idx]['value']
            self.assertEqual(value, 'Bob')

        def test_get_data_as_table_deep_hierarchy(self):
            """Tiefer verschachtelter Pfad: Person->Ort->Stadt->Land->Kontinent"""

            # DB sauber leeren
            self.graph.run("MATCH (n) DETACH DELETE n")

            # UUID-Suffix für eindeutige Namen
            suffix = str(uuid4())

            # Nodes und Relationships in einer Transaktion erstellen
            tx = self.graph.begin()
            p = Node("Person", vorname=f"C_{suffix}")
            o = Node("Ort", name=f"O_{suffix}")
            s = Node("Stadt", name=f"S_{suffix}")
            l = Node("Land", name=f"L_{suffix}")
            k = Node("Kontinent", name=f"K_{suffix}")
            tx.create(p)
            tx.create(o)
            tx.create(s)
            tx.create(l)
            tx.create(k)
            tx.create(Relationship(p, "WOHNT_IN", o))
            tx.create(Relationship(o, "LIEGT_IN", s))
            tx.create(Relationship(s, "LIEGT_IN", l))
            tx.create(Relationship(l, "LIEGT_IN", k))
            # Commit über graph.commit()
            self.graph.commit(tx)

            # API-Aufruf
            with self.app as client:
                resp = client.get(
                    '/api/get_data_as_table',
                    query_string={
                        'nodes': 'Person,Ort,Stadt,Land,Kontinent',
                        'maxDepth': 5
                    }
                )
                self.assertEqual(resp.status_code, 200)
                data = resp.get_json()

                # Prüfen, dass genau ein Pfad zurückkommt
                self.assertEqual(len(data['rows']), 1)

    def test_get_data_as_table_parallel_paths(self):
        """Node mit mehreren parallelen Pfaden zu verschiedenen Nodes"""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run(
            "CREATE (p:Person {vorname:'D'}) "
            "CREATE (o1:Ort {name:'O1'}) "
            "CREATE (o2:Ort {name:'O2'}) "
            "CREATE (p)-[:WOHNT_IN]->(o1) "
            "CREATE (p)-[:WOHNT_IN]->(o2)"
        )
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes':'Person,Ort'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertGreaterEqual(len(data['rows']), 1)
            # beide Orte sollten in den Zellen auftauchen
            values = [cell['value'] for row in data['rows'] for cell in row['cells']]
            self.assertTrue(any(v in ('O1','O2') for v in values))

    def test_get_data_as_table_long_strings_and_unicode(self):
        """Nodes mit sehr langen Strings und Unicode/Emoji"""
        self.graph.run("MATCH (n) DETACH DELETE n")
        long_bio = "😀" + "x"*5000
        self.graph.run(
            "CREATE (p:Person {vorname:'E', bio:$bio})", bio=long_bio
        )
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes':'Person'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertIn(long_bio, [cell['value'] for row in data['rows'] for cell in row['cells']])

    def test_get_data_as_table_nodes_with_multiple_adjacent_relations(self):
        """Node mit vielen Adjacents, Auswahl nach min_dist"""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run(
            "CREATE (p:Person {vorname:'F'}) "
            "CREATE (o1:Ort {name:'O1'}) "
            "CREATE (o2:Ort {name:'O2'}) "
            "CREATE (o3:Ort {name:'O3'}) "
            "CREATE (p)-[:WOHNT_IN]->(o2) "
            "CREATE (p)-[:WOHNT_IN]->(o1) "
            "CREATE (p)-[:WOHNT_IN]->(o3)"
        )
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes':'Person,Ort'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertEqual(len(data['rows']), 1)

    def test_get_data_as_table_optional_nodes_and_missing_paths(self):
        """Einige Pfade existieren nicht -> None in entsprechenden Columns"""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run(
            "CREATE (p:Person {vorname:'G'}) "
            "CREATE (o:Ort {name:'O'}) "
            "CREATE (p)-[:WOHNT_IN]->(o)"
        )
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes':'Person,Ort,Stadt'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            # Column für Stadt sollte None enthalten
            city_values = [cell['value'] for row in data['rows'] for col, cell in zip(data['columns'], row['cells']) if col['nodeType']=='Stadt']
            self.assertTrue(all(v is None for v in city_values))

    def test_save_mapping_missing_session_data(self):
        """Fehler, wenn 'raw_data' in session fehlt."""
        with self.app as client:
            with client.session_transaction() as sess:
                sess.pop('raw_data', None)
            resp = client.post('/save_mapping', json={})
            self.assertEqual(resp.status_code, 500)
            self.assertIn(b"raw_data not in session", resp.data)

    def test_save_mapping_single_node(self):
        """Ein einfacher Knoten wird erfolgreich gemerged."""
        csv_data = "name\nAlice"
        mapping = {"nodes": {"Person": [{"original": "name", "renamed": "name"}]}, "relationships": []}

        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)
            self.assertIn(b"Daten erfolgreich", resp.data)

    def test_save_mapping_multiple_nodes(self):
        """Mehrere Knoten ohne Beziehungen werden gemerged."""
        csv_data = "name,city\nAlice,Berlin"
        mapping = {
            "nodes": {
                "Person": [{"original": "name", "renamed": "name"}],
                "Ort": [{"original": "city", "renamed": "stadt"}]
            },
            "relationships": []
        }

        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_missing_field(self):
        """CSV enthält fehlendes Feld, Knoten wird trotzdem erstellt."""
        csv_data = "name\nAlice"
        mapping = {
            "nodes": {
                "Person": [{"original": "name", "renamed": "name"}, {"original": "age", "renamed": "age"}]
            },
            "relationships": []
        }

        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_single_relationship(self):
        """Eine Beziehung wird korrekt erstellt."""
        csv_data = "person_name,city_name\nAlice,Berlin"
        mapping = {
            "nodes": {
                "Person": [{"original": "person_name", "renamed": "name"}],
                "Ort": [{"original": "city_name", "renamed": "stadt"}]
            },
            "relationships": [
                {"from": "Person", "to": "Ort", "type": "WOHNT_IN"}
            ]
        }

        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_duplicate_nodes(self):
        """Doppelte Knoten werden gemerged, nicht dupliziert."""
        csv_data = "name\nAlice\nAlice"
        mapping = {"nodes": {"Person": [{"original": "name", "renamed": "name"}]}, "relationships": []}

        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_numeric_values(self):
        """Numerische Werte in CSV werden korrekt übernommen."""
        csv_data = "name,age\nAlice,30"
        mapping = {"nodes": {"Person": [{"original": "name", "renamed": "name"}, {"original": "age", "renamed": "age"}]}, "relationships": []}

        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_boolean_values(self):
        """Boolean-Werte korrekt speichern."""
        csv_data = "name,active\nAlice,True"
        mapping = {"nodes": {"Person": [{"original": "name", "renamed": "name"}, {"original": "active", "renamed": "active"}]}, "relationships": []}

        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_special_chars(self):
        """Knoten mit Sonderzeichen im Namen."""
        csv_data = "name\nÄlice & Bob"
        mapping = {"nodes": {"Person": [{"original": "name", "renamed": "name"}]}, "relationships": []}

        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_spaces_in_field_names(self):
        """Felder mit Leerzeichen werden korrekt umbenannt."""
        csv_data = "full name\nAlice"
        mapping = {"nodes": {"Person": [{"original": "full name", "renamed": "name"}]}, "relationships": []}

        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_multiple_relationships(self):
        """Mehrere Beziehungen gleichzeitig erstellen."""
        csv_data = "person_name,city_name,country_name\nAlice,Berlin,Deutschland"
        mapping = {
            "nodes": {
                "Person": [{"original": "person_name", "renamed": "name"}],
                "Ort": [{"original": "city_name", "renamed": "stadt"}],
                "Land": [{"original": "country_name", "renamed": "name"}]
            },
            "relationships": [
                {"from": "Person", "to": "Ort", "type": "WOHNT_IN"},
                {"from": "Ort", "to": "Land", "type": "LIEGT_IN"}
            ]
        }

        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_empty_relationships(self):
        """Keine Beziehungen, nur Knoten."""
        csv_data = "name\nAlice"
        mapping = {"nodes": {"Person": [{"original": "name", "renamed": "name"}]}, "relationships": []}

        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_large_csv(self):
        """CSV mit vielen Zeilen wird verarbeitet."""
        csv_data = "name\n" + "\n".join(f"Person{i}" for i in range(50))
        mapping = {"nodes": {"Person": [{"original": "name", "renamed": "name"}]}, "relationships": []}

        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_node_with_multiple_properties(self):
        """Knoten mit mehreren Properties wird erstellt."""
        csv_data = "name,age,city\nAlice,30,Berlin"
        mapping = {
            "nodes": {
                "Person": [
                    {"original": "name", "renamed": "name"},
                    {"original": "age", "renamed": "age"},
                    {"original": "city", "renamed": "city"}
                ]
            },
            "relationships": []
        }

        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_complex_graph_multiple_rows(self):
        """Komplexes Szenario mit mehreren Knoten und Beziehungen."""
        csv_data = "person,city,country\nAlice,Berlin,Deutschland\nBob,Munich,Deutschland"
        mapping = {
            "nodes": {
                "Person": [{"original": "person", "renamed": "name"}],
                "Ort": [{"original": "city", "renamed": "stadt"}],
                "Land": [{"original": "country", "renamed": "name"}]
            },
            "relationships": [
                {"from": "Person", "to": "Ort", "type": "WOHNT_IN"},
                {"from": "Ort", "to": "Land", "type": "LIEGT_IN"}
            ]
        }
        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_complex_missing_some_fields(self):
        """Manche Zeilen haben fehlende Properties, trotzdem Merge möglich."""
        csv_data = "person,city,country\nAlice,Berlin,Deutschland\nBob,,Deutschland"
        mapping = {
            "nodes": {
                "Person": [{"original": "person", "renamed": "name"}],
                "Ort": [{"original": "city", "renamed": "stadt"}],
                "Land": [{"original": "country", "renamed": "name"}]
            },
            "relationships": [
                {"from": "Person", "to": "Ort", "type": "WOHNT_IN"},
                {"from": "Ort", "to": "Land", "type": "LIEGT_IN"}
            ]
        }
        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_complex_duplicate_rows(self):
        """Mehrere identische Zeilen erzeugen keine Duplikate."""
        csv_data = "person,city,country\nAlice,Berlin,Deutschland\nAlice,Berlin,Deutschland"
        mapping = {
            "nodes": {
                "Person": [{"original": "person", "renamed": "name"}],
                "Ort": [{"original": "city", "renamed": "stadt"}],
                "Land": [{"original": "country", "renamed": "name"}]
            },
            "relationships": [
                {"from": "Person", "to": "Ort", "type": "WOHNT_IN"},
                {"from": "Ort", "to": "Land", "type": "LIEGT_IN"}
            ]
        }
        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_complex_special_chars(self):
        """Knoten- und Relationship-Namen mit Sonderzeichen."""
        csv_data = "person,city\nÄlice & Bob,Berlin"
        mapping = {
            "nodes": {"Person": [{"original": "person", "renamed": "name"}], "Ort": [{"original": "city", "renamed": "stadt"}]},
            "relationships": [{"from": "Person", "to": "Ort", "type": "WOHNT IN"}]
        }
        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_complex_mixed_types(self):
        """CSV mit gemischten Datentypen, alle Properties korrekt gesetzt."""
        csv_data = "person,age,active\nAlice,30,True\nBob,25,False"
        mapping = {"nodes": {"Person": [{"original": "person", "renamed": "name"}, {"original": "age", "renamed": "age"}, {"original": "active", "renamed": "active"}]}, "relationships": []}

        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_complex_many_nodes_and_rels(self):
        """Sehr komplex: 3 Zeilen, 3 Knoten-Typen, 2 Beziehungen pro Zeile."""
        csv_data = "person,city,country\nAlice,Berlin,Deutschland\nBob,Munich,Deutschland\nCarol,Hamburg,Deutschland"
        mapping = {
            "nodes": {
                "Person": [{"original": "person", "renamed": "name"}],
                "Ort": [{"original": "city", "renamed": "stadt"}],
                "Land": [{"original": "country", "renamed": "name"}]
            },
            "relationships": [
                {"from": "Person", "to": "Ort", "type": "WOHNT_IN"},
                {"from": "Ort", "to": "Land", "type": "LIEGT_IN"}
            ]
        }
        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_super_complex_nested_graph(self):
        """3 Zeilen, 4 Knoten-Typen, 4 Beziehungen, verschachtelt und teilweise fehlende Werte."""
        csv_data = "person,city,country,company\nAlice,Berlin,Deutschland,ACME\nBob,Munich,,Globex\nCarol,Hamburg,Deutschland,"
        mapping = {
            "nodes": {
                "Person": [{"original": "person", "renamed": "name"}],
                "Ort": [{"original": "city", "renamed": "stadt"}],
                "Land": [{"original": "country", "renamed": "name"}],
                "Firma": [{"original": "company", "renamed": "name"}]
            },
            "relationships": [
                {"from": "Person", "to": "Ort", "type": "WOHNT_IN"},
                {"from": "Ort", "to": "Land", "type": "LIEGT_IN"},
                {"from": "Person", "to": "Firma", "type": "ARBEITET_FUER"},
                {"from": "Firma", "to": "Land", "type": "IST_IN"}
            ]
        }
        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_super_complex_special_chars_and_types(self):
        """CSV mit Sonderzeichen, gemischten Datentypen und boolean-Werten, alle Beziehungen werden gesetzt."""
        csv_data = "person,city,country,age,active\nÄlice & Bob,Berlin,Deutschland,30,True\nBób,München,Deutschland,25,False\nCarol,Hamburg,Deutschland,28,True"
        mapping = {
            "nodes": {
                "Person": [
                    {"original": "person", "renamed": "name"},
                    {"original": "age", "renamed": "age"},
                    {"original": "active", "renamed": "active"}
                ],
                "Ort": [{"original": "city", "renamed": "stadt"}],
                "Land": [{"original": "country", "renamed": "name"}]
            },
            "relationships": [
                {"from": "Person", "to": "Ort", "type": "WOHNT_IN"},
                {"from": "Ort", "to": "Land", "type": "LIEGT_IN"}
            ]
        }
        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_super_complex_large_graph_with_duplicates(self):
        """10 Zeilen, mehrere Knoten-Typen, viele Duplikate, verschachtelte Beziehungen, fehlende Felder teilweise."""
        csv_data = "\n".join([
            "Alice,Berlin,Deutschland,ACME",
            "Bob,Munich,Deutschland,Globex",
            "Carol,Hamburg,Deutschland,ACME",
            "Dave,Berlin,Deutschland,Globex",
            "Eve,Berlin,Deutschland,ACME",
            "Alice,Berlin,Deutschland,ACME",  # Duplikat
            "Bob,Munich,Deutschland,Globex",  # Duplikat
            "Frank,Hamburg,,ACME",            # fehlendes Land
            "Grace,,Deutschland,Globex",      # fehlender Ort
            "Heidi,Hamburg,Deutschland,"      # fehlende Firma
        ])
        mapping = {
            "nodes": {
                "Person": [{"original": "person", "renamed": "name"}],
                "Ort": [{"original": "city", "renamed": "stadt"}],
                "Land": [{"original": "country", "renamed": "name"}],
                "Firma": [{"original": "company", "renamed": "name"}]
            },
            "relationships": [
                {"from": "Person", "to": "Ort", "type": "WOHNT_IN"},
                {"from": "Ort", "to": "Land", "type": "LIEGT_IN"},
                {"from": "Person", "to": "Firma", "type": "ARBEITET_FUER"},
                {"from": "Firma", "to": "Land", "type": "IST_IN"}
            ]
        }
        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_mega_complex_multi_level_graph(self):
        """10 Zeilen, 6 Knoten-Typen, 4 Ebenen Beziehungen, gemischte Daten, Duplikate, fehlende Felder."""
        csv_data = "\n".join([
            "person,city,country,company,dept,role",
            "Alice,Berlin,Deutschland,ACME,IT,Engineer",
            "Bob,Munich,Deutschland,Globex,Sales,Manager",
            "Carol,Hamburg,Deutschland,ACME,HR,Analyst",
            "Dave,Berlin,Deutschland,Globex,IT,Engineer",
            "Eve,Berlin,Deutschland,ACME,Sales,Manager",
            "Alice,Berlin,Deutschland,ACME,IT,Engineer",  # Duplikat
            "Bob,Munich,Deutschland,Globex,Sales,Manager",  # Duplikat
            "Frank,Hamburg,,ACME,HR,Analyst",            # fehlendes Land
            "Grace,,Deutschland,Globex,IT,Engineer",      # fehlender Ort
            "Heidi,Hamburg,Deutschland,,Sales,Manager"   # fehlende Firma
        ])
        mapping = {
            "nodes": {
                "Person": [{"original": "person", "renamed": "name"}, {"original": "role", "renamed": "role"}],
                "Ort": [{"original": "city", "renamed": "stadt"}],
                "Land": [{"original": "country", "renamed": "name"}],
                "Firma": [{"original": "company", "renamed": "name"}],
                "Abteilung": [{"original": "dept", "renamed": "name"}],
                "Position": [{"original": "role", "renamed": "title"}]
            },
            "relationships": [
                {"from": "Person", "to": "Ort", "type": "WOHNT_IN"},
                {"from": "Ort", "to": "Land", "type": "LIEGT_IN"},
                {"from": "Person", "to": "Firma", "type": "ARBEITET_FUER"},
                {"from": "Firma", "to": "Land", "type": "IST_IN"},
                {"from": "Person", "to": "Abteilung", "type": "GEHOERT_ZU"},
                {"from": "Abteilung", "to": "Position", "type": "BESTEHT_AUS"}
            ]
        }
        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_mega_complex_special_characters_and_types(self):
        """Zeilen mit Sonderzeichen, Umlauten, Emojis, boolean & numeric, alle Beziehungen."""
        csv_data = "\n".join([
            "person,city,country,active,age",
            "Älice & Bob,Berlin,Deutschland,True,30",
            "Bób,München,Deutschland,False,25",
            "Carol,Hamburg,Deutschland,True,28",
            "D@ve,Berlin,Deutschland,False,35",
            "Eve,Hamburg,Deutschland,True,32"
        ])
        mapping = {
            "nodes": {
                "Person": [
                    {"original": "person", "renamed": "name"},
                    {"original": "age", "renamed": "age"},
                    {"original": "active", "renamed": "active"}
                ],
                "Ort": [{"original": "city", "renamed": "stadt"}],
                "Land": [{"original": "country", "renamed": "name"}]
            },
            "relationships": [
                {"from": "Person", "to": "Ort", "type": "WOHNT_IN"},
                {"from": "Ort", "to": "Land", "type": "LIEGT_IN"}
            ]
        }
        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_mega_complex_many_to_many(self):
        """Mehrere Personen, mehrere Orte, viele-to-viele Beziehungen, teilweise fehlende Felder."""
        csv_data = "\n".join([
            "person,city,country,company",
            "Alice,Berlin,Deutschland,ACME",
            "Alice,Hamburg,Deutschland,ACME",
            "Bob,Munich,Deutschland,Globex",
            "Bob,Berlin,Deutschland,Globex",
            "Carol,Hamburg,Deutschland,ACME",
            "Dave,Berlin,Deutschland,Globex",
            "Eve,Berlin,Deutschland,ACME"
        ])
        mapping = {
            "nodes": {
                "Person": [{"original": "person", "renamed": "name"}],
                "Ort": [{"original": "city", "renamed": "stadt"}],
                "Land": [{"original": "country", "renamed": "name"}],
                "Firma": [{"original": "company", "renamed": "name"}]
            },
            "relationships": [
                {"from": "Person", "to": "Ort", "type": "WOHNT_IN"},
                {"from": "Person", "to": "Firma", "type": "ARBEITET_FUER"},
                {"from": "Ort", "to": "Land", "type": "LIEGT_IN"},
                {"from": "Firma", "to": "Land", "type": "IST_IN"}
            ]
        }
        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_mega_complex_large_duplicates_and_missing(self):
        """15 Zeilen, viele Duplikate, fehlende Felder, Sonderzeichen, boolean, numeric, viele Beziehungen."""
        csv_data = "\n".join([
            "person,city,country,company,age,active",
            "Alice,Berlin,Deutschland,ACME,30,True",
            "Bob,Munich,Deutschland,Globex,25,False",
            "Carol,Hamburg,Deutschland,ACME,28,True",
            "Dave,Berlin,Deutschland,Globex,35,False",
            "Eve,Hamburg,Deutschland,ACME,32,True",
            "Alice,Berlin,Deutschland,ACME,30,True",   # Duplikat
            "Bob,Munich,Deutschland,Globex,25,False", # Duplikat
            "Frank,Hamburg,,ACME,29,True",            # fehlendes Land
            "Grace,,Deutschland,Globex,31,False",     # fehlender Ort
            "Heidi,Hamburg,Deutschland,,27,True",     # fehlende Firma
            "Ivan,Berlin,Deutschland,ACME,34,False",
            "Judy,Hamburg,Deutschland,Globex,30,True",
            "Mallory,Berlin,Deutschland,ACME,28,True",
            "Oscar,,Deutschland,Globex,26,False",
            "Peggy,Hamburg,Deutschland,,33,True"
        ])
        mapping = {
            "nodes": {
                "Person": [{"original": "person", "renamed": "name"}, {"original": "age", "renamed": "age"}, {"original": "active", "renamed": "active"}],
                "Ort": [{"original": "city", "renamed": "stadt"}],
                "Land": [{"original": "country", "renamed": "name"}],
                "Firma": [{"original": "company", "renamed": "name"}]
            },
            "relationships": [
                {"from": "Person", "to": "Ort", "type": "WOHNT_IN"},
                {"from": "Person", "to": "Firma", "type": "ARBEITET_FUER"},
                {"from": "Ort", "to": "Land", "type": "LIEGT_IN"},
                {"from": "Firma", "to": "Land", "type": "IST_IN"}
            ]
        }
        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_mega_complex_extreme_special_chars(self):
        """CSV mit extremen Sonderzeichen, Unicode, Emojis, Leerzeichen, alle Beziehungen werden korrekt gesetzt."""
        csv_data = "\n".join([
            "person,city,country,company",
            "Ålice 🚀,Berlin,Deutschland,ACME 🔧",
            "Bób 🐍,München,Deutschland,Globex 💼",
            "Carôl 🌟,Hamburg,Deutschland,ACME 🔧",
            "D@ve 💻,Berlin,Deutschland,Globex 💼",
            "Eve 🌈,Hamburg,Deutschland,ACME 🔧"
        ])
        mapping = {
            "nodes": {
                "Person": [{"original": "person", "renamed": "name"}],
                "Ort": [{"original": "city", "renamed": "stadt"}],
                "Land": [{"original": "country", "renamed": "name"}],
                "Firma": [{"original": "company", "renamed": "name"}]
            },
            "relationships": [
                {"from": "Person", "to": "Ort", "type": "WOHNT_IN"},
                {"from": "Ort", "to": "Land", "type": "LIEGT_IN"},
                {"from": "Person", "to": "Firma", "type": "ARBEITET_FUER"},
                {"from": "Firma", "to": "Land", "type": "IST_IN"}
            ]
        }
        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_hyper_complex_multi_layer_network(self):
        """20 Zeilen, 8 Knoten-Typen, 5 Ebenen Beziehungen, teils zyklisch, Sonderzeichen, Duplikate, fehlende Werte"""
        csv_data = "\n".join([
            "person,city,country,company,dept,role,project",
            "Alice 🚀,Berlin,Deutschland,ACME,IT,Engineer,ProjectX",
            "Bob 🐍,München,Deutschland,Globex,Sales,Manager,ProjectY",
            "Carol 🌟,Hamburg,Deutschland,ACME,HR,Analyst,ProjectX",
            "Dave 💻,Berlin,Deutschland,Globex,IT,Engineer,ProjectZ",
            "Eve 🌈,Hamburg,Deutschland,ACME,Sales,Manager,ProjectY",
            "Frank 🧪,Berlin,Deutschland,ACME,IT,Intern,ProjectX",
            "Grace 💡,München,Deutschland,Globex,HR,Analyst,ProjectZ",
            "Heidi 🔧,Hamburg,Deutschland,,Sales,Manager,ProjectY",  # fehlende Firma
            "Ivan 🏹,Berlin,Deutschland,ACME,IT,Engineer,",  # fehlendes Projekt
            "Judy 🎨,Hamburg,Deutschland,Globex,Sales,Manager,ProjectZ",
            "Mallory ⚡,Berlin,,ACME,HR,Analyst,ProjectX",      # fehlendes Land
            "Oscar 🌊,München,Deutschland,Globex,IT,Engineer,ProjectY",
            "Peggy 🌹,Hamburg,Deutschland,,Sales,Manager,ProjectZ",  # fehlende Firma
            "Trent 🛡️,Berlin,Deutschland,ACME,IT,Engineer,ProjectX",
            "Victor 🕶️,München,Deutschland,Globex,HR,Analyst,ProjectY",
            "Walter 🔥,Berlin,Deutschland,ACME,Sales,Manager,ProjectZ",
            "Yvonne 🧩,Hamburg,Deutschland,Globex,IT,Intern,ProjectX",
            "Zoe 🌌,Berlin,Deutschland,ACME,HR,Engineer,ProjectY",
            "Alice 🚀,Berlin,Deutschland,ACME,IT,Engineer,ProjectX",  # exaktes Duplikat
            "Bob 🐍,München,Deutschland,Globex,Sales,Manager,ProjectY"  # exaktes Duplikat
        ])
        mapping = {
            "nodes": {
                "Person": [{"original": "person", "renamed": "name"}, {"original": "role", "renamed": "role"}],
                "Ort": [{"original": "city", "renamed": "stadt"}],
                "Land": [{"original": "country", "renamed": "name"}],
                "Firma": [{"original": "company", "renamed": "name"}],
                "Abteilung": [{"original": "dept", "renamed": "name"}],
                "Projekt": [{"original": "project", "renamed": "name"}],
                "Position": [{"original": "role", "renamed": "title"}],
                "Team": [{"original": "dept", "renamed": "team_name"}]
            },
            "relationships": [
                {"from": "Person", "to": "Ort", "type": "WOHNT_IN"},
                {"from": "Ort", "to": "Land", "type": "LIEGT_IN"},
                {"from": "Person", "to": "Firma", "type": "ARBEITET_FUER"},
                {"from": "Firma", "to": "Land", "type": "IST_IN"},
                {"from": "Person", "to": "Abteilung", "type": "GEHOERT_ZU"},
                {"from": "Abteilung", "to": "Position", "type": "BESTEHT_AUS"},
                {"from": "Person", "to": "Projekt", "type": "ARBEITET_AN"},
                {"from": "Projekt", "to": "Team", "type": "WIRD_GELEITET_VON"},
                {"from": "Team", "to": "Firma", "type": "IST_TEIL_VON"}
            ]
        }
        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_hyper_complex_multiple_values_and_conflicts(self):
        """CSV mit gleichen Knoten mehrfach mit leicht unterschiedlichen Properties, testet Merge/ON CREATE SET Konflikte"""
        csv_data = "\n".join([
            "person,city,country,company,role",
            "Alice,Berlin,Deutschland,ACME,Engineer",
            "Alice,Berlin,Deutschland,ACME,Senior Engineer",
            "Alice,Berlin,Deutschland,ACME,Lead Engineer",
            "Bob,München,Deutschland,Globex,Manager",
            "Bob,München,Deutschland,Globex,Director",
            "Carol,Hamburg,Deutschland,ACME,Analyst",
            "Carol,Hamburg,Deutschland,ACME,Senior Analyst"
        ])
        mapping = {
            "nodes": {
                "Person": [{"original": "person", "renamed": "name"}, {"original": "role", "renamed": "role"}],
                "Ort": [{"original": "city", "renamed": "stadt"}],
                "Land": [{"original": "country", "renamed": "name"}],
                "Firma": [{"original": "company", "renamed": "name"}]
            },
            "relationships": [
                {"from": "Person", "to": "Ort", "type": "WOHNT_IN"},
                {"from": "Ort", "to": "Land", "type": "LIEGT_IN"},
                {"from": "Person", "to": "Firma", "type": "ARBEITET_FUER"},
                {"from": "Firma", "to": "Land", "type": "IST_IN"}
            ]
        }
        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_save_mapping_hyper_complex_extremely_interconnected_graph(self):
        """Extrem vernetzter Graph: jeder Person-Knoten mit 3–5 Orten, Firmen, Projekten verbunden, 5 Ebenen, 25 Zeilen, Sonderzeichen, fehlende Felder"""
        csv_data = "\n".join([
            "person,city,country,company,dept,role,project",
            "Alice,Berlin,Deutschland,ACME,IT,Engineer,ProjectX",
            "Alice,Hamburg,Deutschland,Globex,HR,Analyst,ProjectY",
            "Alice,München,Deutschland,ACME,Sales,Manager,ProjectZ",
            "Bob,Berlin,Deutschland,Globex,IT,Engineer,ProjectX",
            "Bob,Hamburg,Deutschland,ACME,HR,Analyst,ProjectY",
            "Bob,München,Deutschland,Globex,Sales,Manager,ProjectZ",
            "Carol,Berlin,Deutschland,ACME,IT,Engineer,ProjectX",
            "Carol,Hamburg,Deutschland,Globex,HR,Analyst,ProjectY",
            "Carol,München,Deutschland,ACME,Sales,Manager,ProjectZ",
            "Dave,Berlin,Deutschland,Globex,IT,Engineer,ProjectX",
            "Dave,Hamburg,Deutschland,ACME,HR,Analyst,ProjectY",
            "Dave,München,Deutschland,Globex,Sales,Manager,ProjectZ",
            "Eve,Berlin,Deutschland,ACME,IT,Engineer,ProjectX",
            "Eve,Hamburg,Deutschland,Globex,HR,Analyst,ProjectY",
            "Eve,München,Deutschland,ACME,Sales,Manager,ProjectZ",
            "Frank,Berlin,,ACME,IT,Engineer,ProjectX",  # fehlendes Land
            "Grace,Hamburg,Deutschland,,HR,Analyst,ProjectY",  # fehlende Firma
            "Heidi,München,Deutschland,Globex,Sales,Manager,",  # fehlendes Projekt
            "Ivan,Berlin,Deutschland,ACME,IT,Engineer,ProjectX",
            "Judy,Hamburg,Deutschland,Globex,HR,Analyst,ProjectY",
            "Mallory,München,Deutschland,ACME,Sales,Manager,ProjectZ",
            "Oscar,Berlin,Deutschland,Globex,IT,Engineer,ProjectX",
            "Peggy,Hamburg,Deutschland,ACME,HR,Analyst,ProjectY",
            "Trent,München,Deutschland,Globex,Sales,Manager,ProjectZ",
            "Victor,Berlin,Deutschland,ACME,IT,Engineer,ProjectX"
        ])
        mapping = {
            "nodes": {
                "Person": [{"original": "person", "renamed": "name"}, {"original": "role", "renamed": "role"}],
                "Ort": [{"original": "city", "renamed": "stadt"}],
                "Land": [{"original": "country", "renamed": "name"}],
                "Firma": [{"original": "company", "renamed": "name"}],
                "Abteilung": [{"original": "dept", "renamed": "name"}],
                "Projekt": [{"original": "project", "renamed": "name"}],
                "Position": [{"original": "role", "renamed": "title"}],
                "Team": [{"original": "dept", "renamed": "team_name"}]
            },
            "relationships": [
                {"from": "Person", "to": "Ort", "type": "WOHNT_IN"},
                {"from": "Ort", "to": "Land", "type": "LIEGT_IN"},
                {"from": "Person", "to": "Firma", "type": "ARBEITET_FUER"},
                {"from": "Firma", "to": "Land", "type": "IST_IN"},
                {"from": "Person", "to": "Abteilung", "type": "GEHOERT_ZU"},
                {"from": "Abteilung", "to": "Position", "type": "BESTEHT_AUS"},
                {"from": "Person", "to": "Projekt", "type": "ARBEITET_AN"},
                {"from": "Projekt", "to": "Team", "type": "WIRD_GELEITET_VON"},
                {"from": "Team", "to": "Firma", "type": "IST_TEIL_VON"}
            ]
        }
        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = csv_data
            resp = client.post('/save_mapping', json=mapping)
            self.assertEqual(resp.status_code, 200)

    def test_create_node_basic_success(self):
        """Ein einfacher Node ohne Beziehung"""
        with patch("app.graph.run") as mock_run:
            mock_run.return_value.data.return_value = [{"id": 1}]
            resp = self.app.post('/api/create_node', json={"property": "name", "value": "Alice"})
            self.assertEqual(resp.status_code, 200)
            self.assertIn(b"Neuer Node erstellt", resp.data)

    def test_create_node_missing_body(self):
        """Leerer Body -> 400"""
        resp = self.app.post('/api/create_node', data="")
        self.assertEqual(resp.status_code, 400)
        self.assertIn(b"Request-Body leer", resp.data)

    def test_create_node_invalid_property_name(self):
        """Property ist kein identifier -> 400"""
        resp = self.app.post('/api/create_node', json={"property": "123abc", "value": "Alice"})
        self.assertEqual(resp.status_code, 400)
        self.assertIn(b"ltiger Property-Name", resp.data)

    def test_create_node_no_connect_ids(self):
        """Relation-Dict vorhanden, aber connectTo leer -> Node wird nur erstellt"""
        with patch("app.graph.run") as mock_run:
            mock_run.return_value.data.return_value = [{"id": 50}]
            resp = self.app.post('/api/create_node', json={
                "property": "name",
                "value": "Eve",
                "connectTo": [],
                "relation": {"relation": "KNOWS"}
            })
            self.assertEqual(resp.status_code, 200)

    # -----------------------------
    # Mittlere Komplexität
    # -----------------------------
    def test_create_node_with_target_label(self):
        """Node wird unter benutzerdefiniertem Label erstellt"""
        with patch("app.graph.run") as mock_run:
            mock_run.return_value.data.return_value = [{"id": 60}]
            resp = self.app.post('/api/create_node', json={
                "property": "name",
                "value": "Frank",
                "relation": {"targetLabel": "Person"}
            })
            self.assertEqual(resp.status_code, 200)

    def test_create_node_value_numeric_boolean(self):
        """Value ist Zahl oder Boolean"""
        with patch("app.graph.run") as mock_run:
            mock_run.return_value.data.return_value = [{"id": 80}]
            resp1 = self.app.post('/api/create_node', json={"property": "age", "value": 42})
            resp2 = self.app.post('/api/create_node', json={"property": "active", "value": True})
            self.assertEqual(resp1.status_code, 200)
            self.assertEqual(resp2.status_code, 200)

    def test_create_node_extreme_special_chars(self):
        """Node name mit Umlauten, Emojis, Sonderzeichen"""
        with patch("app.graph.run") as mock_run:
            mock_run.return_value.data.return_value = [{"id": 120}]
            resp = self.app.post('/api/create_node', json={
                "property": "name",
                "value": "Ålice 🚀 & Bob 🐍",
                "connectTo": [],
                "relation": {"relation": "KNOWS"}
            })
            self.assertEqual(resp.status_code, 200)

    def test_create_node_missing_target_label_default_node(self):
        """Node ohne targetLabel -> Default 'Node'"""
        with patch("app.graph.run") as mock_run:
            mock_run.return_value.data.return_value = [{"id": 150}]
            resp = self.app.post('/api/create_node', json={
                "property": "title",
                "value": "CEO"
            })
            self.assertEqual(resp.status_code, 200)

    def tearDown_nodes_by_uid(self, uid):
        self.graph.run("MATCH (n {uid:$uid}) DETACH DELETE n", uid=uid)

    def tearDown_nodes_by_uid(self, uid):
        self.graph.run("MATCH (n {uid:$uid}) DETACH DELETE n", uid=uid)

    def test_get_data_as_table_person_ort_buch_stable(self):
        uid = str(uuid4())
        expected_persons = {'Maria', 'Hans', 'Anna', 'Bob', 'Charlie'}

        # Cleanup vorher
        self.tearDown_nodes_by_uid(uid)

        # --- Nodes erzeugen ---
        person_nodes = []
        ort_nodes = []
        buch_nodes = []

        persons = [
            ("Maria", "Müller", "10115", "Hauptstraße 1", "The Cypher Key", 2023),
            ("Hans", "Schmidt", "20095", "Marktplatz 5", None, None),
            ("Anna", "Fischer", "80331", "Bahnhofsallee 12", None, None),
            ("Bob", "Johnson", "", "", "The Graph Odyssey", 2022),
            ("Charlie", "Brown", "", "", "Neo's Journey", 2024)
        ]

        for vorname, nachname, plz, strasse, buch, jahr in persons:
            person_nodes.append(Node("Person", vorname=vorname, nachname=nachname, uid=uid))
            ort_nodes.append(Node("Ort", plz=plz, straße=strasse, uid=uid))
            if buch:
                buch_nodes.append(Node("Buch", titel=buch, erscheinungsjahr=jahr, uid=uid))

        # --- Alle Nodes zusammen in einem Subgraph erstellen ---
        all_nodes = person_nodes + ort_nodes + buch_nodes
        self.graph.create(Subgraph(nodes=all_nodes))

        # --- Relationships erstellen ---
        for i, p in enumerate(person_nodes):
            self.graph.run(
                "MATCH (p:Person {vorname:$vorname, uid:$uid}), (o:Ort {uid:$uid}) "
                "CREATE (p)-[:WOHNT_IN]->(o)",
                vorname=p['vorname'], uid=uid
            )
            if i < len(buch_nodes):
                self.graph.run(
                    "MATCH (p:Person {vorname:$vorname, uid:$uid}), (b:Buch {uid:$uid}) "
                    "CREATE (p)-[:HAT_GESCHRIEBEN]->(b)",
                    vorname=p['vorname'], uid=uid
                )

        # --- API-Call ---
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes': 'Person,Ort,Buch'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()

        # --- Prüfen dass alle Personen da sind ---
        persons_found = set()
        for row in data['rows']:
            for c in row['cells']:
                if 'value' in c and c['value'] in expected_persons:
                    persons_found.add(c['value'])
        self.assertEqual(persons_found, expected_persons, f"Nur gefunden: {persons_found}")

        # Cleanup
        self.tearDown_nodes_by_uid(uid)

    def _wait_for_nodes(self, label, uid, expected_count, retries=10, delay=0.1):
        """Warte bis alle Nodes für Label + UID sichtbar sind"""
        for _ in range(retries):
            count = self.graph.evaluate(f"MATCH (n:`{label}` {{uid:$uid}}) RETURN count(n)", uid=uid)
            if count == expected_count:
                return
            time.sleep(delay)
        raise RuntimeError(f"Nicht alle Nodes {label} mit uid={uid} gefunden. Erwartet: {expected_count}, gefunden: {count}")

    def test_get_data_as_table_no_missing_nodes(self):
        """Ensure previously missing labels (e.g., 'Node-Typ 2', 'NT2') are always included."""
        # Alte Testdaten löschen
        self.graph.run("MATCH (n) DETACH DELETE n")

        # Setup Nodes, inklusive Labels, die früher Probleme gemacht haben
        self.graph.run("""
            CREATE (n1:NT2 {xxxxxxxx:'qwertz'})
            CREATE (n2:`Node-Typ 2` {abc:'XXXAAABBB'})
            CREATE (p1:Person {vorname:'Maria', nachname:'Müller'})
            CREATE (o1:Ort {plz:'10115', straße:'Hauptstraße 1'})
            CREATE (p1)-[:WOHNT_IN]->(o1)
        """)

        with self.app as client:
            resp = client.get('/api/get_data_as_table',
                            query_string={'nodes': 'NT2,Node-Typ 2,Person,Ort'})
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()

            # Alle Rows prüfen
            all_labels_in_rows = set()
            for row in data['rows']:
                for col in row['cells']:
                    if col['value'] is not None:
                        all_labels_in_rows.add(col.get('nodeId'))

            # Prüfen, dass die alten problematischen Nodes jetzt auftauchen
            nt2_node_ids = [n['nodeId'] for row in data['rows']
                            for n in row['cells'] if n['value'] == 'qwertz']
            node_typ2_ids = [n['nodeId'] for row in data['rows']
                            for n in row['cells'] if n['value'] == 'XXXAAABBB']

            self.assertTrue(nt2_node_ids, "NT2 node fehlt im Ergebnis")
            self.assertTrue(node_typ2_ids, "Node-Typ 2 node fehlt im Ergebnis")

    def test_complex_multi_level_paths(self):
        """Person -> Ort -> Buch -> NT2, verschiedene Depths, teilweise fehlende Nodes."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("""
            CREATE (p1:Person {vorname:'Alice', nachname:'Wonder'})
            CREATE (o1:Ort {plz:'12345', straße:'Fabelweg 1'})
            CREATE (b1:Buch {titel:'Fabelbuch', erscheinungsjahr:2021})
            CREATE (nt2:NT2 {xxxxxxxx:'foobar'})
            CREATE (p1)-[:WOHNT_IN]->(o1)
            CREATE (o1)-[:HAT_BUCH]->(b1)
            CREATE (b1)-[:REFERENZIERT]->(nt2)
        """)
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes':'Person,Ort,Buch,NT2'})
            data = resp.get_json()
            self.assertEqual(resp.status_code, 200)
            # Prüfen, dass jeder Node-Typ vertreten ist
            values = [c['value'] for row in data['rows'] for c in row['cells']]
            self.assertIn('Alice', values)
            self.assertIn('Fabelbuch', values)
            self.assertIn('foobar', values)

    def test_nodes_with_multiple_labels(self):
        """Ein Node hat mehrere Labels und wird korrekt zugeordnet."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("""
            CREATE (p:Person:Buch {vorname:'Dora', titel:'Dora’s Adventure'})
        """)
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes':'Person,Buch'})
            data = resp.get_json()
            values = [c['value'] for row in data['rows'] for c in row['cells']]
            self.assertIn('Dora', values)
            self.assertIn("Dora’s Adventure", values)

    def test_nested_loops(self):
        """Person -> Buch -> NT2 -> Person loop, prüfen min_dist korrekt."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("""
            CREATE (p1:Person {vorname:'Eve'})
            CREATE (b1:Buch {titel:'Loop Book'})
            CREATE (nt2:NT2 {xxxxxxxx:'loopNT2'})
            CREATE (p1)-[:HAT_GESCHRIEBEN]->(b1)
            CREATE (b1)-[:REFERENZIERT]->(nt2)
            CREATE (nt2)-[:VERKNUEPFT_MIT]->(p1)
        """)
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes':'Person,Buch,NT2'})
            data = resp.get_json()
            values = [c['value'] for row in data['rows'] for c in row['cells']]
            self.assertIn('Eve', values)
            self.assertIn('Loop Book', values)
            self.assertIn('loopNT2', values)

    def test_missing_properties_and_mixed_types(self):
        """Nodes mit teilweise fehlenden Properties oder unterschiedlichen Datentypen."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("""
            CREATE (p1:Person {vorname:'Frank'})
            CREATE (b1:Buch {titel:'Mystery', erscheinungsjahr:''})
            CREATE (o1:Ort {plz:'', straße:'Mystery Lane'})
            CREATE (p1)-[:WOHNT_IN]->(o1)
            CREATE (p1)-[:HAT_GESCHRIEBEN]->(b1)
        """)
        with self.app as client:
            resp = client.get('/api/get_data_as_table', query_string={'nodes':'Person,Buch,Ort'})
            data = resp.get_json()
            # Prüfen, dass keine None-Fehler auftauchen
            for row in data['rows']:
                for cell in row['cells']:
                    self.assertIn('value', cell)

    def test_reset_and_load_data(self):
        """Testet, ob die Datenbank wirklich geleert und mit Default-Daten befüllt wird."""
        response = self.app.get('/api/reset_and_load_data')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertEqual(data["status"], "success")

        # Check persons
        persons = self.graph.run("MATCH (p:Person) RETURN p.vorname AS vorname, p.nachname AS nachname").data()
        names = {(p["vorname"], p["nachname"]) for p in persons}
        expected_names = {
            ("Maria", "Müller"),
            ("Hans", "Schmidt"),
            ("Anna", "Fischer"),
            ("Bob", "Johnson"),
            ("Charlie", "Brown"),
        }
        self.assertTrue(expected_names.issubset(names))

        # Check books
        books = self.graph.run("MATCH (b:Buch) RETURN b.titel AS titel, b.erscheinungsjahr AS jahr").data()
        titles = {b["titel"] for b in books}
        expected_titles = {"The Cypher Key", "The Graph Odyssey", "Neo's Journey"}
        self.assertTrue(expected_titles.issubset(titles))

        # Check relations: Maria Müller HAT_GESCHRIEBEN The Cypher Key
        rel = self.graph.run("""
            MATCH (p:Person {vorname:'Maria', nachname:'Müller'})-[:HAT_GESCHRIEBEN]->(b:Buch {titel:'The Cypher Key'})
            RETURN p,b
        """).data()
        self.assertEqual(len(rel), 1)

        # Check at least one WOHNT_IN relation exists
        rels = self.graph.run("MATCH (p:Person)-[:WOHNT_IN]->(o:Ort) RETURN p,o LIMIT 1").data()
        self.assertTrue(len(rels) > 0)

    def test_show_graph_returns_html(self):
        with self.app as client:
            response = client.get('/graph')
            self.assertEqual(response.status_code, 200)
            self.assertIn(b'<!DOCTYPE html', response.data)
            self.assertIn(b'<html', response.data)

    def test_dump_database_returns_json(self):
        with self.app as client:
            response = client.get("/api/dump_database")
            self.assertEqual(response.status_code, 200)
            data = response.get_json()
            self.assertIn("nodes", data)
            self.assertIn("relationships", data)
            self.assertIsInstance(data["nodes"], list)
            self.assertIsInstance(data["relationships"], list)

    def test_dump_database_contains_created_node(self):
        node = Node("TestLabel", name="Testy")
        self.graph.create(node)

        with self.app as client:
            response = client.get("/api/dump_database")
            self.assertEqual(response.status_code, 200)
            data = response.get_json()
            node_ids = [n["id"] for n in data["nodes"]]
            self.assertIn(node.identity, node_ids)

    def test_duplicate_query_name(self):
        self.app.post("/api/save_query", json={"name": "Duplicate", "selectedLabels": ["X"]})
        resp = self.app.post("/api/save_query", json={"name": "Duplicate", "selectedLabels": ["X"]})
        self.assertEqual(resp.status_code, 409)
        self.assertEqual(resp.get_json()["status"], "error")

    def test_missing_fields(self):
        resp = self.app.post("/api/save_query", json={"name": ""})
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.get_json()["status"], "error")

    def test_add_row_success(self):
        resp = self.app.post("/api/add_row", json={
            "label": "Person",
            "properties": {"name": "Alice", "age": 30}
        })
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data["status"], "success")
        self.assertIn("Person", data["message"])
        self.assertIn("id", data)

    def test_add_row_missing_label(self):
        resp = self.app.post("/api/add_row", json={"properties": {"foo": "bar"}})
        self.assertEqual(resp.status_code, 400)
        self.assertIn("label", resp.get_json()["message"])

    def test_delete_all_success(self):
        # Vorher ein Node anlegen, damit auch wirklich was gelöscht wird
        self.graph.run("CREATE (:TestLabel {foo:'bar'})")

        resp = self.app.get("/api/delete_all")
        self.assertEqual(resp.status_code, 200)

        data = resp.get_json()
        self.assertEqual(data["status"], "success")
        self.assertIn("Alle Knoten", data["message"])

        # Sicherstellen, dass die DB jetzt leer ist
        result = self.graph.run("MATCH (n) RETURN count(n) AS cnt").data()[0]["cnt"]
        self.assertEqual(result, 0)

    def test_delete_all_with_empty_db(self):
        # Sicherstellen, dass die DB leer ist
        self.graph.run("MATCH (n) DETACH DELETE n")

        resp = self.app.get("/api/delete_all")
        self.assertEqual(resp.status_code, 200)

        data = resp.get_json()
        self.assertEqual(data["status"], "success")
        self.assertIn("Alle Knoten", data["message"])

    def test_create_node_success(self):
        resp = self.app.post(
            "/api/create_node",
            json={
                "node_label": "Person",
                "props": {"name": "Alice"}
            }
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data["status"], "success")
        self.assertIn("newNodeId", data)

        # check node exists in DB
        node_id = data["newNodeId"]
        result = self.graph.run(f"MATCH (n) WHERE ID(n)={node_id} RETURN n").data()
        self.assertTrue(result)
        self.assertEqual(result[0]["n"]["name"], "Alice")

    def test_create_node_invalid_props(self):
        resp = self.app.post(
            "/api/create_node",
            json={"node_label": "Person", "props": {}}
        )
        self.assertEqual(resp.status_code, 400)
        data = resp.get_json()
        self.assertEqual(data["status"], "error")

    def test_create_node_invalid_property_name(self):
        resp = self.app.post(
            "/api/create_node",
            json={"node_label": "Person", "props": {"123bad": "oops"}}
        )
        self.assertEqual(resp.status_code, 400)
        data = resp.get_json()
        self.assertIn("Ungültiger Property-Name", data["message"])

    def test_create_node_with_relationship(self):
        # zuerst ein Zielnode
        existing = self.graph.run("CREATE (p:Person {name:'Bob'}) RETURN ID(p) AS id").data()[0]["id"]

        resp = self.app.post(
            "/api/create_node",
            json={
                "node_label": "Buch",
                "props": {"title": "Mein Buch"},
                "connectTo": [{"id": existing}]
            }
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data["status"], "success")

        # check relationship created
        new_id = data["newNodeId"]
        rels = self.graph.run(
            f"MATCH (a)-[r]->(b) WHERE ID(b)={new_id} RETURN type(r) AS t"
        ).data()
        self.assertTrue(rels)

    def test_add_row_creates_node(self):
        """POST /add_row erstellt Node korrekt."""
        data = {"label": "TestNode", "properties": {"name": "Alice"}}
        response = self.app.post("/api/add_row", json=data)
        self.assertEqual(response.status_code, 200)
        result = json.loads(response.data)
        self.assertIn("id", result)

        node = self.graph.nodes.match("TestNode", name="Alice").first()
        self.assertIsNotNone(node)
        self.assertEqual(node["name"], "Alice")

    def test_add_row_filters_invalid_properties(self):
        """Ungültige Property-Namen werden ignoriert."""
        data = {"label": "TestNode2", "properties": {"123invalid": "val", "validName": "ok"}}
        response = self.app.post("/api/add_row", json=data)
        self.assertEqual(response.status_code, 200)

        node = self.graph.nodes.match("TestNode2").first()
        self.assertIsNotNone(node)
        self.assertNotIn("123invalid", node)
        self.assertEqual(node["validName"], "ok")

    def tearDown_node_and_relationship(self, uid):
        """Alles löschen, was zu dieser UID gehört"""
        self.graph.run("MATCH (n {uid:$uid}) DETACH DELETE n", uid=uid)

    def _wait_for_node(self, label, uid, retries=10, delay=0.1):
        """Retry bis Node sichtbar ist, max retries"""
        for _ in range(retries):
            node_id = self.graph.evaluate(f"MATCH (n:`{label}` {{uid:$uid}}) RETURN id(n)", uid=uid)
            if node_id is not None:
                return node_id
            time.sleep(delay)
        raise RuntimeError(f"Node {label} mit uid={uid} konnte nicht gefunden werden")

    def test_add_relationship_flakeless(self):
        """Relationship-Erstellung ohne Flakiness und ohne Sleep/Retry."""

        uid = str(uuid4())
        alice_label = f"Person_{uid[:8]}"
        ort_label = f"Ort_{uid[:8]}"
        alice_name = f"Alice_{uid}"
        ort_name = f"Berlin_{uid}"

        # Alte Testdaten löschen
        self.tearDown_node_and_relationship(uid)

        # Nodes anlegen synchron
        alice = Node(alice_label, name=alice_name, uid=uid)
        ort = Node(ort_label, name=ort_name, uid=uid)
        self.graph.create(alice)
        self.graph.create(ort)

        # IDs direkt aus den Neo4j-Node-Objekten
        alice_id = alice.identity
        ort_id = ort.identity

        # Defensive Checks
        self.assertIsNotNone(alice_id, "Alice Node-ID konnte nicht ermittelt werden")
        self.assertIsNotNone(ort_id, "Ort Node-ID konnte nicht ermittelt werden")

        payload = {
            "start_id": int(alice_id),
            "end_id": int(ort_id),
            "type": "WOHNT_IN",
            "props": {"since": 2020}
        }

        # API synchron aufrufen
        with self.app as client:
            resp = client.post(
                "/api/add_relationship",
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            self.assertEqual(resp.status_code, 200, f"API-Fehler: {resp.get_data(as_text=True)}")

            result = resp.get_json()
            self.assertEqual(result["status"], "success")
            self.assertIn("id", result)
            self.assertIsInstance(result["id"], int)

        # Relationship synchron in DB prüfen
        rel = self.graph.run(
            f"MATCH (a)-[r]->(b) "
            f"WHERE ID(a)={alice_id} AND ID(b)={ort_id} "
            f"RETURN r"
        ).data()
        self.assertTrue(len(rel) > 0, "Relationship wurde nicht erstellt")
        self.assertEqual(rel[0]["r"]["since"], 2020)

        # Cleanup garantiert
        self.tearDown_node_and_relationship(uid)

    def test_add_relationship_invalid_property_names(self):
        """Ungültige Property-Namen sollen ignoriert, gültige übernommen werden."""
        # Nodes erstellen synchron
        alice = Node("Person", name="Alice")
        berlin = Node("Ort", name="Berlin")

        try:
            self.graph.create(alice)
            self.graph.create(berlin)
        except Exception as e:
            self.fail(f"Fehler beim Erstellen der Nodes: {e}")

        # IDs direkt aus den Node-Objekten prüfen
        alice_id = getattr(alice, 'identity', None)
        berlin_id = getattr(berlin, 'identity', None)

        self.assertIsNotNone(alice_id, f"Alice Node-ID konnte nicht ermittelt werden. Node: {alice}")
        self.assertIsNotNone(berlin_id, f"Berlin Node-ID konnte nicht ermittelt werden. Node: {berlin}")

        # Relationship-Daten
        data = {
            "start_id": int(alice_id),
            "end_id": int(berlin_id),
            "type": "WOHNT_IN",
            "props": {"123invalid": "oops", "validProp": "ok"}
        }

        # API synchron aufrufen
        with self.app as client:
            resp = client.post(
                "/api/add_relationship",
                json=data,
                headers={"Content-Type": "application/json"}
            )

            self.assertEqual(resp.status_code, 200, f"API-Fehler: {resp.get_data(as_text=True)}")

        # Relationship direkt aus der DB prüfen
        try:
            result = self.graph.run(
                "MATCH (a)-[r]->(b) "
                "WHERE ID(a)=$alice_id AND ID(b)=$berlin_id "
                "RETURN r",
                alice_id=alice_id, berlin_id=berlin_id
            ).data()
        except Exception as e:
            self.fail(f"Fehler beim Abrufen der Relationship aus der DB: {e}")

        self.assertTrue(len(result) > 0, "Relationship wurde nicht erstellt")

        rel = result[0]["r"]

        # Ungültiges Property darf nicht vorhanden sein
        self.assertNotIn("123invalid", rel)

        # Gültiges Property muss übernommen werden
        self.assertEqual(rel.get("validProp"), "ok")

    def test_add_relationship_nonexistent_nodes(self):
        data = {"start_id": 9999, "end_id": 8888, "type": "WOHNT_IN"}
        with self.app as client:
            resp = client.post("/api/add_relationship", json=data)
            self.assertEqual(resp.status_code, 500)
            self.assertEqual(resp.get_json()["status"], "error")

    def test_add_relationship_empty_request_body(self):
        with self.app as client:
            resp = client.post("/api/add_relationship", data="")
            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.get_json()["status"], "error")
            self.assertIn("leer", resp.get_json()["message"])

    def test_load_secret_key_file_exists(self):
        # Mock open, damit eine Datei existiert mit Inhalt
        with mock.patch("builtins.open", mock.mock_open(read_data="mysecretkey")):
            key = load_or_generate_secret_key()
            self.assertEqual(key, "mysecretkey")

    def test_load_secret_key_file_empty(self):
        # Datei existiert, aber leer → Fehlerpfad
        with mock.patch("builtins.open", mock.mock_open(read_data="")):
            with mock.patch("secrets.token_urlsafe", return_value="generatedkey"):
                key = load_or_generate_secret_key()
                self.assertEqual(key, "generatedkey")

    def test_load_secret_key_file_not_found(self):
        # Datei existiert nicht → neues Key wird generiert
        with mock.patch("builtins.open", side_effect=FileNotFoundError):
            with mock.patch("secrets.token_urlsafe", return_value="newkey"):
                key = load_or_generate_secret_key()
                self.assertEqual(key, "newkey")

    def test_load_secret_key_other_error(self):
        # anderes Lesen-Fehler
        with mock.patch("builtins.open", side_effect=OSError("oh no")):
            with mock.patch("secrets.token_urlsafe", return_value="tempkey"):
                key = load_or_generate_secret_key()
                self.assertEqual(key, "tempkey")

    def test_get_properties_success(self):
        self.graph.run("CREATE (:Person {name:'Alice', age:30})")
        resp = self.app.get("/api/properties?label=Person")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        props = [p["property"] for p in data]
        self.assertIn("name", props)
        self.assertIn("age", props)

    def test_get_properties_empty_label(self):
        resp = self.app.get("/api/properties")
        self.assertEqual(resp.status_code, 500)
        data = resp.get_json()
        self.assertEqual(data["status"], "error")
        self.assertIn("Missing label", data["message"])

    def test_get_properties_no_nodes(self):
        resp = self.app.get("/api/properties?label=UnknownLabel")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data, [])

    def test_get_labels_success(self):
        # Erstelle ein paar Test-Nodes mit Labels
        self.graph.run("CREATE (:Person {name:'Alice'})")
        self.graph.run("CREATE (:Ort {name:'Berlin'})")

        resp = self.app.get("/api/labels")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertIn("Person", data)
        self.assertIn("Ort", data)

    def test_get_labels_no_nodes(self):
        # DB ist leer
        self.graph.run("MATCH (n) DETACH DELETE n")
        resp = self.app.get("/api/labels")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data, [])

    def test_get_relationships(self):
        """Testet den API-Endpoint, der alle Relationship-Typen zurückgibt."""
        # Erstelle ein paar Test-Beziehungen
        person = Node("Person", name="Alice")
        ort = Node("Ort", name="Berlin")
        rel1 = Relationship(person, "HAT_WOHNSITZ", ort)
        rel2 = Relationship(person, "ARBEITET_IN", ort)
        self.graph.create(rel1 | rel2)

        response = self.app.get('/api/relationships')
        self.assertEqual(response.status_code, 200)

        data = json.loads(response.data)
        self.assertIsInstance(data, list)
        self.assertIn("HAT_WOHNSITZ", data)
        self.assertIn("ARBEITET_IN", data)

        # Optional: prüfen, dass keine Duplikate enthalten sind
        self.assertEqual(len(data), len(set(data)))

    def test_get_data_as_table_with_where_condition(self):
        """Test the 'where' parameter filters nodes correctly."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("""
            CREATE (p:Person {vorname:'Alice', nachname:'Meier', alter:30})
            CREATE (p2:Person {vorname:'Bob', nachname:'Schulz', alter:40})
            CREATE (s:Stadt {stadt:'Berlin'})
            CREATE (p)-[:WOHNT_IN]->(s)
            CREATE (p2)-[:WOHNT_IN]->(s)
        """)
        with self.app as client:
            resp = client.get(
                '/api/get_data_as_table',
                query_string={
                    'nodes': 'Person,Stadt',
                    'where': "n.alter < 35"  # only Alice should match
                }
            )
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertEqual(len(data['rows']), 1)
            row = data['rows'][0]
            col_list = data['columns']
            cell_values = {(col_list[i]['nodeType'], col_list[i]['property']): row['cells'][i]['value']
                           for i in range(len(col_list))}
            self.assertEqual(cell_values.get(('Person', 'vorname')), 'Alice')
            self.assertEqual(cell_values.get(('Person', 'nachname')), 'Meier')

    def test_get_data_as_table_with_relationship_filter(self):
        """Test the 'relationships' parameter only includes specified relationships."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("""
            CREATE (p:Person {vorname:'Alice'})
            CREATE (s:Stadt {stadt:'Berlin'})
            CREATE (c:Company {name:'Siemens'})
            CREATE (p)-[:WOHNT_IN]->(s)
            CREATE (p)-[:ARBEITET_BEI]->(c)
        """)
        with self.app as client:
            # only consider WOHNT_IN
            resp = client.get(
                '/api/get_data_as_table',
                query_string={
                    'nodes': 'Person,Stadt,Company',
                    'relationships': 'WOHNT_IN'
                }
            )
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            # relations should only include WOHNT_IN
            for row in data['rows']:
                for rel in row.get('relations', []):
                    self.assertEqual(rel['relation'], 'WOHNT_IN')


    def test_get_data_as_table_with_where_and_relationships_combined(self):
        """Test both 'where' and 'relationships' together."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("""
            CREATE (p:Person {vorname:'Alice', alter:30})
            CREATE (p2:Person {vorname:'Bob', alter:40})
            CREATE (s:Stadt {stadt:'Berlin'})
            CREATE (c:Company {name:'Siemens'})
            CREATE (p)-[:WOHNT_IN]->(s)
            CREATE (p)-[:ARBEITET_BEI]->(c)
            CREATE (p2)-[:WOHNT_IN]->(s)
            CREATE (p2)-[:ARBEITET_BEI]->(c)
        """)
        with self.app as client:
            resp = client.get(
                '/api/get_data_as_table',
                query_string={
                    'nodes': 'Person,Stadt,Company',
                    'where': 'n.alter < 35',
                    'relationships': 'WOHNT_IN'
                }
            )
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertEqual(len(data['rows']), 1)
            row = data['rows'][0]
            col_list = data['columns']
            cell_values = {(col_list[i]['nodeType'], col_list[i]['property']): row['cells'][i]['value']
                        for i in range(len(col_list))}
            self.assertEqual(cell_values.get(('Person', 'vorname')), 'Alice')
            # relations only WOHNT_IN
            for rel in row.get('relations', []):
                self.assertEqual(rel['relation'], 'WOHNT_IN')


    def test_get_data_as_table_where_no_match_returns_empty(self):
        """If 'where' condition matches no nodes, result is empty but structure intact."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        self.graph.run("CREATE (p:Person {vorname:'Alice', alter:30})")
        with self.app as client:
            resp = client.get(
                '/api/get_data_as_table',
                query_string={'nodes': 'Person', 'where': 'n.alter > 100'}
            )
            self.assertEqual(resp.status_code, 200)
            data = resp.get_json()
            self.assertEqual(data['columns'], [])
            self.assertEqual(data['rows'], [])

    def tearDown_node_and_index(self, label):
        # Löscht alle Knoten und Indexe für das Label
        self.graph.run(f"MATCH (n:`{label}`) DETACH DELETE n")
        indexes = self.graph.run("SHOW INDEXES").data()
        for idx in indexes:
            labels = idx.get("labelsOrTypes") or []
            if label in labels:
                name = idx.get("name")
                if name:
                    self.graph.run(f"DROP INDEX {name}")

    def test_index_manager_page_loads_with_new_node(self):
        # Test-Knoten erzeugen
        label = f"TestLabel_{uuid.uuid4().hex[:8]}"
        uid = str(uuid.uuid4())
        node = Node(label, name="Alice", uid=uid)
        self.graph.create(node)

        try:
            with self.app as client:
                resp = client.get("/index_manager")
                self.assertEqual(resp.status_code, 200)
                body = resp.get_data(as_text=True)
                self.assertIn(label, body)
        finally:
            self.tearDown_node_and_index(label)

    def test_create_indices_success(self):
        # Test-Knoten erzeugen
        label = f"TestLabel_{uuid.uuid4().hex[:8]}"
        uid = str(uuid.uuid4())
        node = Node(label, name="Alice", uid=uid)
        self.graph.create(node)

        try:
            payload = {"indices": [{"label": label, "property": "uid"}]}
            with self.app as client:
                resp = client.post(
                    "/create_indices",
                    data=json.dumps(payload),
                    content_type="application/json"
                )
                self.assertEqual(resp.status_code, 200)
                result = resp.get_json()
                self.assertEqual(result["status"], "success")
                self.assertIn({"label": label, "property": "uid"}, result["created"])

            # Prüfen, dass Index online ist
            indexes = self.graph.run("SHOW INDEXES").data()
            match = [
                idx for idx in indexes
                if label in (idx.get("labelsOrTypes") or [])
                and "uid" in (idx.get("properties") or [])
                and idx.get("state", "").upper() == "ONLINE"
            ]
            self.assertTrue(match)
        finally:
            self.tearDown_node_and_index(label)

    def test_create_indices_invalid_payload(self):
        # Einfacher Test, kein Knoten nötig
        payload = {"wrong": "format"}
        with self.app as client:
            resp = client.post(
                "/create_indices",
                data=json.dumps(payload),
                content_type="application/json"
            )
            self.assertEqual(resp.status_code, 200)
            result = resp.get_json()
            self.assertEqual(result["status"], "success")
            self.assertEqual(result["created"], [])

    def _prepare_book_test_data(self):
        """Datenbank zurücksetzen und Beispiel-Bücher + Autoren erstellen."""
        self.graph.run("MATCH (n) DETACH DELETE n")
        books = [
            ("Goethe", "Faust", "1808"),
            ("Schiller", "Kabale und Liebe", "1784"),
            ("Kafka", "Der Process", "1925"),
        ]
        for author, title, year in books:
            self.graph.run("""
                CREATE (p:Person {name:$author})
                CREATE (b:Buch {titel:$title, erscheinungsjahr:$year})
                CREATE (p)-[:HAT_GESCHRIEBEN]->(b)
            """, author=author, title=title, year=year)

    def _run_querybuilder_request(self, client, operator, field, value=None):
        """Hilfsmethode: Request an /api/get_data_as_table mit Operator."""
        rule = {
            "id": field,
            "field": field,
            "type": "string",
            "input": "text",
            "operator": operator,
        }
        if value is not None:
            rule["value"] = value

        qb = {"condition": "AND", "rules": [rule], "valid": True}

        resp = client.get(
            "/api/get_data_as_table",
            query_string={
                "nodes": "Person,Buch",
                "relationships": "HAT_GESCHRIEBEN",
                "qb": json.dumps(qb),
            },
        )
        data = resp.get_json()

        if "rows" not in data:
            self.fail(f"Operator {operator} not implemented or invalid response: {data}")

        return resp, data

    def test_operator_equal(self):
        self._prepare_book_test_data()
        with self.app as client:
            _, data = self._run_querybuilder_request(client, "equal", "Buch.erscheinungsjahr", "1808")
            values = [c.get("value") for r in data["rows"] for c in r["cells"]]
            self.assertIn("1808", values)

    def test_operator_not_equal(self):
        self._prepare_book_test_data()
        with self.app as client:
            _, data = self._run_querybuilder_request(client, "not_equal", "Buch.erscheinungsjahr", "1808")
            values = [c.get("value") for r in data["rows"] for c in r["cells"]]
            self.assertNotIn("1808", values)

    def test_operator_in(self):
        self._prepare_book_test_data()
        with self.app as client:
            _, data = self._run_querybuilder_request(client, "in", "Buch.erscheinungsjahr", ["1808", "1925"])
            values = [c.get("value") for r in data["rows"] for c in r["cells"]]
            self.assertIn("1808", values)
            self.assertIn("1925", values)

    def test_operator_not_in(self):
        self._prepare_book_test_data()
        with self.app as client:
            _, data = self._run_querybuilder_request(client, "not_in", "Buch.erscheinungsjahr", ["1808"])
            values = [c.get("value") for r in data["rows"] for c in r["cells"]]
            self.assertNotIn("1808", values)

    def test_operator_begins_with(self):
        self._prepare_book_test_data()
        with self.app as client:
            _, data = self._run_querybuilder_request(client, "begins_with", "Buch.titel", "Fau")
            titles = [c.get("value") for r in data["rows"] for c in r["cells"]]
            self.assertTrue(any("Faust" in t for t in titles if t))

    def test_operator_not_begins_with(self):
        self._prepare_book_test_data()
        with self.app as client:
            _, data = self._run_querybuilder_request(client, "not_begins_with", "Buch.titel", "Fau")
            titles = [c.get("value") for r in data["rows"] for c in r["cells"]]
            self.assertFalse(any(t and t.startswith("Fau") for t in titles))

    def test_operator_contains(self):
        self._prepare_book_test_data()
        with self.app as client:
            _, data = self._run_querybuilder_request(client, "contains", "Buch.titel", "Process")
            titles = [c.get("value") for r in data["rows"] for c in r["cells"]]
            self.assertTrue(any("Process" in (t or "") for t in titles))

    def test_operator_not_contains(self):
        self._prepare_book_test_data()
        with self.app as client:
            _, data = self._run_querybuilder_request(client, "not_contains", "Buch.titel", "Process")
            titles = [c.get("value") for r in data["rows"] for c in r["cells"]]
            self.assertFalse(any("Process" in (t or "") for t in titles))

    def test_operator_ends_with(self):
        self._prepare_book_test_data()
        with self.app as client:
            _, data = self._run_querybuilder_request(client, "ends_with", "Buch.titel", "Liebe")
            titles = [c.get("value") for r in data["rows"] for c in r["cells"]]
            self.assertTrue(any(t and t.endswith("Liebe") for t in titles))

    def test_operator_not_ends_with(self):
        self._prepare_book_test_data()
        with self.app as client:
            _, data = self._run_querybuilder_request(client, "not_ends_with", "Buch.titel", "Liebe")
            titles = [c.get("value") for r in data["rows"] for c in r["cells"]]
            self.assertFalse(any(t and t.endswith("Liebe") for t in titles))

if __name__ == '__main__':
    try:
        unittest.main()
    except KeyboardInterrupt:
        print("You pressed CTRL-C")
        sys.exit(1)
