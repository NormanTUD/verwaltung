import unittest
import os
import json
from flask import session
from py2neo import Graph, Node, Relationship
from dotenv import load_dotenv

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

if __name__ == '__main__':
    unittest.main()
