import unittest
import os
import json
from flask import session
from py2neo import Graph, Node
from dotenv import load_dotenv

# Lade Umgebungsvariablen aus der .env.test-Datei für die Tests
load_dotenv('.env.test')

# Wichtig: Importieren Sie die App-Instanz aus Ihrer Hauptdatei
# Stellen Sie sicher, dass Ihre Hauptdatei app.py heißt
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
        Stellt die Verbindung zur Testdatenbank her und leert sie.
        """
        cls.graph = Graph(os.getenv('NEO4J_URI'), auth=(os.getenv('NEO4J_USER'), os.getenv('NEO4J_PASS')))
        
        # Leere die Datenbank für einen sauberen Test
        cls.graph.run("MATCH (n) DETACH DELETE n")
        print("Testdatenbank wurde erfolgreich geleert.")
    
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
        Erstellt einen Flask-Test-Client.
        """
        self.app = app.test_client()
        self.app.testing = True

    def tearDown(self):
        """
        Wird nach jedem Test ausgeführt.
        Löscht die Sitzungsdaten, um saubere Bedingungen zu gewährleisten.
        """
        with self.app as client:
            with client.session_transaction() as session:
                session.clear()

    # --- Testfälle für die Endpunkte ---

    def test_index_page(self):
        """Testet, ob die Startseite erreichbar ist."""
        response = self.app.get('/')
        self.assertEqual(response.status_code, 200)

    def test_upload_valid_data(self):
        """Testet den Upload von gültigen CSV-Daten."""
        response = self.app.post('/upload', data={'data': SAMPLE_CSV_DATA}, content_type='multipart/form-data')
        self.assertEqual(response.status_code, 200)
        
        # Überprüfe, ob die Header in der Session gespeichert wurden
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

    def test_save_mapping_success(self):
        """Testet das Speichern der zugeordneten Daten in der Datenbank."""
        # Schritt 1: Simuliere einen Upload, um die Session zu füllen
        with self.app as client:
            with client.session_transaction() as sess:
                sess['raw_data'] = SAMPLE_CSV_DATA
                sess['headers'] = ['id', 'name', 'city', 'country']

        # Schritt 2: Sende die Zuordnung und überprüfe die Datenbank
        response = self.app.post('/save_mapping', data=json.dumps(SAMPLE_MAPPING), content_type='application/json')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Daten erfolgreich in Neo4j importiert", response.data)

        # Schritt 3: Verifiziere die Nodes und Relationships in der Datenbank
        person_nodes = self.graph.run("MATCH (n:Person) RETURN n").data()
        location_nodes = self.graph.run("MATCH (n:Location) RETURN n").data()
        relationships = self.graph.run("MATCH (:Person)-[r:LIVES_IN]->(:Location) RETURN r").data()

        self.assertEqual(len(person_nodes), 3)
        self.assertEqual(len(location_nodes), 3)
        self.assertEqual(len(relationships), 3)

    def test_query_data_single_label(self):
        """Testet die Abfrage mit einem einzelnen Label."""
        # Erstelle einen Test-Node
        test_node = Node("TestNode", name="TestName")
        self.graph.create(test_node)
        
        response = self.app.post('/api/query_data', data=json.dumps({"selectedLabels": ["TestNode"]}), content_type='application/json')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertGreaterEqual(len(data), 1)
        self.assertEqual(data[0]['testnode']['labels'], ["TestNode"])

    def test_query_data_multiple_labels(self):
        """Testet die Abfrage mit mehreren verbundenen Labels."""
        # Erstelle verbundene Test-Nodes
        node1 = Node("Person", name="TestPerson")
        node2 = Node("City", name="TestCity")
        self.graph.create(node1)
        self.graph.create(node2)
        self.graph.create((node1, "LIVES_IN", node2))

        response = self.app.post('/api/query_data', data=json.dumps({"selectedLabels": ["Person", "City"]}), content_type='application/json')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertGreaterEqual(len(data), 1)
        self.assertEqual(data[0]['person']['labels'], ["Person"])
        self.assertEqual(data[0]['city']['labels'], ["City"])

    def test_update_node_property(self):
        """Testet die Aktualisierung eines Nodes."""
        node = Node("UpdateNode", status="old")
        self.graph.create(node)
        node_id = node.identity
        
        response = self.app.put(f'/api/update_node/{node_id}', data=json.dumps({"property": "status", "value": "new"}), content_type='application/json')
        self.assertEqual(response.status_code, 200)
        
        # Überprüfe die Datenbank
        updated_node = self.graph.run(f"MATCH (n) WHERE ID(n) = {node_id} RETURN n").data()[0]['n']
        self.assertEqual(updated_node['status'], "new")

    def test_delete_node(self):
        """Testet das Löschen eines Nodes."""
        node = Node("DeleteNode", name="Temp")
        self.graph.create(node)
        node_id = node.identity
        
        response = self.app.delete(f'/api/delete_node/{node_id}')
        self.assertEqual(response.status_code, 200)

        # Überprüfe die Datenbank, ob der Node existiert
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
        
        # Überprüfe die Datenbank
        results = self.graph.run(f"MATCH (n) WHERE ID(n) IN {node_ids} RETURN n.status AS status").data()
        for res in results:
            self.assertEqual(res['status'], "updated")

if __name__ == '__main__':
    unittest.main()
