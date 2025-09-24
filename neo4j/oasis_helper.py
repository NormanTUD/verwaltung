import time
import os
import sys
import secrets
from py2neo import Graph

def load_or_generate_secret_key():
    path = "/etc/oasis/secret_key"
    try:
        # Datei existiert
        with open(path, "r") as f:
            key = f.read().strip()
            if not key:
                raise ValueError("Secret-Key-Datei ist leer")
            return key
    except FileNotFoundError:
        # Datei existiert nicht
        key = secrets.token_urlsafe(64)
        return key
    except Exception as e:
        # Andere Fehler beim Lesen
        key = secrets.token_urlsafe(64)
        print(f"Fehler beim Laden des Secret-Keys ({e}). Temporärer Key wird verwendet: {key}")
        return key

def get_graph_db_connection():
    graph = None

    try:
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

    return graph
