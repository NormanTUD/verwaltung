import pytest, os
import t_helpers
from neo4j import GraphDatabase
from api.neo4j_interface import Neo4jDB

# Fixtures in this file will be available to all other files in the folder

URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
AUTH =(
    os.getenv("NEO4J_USER", "neo4j"),
    os.getenv("NEO4J_PASS", "testTEST12345678")
)

@pytest.fixture
def driver():
    t_driver = GraphDatabase.driver(URI, auth=AUTH)
    with t_driver.session() as session:
                session.run("MATCH (n) DETACH DELETE n")

    t_helpers.main(t_driver)
    with t_driver as d:
        yield d
        d.close()

@pytest.fixture
def db_cls(driver):
    return Neo4jDB(driver)

