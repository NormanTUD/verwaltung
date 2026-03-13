import pytest, os
import t_helpers
from neo4j import GraphDatabase
from api.neo4j_interface import Neo4jDB
from t_helpers import add_node, connect_nodes


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
def db(driver):
    return Neo4jDB(driver)

@pytest.fixture
def load_academic_graph(driver, academic_graph_fixture):
    """
    Takes the academic_graph_fixture() and loads all nodes + relationships
    into Neo4j. Assumes driver is an empty DB.
    """
    t_driver = GraphDatabase.driver(URI, auth=AUTH)
    with t_driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")

    data = academic_graph_fixture
    nodes = data["nodes"]
    rels = data["relationships"]

    # 1. Create all nodes
    for category, items in nodes.items():
        for obj in items:
            label = obj.__class__.__name__
            props = obj.__dict__
            add_node(driver, label, props)

    # 2. Create all relationships
    for src, rel_type, dst in rels:
        src_label = src.__class__.__name__
        dst_label = dst.__class__.__name__

        # Use full property identity, since test_db2 does not define unique keys
        src_props = src.__dict__
        dst_props = dst.__dict__

        connect_nodes(
            driver,
            src_label=src_label,
            src_props=src_props,
            rel_type=rel_type,
            dst_label=dst_label,
            dst_props=dst_props
        )

    return Neo4jDB(driver)
