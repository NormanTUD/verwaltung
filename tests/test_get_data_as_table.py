import pytest
from api.neo4j_interface import Neo4jDB, ReadRequest
from neo4j import GraphDatabase
import os
import t_helpers

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

    return t_driver

@pytest.fixture
def db_cls(driver):
    return Neo4jDB(driver)


def test_read_from_db(db_cls: Neo4jDB):
    # Simple Request
    label1 = t_helpers.STUDENT_KEYS[0]
    req = ReadRequest([label1], label1, 3, None, None, None, None)
    records = list(db_cls.read_data(req))
    for s in t_helpers.STUDENTS:
        assert s[1] in [r.data()["n"]["f_name"] for r in records]

    label2 = t_helpers.CLASS_KEYS[0]
    req = ReadRequest([label2], label2, 3, None, None, None, None)
    records = list(db_cls.read_data(req))
    for c in t_helpers.CLASSES:
        assert c[1] in [r.data()["n"][t_helpers.CLASS_KEYS[1]] for r in records]

    # Limits
    for i in range(8):
        req = ReadRequest([label1], label1, 3, i, None, None, None)
        records = list(db_cls.read_data(req))
        assert len(records) == i

    # Where
    names = [s[1] for s in t_helpers.STUDENTS]

    for n in names:
        req = ReadRequest([label1],
                          label1,
                          3,
                          None,
                          {t_helpers.STUDENT_KEYS[2]:n},
                          None,
                          None)
        records = list(db_cls.read_data(req))

        assert n in [r.data()["n"][t_helpers.STUDENT_KEYS[2]] for r in records]
        assert "Cookiebert Strauss" not in [r.data()["n"][t_helpers.STUDENT_KEYS[1]] for r in records]

