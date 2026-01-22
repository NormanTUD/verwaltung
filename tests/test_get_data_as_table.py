import pytest
from api.neo4j_interface import Neo4jDB, ReadRequest
from neo4j import GraphDatabase
import os
import t_helpers, conftest


URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
AUTH =(
    os.getenv("NEO4J_USER", "neo4j"),
    os.getenv("NEO4J_PASS", "testTEST12345678")
)


def test_simple_db_reads(db: "Neo4jDB"):
    "Basic Read requests from the Data-Layer Neo4jDB class"
    # Simple Request

    label1 = "Student"
    req = ReadRequest([label1], label1, 3,  None, None, None)
    records = list(db.read_data(req))
    for s in t_helpers.STUDENTS:
        assert s.f_name in [r.data()["n"]["f_name"] for r in records]

    label2 = "Seminar"
    req = ReadRequest([label2], label2, 3,  None, None, None)
    records = list(db.read_data(req))
    for c in t_helpers.SEMINARS:
        assert c.name in [r.data()["n"]["title"] for r in records]

def test_where_request(db):
    "Basic where requests from the Data-Layer Neo4jDB class"
    lbl= "Student"
    # Where
    names = [s.f_name for s in t_helpers.STUDENTS]

    for n in names:
        req = ReadRequest([lbl],
                          lbl,
                          3,
                          None,
                          {"f_name":n},
                          None)
        records = list(db.read_data(req))

        assert n in [r.data()["n"]["f_name"] for r in records] and len(records) == 1
        assert "Cookiebert Strauss" not in [r.data()["n"]["f_name"] for r in records]

def test_limit_request(db):
    "Basic iterative limit requests from the Data-Layer Neo4jDB class"
    # Limits
    lbl= "Student"
    for i in range(8):
        req = ReadRequest([lbl], lbl, 3, i,  None, None)
        records = list(db.read_data(req))
        assert len(records) == i

def test_simple_relationships(db: "Neo4jDB"):
    label1 = "Student"

    req = ReadRequest([label1], label1, 3,  None, None, ["ENROLLED_IN"])
    records = list(db.read_data(req))

    assert len(records) > 5, f"To little records for student-class enrolled relation {records}"
    for r in records:
         relation = r.data()["r"]
         assert relation[1] == "ENROLLED_IN", f"[Test] , no relation in {relation}, or at least not at index 1? "

def test_unpresent_label(db):
    m_label = "COOOOKIES"
    req = ReadRequest([m_label], m_label, 3,  None, None, ["ENROLLED_IN"])
    records = list(db.read_data(req))
    assert len(records) == 0
