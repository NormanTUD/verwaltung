import pytest
from api.neo4j_interface import Neo4jDB, ReadRequest
from neo4j import GraphDatabase
from neo4j.exceptions import ClientError
import os
import t_helpers, conftest
from conftest import URI, AUTH


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

def test_complex_where_requests(db):
    label = "Thesis"
    wild_filters = [
        {"department": "Science",
         "is_published": True,
         "year": 1998},
        {"grade": 99.9},
        {"topic": "Economics"},
        {"pages": 300, "keywords": ["plants", "dangerous", "forest"]},
        {"department": "Biology", "is_published": False},
        {"year": 1999, "grade": 88.0},
    ]

    for filter in wild_filters:
        req = ReadRequest([label],
                          label,
                          3,
                          None,
                          filter,
                          None)
        records = db.read_data(req)
        assert len(records) > 0, f"complex where request with {filter=} could not find a thesis"







def test_limit_request(db):
    "Basic iterative limit requests from the Data-Layer Neo4jDB class"
    # Limits
    lbl= "Student"
    for i in range(8):
        req = ReadRequest([lbl], lbl, 3, i,  None, None)
        records = list(db.read_data(req))
        assert len(records) == i

def test_simple_relationships(db: "Neo4jDB"):
    "Basic relationship requests from the Data-Layer Neo4jDB class"
    label1 = "Student"

    req = ReadRequest([label1], label1, 3,  None, None, ["ENROLLED_IN"])
    records = list(db.read_data(req))

    assert len(records) > 5, f"To little records for student-class enrolled relation {records}"
    for r in records:
         relation = r.data()["r"]
         assert relation[1] == "ENROLLED_IN", f"[Test] , no relation in {relation}, or at least not at index 1? "

def test_unpresent_label(db):
    " Basic requests for a label that doesnt exist from the Data-Layer Neo4jDB class"
    m_label = "COOOOKIES"
    req = ReadRequest([m_label], m_label, 3,  None, None, ["ENROLLED_IN"])
    records = list(db.read_data(req))
    assert len(records) == 0

def test_bad_limit(db):
    " Expected behavior with wrong label input"
    label1 = "Student"
    bad_limits = {-3, "f", "", 14.13}
    for bad_lim in bad_limits:
        req = ReadRequest(selected_labels=[label1],
                          main_label= label1,
                           max_depth= 3,
                            limit= bad_lim,
                            filter_labels=None,
                            rel_fitler=None)
        with pytest.raises(ClientError):
            db.read_data(req)

