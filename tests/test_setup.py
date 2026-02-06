"""
Testing the Setup/Helper Functions of the Neo4j interface tests.
"""
import os, pytest
from t_helpers import add_node, connect_nodes, GraphDatabase, STUDENTS, main
import conftest


@pytest.fixture
def empty_driver():
    """ WARNING: Wipes the Database!
    Is safe in regard to that it can only ever run when testing """
    if not os.environ.get("PYTEST_VERSION"): raise RuntimeError("The Programm just tried to wipe the database outside of test mode")

    t_driver = GraphDatabase.driver(conftest.URI, auth=conftest.AUTH)
    with t_driver.session() as session:
                session.run("MATCH (n) DETACH DELETE n")

    return t_driver


def test_add_node(empty_driver):
    assert empty_driver
    label = "TestLabel"
    key_props = {"key": "value"}
    add_node(empty_driver, label, key_props)
    query = f"MATCH (n:{label}) RETURN n"
    result = empty_driver.session().run(query)
    assert result.single()["n"] is not None

# Test connecting nodes
def test_connect_nodes(empty_driver):
    from_label = "FromLabel"
    from_key = {"fkey": "fvalue"}
    rel_type = "REL_TYPE"
    to_key = {"to_key": "to_value"}
    to_label = "ToLabel"
    add_node(empty_driver, from_label, from_key)
    add_node(empty_driver, to_label, to_key)
    connect_nodes(empty_driver, from_label, from_key, rel_type, to_label, to_key)
    query = f"MATCH (n:{from_label})-[r:{rel_type}]->(m:{to_label}) RETURN n, r"
    result = empty_driver.session().run(query)
    s_res = result.single()
    assert s_res["r"] is not None and s_res["n"] is not None


def test_enroll_students_randomly(empty_driver):
    student_ids = [s.student_id for s in STUDENTS]
    main(empty_driver)

    CYPHER_QUERY = """
        MATCH (s:Student {student_id: $student_id})  -[:ENROLLED]->(:Seminar)
        RETURN s
        """

    for sid in student_ids:
        result = list(empty_driver.session().run(CYPHER_QUERY, {"student_id": sid}))
        assert result, f"Student {sid} is not enrolled in any seminar"


