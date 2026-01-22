from t_helpers import *
import conftest

@pytest.fixture
def empty_driver():
    t_driver = GraphDatabase.driver(conftest.URI, auth=conftest.AUTH)
    with t_driver.session() as session:
                session.run("MATCH (n) DETACH DELETE n")

    return t_driver


def test_add_node(empty_driver):
    assert empty_driver
    label = "TestLabel"
    key_props = {"key": "value"}
    other_props = {"other_key": "other_value"}
    add_node(empty_driver, label, key_props, other_props)
    query = f"MATCH (n:{label}) RETURN n"
    result = empty_driver.session().run(query)
    assert result.single()["n"] is not None

# Test connecting nodes
def test_connect_nodes(empty_driver):
    from_label = "FromLabel"
    from_key = {"fkey": "fvalue"}
    rel_type = "REL_TYPE"
    to_label = "ToLabel"
    to_key = {"to_key": "to_value"}
    add_node(empty_driver, from_label, from_key)
    add_node(empty_driver, to_label, to_key)
    connect_nodes(empty_driver, from_label, from_key, rel_type, to_label, to_key)
    query = f"MATCH (n:{from_label})-[r:{rel_type}]->(m:{to_label}) RETURN n, r"
    result = empty_driver.session().run(query)
    s_res = result.single()
    assert s_res["r"] is not None and s_res["n"] is not None

# Test enrolling students randomly
def test_enroll_students_randomly(empty_driver):
    student_ids = [student[0] for student in STUDENTS]
    class_codes = [class_info[0] for class_info in CLASSES]
    for class_info in CLASSES:
        add_node(empty_driver, CLASS_KEYS[0], {CLASS_KEYS[1]: class_info[1]}, {CLASS_KEYS[2]: class_info[0]})
    for student in STUDENTS:
        add_node(empty_driver, STUDENT_KEYS[0], {STUDENT_KEYS[1]: student[0]}, {STUDENT_KEYS[2]: student[1], STUDENT_KEYS[3]: student[2]})
    enroll_students_randomly(empty_driver, student_ids, class_codes)

    query = f"MATCH (s:{STUDENT_KEYS[0]} {{ {STUDENT_KEYS[1]}: $student_id }})-[:ENROLLED_IN]->(c) RETURN s, c"
    for student_id in student_ids:
        result = list(empty_driver.session().run(query, {"student_id": student_id}))
        for r  in result:
             assert r["c"] is not None, f"{r=} in {result=} has no class? {r["c"]=}"

# Test main function
def test_main(driver):
    main(driver)
    query = f"RETURN *"
    result = driver.session().run(query)
    assert result is not None
