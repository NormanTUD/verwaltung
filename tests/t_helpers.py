from neo4j import GraphDatabase
import random
import os
URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
AUTH =(
    os.getenv("NEO4J_USER", "neo4j"),
    os.getenv("NEO4J_PASS", "testTEST12345678")
)
STUDENTS = [
    (1, "Hermoine", "Granger"),
    (2, "Ron", "Weasley"),
    (3, "Harry", "Potter"),
    (4, "Luna", "Lovegood"),
    (5, "Neville", "Longbottom"),
    (6, "Ginny", "Weasley"),
    (7, "Cho", "Chang"),
    (8, "Dumbledore", "Army"),
]

CLASSES = [
    ("Math", "class_1"),
    ("Science", "class_2"),
    ("History", "class_3"),
    ("English", "class_4"),
]

TEACHERS = [
    (1, "Professor", "McGonagall", "Transfiguration"),
    (2, "Professor", "Snape", "Potions"),
    (3, "Professor", "Dumbledore", "Defense Against the Dark Arts"),
    (4, "Professor", "Sprout", "Herbology"),
]

from neo4j import GraphDatabase
import random

# Define helper function to connect nodes
def connect_nodes(driver, from_label, from_key, rel_type, to_label, to_key):
    query = f"""
        MATCH (a:{from_label} {{ {', '.join([f'{k}: ${k}' for k in from_key.keys()])} }})
        MATCH (b:{to_label} {{ {', '.join([f'{k}: ${k}' for k in to_key.keys()])} }})
        MERGE (a)-[:{rel_type}]->(b)
    """
    driver.session().run(query, {**from_key, **to_key})

# Define helper function to add node
def add_node(driver, label, key_props, other_props={}):
    props = ", ".join([f"{k}: ${k}" for k in key_props.keys()])
    set_props = ", ".join([f"{k}: ${k}" for k in other_props.keys()])
    query = f"""
        MERGE (n:{label} {{ {props}, {set_props} }})
        RETURN id(n)
    """
    driver.session().run(query, {**key_props, **other_props})

# Define entities
STUDENTS = [
    (1, "Hermoine", "Granger"),
    (2, "Ron", "Weasley"),
    (3, "Harry", "Potter"),
    (4, "Luna", "Lovegood"),
    (5, "Neville", "Longbottom"),
    (6, "Ginny", "Weasley"),
    (7, "Cho", "Chang"),
    (8, "Dumbledore", "Army"),
]

CLASSES = [
    ("Math", "class_1"),
    ("Science", "class_2"),
    ("History", "class_3"),
    ("English", "class_4"),
]

TEACHERS = [
    (1, "Professor", "McGonagall", "Transfiguration"),
    (2, "Professor", "Snape", "Potions"),
    (3, "Professor", "Dumbledore", "Defense Against the Dark Arts"),
    (4, "Professor", "Sprout", "Herbology"),
]

# Function to enroll students randomly
def enroll_students_randomly(driver, student_ids, class_codes, max_classes_per_student=3):
    for student_id in student_ids:
        chosen_classes = random.sample(class_codes, min(max_classes_per_student, len(class_codes)))
        for class_code in chosen_classes:
            connect_nodes(driver, "Student", {"student_id": student_id}, "ENROLLED_IN", "Class", {"class_code": class_code})
            connect_nodes(driver, "Class", {"class_code": class_code}, "TAKEN_BY", "Student", {"student_id": student_id})

# Function to connect teacher to class
def teacher_to_class_connection(driver, teacher_id, class_code):
    connect_nodes(driver, "Teacher", {"teacher_id": teacher_id}, "TEACHES", "Class", {"class_code": class_code})
    connect_nodes(driver, "Class", {"class_code": class_code}, "HELD_BY", "Teacher", {"teacher_id": teacher_id})

# Main function
def main(driver):
    student_ids = [student[0] for student in STUDENTS]
    class_codes = [class_info[1] for class_info in CLASSES]
    teacher_ids = [teacher[0] for teacher in TEACHERS]

    print("Creating Classes")
    for class_info in CLASSES:
        add_node(driver, "Class", {"class_code": class_info[1]}, {"title": class_info[0]})

    print("Creating Students")
    for student in STUDENTS:
        add_node(driver, "Student", {"student_id": student[0]}, {"f_name": student[1], "l_name": student[2]})

    print("Enrolling")
    enroll_students_randomly(driver, student_ids, class_codes)

    print("Creating Teachers")
    for teacher in TEACHERS:
        add_node(driver, "Teacher", {"teacher_id": teacher[0]}, {"f_name": teacher[1], "l_name": teacher[2], "title": teacher[3]})

    print("Assigning Teachers to Classes")
    for class_code in class_codes:
        teacher_id = random.choice(teacher_ids)
        teacher_to_class_connection(driver, teacher_id, class_code)

# Usage
if __name__ == "__main__":
    driver = GraphDatabase.driver(URI, auth=AUTH)

    try:
        main(driver)
    finally:
        driver.close()
