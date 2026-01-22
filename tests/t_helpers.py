from neo4j import GraphDatabase
import random
import os
import pytest




STUDENT_KEYS = ("Student", "student_id", "f_name", "l_name")
CLASS_KEYS = ("Class", "class_code",  "title")
TEACHER_KEYS = ("Teacher", "teacher_id", "f_name", "l_name", "title")

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

def connect_nodes(driver, from_label, from_key, rel_type, to_label, to_key, rel_properties=None):
    """

    """

    if not from_key or not to_key:
        raise ValueError("Both from_key and to_key must be provided")

    match_from = ", ".join(f"{k}: ${k}" for k in from_key)
    match_to = ", ".join(f"{k}: ${k}" for k in to_key)

    cypher = f"""
    MATCH (a:{from_label} {{ {match_from} }})
    MATCH (b:{to_label} {{ {match_to} }})
    MERGE (a)-[r:{rel_type}]->(b)
    """

    if rel_properties:
        set_properties = ", ".join(f"r.{k} = ${k}" for k in rel_properties)
        cypher += f" SET {set_properties}"

    params = {
        **from_key,
        **to_key,
        **(rel_properties or {})
    }

    with driver.session() as session:
        session.run(cypher, params)
# Define helper function to add node
def add_node(driver, label, key_props:dict, other_props:dict={}):
    props = ", ".join([f"{k}: ${k}" for k in key_props.keys()])
    set_props = ", ".join([f"{k}: ${k}" for k in other_props.keys()])

    props_str = f"{props}" if props else ""
    set_props_str = f", {set_props}" if set_props else ""

    query = f"""
            MERGE (n:{label} {{ {props_str}{set_props_str} }})
            RETURN id(n)
        """
    driver.session().run(query, {**key_props, **other_props})


# Function to enroll students randomly
def enroll_students_randomly(driver, student_ids, class_codes, max_classes_per_student=3):
    for student_id in student_ids:
        chosen_classes = random.sample(class_codes, min(max_classes_per_student, len(class_codes)))
        for class_code in chosen_classes:
            connect_nodes(driver, STUDENT_KEYS[0], {STUDENT_KEYS[1]: student_id}, "ENROLLED_IN", CLASS_KEYS[0], {CLASS_KEYS[1]: class_code})
            connect_nodes(driver, CLASS_KEYS[0], {CLASS_KEYS[1]: class_code}, "TAKEN_BY", STUDENT_KEYS[0], {STUDENT_KEYS[1]: student_id})
    print(student_ids ,"and", class_codes, "were connected")
# Function to connect teacher to class
def teacher_to_class_connection(driver, teacher_id, class_code):
    connect_nodes(driver, TEACHER_KEYS[0], {TEACHER_KEYS[1]: teacher_id}, "TEACHES", CLASS_KEYS[0], {CLASS_KEYS[1]: class_code})
    connect_nodes(driver, CLASS_KEYS[0], {CLASS_KEYS[1]: class_code}, "HELD_BY", TEACHER_KEYS[0], {TEACHER_KEYS[1]: teacher_id})


# Main function
def main(driver):
    student_ids = [student[0] for student in STUDENTS]
    class_codes = [class_info[0] for class_info in CLASSES]
    teacher_ids = [teacher[0] for teacher in TEACHERS]

    print("Creating Classes")
    for class_info in CLASSES:
        add_node(driver, CLASS_KEYS[0], {CLASS_KEYS[1]: class_info[1]}, {CLASS_KEYS[2]: class_info[0]})

    print("Creating Students")
    for student in STUDENTS:
        add_node(driver, STUDENT_KEYS[0], {STUDENT_KEYS[1]: student[0]}, {STUDENT_KEYS[2]: student[1], STUDENT_KEYS[3]: student[2]})

    print("Enrolling")
    enroll_students_randomly(driver, student_ids, class_codes)

    print("Creating Teachers")
    for teacher in TEACHERS:
        add_node(driver, TEACHER_KEYS[0], {TEACHER_KEYS[1]: teacher[0]}, {TEACHER_KEYS[2]: teacher[1], TEACHER_KEYS[3]: teacher[2], TEACHER_KEYS[4]: teacher[3]})

    print("Assigning Teachers to Classes")
    for class_code in class_codes:
        teacher_id = random.choice(teacher_ids)
        teacher_to_class_connection(driver, teacher_id, class_code)


