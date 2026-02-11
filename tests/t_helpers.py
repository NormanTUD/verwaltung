from neo4j import GraphDatabase
import random
import os
import pytest
from dataclasses import dataclass

@dataclass(frozen=True, slots=True)
class Student:
    student_id: int
    f_name: str
    l_name: str
    node_name: str = "Student"

@dataclass(frozen=True, slots=True)
class Seminar:
    id: int
    name: str
    level: int
    node_name: str = "Seminar"

@dataclass(frozen=True, slots=True)
class Teacher:
    id: int
    title: str
    f_name: str
    l_name: str
    node_name: str = "Seminar"

@dataclass(frozen=True, slots=True)
class MasterThesis:
    id: int
    title: str
    year: int
    grade: float
    is_published: bool
    pages: int
    submission_date: str
    topic: str
    keywords: list[str]
    department: str
    student_id: int
    supervisor_ids: list[int]
    node_name: str = "Thesis"

THESES = [
    MasterThesis(
        id=101,
        title="Advanced Potions and their interactions with Muggles",
        year=1998,
        grade=95.5,
        is_published=True,
        pages=120,
        submission_date="1998-06-15",
        topic="Potions",
        keywords=["chemistry", "magic", "safety"],
        department="Science",
        student_id=1,  # Hermoine
        supervisor_ids=[2, 3] # Snape, Dumbledore
    ),
    MasterThesis(
        id=102,
        title="The impact of Quidditch on Wizarding Economics",
        year=1999,
        grade=88.0,
        is_published=False,
        pages=85,
        submission_date="1999-05-20",
        topic="Economics",
        keywords=["sports", "finance", "galleons"],
        department="History",
        student_id=2,  # Ron
        supervisor_ids=[1] # McGonagall
    ),
    MasterThesis(
        id=103,
        title="Defense Against Dark Arts: A Practical Guide",
        year=1998,
        grade=92.0,
        is_published=True,
        pages=200,
        submission_date="1998-07-01",
        topic="Defense",
        keywords=["combat", "spells", "survival"],
        department="Defense",
        student_id=3,  # Harry
        supervisor_ids=[3, 2] # Dumbledore, Snape
    ),
    MasterThesis(
        id=104,
        title="Flora of the Forbidden Forest",
        year=2000,
        grade=99.9,
        is_published=True,
        pages=300,
        submission_date="2000-04-10",
        topic="Herbology",
        keywords=["plants", "dangerous", "forest"],
        department="Biology",
        student_id=5,  # Neville
        supervisor_ids=[4] # Sprout
    ),
    MasterThesis(
        id=105,
        title="Invisible Creatures and Where to Find Them",
        year=1999,
        grade=85.5,
        is_published=False,
        pages=90,
        submission_date="1999-08-22",
        topic="Magizoology",
        keywords=["creatures", "invisible", "spectrespecs"],
        department="Biology",
        student_id=4,  # Luna
        supervisor_ids=[1, 4] # McGonagall, Sprout
    ),
]


STUDENTS = [
    Student(1, "Hermoine", "Granger"),
    Student(2, "Ron",      "Weasley"),
    Student(3, "Harry",    "Potter"),
    Student(4, "Luna",     "Lovegood"),
    Student(5, "Neville",  "Longbottom"),
    Student(6, "Ginny",    "Weasley"),
    Student(7, "Cho",      "Chang"),
    Student(8, "Dumbledore","Army"),
]

SEMINARS = [
    Seminar(1, "Math", 5),
    Seminar(2, "Science", 3),
    Seminar(3, "History", 2),
    Seminar(4, "English", 1),
]

TEACHERS = [
    Teacher(1, "Professor", "McGonagall", "Transfiguration"),
    Teacher(2, "Professor", "Snape",      "Potions"),
    Teacher(3, "Professor", "Dumbledore", "Defense Against the Dark Arts"),
    Teacher(4, "Professor", "Sprout",     "Herbology"),
]

CONNECTIONS =  {(Teacher, Seminar): "teaches",
                (Seminar, Teacher): "held_by",
                (Student, Seminar): "enrolled",
                (Seminar, Student): "taken_by",
                (Teacher, MasterThesis): "supervises",
                (Student, MasterThesis): "authored"}

from neo4j import GraphDatabase
import random

def connect_nodes(
    driver,
    src_label: str,
    src_props: dict,
    rel_type: str,
    dst_label: str,
    dst_props: dict,
) -> None:
    """
    MATCH (a:SrcLabel) WHERE a.<key> = $src_<key>
    MATCH (b:DstLabel) WHERE b.<key> = $dst_<key>
    MERGE (a)-[r:REL_TYPE]->(b)
    """
    # build the WHERE clauses
    src_where = " AND ".join(f"a.{k} = $src_{k}" for k in src_props)
    dst_where = " AND ".join(f"b.{k} = $dst_{k}" for k in dst_props)

    query = f"""
    MATCH (a:{src_label})
    WHERE {src_where}
    MATCH (b:{dst_label})
    WHERE {dst_where}
    MERGE (a)-[r:{rel_type}]->(b)
    RETURN r
    """

    # flatten the dictionaries into a single param dict
    params = {f"src_{k}": v for k, v in src_props.items()}
    params.update({f"dst_{k}": v for k, v in dst_props.items()})


    with driver.session() as sess:
        sess.run(query, params)
# Define helper function to add node
def add_node(driver, label: str, properties: dict) -> None:
    """
    Create a node in Neo4j.

    *driver* – neo4j driver/session object
    *label*  – node label e.g. "Student"
    *properties* – full property map for the node
    """
    with driver.session() as sess:
        query = f"CREATE (n:{label} $props)"
        sess.run(query, props=properties)

def enroll_students_randomly(
    driver,
    student_ids: list[int],
    class_codes: list[int],
    max_classes_per_student: int = 3,
) -> None:
    """
    Each student is enrolled in up to *max_classes_per_student* distinct classes.
    """
    for student_id in student_ids:
        # pick a random subset of classes for this student
        chosen = random.sample(
            class_codes, k=min(max_classes_per_student, len(class_codes))
        )
        for class_code in chosen:
            src = {"student_id": student_id}
            dst = {"id": class_code}
            connect_nodes(
                driver,
                src_label="Student",
                src_props=src,
                rel_type=CONNECTIONS[Student, Seminar],
                dst_label="Seminar",
                dst_props=dst,
            )
            connect_nodes(
                driver,
                src_label="Seminar",
                src_props=src,
                rel_type=CONNECTIONS[Seminar, Student],
                dst_label="Student",
                dst_props=dst
            )

# Function to connect teacher to class
def teacher_to_class_connection(driver, teacher_id: int, class_code: int) -> None:
    """
    Create the bidirectional relationship between a teacher and a class.
    """
    src = {"teacher_id": teacher_id}
    dst = {"class_code": class_code}

    # Teacher → Class
    connect_nodes(
        driver,
        src_label="Teacher",
        src_props=src,
        rel_type=CONNECTIONS[(Teacher, Seminar)],
        dst_label="Class",
        dst_props=dst,
    )
    # Class → Teacher (optional, shows the reverse direction)
    connect_nodes(
        driver,
        src_label="Class",
        src_props=dst,
        rel_type=CONNECTIONS[(Seminar, Teacher)],
        dst_label="Teacher",
        dst_props=src,
    )

# Main function
def main(driver):
    # ------------------------------------------------------------------
    # 1. Create Class nodes
    # ------------------------------------------------------------------
    for s in SEMINARS:
        props = {"id": s.id, "title": s.name, "level": s.level}
        add_node(driver, s.node_name, props)

    # ------------------------------------------------------------------
    # 2. Create Student nodes
    # ------------------------------------------------------------------
    for stu in STUDENTS:
        props = {
            "student_id": stu.student_id,
            "f_name":     stu.f_name,
            "l_name":     stu.l_name,
        }
        add_node(driver, "Student", props)

    # ------------------------------------------------------------------
    # 3. Enroll students randomly
    # ------------------------------------------------------------------
    student_ids = [s.student_id for s in STUDENTS]
    seminar_ids  = [s.id for s in SEMINARS]
    enroll_students_randomly(driver, student_ids, seminar_ids)

    # ------------------------------------------------------------------
    # 4. Create Teacher nodes
    # ------------------------------------------------------------------
    for tch in TEACHERS:
        props = {
            "teacher_id": tch.id,
            "title":      tch.title,
            "f_name":     tch.f_name,
            "l_name":     tch.l_name,
        }
        add_node(driver, "Teacher", props)

    # ------------------------------------------------------------------
    # 5. Assign a random teacher to each class
    # ------------------------------------------------------------------
    teacher_ids = [t.id for t in TEACHERS]
    for class_code in seminar_ids:
        teacher_id = random.choice(teacher_ids)
        teacher_to_class_connection(driver, teacher_id, class_code)

    # ------------------------------------------------------------------
    # 6. Create Master Thesis nodes (New Entity)
    # ------------------------------------------------------------------
    for thesis in THESES:
        # We exclude the ID lists from the node properties to keep the graph clean,
        # or we can include them if we want to test array properties.
        # Here we include them to satisfy the "lot of fields" requirement.
        props = {
            "id": thesis.id,
            "title": thesis.title,
            "year": thesis.year,
            "grade": thesis.grade,
            "is_published": thesis.is_published,
            "pages": thesis.pages,
            "submission_date": thesis.submission_date,
            "topic": thesis.topic,
            "keywords": thesis.keywords,
            "department": thesis.department
        }
        add_node(driver, thesis.node_name, props)

        # ------------------------------------------------------------------
        # 7. Link Thesis to Student (Author) and Teachers (Supervisors)
        # ------------------------------------------------------------------

        # Link Student -> Thesis
        connect_nodes(
            driver,
            src_label="Student",
            src_props={"student_id": thesis.student_id},
            rel_type=CONNECTIONS[Student, MasterThesis],
            dst_label="Thesis",
            dst_props={"id": thesis.id}
        )

        # Link Teachers -> Thesis
        for supervisor_id in thesis.supervisor_ids:
            connect_nodes(
                driver,
                src_label="Teacher",
                src_props={"teacher_id": supervisor_id},
                rel_type=CONNECTIONS[Teacher, MasterThesis],
                dst_label="Thesis",
                dst_props={"id": thesis.id}
            )

