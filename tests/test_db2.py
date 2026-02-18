from dataclasses import dataclass, field
from typing import List, Dict, Any
from pytest import fixture

# --- Node Definitions ---
@dataclass
class Student: id: int; name: str; email: str; enrollment_year: int; gpa: float
@dataclass
class Teacher: id: int; name: str; department_id: int; hire_date: str; tenure: bool
@dataclass
class Department: id: int; name: str; code: str; head_id: int; building: str
@dataclass
class Course: id: int; code: str; title: str; credits: int; level: int; prerequisites: List[int]
@dataclass
class Seminar: id: int; course_id: int; section: str; semester: str; year: int
@dataclass
class ResearchProject: id: int; title: str; start_date: str; end_date: str; status: str; budget: float
@dataclass
class Publication: id: int; title: str; journal: str; year: int; doi: str; citations: int
@dataclass
class Grant: id: int; title: str; agency: str; amount: float; start_date: str; end_date: str
@dataclass
class Facility: id: int; name: str; type: str; building: str; room: str; capacity: int
@dataclass
class Equipment: id: int; name: str; model: str; manufacturer: str; purchase_date: str
@dataclass
class Event: id: int; name: str; type: str; date: str; venue_id: int; organizer_id: int
@dataclass
class Club: id: int; name: str; type: str; founded_year: int; president_id: int
@dataclass
class Alumni: id: int; student_id: int; graduation_year: int; degree: str; company: str

# --- Relationship Factory ---
class RelationshipFactory:
    def __init__(self):
        self.relationships = []

    def add_chain(self, nodes, rel_types):
        for i in range(len(nodes) - 1):
            self.relationships.append((nodes[i], rel_types[i], nodes[i+1]))

    def add_fork(self, source, rel_type, targets):
        for target in targets:
            self.relationships.append((source, rel_type, target))

    def add_self_ref(self, node, rel_type):
        self.relationships.append((node, rel_type, node))

    def get_relationships(self):
        return self.relationships

# --- Fixture Example ---
@fixture
def academic_graph_fixture():
    # Create nodes
    students = [Student(i, f"Student{i}", f"s{i}@uni.edu", 2020+i%4, 3.0+0.1*i) for i in range(1, 6)]
    teachers = [Teacher(i, f"Teacher{i}", i%3+1, f"201{5+i}-09-01", bool(i%2)) for i in range(1, 4)]
    departments = [Department(i, f"Dept{i}", f"D{i}", i, f"Bldg{i}") for i in range(1, 4)]
    courses = [Course(i, f"C{i}", f"Course{i}", 3, 1+i%3, []) for i in range(1, 4)]
    seminars = [Seminar(i, i, f"S{i}", "Fall", 2022) for i in range(1, 4)]
    # ... (other node types similarly)

    # Create relationships
    rel_factory = RelationshipFactory()
    # Chain: Department → Teacher → Course → Seminar
    rel_factory.add_chain([departments[0], teachers[0], courses[0], seminars[0]],
                          ["has_teacher", "teaches", "includes", "hosts"])
    # Fork: Student → [Seminar, Club, Event]
    clubs = [Club(1, "Chess Club", "Academic", 2010, 1)]
    events = [Event(1, "Welcome", "Orientation", "2022-09-01", 1, 1)]
    rel_factory.add_fork(students[0], "participates_in", [seminars[0], clubs[0], events[0]])
    # Self-ref: Course → Course (prerequisite)
    rel_factory.add_self_ref(courses[0], "prerequisite_of")
    # ... (add more as needed)

    # Return as fixture data
    return {
        "nodes": {
            "students": students,
            "teachers": teachers,
            "departments": departments,
            "courses": courses,
            "seminars": seminars,
            "clubs": clubs,
            "events": events,
            # ... (other node types)
        },
        "relationships": rel_factory.get_relationships()
    }
