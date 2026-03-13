import logging
import os
import random
from typing import Any
from neo4j import GraphDatabase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("[DataGenerator]")

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------
URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
AUTH =(
    os.getenv("NEO4J_USER", "neo4j"),
    os.getenv("NEO4J_PASS", "testTEST12345678")
)
CLEAR_DB_ON_START = True


# ---------------------------------------------------------
# Random Data Pools (No external dependencies needed)
# ---------------------------------------------------------
FIRST_NAMES = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy", "Karl", "Luna", "Mallory", "Niaj", "Olivia"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor", "Anderson", "Thomas", "Jackson"]
BUZZWORDS = ["Quantum", "Nano", "Bio", "Cyber", "Astro", "Neuro", "Data", "Eco", "Plasma", "Geo"]
DISCIPLINES = ["Physics", "Biology", "Chemistry", "Computer Science", "Mathematics", "Sociology", "Medicine", "Engineering"]

# ---------------------------------------------------------
# Data Structures
# ---------------------------------------------------------
class ScienceGraphBuilder:
    def __init__(self):
        # Dictionary holding lists of nodes per label
        self.nodes: dict[str, list[dict[str, Any]]] = {
            "Institute": [],
            "Department": [],
            "Person": [],
            "Project": [],
            "Grant": [],
            "Publication": [],
            "Facility": [],
            "Equipment": [],
            "Committee": [],
            "Event": []
        }

        # Edges grouped by (from_label, rel_type, to_label)
        # Value is a list of dicts: {"from_uid": X, "to_uid": Y}
        self.edges: dict[tuple[str, str, str], list[dict[str, str]]] = {}

    def add_node(self, label: str, properties: dict[str, Any]) -> str:
        """Adds a node and returns its uid."""
        if "uid" not in properties:
            raise ValueError("All nodes must have a unique 'uid' property.")
        self.nodes[label].append(properties)
        return properties["uid"]

    def add_edge(self, from_label: str, from_uid: str, rel_type: str, to_label: str, to_uid: str):
        """Registers a relationship to be created."""
        key = (from_label, rel_type, to_label)
        if key not in self.edges:
            self.edges[key] = []
        self.edges[key].append({"from_uid": from_uid, "to_uid": to_uid})

    def generate_randomized_bulk_data(self):
        """Generates the base bulk data."""
        logger.info("Generating Nodes...")

        # 1. Institutes (5)
        for i in range(1, 6):
            self.add_node("Institute", {
                "uid": f"inst_{i}", "name": f"Institute of {random.choice(BUZZWORDS)} Sciences",
                "founded_year": random.randint(1900, 2010)
            })

        # 2. Departments (20)
        for i in range(1, 21):
            self.add_node("Department", {
                "uid": f"dept_{i}", "name": f"Department of {random.choice(DISCIPLINES)}",
                "cost_center": f"CC-{random.randint(1000, 9999)}"
            })

        # 3. Persons (100)
        for i in range(1, 101):
            self.add_node("Person", {
                "uid": f"person_{i}", "first_name": random.choice(FIRST_NAMES),
                "last_name": random.choice(LAST_NAMES), "email": f"person{i}@science.edu",
                "hire_year": random.randint(2000, 2023)
            })

        # 4. Projects (50)
        for i in range(1, 51):
            self.add_node("Project", {
                "uid": f"proj_{i}", "title": f"The {random.choice(BUZZWORDS)} Initiative",
                "budget": round(random.uniform(10000, 500000), 2), "status": random.choice(["Active", "Completed", "Planning"])
            })

        # 5. Grants (30)
        for i in range(1, 31):
            self.add_node("Grant", {
                "uid": f"grant_{i}", "reference_code": f"GRNT-{random.randint(100,999)}",
                "amount": random.randint(50000, 2000000)
            })

        # 6. Publications (150)
        for i in range(1, 151):
            self.add_node("Publication", {
                "uid": f"pub_{i}", "title": f"Analysis of {random.choice(BUZZWORDS)} Systems",
                "doi": f"10.1000/xyz{i}", "year": random.randint(2015, 2024)
            })

        # 7. Facilities (15)
        for i in range(1, 16):
            self.add_node("Facility", {
                "uid": f"fac_{i}", "building_name": f"Building {chr(64 + i)}",
                "room_number": f"{random.randint(1, 9)}{random.randint(0, 9)}{random.randint(0, 9)}",
                "capacity": random.randint(10, 200)
            })

        # 8. Equipment (100)
        for i in range(1, 101):
            self.add_node("Equipment", {
                "uid": f"eq_{i}", "name": f"{random.choice(BUZZWORDS)} Spectrometer",
                "asset_tag": f"AST-{random.randint(10000, 99999)}", "is_calibrated": random.choice([True, False])
            })

        # 9. Committees (10)
        for i in range(1, 11):
            self.add_node("Committee", {
                "uid": f"com_{i}", "name": f"{random.choice(DISCIPLINES)} Review Board",
                "meeting_frequency": random.choice(["Weekly", "Monthly", "Quarterly"])
            })

        # 10. Events (40)
        for i in range(1, 41):
            self.add_node("Event", {
                "uid": f"ev_{i}", "title": f"Annual {random.choice(BUZZWORDS)} Symposium",
                "date": f"202{random.randint(4,6)}-0{random.randint(1,9)}-15", "expected_attendees": random.randint(20, 500)
            })

        self._generate_random_edges()

    def _generate_random_edges(self):
        """Randomly connects the bulk data logically."""
        logger.info("Generating Background Relationships...")

        # Depts belong to Institutes
        for dept in self.nodes["Department"]:
            inst = random.choice(self.nodes["Institute"])
            self.add_edge("Department", dept["uid"], "belongs_to", "Institute", inst["uid"])

        # Persons belong to Departments
        for person in self.nodes["Person"]:
            dept = random.choice(self.nodes["Department"])
            self.add_edge("Person", person["uid"], "works_in", "Department", dept["uid"])

            # Authored publications
            for _ in range(random.randint(0, 3)):
                pub = random.choice(self.nodes["Publication"])
                self.add_edge("Person", person["uid"], "authored", "Publication", pub["uid"])

        # Projects use Equipment and Grants fund Projects
        for proj in self.nodes["Project"]:
            grnt = random.choice(self.nodes["Grant"])
            self.add_edge("Grant", grnt["uid"], "FUNDS", "Project", proj["uid"])
            for _ in range(random.randint(1, 3)):
                eq = random.choice(self.nodes["Equipment"])
                self.add_edge("Project", proj["uid"], "USES", "Equipment", eq["uid"])

        # Events held at Facilities
        for ev in self.nodes["Event"]:
            fac = random.choice(self.nodes["Facility"])
            self.add_edge("Event", ev["uid"], "HELD_AT", "Facility", fac["uid"])

    def inject_complex_topologies(self):
        """Explicitly builds edge-cases for the topology detector."""
        logger.info("Injecting Complex Topologies (Loops, Diamonds, Chains)...")

        # -------------------------------------------------------------
        # TOPOLOGY 1: Multi-depth Same-Type Chain & Same-Type Loop
        # Tests: Person -> manages -> Person -> supervises -> Person
        # -------------------------------------------------------------
        # Chain
        self.add_edge("Person", "person_1", "manages", "Person", "person_2")
        self.add_edge("Person", "person_2", "manages", "Person", "person_3")
        self.add_edge("Person", "person_3", "manages", "Person", "person_4")

        # Same-Type Loop (Administrative Paradox!)
        self.add_edge("Person", "person_5", "manages", "Person", "person_6")
        self.add_edge("Person", "person_6", "supervises", "Person", "person_7")
        self.add_edge("Person", "person_7", "manages", "Person", "person_5") # Closes the loop

        # -------------------------------------------------------------
        # TOPOLOGY 2: The Diamond Configuration
        # Tests: Multiple paths converging on a single node
        # A Joint-Project funded by one grant, hosted by two departments
        # -------------------------------------------------------------
        # grant_1 -> allocates_to -> dept_1 & dept_2
        self.add_edge("Grant", "grant_1", "allocates_to", "Department", "dept_1")
        self.add_edge("Grant", "grant_1", "allocates_to", "Department", "dept_2")
        # dept_1 & dept_2 -> co_hosts -> project_1
        self.add_edge("Department", "dept_1", "CO_HOSTS", "Project", "proj_1")
        self.add_edge("Department", "dept_2", "CO_HOSTS", "Project", "proj_1")

        # -------------------------------------------------------------
        # TOPOLOGY 3: The Long Cross-Type Loop
        # Tests: A->B->C->D->E->A. Ensures `get_longest_path` and `CROSS_TYPE` cycle catches it.
        # -------------------------------------------------------------
        self.add_edge("Department", "dept_3", "provides_chair_for", "Committee", "com_1")
        self.add_edge("Committee", "com_1", "OVERSEES", "Facility", "fac_1")
        self.add_edge("Facility", "fac_1", "HOUSES", "Equipment", "eq_1")
        self.add_edge("Equipment", "eq_1", "purchased_through", "Grant", "grant_2")
        self.add_edge("Grant", "grant_2", "managed_by", "Department", "dept_3") # Closes loop

        # -------------------------------------------------------------
        # TOPOLOGY 4: Multiple relations between the exact same nodes
        # Tests: Frontend grouping and relation arrays
        # -------------------------------------------------------------
        self.add_edge("Person", "person_10", "teaches_at", "Facility", "fac_2")
        self.add_edge("Person", "person_10", "maintains", "Facility", "fac_2")
        self.add_edge("Person", "person_10", "inspected", "Facility", "fac_2")


def ingest_to_neo4j(driver, graph: ScienceGraphBuilder):
    """Executes parameterized queries to push the constructed graph into Neo4j."""
    with driver.session() as session:
        if CLEAR_DB_ON_START:
            if not input("Are you really sure that you want to wipe the DB? (y/n)") == "y":
                return 1
            logger.info("Wiping existing database...")
            session.run("MATCH (n) DETACH DELETE n")

        # 1. Ingest Nodes (Iterate over the 10 Labels)
        for label, nodes in graph.nodes.items():
            if not nodes: continue
            logger.info(f"Ingesting {len(nodes)} '{label}' nodes...")

            # Using UNWIND for bulk write efficiency
            query = f"""
            UNWIND $batch AS row
            CREATE (n:{label})
            SET n = row
            """
            session.run(query, batch=nodes)

        # Create constraints to speed up edge insertion and ensure uniqueness
        for label in graph.nodes.keys():
            try:
                session.run(f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.uid IS UNIQUE")
            except Exception as e:
                logger.debug(f"Could not create constraint for {label}: {e}")

        # 2. Ingest Edges (Iterate over grouped relationships)
        logger.info(f"Ingesting {len(graph.edges)} types of relationships...")
        for (from_label, rel_type, to_label), edges in graph.edges.items():

            # Match by UID. Explicit labels avoid full DB scans.
            query = f"""
            UNWIND $batch AS row
            MATCH (a:{from_label} {{uid: row.from_uid}})
            MATCH (b:{to_label} {{uid: row.to_uid}})
            MERGE (a)-[r:{rel_type}]->(b)
            """
            session.run(query, batch=edges)

    logger.info("Data successfully loaded into Neo4j!")


if __name__ == "__main__":
    builder = ScienceGraphBuilder()
    builder.generate_randomized_bulk_data()
    builder.inject_complex_topologies()

    # Connect and Execute
    driver = GraphDatabase.driver(URI, auth=AUTH)
    try:
        # Verify connectivity before wiping/loading
        driver.verify_connectivity()
        ingest_to_neo4j(driver, builder)
    except Exception as e:
        logger.error(f"Failed to connect or ingest data: {e}")
    finally:
        driver.close()
