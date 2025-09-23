import logging
from flask import Blueprint, request, jsonify

def create_reset_and_load_data_bp(graph):
    bp = Blueprint("reset_and_load_data", __name__)

    def fn_clear_database():
        query = """
            MATCH (n)
            DETACH DELETE n
        """
        fn_debug("Clearing database", "Deleting all nodes and relationships")
        graph.run(query)
        fn_debug("Database cleared", "All nodes and relationships removed")

    def fn_create_person(vorname, nachname):
        query = """
            CREATE (p:Person {vorname:$vorname, nachname:$nachname})
            RETURN ID(p) AS id
        """
        fn_debug("Creating person", {"vorname": vorname, "nachname": nachname})
        result = graph.run(query, vorname=vorname, nachname=nachname).data()
        person_id = result[0]["id"]
        fn_debug("Created person ID", person_id)
        return person_id

    def fn_debug(label, data):
        print(f"DEBUG [reset_load]: {label}: {data}")

    def fn_create_relation_lives_in(person_vorname, person_nachname, ort_id):
        query = """
            MATCH (p:Person {vorname:$vorname, nachname:$nachname})
            MATCH (o:Ort) WHERE ID(o)=$ort_id
            MERGE (p)-[:WOHNT_IN]->(o)
        """
        fn_debug("Creating WOHNT_IN relation", {"person": f"{person_vorname} {person_nachname}", "ort_id": ort_id})
        graph.run(query, vorname=person_vorname, nachname=person_nachname, ort_id=ort_id)

    def fn_create_relation_located_in(ort_id, stadt_name):
        query = """
            MATCH (o:Ort) WHERE ID(o)=$ort_id
            MATCH (s:Stadt {name:$stadt})
            MERGE (o)-[:LIEGT_IN]->(s)
        """
        fn_debug("Creating LIEGT_IN relation", {"ort_id": ort_id, "stadt": stadt_name})
        graph.run(query, ort_id=ort_id, stadt=stadt_name)

    def fn_create_address(street, plz):
        query = """
            CREATE (o:Ort {straße:$street, plz:$plz})
            RETURN ID(o) AS id
        """
        fn_debug("Creating address", {"straße": street, "plz": plz})
        result = graph.run(query, street=street, plz=plz).data()
        addr_id = result[0]["id"]
        fn_debug("Address ID", addr_id)
        return addr_id

    def fn_create_book(title, year):
        query = """
            CREATE (b:Buch {titel:$title, erscheinungsjahr:$year})
            RETURN ID(b) AS id
        """
        fn_debug("Creating book", {"titel": title, "erscheinungsjahr": year})
        result = graph.run(query, title=title, year=year).data()
        book_id = result[0]["id"]
        fn_debug("Book ID", book_id)
        return book_id

    def fn_create_relation_has_written(person_vorname, person_nachname, book_title):
        query = """
            MATCH (p:Person {vorname:$vorname, nachname:$nachname})
            MATCH (b:Buch {titel:$title})
            MERGE (p)-[:HAT_GESCHRIEBEN]->(b)
        """
        fn_debug("Creating HAT_GESCHRIEBEN relation", {"person": f"{person_vorname} {person_nachname}", "buch": book_title})
        graph.run(query, vorname=person_vorname, nachname=person_nachname, title=book_title)

    @bp.route('/reset_and_load_data')
    def api_reset_and_load_data():
        try:
            fn_debug("Start API", "Reset and load data endpoint called")

            # 1. Clear DB
            fn_clear_database()

            # 2. Person & Address Data
            person_data = [
                {"vorname": "Maria", "nachname": "Müller", "straße": "Hauptstraße 1", "stadt": "Berlin", "plz": "10115"},
                {"vorname": "Hans", "nachname": "Schmidt", "straße": "Marktplatz 5", "stadt": "Hamburg", "plz": "20095"},
                {"vorname": "Anna", "nachname": "Fischer", "straße": "Bahnhofsallee 12", "stadt": "München", "plz": "80331"},
                {"vorname": "Bob", "nachname": "Johnson", "straße": "", "stadt": "", "plz": ""},
                {"vorname": "Charlie", "nachname": "Brown", "straße": "", "stadt": "", "plz": ""}
            ]

            ort_ids = {}
            for person in person_data:
                fn_create_person(person["vorname"], person["nachname"])
                ort_id = fn_create_address(person["straße"], person["plz"])
                ort_ids[f"{person['vorname']}_{person['nachname']}"] = ort_id
                fn_create_relation_lives_in(person["vorname"], person["nachname"], ort_id)
                fn_create_relation_located_in(ort_id, person["stadt"])

            # 3. Books
            book_data = [
                {"titel": "The Cypher Key", "erscheinungsjahr": 2023, "vorname": "Maria", "nachname": "Müller"},
                {"titel": "The Graph Odyssey", "erscheinungsjahr": 2022, "vorname": "Bob", "nachname": "Johnson"},
                {"titel": "Neo's Journey", "erscheinungsjahr": 2024, "vorname": "Charlie", "nachname": "Brown"}
            ]

            for book in book_data:
                fn_create_book(book["titel"], book["erscheinungsjahr"])
                fn_create_relation_has_written(book["vorname"], book["nachname"], book["titel"])

            fn_debug("Finished API", "Database reset and data loaded successfully")
            return jsonify({"status": "success", "message": "Database cleared and data loaded successfully"})

        except Exception as e:
            fn_debug("Exception in reset_and_load_data", e)
            return jsonify({"status": "error", "message": str(e)}), 500

    return bp
