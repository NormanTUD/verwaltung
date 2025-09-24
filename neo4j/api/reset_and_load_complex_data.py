import logging
from flask import Blueprint, request, jsonify

def create_complex_data_bp(graph):
    bp = Blueprint("complex_data", __name__)

    # --- Hilfsfunktionen ---
    def fn_clear_database():
        query = "MATCH (n) DETACH DELETE n"
        graph.run(query)

    def fn_create_person(vorname, nachname, geburtsjahr=None, email=None):
        query = """
            CREATE (p:Person {vorname:$vorname, nachname:$nachname, geburtsjahr:$geburtsjahr, email:$email})
            RETURN ID(p) AS id
        """
        result = graph.run(query, vorname=vorname, nachname=nachname, geburtsjahr=geburtsjahr, email=email).data()
        return result[0]["id"]

    def fn_create_address(street, plz, land="Deutschland"):
        query = """
            CREATE (a:Ort {straße:$street, plz:$plz, land:$land})
            RETURN ID(a) AS id
        """
        result = graph.run(query, street=street, plz=plz, land=land).data()
        return result[0]["id"]

    def fn_create_organization(name, typ):
        query = """
            CREATE (o:Organisation {name:$name, typ:$typ})
            RETURN ID(o) AS id
        """
        result = graph.run(query, name=name, typ=typ).data()
        return result[0]["id"]

    def fn_create_book(title, year, genre=None):
        query = """
            CREATE (b:Buch {titel:$title, erscheinungsjahr:$year, genre:$genre})
            RETURN ID(b) AS id
        """
        result = graph.run(query, title=title, year=year, genre=genre).data()
        return result[0]["id"]

    def fn_create_event(name, datum, ort_id=None):
        query = """
            CREATE (e:Event {name:$name, datum:$datum})
            RETURN ID(e) AS id
        """
        result = graph.run(query, name=name, datum=datum).data()
        event_id = result[0]["id"]
        if ort_id:
            graph.run("MATCH (e:Event) WHERE ID(e)=$eid MATCH (o:Ort) WHERE ID(o)=$oid MERGE (e)-[:FINDET_STATT_IN]->(o)",
                      eid=event_id, oid=ort_id)
        return event_id

    def fn_create_rel_person_organization(person_vorname, person_nachname, org_name, rolle):
        query = """
            MATCH (p:Person {vorname:$vorname, nachname:$nachname})
            MATCH (o:Organisation {name:$org_name})
            MERGE (p)-[:ARBEITET_FÜR {rolle:$rolle}]->(o)
        """
        graph.run(query, vorname=person_vorname, nachname=person_nachname, org_name=org_name, rolle=rolle)

    def fn_create_rel_person_book(person_vorname, person_nachname, book_title, rolle="Autor"):
        query = """
            MATCH (p:Person {vorname:$vorname, nachname:$nachname})
            MATCH (b:Buch {titel:$title})
            MERGE (p)-[:HAT_GESCHRIEBEN {rolle:$rolle}]->(b)
        """
        graph.run(query, vorname=person_vorname, nachname=person_nachname, title=book_title, rolle=rolle)

    def fn_create_rel_event_person(event_name, person_vorname, person_nachname, rolle):
        query = """
            MATCH (e:Event {name:$event_name})
            MATCH (p:Person {vorname:$vorname, nachname:$nachname})
            MERGE (p)-[:NIMMT_TEIL_AN {rolle:$rolle}]->(e)
        """
        graph.run(query, event_name=event_name, vorname=person_vorname, nachname=person_nachname, rolle=rolle)

    @bp.route('/reset_and_load_complex_data')
    def api_reset_and_load_complex_data():
        try:
            fn_clear_database()

            # --- Personen ---
            personen = [
                {"vorname": "Alice", "nachname": "Meyer", "geburtsjahr": 1985, "email": "alice@example.com"},
                {"vorname": "Bob", "nachname": "Schneider", "geburtsjahr": 1990},
                {"vorname": "Charlie", "nachname": "Fischer", "geburtsjahr": 1975, "email": "charlie@example.net"},
                {"vorname": "Dana", "nachname": "Klein"},
                {"vorname": "Eva", "nachname": "Wolf", "geburtsjahr": 1988}
            ]

            person_ids = {}
            for p in personen:
                pid = fn_create_person(p["vorname"], p["nachname"], p.get("geburtsjahr"), p.get("email"))
                person_ids[f"{p['vorname']}_{p['nachname']}"] = pid

            # --- Adressen ---
            adressen = [
                {"straße": "Hauptstraße 1", "plz": "10115"},
                {"straße": "Marktplatz 5", "plz": "20095"},
                {"straße": "Bahnhofstraße 12", "plz": "80331"},
                {"straße": "Kirchweg 3", "plz": "50667"}
            ]

            ort_ids = {}
            for i, addr in enumerate(adressen):
                oid = fn_create_address(addr["straße"], addr["plz"])
                ort_ids[f"Ort_{i}"] = oid

            # --- Organisationen ---
            orgs = [
                {"name": "TechCorp", "typ": "Firma"},
                {"name": "UniBerlin", "typ": "Universität"},
                {"name": "BookPublishers GmbH", "typ": "Verlag"}
            ]

            org_ids = {}
            for org in orgs:
                oid = fn_create_organization(org["name"], org["typ"])
                org_ids[org["name"]] = oid

            # --- Bücher ---
            books = [
                {"titel": "Advanced Graphs", "erscheinungsjahr": 2021, "genre": "Technik", "autor": "Alice Meyer"},
                {"titel": "Neo4j Deep Dive", "erscheinungsjahr": 2022, "genre": "Technik", "autor": "Bob Schneider"},
                {"titel": "Mystery in Berlin", "erscheinungsjahr": 2023, "genre": "Roman", "autor": "Charlie Fischer"},
                {"titel": "Data Science 101", "erscheinungsjahr": 2020, "genre": "Lehrbuch", "autor": "Dana Klein"},
            ]

            for book in books:
                fn_create_book(book["titel"], book["erscheinungsjahr"], book.get("genre"))
                autor_vorname, autor_nachname = book["autor"].split(" ")
                fn_create_rel_person_book(autor_vorname, autor_nachname, book["titel"])

            # --- Beziehungen Person -> Organisation ---
            fn_create_rel_person_organization("Alice", "Meyer", "TechCorp", "Entwicklerin")
            fn_create_rel_person_organization("Bob", "Schneider", "UniBerlin", "Dozent")
            fn_create_rel_person_organization("Charlie", "Fischer", "BookPublishers GmbH", "Autor")
            fn_create_rel_person_organization("Dana", "Klein", "TechCorp", "Data Scientist")
            fn_create_rel_person_organization("Eva", "Wolf", "UniBerlin", "Studentin")

            # --- Events ---
            events = [
                {"name": "GraphConf 2025", "datum": "2025-10-01", "ort_id": ort_ids["Ort_0"]},
                {"name": "Data Science Meetup", "datum": "2025-11-15", "ort_id": ort_ids["Ort_1"]},
                {"name": "Book Fair Berlin", "datum": "2025-12-05", "ort_id": ort_ids["Ort_2"]}
            ]

            for event in events:
                fn_create_event(event["name"], event["datum"], event.get("ort_id"))

            # --- Beziehungen Person -> Event ---
            fn_create_rel_event_person("GraphConf 2025", "Alice", "Meyer", "Speaker")
            fn_create_rel_event_person("GraphConf 2025", "Bob", "Schneider", "Attendee")
            fn_create_rel_event_person("Data Science Meetup", "Dana", "Klein", "Organizer")
            fn_create_rel_event_person("Book Fair Berlin", "Charlie", "Fischer", "Exhibitor")
            fn_create_rel_event_person("Book Fair Berlin", "Eva", "Wolf", "Visitor")

            return jsonify({"status": "success", "message": "Complex database cleared and data loaded successfully"})

        except Exception as e:
            logging.exception("Error loading complex data")
            return jsonify({"status": "error", "message": str(e)}), 500

    return bp
