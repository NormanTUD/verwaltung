import logging
import random
from flask import Blueprint, jsonify
from faker import Faker

fake = Faker()

def create_complex_data_bp(graph):
    bp = Blueprint("complex_data", __name__)

    def fn_clear_database():
        graph.run("MATCH (n) DETACH DELETE n")

    # --- Entity Creators ---
    def fn_create_node(label, props):
        query = f"CREATE (n:{label} $props) RETURN ID(n) AS id"
        result = graph.run(query, props=props).data()
        return result[0]["id"]

    def fn_create_relationship(from_id, to_label, to_props, rel_type, rel_props=None):
        props_str = ""
        if rel_props:
            props_str = " {" + ", ".join(f"{k}: ${k}" for k in rel_props.keys()) + "}"
        query = f"""
            MATCH (f) WHERE ID(f)=$fid
            MATCH (t:{to_label} {{{', '.join(f'{k}: ${k}' for k in to_props.keys())}}})
            MERGE (f)-[r:{rel_type}{props_str}]->(t)
        """
        params = {"fid": from_id}
        if to_props:
            params.update(to_props)
        if rel_props:
            params.update(rel_props)
        graph.run(query, **params)

    @bp.route('/reset_and_load_complex_data')
    def api_reset_and_load_complex_data():
        try:
            fn_clear_database()

            # --- Personen ---
            person_ids = {}
            for _ in range(1000):
                props = {
                    "vorname": fake.first_name(),
                    "nachname": fake.last_name(),
                    "geburtsjahr": random.randint(1950, 2005),
                    "email": fake.email(),
                    "telefon": fake.phone_number(),
                    "geschlecht": random.choice(["männlich","weiblich","divers"])
                }
                pid = fn_create_node("Person", props)
                person_ids[f"{props['vorname']}_{props['nachname']}"] = pid

            # --- Orte / Gebäude ---
            ort_ids = {}
            for _ in range(200):
                props = {
                    "strasse": fake.street_address(),
                    "plz": fake.postcode(),
                    "stadt": fake.city(),
                    "land": "Deutschland",
                    "typ": random.choice(["Büro", "Labor", "Lager", "Wohnhaus"])
                }
                oid = fn_create_node("Ort", props)
                ort_ids[f"{props['stadt']}_{props['plz']}"] = oid

            # --- Organisationen und Abteilungen ---
            org_ids = {}
            dept_ids = {}
            for _ in range(100):
                org_props = {
                    "name": fake.company(),
                    "typ": random.choice(["Firma","Universität","Startup","Verlag"]),
                    "branche": fake.bs(),
                    "mitarbeiterzahl": random.randint(10,5000)
                }
                oid = fn_create_node("Organisation", org_props)
                org_ids[org_props["name"]] = oid

                # Abteilungen
                for _ in range(random.randint(1,5)):
                    dept_props = {"name": fake.bs(), "mitarbeiter": random.randint(1,200)}
                    did = fn_create_node("Abteilung", dept_props)
                    dept_ids[dept_props["name"]] = did
                    fn_create_relationship(did, "Organisation", {"name": org_props["name"]}, "GEHÖRT_ZU")

            # --- Projekte ---
            project_ids = {}
            for _ in range(200):
                props = {
                    "name": fake.catch_phrase(),
                    "start": fake.date_this_decade().isoformat(),
                    "ende": fake.date_this_decade().isoformat(),
                    "budget": round(random.uniform(1000,1000000),2),
                    "status": random.choice(["aktiv","abgeschlossen","geplant"])
                }
                pid = fn_create_node("Projekt", props)
                project_ids[props["name"]] = pid

            # --- Geräte / Fahrzeuge ---
            device_ids = {}
            for _ in range(300):
                props = {
                    "name": fake.word(),
                    "typ": random.choice(["Laptop","Drucker","Maschine","Fahrzeug"]),
                    "seriennummer": fake.uuid4(),
                    "standort": fake.city()
                }
                did = fn_create_node("Geraet", props)
                device_ids[props["seriennummer"]] = did

            # --- Bücher ---
            book_ids = {}
            genres = ["Technik","Roman","Lehrbuch","Science-Fiction","Biografie"]
            for _ in range(300):
                props = {
                    "titel": fake.sentence(nb_words=4),
                    "erscheinungsjahr": random.randint(1990,2025),
                    "genre": random.choice(genres),
                    "seiten": random.randint(50,1500),
                    "isbn": fake.isbn13()
                }
                bid = fn_create_node("Buch", props)
                book_ids[props["titel"]] = bid
                # Autor zufällig
                autor_key = random.choice(list(person_ids.keys()))
                fn_create_relationship(person_ids[autor_key], "Buch", {"titel": props["titel"]}, "HAT_GESCHRIEBEN", {"rolle":"Autor"})

            # --- Events ---
            event_ids = {}
            themen = ["Tech","Data Science","Literatur","Kunst","Forschung"]
            for _ in range(150):
                props = {
                    "name": fake.catch_phrase(),
                    "datum": fake.date_between(start_date='-1y', end_date='+1y').isoformat(),
                    "thema": random.choice(themen),
                    "dauer": random.randint(1,5)
                }
                ort_key = random.choice(list(ort_ids.keys()))
                eid = fn_create_node("Event", props)
                event_ids[props["name"]] = eid
                fn_create_relationship(eid, "Ort", {"stadt": ort_ids[ort_key]}, "FINDET_STATT_IN")

                # Teilnehmer zufällig
                for _ in range(random.randint(5,30)):
                    person_key = random.choice(list(person_ids.keys()))
                    fn_create_relationship(person_ids[person_key], "Event", {"name": props["name"]}, "NIMMT_TEIL_AN", {"rolle": random.choice(["Speaker","Attendee","Organizer"])})

            return jsonify({"status":"success","message":"Sehr komplexer Datensatz erstellt mit vielen Entity-Typen!"})

        except Exception as e:
            logging.exception("Error loading complex data")
            return jsonify({"status":"error","message":str(e)}),500

    return bp
