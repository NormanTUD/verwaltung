import logging
import random
from flask import Blueprint, jsonify
from faker import Faker

fake = Faker()

def create_complex_data_bp(graph):
    bp = Blueprint("complex_data", __name__)

    # --- Hilfsfunktionen ---
    def fn_clear_database():
        query = "MATCH (n) DETACH DELETE n"
        graph.run(query)

    def fn_create_person(vorname, nachname, geburtsjahr=None, email=None, telefon=None, geschlecht=None):
        query = """
            CREATE (p:Person {
                vorname:$vorname,
                nachname:$nachname,
                geburtsjahr:$geburtsjahr,
                email:$email,
                telefon:$telefon,
                geschlecht:$geschlecht
            })
            RETURN ID(p) AS id
        """
        result = graph.run(query, vorname=vorname, nachname=nachname, geburtsjahr=geburtsjahr, email=email, telefon=telefon, geschlecht=geschlecht).data()
        return result[0]["id"]

    def fn_create_address(street, plz, stadt, land="Deutschland"):
        query = """
            CREATE (a:Ort {
                strasse:$street,
                plz:$plz,
                stadt:$stadt,
                land:$land
            })
            RETURN ID(a) AS id
        """
        result = graph.run(query, street=street, plz=plz, stadt=stadt, land=land).data()
        return result[0]["id"]

    def fn_create_organization(name, typ, branche=None, mitarbeiterzahl=None):
        query = """
            CREATE (o:Organisation {
                name:$name,
                typ:$typ,
                branche:$branche,
                mitarbeiterzahl:$mitarbeiterzahl
            })
            RETURN ID(o) AS id
        """
        result = graph.run(query, name=name, typ=typ, branche=branche, mitarbeiterzahl=mitarbeiterzahl).data()
        return result[0]["id"]

    def fn_create_book(title, year, genre=None, seiten=None, isbn=None):
        query = """
            CREATE (b:Buch {
                titel:$title,
                erscheinungsjahr:$year,
                genre:$genre,
                seiten:$seiten,
                isbn:$isbn
            })
            RETURN ID(b) AS id
        """
        result = graph.run(query, title=title, year=year, genre=genre, seiten=seiten, isbn=isbn).data()
        return result[0]["id"]

    def fn_create_event(name, datum, ort_id=None, thema=None, dauer=None):
        query = """
            CREATE (e:Event {
                name:$name,
                datum:$datum,
                thema:$thema,
                dauer:$dauer
            })
            RETURN ID(e) AS id
        """
        result = graph.run(query, name=name, datum=datum, thema=thema, dauer=dauer).data()
        event_id = result[0]["id"]
        if ort_id:
            graph.run(
                "MATCH (e:Event) WHERE ID(e)=$eid MATCH (o:Ort) WHERE ID(o)=$oid MERGE (e)-[:FINDET_STATT_IN]->(o)",
                eid=event_id, oid=ort_id
            )
        return event_id

    def fn_create_rel(person_id=None, target_label=None, target_props=None, rel_type=None, rel_props=None):
        """
        Generic relationship creator.
        """
        if person_id is None or target_label is None or rel_type is None:
            return
        props_str = ""
        if rel_props:
            props_str = " {" + ", ".join(f"{k}: ${k}" for k in rel_props.keys()) + "}"
        query = f"""
            MATCH (p) WHERE ID(p)=$pid
            MATCH (t:{target_label} {{{', '.join(f'{k}: ${k}' for k in target_props.keys())}}})
            MERGE (p)-[r:{rel_type}{props_str}]->(t)
        """
        params = {"pid": person_id}
        if target_props:
            params.update(target_props)
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
                vorname = fake.first_name()
                nachname = fake.last_name()
                geburtsjahr = random.randint(1950, 2005)
                email = fake.email()
                telefon = fake.phone_number()
                geschlecht = random.choice(["männlich", "weiblich", "divers"])
                pid = fn_create_person(vorname, nachname, geburtsjahr, email, telefon, geschlecht)
                person_ids[f"{vorname}_{nachname}"] = pid

            # --- Adressen ---
            ort_ids = {}
            for _ in range(500):
                street = fake.street_address()
                plz = fake.postcode()
                stadt = fake.city()
                oid = fn_create_address(street, plz, stadt)
                ort_ids[f"{stadt}_{plz}"] = oid

            # --- Organisationen ---
            org_ids = {}
            for _ in range(200):
                name = fake.company()
                typ = random.choice(["Firma", "Universität", "Verlag", "Startup"])
                branche = fake.bs()
                mitarbeiterzahl = random.randint(10, 5000)
                oid = fn_create_organization(name, typ, branche, mitarbeiterzahl)
                org_ids[name] = oid

            # --- Bücher ---
            book_ids = {}
            genres = ["Technik", "Roman", "Lehrbuch", "Science-Fiction", "Biografie", "Historisch"]
            for _ in range(500):
                title = fake.sentence(nb_words=4)
                year = random.randint(1990, 2025)
                genre = random.choice(genres)
                seiten = random.randint(50, 1500)
                isbn = fake.isbn13()
                bid = fn_create_book(title, year, genre, seiten, isbn)
                book_ids[title] = bid
                # zufälliger Autor
                autor_key = random.choice(list(person_ids.keys()))
                autor_vorname, autor_nachname = autor_key.split("_")
                fn_create_rel(person_ids[autor_key], "Buch", {"titel": title}, "HAT_GESCHRIEBEN", {"rolle": "Autor"})

            # --- Events ---
            event_ids = {}
            themen = ["Tech", "Data Science", "Literatur", "Kunst", "Forschung"]
            for _ in range(200):
                name = fake.catch_phrase()
                datum = fake.date_between(start_date='-1y', end_date='+1y').isoformat()
                ort_key = random.choice(list(ort_ids.keys()))
                ort_id = ort_ids[ort_key]
                thema = random.choice(themen)
                dauer = random.randint(1, 5)  # Tage
                eid = fn_create_event(name, datum, ort_id, thema, dauer)
                event_ids[name] = eid
                # zufällige Teilnehmer
                for _ in range(random.randint(5, 50)):
                    person_key = random.choice(list(person_ids.keys()))
                    fn_create_rel(person_ids[person_key], "Event", {"name": name}, "NIMMT_TEIL_AN", {"rolle": random.choice(["Speaker", "Attendee", "Organizer", "Visitor"])})

            return jsonify({"status": "success", "message": "Mega komplexer Datensatz erstellt: 1000+ Personen, hunderte Events, Bücher, Organisationen"})

        except Exception as e:
            logging.exception("Error loading complex data")
            return jsonify({"status": "error", "message": str(e)}), 500

    return bp
