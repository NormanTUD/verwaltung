import logging
from flask import Blueprint, request, jsonify

def create_reset_and_load_data_bp(graph):
    bp = Blueprint("reset_and_load_data", __name__)

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
