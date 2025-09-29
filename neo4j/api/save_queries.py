import json
import os

from flask import Blueprint, request, jsonify

SAVED_QUERIES_FILE = 'saved_queries.json'

def create_save_queries():
    bp = Blueprint("save_queries", __name__)

    def load_saved_queries():
        """Lädt die gespeicherten Abfragen aus der Datei."""
        if not os.path.exists(SAVED_QUERIES_FILE):
            return []
        with open(SAVED_QUERIES_FILE, encoding="utf-8", mode='r') as f:
            try:
                content = f.read().strip()
                if not content:
                    return []
                return json.loads(content)
            except json.JSONDecodeError:
                return []

    def save_queries_to_file(queries):
        """Speichert die Abfragen in der Datei."""
        with open(SAVED_QUERIES_FILE, encoding="utf-8", mode='w') as f:
            json.dump(queries, f, indent=4)

    @bp.route('/save_query', methods=['POST'])
    def save_query():
        try:
            data = request.json
            name = data.get('name')
            labels = data.get('selectedLabels')
            if not name or not labels:
                return jsonify({'status': 'error', 'message': 'Name und Labels sind erforderlich.'}), 400

            queries = load_saved_queries()

            # Prüfe, ob die Abfrage bereits existiert
            if any(q['name'] == name for q in queries):
                return jsonify({'status': 'error', 'message': f'Abfrage mit dem Namen "{name}" existiert bereits.'}), 409

            queries.append({'name': name, 'labels': labels})
            save_queries_to_file(queries)
            return jsonify({'status': 'success', 'message': 'Abfrage erfolgreich gespeichert.'})
        except Exception as e:
            return jsonify({'status': 'error', 'message': str(e)}), 500

    @bp.route('/get_saved_queries')
    def get_saved_queries():
        """Gibt alle gespeicherten Abfragen zurück."""
        try:
            queries = load_saved_queries()
            return jsonify(queries)
        except Exception as e:
            return jsonify({'status': 'error', 'message': str(e)}), 500

    return bp
