import json
import os
from flask import Blueprint, request, jsonify
from oasis_helper import conditional_login_required

SAVED_QUERIES_FILE = 'saved_queries.json'

def create_query_overview():
    bp = Blueprint("query_overview", __name__)

    def load_saved_queries():
        if not os.path.exists(SAVED_QUERIES_FILE):
            return []
        with open(SAVED_QUERIES_FILE, encoding="utf-8") as f:
            try:
                content = f.read().strip()
                return json.loads(content) if content else []
            except json.JSONDecodeError:
                return []

    def save_queries_to_file(queries):
        with open(SAVED_QUERIES_FILE, encoding="utf-8", mode='w') as f:
            json.dump(queries, f, indent=4, ensure_ascii=False)

    @bp.route('/delete_query', methods=['POST'])
    @conditional_login_required
    def delete_query():
        try:
            data = request.json
            name = data.get('name')
            if not name:
                return jsonify({'status': 'error', 'message': 'Parameter "name" ist erforderlich.'}), 400

            queries = load_saved_queries()
            queries = [q for q in queries if q.get('name') != name]
            save_queries_to_file(queries)
            return jsonify({'status': 'success', 'message': f'Abfrage "{name}" gelöscht.'})
        except Exception as e:
            return jsonify({'status': 'error', 'message': str(e)}), 500

    @bp.route('/rename_query', methods=['POST'])
    @conditional_login_required
    def rename_query():
        try:
            data = request.json
            old_name = data.get('oldName')
            new_name = data.get('newName')
            if not old_name or not new_name:
                return jsonify({'status': 'error', 'message': 'Beide Namen sind erforderlich.'}), 400

            queries = load_saved_queries()
            if any(q['name'] == new_name for q in queries):
                return jsonify({'status': 'error', 'message': f'Abfrage "{new_name}" existiert bereits.'}), 409

            updated = False
            for q in queries:
                if q['name'] == old_name:
                    q['name'] = new_name
                    updated = True
                    break
            if not updated:
                return jsonify({'status': 'error', 'message': f'Keine Abfrage "{old_name}" gefunden.'}), 404

            save_queries_to_file(queries)
            return jsonify({'status': 'success', 'message': f'Abfrage "{old_name}" umbenannt zu "{new_name}".'})
        except Exception as e:
            return jsonify({'status': 'error', 'message': str(e)}), 500

    return bp
