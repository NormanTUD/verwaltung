# TOOD: neue person:
# - welches sekretariat? (person, sekretariat fremder professur, kann leer sein)
# - verschiedene kostenstellen pro alles wo kostenstellen sind (drittmittelprojekte)
# - welcher PI? welcher abteilungsleiter?

import argparse
import sys
import traceback
import re
import platform
import shutil
import os
import subprocess
from datetime import date
import datetime
from copy import deepcopy
import csv
import uuid
import json
from collections import defaultdict
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, urlunparse

auto_is_authenticated = False

parser = argparse.ArgumentParser(description="Starte die Flask-App mit konfigurierbaren Optionen.")
parser.add_argument('--debug', action='store_true', help='Aktiviere den Debug-Modus')
parser.add_argument('--disable_login', action='store_true', help='Deaktivier den Login')
parser.add_argument('--port', type=int, default=5000, help='Port für die App (Standard: 5000)')
parser.add_argument('--engine-db', type=str, default='sqlite:///instance/database.db', help='URI für create_engine()')
args = parser.parse_args()

def normalize_sqlite_uri(uri: str) -> str:
    """
    Wenn es sich um eine SQLite URI handelt, konvertiere sie zu einem absoluten Pfad
    in ./instance/, behalte den Dateinamen bei.
    """
    parsed = urlparse(uri)

    if parsed.scheme != 'sqlite':
        # Keine SQLite URI, return original
        return uri

    # Hole den Datenbank-Dateinamen
    db_name = Path(parsed.path).name

    # Absoluter Pfad: $(pwd)/instance/<db_name>
    abs_path = Path.cwd() / 'instance' / db_name
    abs_path.parent.mkdir(parents=True, exist_ok=True)  # ensure ./instance exists

    # Neue URI zusammenbauen
    new_uri = f'sqlite:///{abs_path}'
    return new_uri

args.engine_db = normalize_sqlite_uri(args.engine_db)

db_engine_file = "/etc/db_engine"

if os.path.isfile(db_engine_file):
    #print(f"[DEBUG] {db_engine_file} ist eine Datei", file=sys.stderr)
    if os.access(db_engine_file, os.R_OK):
        #print(f"[DEBUG] {db_engine_file} ist lesbar", file=sys.stderr)
        try:
            with open(db_engine_file, "r", encoding="utf-8") as f:
                file_content = f.read().strip()
                #print(f"[DEBUG] Gelesener Inhalt: '{file_content}'", file=sys.stderr)
                if file_content:
                    args.engine_db = file_content
                    print(f"[DEBUG] args.engine_db auf '{args.engine_db}' gesetzt", file=sys.stderr)
                else:
                    print(f"[WARN] {db_engine_file} ist leer", file=sys.stderr)
        except Exception as e:
            print(f"[ERROR] Fehler beim Lesen von {db_engine_file}: {str(e)}", file=sys.stderr)
    else:
        print(f"[ERROR] Keine Leserechte für {db_engine_file}", file=sys.stderr)

IGNORED_TABLES = {"transaction", "user", "role"}

try:
    import venv
except ModuleNotFoundError:
    print("venv not found. Is python3-venv installed?")
    sys.exit(1)

VENV_PATH = Path.home() / ".verwaltung_venv"
PYTHON_BIN = VENV_PATH / ("Scripts" if platform.system() == "Windows" else "bin") / ("python.exe" if platform.system() == "Windows" else "python")

def get_from_requirements_txt_file(path=None):
    try:
        if path is None:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            path = os.path.join(script_dir, "requirements.txt")

        if not os.path.isfile(path):
            raise FileNotFoundError(f"requirements.txt not found at: {path}")

        with open(path, "r", encoding="utf-8") as _f:
            lines = _f.readlines()

        requirements = []
        for line in lines:
            line = line.strip()
            if line and not line.startswith("#"):
                requirements.append(line)

        return requirements

    except Exception as e:
        print(f"Error reading requirements file: {e}")
        return []

pip_install_modules = [
    PYTHON_BIN, "-m", "pip", "install", "-q", "--upgrade",
    *get_from_requirements_txt_file()
]

def create_and_setup_venv():
    print(f"Creating virtualenv at {VENV_PATH}")
    venv.create(VENV_PATH, with_pip=True)
    subprocess.check_call([PYTHON_BIN, "-m", "pip", "install", "--upgrade", "pip"])
    subprocess.check_call(pip_install_modules)

def restart_with_venv():
    try:
        result = subprocess.run(
            [str(PYTHON_BIN)] + sys.argv,
            text=True,
            check=True,
            env=dict(**os.environ)
        )
        sys.exit(result.returncode)
    except subprocess.CalledProcessError as e:
        print("Subprocess Error:")
        print(f"Exit-Code: {e.returncode}")
        sys.exit(e.returncode)
    except Exception as e:
        print(f"Unexpected error while restarting python: {e}")
        sys.exit(1)

try:
    from importers import importers_bp

    from flask import Flask, request, redirect, url_for, render_template_string, jsonify, send_from_directory, render_template, abort, send_file, flash, g, has_app_context, Response, session
    from flask_login import LoginManager, login_user, logout_user, current_user

    from flask_login import login_required
    from oasis_helper import conditional_login_required

    from markupsafe import Markup

    from sqlalchemy import create_engine, inspect, Date, DateTime, text, func, event, String, Unicode, Text
    from sqlalchemy.orm import sessionmaker, joinedload, Session, Query
    from sqlalchemy.orm.attributes import flag_modified
    from sqlalchemy.orm.exc import NoResultFound, DetachedInstanceError
    from sqlalchemy.exc import SQLAlchemyError
    from sqlalchemy.event import listens_for
    from sqlalchemy_schemadisplay import create_schema_graph
    from sqlalchemy.orm import class_mapper, ColumnProperty, RelationshipProperty
    from sqlalchemy import Integer, Text, Date, Float, Boolean, ForeignKey

    from sqlalchemy.orm.strategy_options import Load
    from sqlalchemy.orm.strategy_options import Load

    from sqlalchemy.orm.attributes import flag_modified

    import sqlalchemy.exc

    from db_defs import *
    from pypdf import PdfReader, PdfWriter
    from pypdf.generic import NameObject
    import io
    from markupsafe import escape
    import html
    import sqlalchemy
    import cryptography
    import aiosqlite
    from PIL import Image
    import datetime

    from werkzeug.security import generate_password_hash, check_password_hash
    from werkzeug.utils import secure_filename

    import tempfile
    import pandas as pd

    from markupsafe import escape

    from db_interface import *

    from auth import admin_required, is_admin_user
    from db import *

    from flask_sqlalchemy import SQLAlchemy
    from flask_admin import Admin
    from flask_admin.contrib.sqla import ModelView
    from wtforms import IntegerField, FloatField
    from flask_admin.form import Select2Widget
    from wtforms.validators import Optional as OptionalValidator
    from wtforms_sqlalchemy.fields import QuerySelectField, QuerySelectMultipleField

    from mypydie import dier

    from dotenv import load_dotenv
    import oasis_helper

    from api.get_data_as_table import create_get_data_bp
    from api.dump_database import create_dump_database_bp
    from api.reset_and_load_data import create_reset_and_load_data_bp
    from api.delete_node import create_delete_node_bp
    from api.delete_nodes import create_delete_nodes_bp
    from api.create_node import create_create_node_bp
    from api.add_property_to_nodes import create_add_property_to_nodes_bp
    from api.delete_all import create_delete_all_bp
    from api.graph_data import create_graph_data_bp
    from api.update_node import create_update_node_bp
    from api.add_row import create_add_row_bp
    from api.add_column import create_add_column_bp
    from api.update_nodes import create_update_nodes_bp
    from api.save_queries import create_save_queries
    from api.add_relationship import create_add_relationship_bp
    from api.reset_and_load_complex_data import create_complex_data_bp
    from api.labels import create_labels_bp
    from api.properties import create_properties_bp
    from api.relationships import create_relationships_bp
    from api.query_overview import create_query_overview

    from index_manager import create_index_bp
except ModuleNotFoundError as e:
    if not VENV_PATH.exists():
        create_and_setup_venv()
    else:
        try:
            subprocess.check_call(pip_install_modules)
        except subprocess.CalledProcessError as e:
            shutil.rmtree(VENV_PATH)
            create_and_setup_venv()
            restart_with_venv()
        except KeyboardInterrupt:
            print("CTRL-c detected")
            sys.exit(0)
    try:
        restart_with_venv()
    except KeyboardInterrupt:
        sys.exit(0)

app = Flask(__name__)
app.register_blueprint(importers_bp)
app.config['SQLALCHEMY_DATABASE_URI'] = args.engine_db

login_manager = LoginManager()
login_manager.init_app(app)

login_manager.login_view = 'login'
login_manager.login_message = "Bitte melde dich an, um fortzufahren."

app.secret_key = oasis_helper.load_or_generate_secret_key()

graph = oasis_helper.get_graph_db_connection()

app.config['GRAPH'] = graph

app.register_blueprint(create_get_data_bp(graph), url_prefix='/api')
app.register_blueprint(create_dump_database_bp(graph), url_prefix='/api')
app.register_blueprint(create_reset_and_load_data_bp(graph), url_prefix='/api')
app.register_blueprint(create_delete_node_bp(graph), url_prefix='/api')
app.register_blueprint(create_add_property_to_nodes_bp(graph), url_prefix='/api')
app.register_blueprint(create_delete_nodes_bp(graph), url_prefix='/api')
app.register_blueprint(create_create_node_bp(graph), url_prefix='/api')
app.register_blueprint(create_delete_all_bp(graph), url_prefix='/api')
app.register_blueprint(create_graph_data_bp(graph), url_prefix='/api')
app.register_blueprint(create_update_node_bp(graph), url_prefix='/api')
app.register_blueprint(create_add_row_bp(graph), url_prefix='/api')
app.register_blueprint(create_add_column_bp(graph), url_prefix='/api')
app.register_blueprint(create_save_queries(), url_prefix='/api')
app.register_blueprint(create_update_nodes_bp(graph), url_prefix='/api')
app.register_blueprint(create_add_relationship_bp(graph), url_prefix='/api')
app.register_blueprint(create_complex_data_bp(graph), url_prefix='/api')
app.register_blueprint(create_labels_bp(graph), url_prefix='/api')
app.register_blueprint(create_properties_bp(graph), url_prefix='/api')
app.register_blueprint(create_relationships_bp(graph), url_prefix='/api')

app.register_blueprint(create_index_bp(graph), url_prefix='/')
app.register_blueprint(create_query_overview(), url_prefix='/')

@login_manager.user_loader
def load_user(user_id):
    my_session = Session()
    ret = my_session.get(User, int(user_id))
    my_session.close()
    return ret

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated or auto_is_authenticated:
        return redirect(url_for('index'))  # Benutzer ist schon eingeloggt → sofort weiterleiten

    my_session = Session()
    try:
        if request.method == 'POST':
            username = request.form.get('username', '')
            password = request.form.get('password', '')

            user = my_session.query(User).filter_by(username=username).first()

            if user:
                if not user.is_active:
                    flash('Benutzer ist noch nicht aktiviert.')
                elif check_password_hash(user.password, password):
                    login_user(user)
                    return redirect(url_for('index'))
                else:
                    flash('Falsches Passwort.')
            else:
                flash('Benutzer nicht gefunden.')
    finally:
        my_session.close()

    return render_template('login.html')

def is_password_complex(password):
    if len(password) < 8:
        return False
    if not re.search(r'[A-Z]', password):  # Großbuchstabe
        return False
    if not re.search(r'[a-z]', password):  # Kleinbuchstabe
        return False
    if not re.search(r'[0-9]', password):  # Ziffer
        return False
    if not re.search(r'[^\w\s]', password):  # Sonderzeichen
        return False
    return True

@app.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated or auto_is_authenticated:
        return redirect(url_for('index'))  # Bereits angemeldet → weiterleiten

    my_session = Session()
    try:
        if request.method == 'POST':
            username = request.form.get('username', '')
            password = request.form.get('password', '')

            # Passwort-Komplexitätsprüfung
            if not is_password_complex(password):
                return render_template(
                    'register.html',
                    error='Passwort muss mindestens 8 Zeichen lang sein und Großbuchstaben, Kleinbuchstaben, Zahlen und mindestens ein Sonderzeichen beinhalten.'
                )

            # Prüfen, ob Benutzername bereits existiert
            existing_user = my_session.query(User).filter_by(username=username).first()
            if existing_user:
                return render_template('register.html', error='Username already taken.')

            # Passwort hashen
            hashed_pw = generate_password_hash(password, method='pbkdf2:sha256')

            # Prüfen, ob dies der erste Benutzer ist
            user_count = my_session.query(User).count()
            if user_count == 0:
                # Admin-Rolle holen oder erstellen
                try:
                    admin_role = my_session.query(Role).filter_by(name='admin').one()
                except NoResultFound:
                    admin_role = Role(name='admin')
                    my_session.add(admin_role)
                    my_session.commit()

                # Erster Benutzer: aktiv und admin
                new_user = User(
                    username=username,
                    password=hashed_pw,
                    is_active=True,
                    role='admin'
                )
                new_user.roles.append(admin_role)
            else:
                # Weitere Benutzer: nicht aktiv
                new_user = User(
                    username=username,
                    password=hashed_pw,
                    is_active=False
                )

            my_session.add(new_user)
            my_session.commit()
            return redirect(url_for('login'))
    finally:
        my_session.close()

    return render_template('register.html')

@app.route('/logout')
@conditional_login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.errorhandler(405)
def method_not_allowed(e):
    return render_template('405.html'), 405

@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404

@app.route('/search')
@conditional_login_required
def search():
    session = Session()

    query = request.args.get('q', '').lower().strip()
    results = []

    show_admin_stuff = is_admin_user(session) or auto_is_authenticated

    if show_admin_stuff:
        if 'admin' in query:
           results.append({'label': '🛠️ Admin', 'url': '/admin'})

    if show_admin_stuff:
        if 'import' in query:
           results.append({'label': '📥 Import', 'url': '/import'})

    if 'overview' in query:
        results.append({'label': '📊 Overview', 'url': '/overview'})

    if 'queries' in query:
        results.append({'label': '🔍 Queries', 'url': '/query_overview'})

    session.close()

    return jsonify(results)

@app.route('/')
@conditional_login_required
def index():
    return render_template("index.html", user=current_user)

@app.route('/import')
@conditional_login_required
def _import():
    return render_template('import.html')

@app.route('/graph')
@conditional_login_required
def show_graph():
    return render_template('graph.html')

@app.route('/upload', methods=['POST'])
@conditional_login_required
def upload_data():
    """Verarbeitet den CSV/TSV-Upload und zeigt die Header für die Zuordnung an."""
    if 'data' not in request.form:
        return "Keine Daten hochgeladen", 400

    data = request.form['data']
    session['raw_data'] = data

    try:
        f = io.StringIO(data)
        dialect = csv.Sniffer().sniff(f.read(1024))
        f.seek(0)
        reader = csv.reader(f, dialect)
        headers = next(reader)

        # Lege Header in der Session ab
        session['headers'] = headers

        return render_template('mapping.html', headers=headers)
    except csv.Error as e:
        return f"Fehler beim Parsen der Daten: {e}", 400

@app.route('/get_rel_types', methods=['GET'])
@conditional_login_required
def get_rel_types():
    """Gibt eine Liste aller existierenden Relationship-Typen in der DB zurück."""
    try:
        # Führe eine Cypher-Abfrage aus, um alle eindeutigen Relationship-Typen zu finden
        query = "MATCH ()-[r]->() RETURN DISTINCT type(r) AS type"
        result = graph.run(query).data()
        types = [d['type'] for d in result]
        return jsonify(types)
    except Exception as e:
        print(f"Fehler beim Abrufen der Relationship-Typen: {e}")
        return jsonify([]), 500

@app.route('/save_mapping', methods=['POST'])
@conditional_login_required
def save_mapping():
    """Hauptfunktion: speichert die zugeordneten Daten in Neo4j."""
    mapping_data = request.get_json()

    if not graph:
        print("Fehler: Datenbank nicht verbunden.")
        return jsonify({"status": "error", "message": "Datenbank nicht verbunden."}), 500

    if 'raw_data' not in session:
        return jsonify({"status": "error", "message": "raw_data not in session."}), 500

    reader = parse_csv_from_session()
    if reader is None:
        return jsonify({"status": "error", "message": "Fehler beim Analysieren der CSV-Daten."}), 400

    tx = graph.begin()

    try:
        for _, row in enumerate(reader):
            process_row(row, mapping_data)

        graph.commit(tx)
        #print("\nGesamtvorgang erfolgreich: Daten wurden in die Neo4j-Datenbank importiert.")
        return jsonify({"status": "success", "message": "Daten erfolgreich in Neo4j importiert."})
    except Exception as e:
        tx.rollback()
        print(f"\n❌ Fehler beim Speichern in der DB: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

def parse_csv_from_session():
    """Liest die CSV-Daten aus der Session und gibt einen DictReader zurück."""
    raw_data = session.pop('raw_data')
    f = io.StringIO(raw_data)
    try:
        dialect = csv.Sniffer().sniff(f.read(1024))
        f.seek(0)
        reader = csv.DictReader(f, dialect=dialect)
        return reader
    except csv.Error as e:
        print(f"Fehler beim Analysieren der CSV-Daten: {e}")
        return None

def process_row(row, mapping_data):
    """Verarbeitet eine Zeile: Knoten mergen und Beziehungen erstellen."""
    nodes_created = {}

    # Knoten erstellen/mergen
    for node_type, fields in mapping_data.get('nodes', {}).items():
        node = merge_node(node_type, fields, row)
        if node:
            nodes_created[node_type] = node

    # Beziehungen erstellen
    for rel_data in mapping_data.get('relationships', []):
        create_relationship(rel_data['from'], rel_data['to'], rel_data['type'], nodes_created)

def merge_node(node_type, fields, row):
    """Merged einen Knoten vom Typ node_type mit gegebenen Properties."""
    node_var = safe_var_name(node_type)
    node_label = f"`{node_type}`"

    all_props = {}
    for field_map in fields:
        original_name = field_map['original']
        renamed_name = field_map['renamed']
        value = row.get(original_name)
        if value:
            all_props[renamed_name] = value

    if not all_props:
        #print(f"  ❌ Keine Daten für den Knoten-Typ '{node_type}' in dieser Zeile. Überspringe.")
        return None

    identifier_key, identifier_value = next(iter(all_props.items()))
    #print(f"  ➡️ Versuche, einen Knoten vom Typ '{node_type}' zu mergen.")
    #print(f"     Identifikator: '{identifier_key}' = '{identifier_value}'")
    #print(f"     Alle Properties: {all_props}")

    cypher_query = f"""
    MERGE ({node_var}:{node_label} {{`{identifier_key}`: $identifier_value}})
    ON CREATE SET {node_var} = $all_props
    RETURN {node_var}
    """

    params = {"identifier_value": identifier_value, "all_props": all_props}
    result = graph.run(cypher_query, **params).data()

    if result:
        return result[0][node_var]

    print(f"  ⚠️ MERGE-Vorgang für '{node_type}' hat nichts zurückgegeben.")
    return None

def create_relationship(from_node_type, to_node_type, rel_type, nodes_created):
    """Erstellt eine Beziehung zwischen zwei vorhandenen Knoten."""
    clean_rel_type = rel_type.replace(' ', '_').upper()
    rel_label = f"`{clean_rel_type}`"

    from_var = safe_var_name(from_node_type)
    to_var = safe_var_name(to_node_type)

    #print(f"  ➡️ Versuche, eine Beziehung '{rel_type}' zu erstellen.")

    if from_node_type in nodes_created and to_node_type in nodes_created:
        from_node = nodes_created[from_node_type]
        to_node = nodes_created[to_node_type]

        rel_query = f"""
        MATCH ({from_var}:`{from_node_type}`) WHERE id({from_var}) = {from_node.identity}
        MATCH ({to_var}:`{to_node_type}`) WHERE id({to_var}) = {to_node.identity}
        MERGE ({from_var})-[rel:{rel_label}]->({to_var})
        """
        graph.run(rel_query)
        #print(f"  ✅ Beziehung '{clean_rel_type}' zwischen '{from_node_type}' und '{to_node_type}' erstellt.")
    #else:
        #print(f"  ❌ Beziehung konnte nicht erstellt werden, Knoten fehlen: '{from_node_type}' (vorhanden: {from_node_type in nodes_created}), '{to_node_type}' (vorhanden: {to_node_type in nodes_created}).")

def get_all_nodes_and_relationships():
    """Holt alle aktuell vorhandenen Node-Typen und Relationship-Typen aus der Datenbank."""
    try:
        node_labels = graph.run("MATCH (n) RETURN DISTINCT labels(n) AS labels").data()
        relationship_types = graph.run("MATCH ()-[r]->() RETURN DISTINCT type(r) AS relType").data()
    except Exception as e:
        raise RuntimeError(f"Fehler beim Abfragen der Datenbank: {e}")

    # Labels kann eine Liste enthalten, also flachziehen
    labels = set()
    for entry in node_labels:
        for label in entry["labels"]:
            labels.add(label)

    rel_types = [entry["relType"] for entry in relationship_types if "relType" in entry]

    return {
        "labels": sorted(labels),
        "types": sorted(rel_types)
    }

@app.route('/overview')
@conditional_login_required
def overview():
    """Zeigt die Übersichtsseite mit allen Node-Typen an."""
    if not graph:
        # Fehler-Meldung ins Template geben
        return render_template(
            'overview.html',
            db_info=None,
            error="Datenbank nicht verbunden."
        ), 500

    db_info = get_all_nodes_and_relationships()

    # Prüfen, ob db_info leer oder nur leere Listen hat
    if not db_info or (
        isinstance(db_info, dict)
        and not db_info.get("labels")
        and not db_info.get("types")
    ):
        return render_template(
            'overview.html',
            db_info=None,
            error="Keine Daten gefunden."
        ), 200

    return render_template('overview.html', db_info=db_info, error=None)

def safe_var_name(label):
    # Ersetzt alle nicht-alphanumerischen Zeichen durch "_"
    return "".join(ch if ch.isalnum() else "_" for ch in label.lower())


@app.context_processor
def inject_sidebar_data():
    my_session = Session()

    # Tabellen wie bisher
    tables = [
        cls.__name__.lower()
        for cls in Base.__subclasses__()
        if hasattr(cls, '__tablename__') and cls.__tablename__ not in IGNORED_TABLES
    ]

    is_authenticated = current_user.is_authenticated

    is_admin = False

    if is_authenticated:
        try:
            # User nochmal frisch aus DB laden mit Rollen eager
            user = my_session.query(User).options(
                joinedload(User.roles)
            ).filter(User.id == current_user.id).one_or_none()

            if user is not None:
                is_admin = any(role.name == 'admin' for role in user.roles)
            else:
                print(f"User mit ID {current_user.id} nicht in DB gefunden")
        except DetachedInstanceError:
            print("DetachedInstanceError: current_user is not bound to my_session")
        except Exception as e:
            print(f"Unbekannter Fehler beim Laden des Users: {e}")

    # Theme
    theme_cookie = request.cookies.get('theme')
    theme = theme_cookie if theme_cookie in ['dark', 'light'] else 'light'

    # Saved Queries einlesen
    saved_queries = []
    try:
        json_path = os.path.join(os.path.dirname(__file__), 'saved_queries.json')
        with open(json_path, 'r', encoding='utf-8') as f:
            saved_queries = json.load(f)
    except FileNotFoundError:
        print("saved_queries.json nicht gefunden")
    except json.JSONDecodeError as e:
        print(f"Fehler beim Parsen von saved_queries.json: {e}")
    except Exception as e:
        print(f"Unbekannter Fehler beim Laden von saved_queries.json: {e}")

    my_session.close()

    if auto_is_authenticated:
        is_authenticated = True

    if auto_is_authenticated:
        is_admin = True

    return dict(
        tables=tables,
        is_authenticated=is_authenticated,
        is_admin=is_admin,
        theme=theme,
        saved_queries=saved_queries  # <-- hier injiziert
    )

@app.route('/query_overview')
@conditional_login_required
def query_overview_page():
    """
    Zeigt die Hauptseite der Query-Übersicht an.
    """
    return render_template("query_overview.html")

@app.route('/admin', methods=['GET', 'POST'])
@login_required
@admin_required
def admin_panel():
    session = Session()
    try:
        if request.method == 'POST' and 'new_username' in request.form:
            username = request.form['new_username']
            password = request.form['new_password']
            role_id = request.form.get('new_role')

            existing_user = session.query(User).filter_by(username=username).first()
            if existing_user:
                flash('Benutzername existiert bereits.')
            else:
                hashed = generate_password_hash(password)
                user = User(username=username, password=hashed, is_active=False)
                if role_id:
                    role = session.query(Role).get(int(role_id))
                    if role:
                        user.roles.append(role)
                session.add(user)
                session.commit()
                flash('Benutzer hinzugefügt.')

            session.close()
            return redirect(url_for('admin_panel'))

        # WICHTIG: Rollen eager-laden, um DetachedInstanceError zu vermeiden
        users = session.query(User).options(joinedload(User.roles)).all()
        roles = session.query(Role).all()

        return render_template('admin_panel.html', users=users, roles=roles)

    except SQLAlchemyError as e:
        session.rollback()
        app.logger.error("Fehler im Admin-Panel: %s", e)
        flash("Fehler im Admin-Panel.")
        return redirect(url_for('index'))

    finally:
        session.close()

@app.route('/admin/delete/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def delete_user(user_id):
    session = Session()
    try:
        user = session.query(User).get(user_id)
        if not user:
            return jsonify(success=False, error="Benutzer nicht gefunden.")
        
        session.delete(user)
        session.commit()
        return jsonify(success=True, message="Benutzer gelöscht.")

    except SQLAlchemyError as e:
        session.rollback()
        app.logger.error("Fehler beim Löschen des Benutzers: %s", e)
        return jsonify(success=False, error="Fehler beim Löschen des Benutzers.")

    finally:
        session.close()

@app.route('/admin/update/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def update_user(user_id):
    session = Session()
    try:
        user = session.query(User).get(user_id)
        if not user:
            flash("Benutzer nicht gefunden.")
            return redirect(url_for('admin_panel'))

        # Aktivieren (falls angefragt und noch nicht aktiv)
        if 'activate_user' in request.form and not user.is_active:
            user.is_active = True
            flash(f"Benutzer {user.username} wurde aktiviert.")

        # Benutzername ändern
        new_username = request.form.get('username')
        if new_username and new_username != user.username:
            existing = session.query(User).filter(User.username == new_username, User.id != user.id).first()
            if existing:
                flash("Benutzername existiert bereits.")
                return redirect(url_for('admin_panel'))
            user.username = new_username

        # Passwort ändern
        new_password = request.form.get('new_password')
        if new_password:
            user.password = generate_password_hash(new_password)

        # Rolle ändern
        new_role_id = request.form.get('role_id')
        user.roles.clear()
        if new_role_id:
            role = session.query(Role).get(int(new_role_id))
            if role:
                user.roles.append(role)

        # ✅ Readonly setzen
        user.readonly = 'readonly' in request.form

        session.commit()
        flash("Benutzer aktualisiert.")
        return redirect(url_for('admin_panel'))

    except SQLAlchemyError as e:
        session.rollback()
        app.logger.error("Fehler beim Aktualisieren des Benutzers: %s", e)
        flash("Fehler beim Aktualisieren des Benutzers.")
        return redirect(url_for('admin_panel'))

    finally:
        session.close()


@app.route('/admin/activate/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def activate_user(user_id):
    session = Session()
    try:
        user = session.query(User).get(user_id)
        if not user:
            return jsonify(success=False, error="Benutzer nicht gefunden"), 404

        if user.is_active:
            return jsonify(success=False, error="Benutzer ist bereits aktiviert"), 400

        user.is_active = True
        session.commit()
        return jsonify(success=True)

    except SQLAlchemyError as e:
        session.rollback()
        app.logger.error("Fehler beim Aktivieren des Benutzers: %s", e)
        return jsonify(success=False, error="Fehler beim Aktivieren des Benutzers"), 500

    finally:
        session.close()

def is_running_in_docker():
    """
    Detects if the current process is running inside a Docker container.

    Returns:
        bool: True if running in Docker, False otherwise.
    """
    try:
        # Check if /.dockerenv file exists (commonly present in Docker)
        if os.path.exists('/.dockerenv'):
            return True

        # Check if 'docker' or 'containerd' is mentioned in cgroup info
        cgroup_path = '/proc/1/cgroup'
        if os.path.exists(cgroup_path):
            with open(cgroup_path, 'r', encoding='utf-8', errors='ignore') as file:
                for line in file:
                    if 'docker' in line or 'containerd' in line:
                        return True

        # Fallback: check environment variables sometimes set in container environments
        docker_env_vars = ['DOCKER_CONTAINER', 'KUBERNETES_SERVICE_HOST', 'CONTAINER']
        for var in docker_env_vars:
            if var in os.environ:
                return True

        return False
    except Exception as e:
        # If detection fails, assume not running in Docker
        print(f"Warning: Docker detection failed due to error: {e}")
        return False

if __name__ == "__main__":
    with app.app_context():
        db.init_app(app)
        Base.metadata.create_all(db.engine)

    print(f"args.engine_db: {args.engine_db}")

    if args.disable_login or is_running_in_docker():
        print("Login not required!")
        app.config["DISABLE_LOGIN"] = True
        auto_is_authenticated = True

    app.run(debug=args.debug, host='0.0.0.0', port=args.port)
