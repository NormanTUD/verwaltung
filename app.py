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

parser = argparse.ArgumentParser(description="Starte die Flask-App mit konfigurierbaren Optionen.")
parser.add_argument('--debug', action='store_true', help='Aktiviere den Debug-Modus')
parser.add_argument('--port', type=int, default=5000, help='Port für die App (Standard: 5000)')
parser.add_argument('--secret', type=str, default='geheim', help='SECRET_KEY für Flask (Standard: "geheim")')
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

    from flask import Flask, request, redirect, url_for, render_template_string, jsonify, send_from_directory, render_template, abort, send_file, flash, g, has_app_context, Response
    from flask_login import LoginManager, login_user, login_required, logout_user, current_user

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

    from sqlalchemy.orm import Session
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

app.config['SECRET_KEY'] = args.secret
app.config['SQLALCHEMY_DATABASE_URI'] = args.engine_db

login_manager = LoginManager()
login_manager.init_app(app)

login_manager.login_view = 'login'
login_manager.login_message = "Bitte melde dich an, um fortzufahren."

@login_manager.user_loader
def load_user(user_id):
    session = Session()
    ret = session.get(User, int(user_id))
    session.close()
    return ret

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('index'))  # Benutzer ist schon eingeloggt → sofort weiterleiten

    session = Session()
    try:
        if request.method == 'POST':
            username = request.form.get('username', '')
            password = request.form.get('password', '')

            user = session.query(User).filter_by(username=username).first()

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
        session.close()

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
    if current_user.is_authenticated:
        return redirect(url_for('index'))  # Bereits angemeldet → weiterleiten

    session = Session()
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
            existing_user = session.query(User).filter_by(username=username).first()
            if existing_user:
                return render_template('register.html', error='Username already taken.')

            # Passwort hashen
            hashed_pw = generate_password_hash(password, method='pbkdf2:sha256')

            # Prüfen, ob dies der erste Benutzer ist
            user_count = session.query(User).count()
            if user_count == 0:
                # Admin-Rolle holen oder erstellen
                try:
                    admin_role = session.query(Role).filter_by(name='admin').one()
                except NoResultFound:
                    admin_role = Role(name='admin')
                    session.add(admin_role)
                    session.commit()

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

            session.add(new_user)
            session.commit()
            return redirect(url_for('login'))
    finally:
        session.close()

    return render_template('register.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404

@app.route('/search')
@login_required
def search():
    session = Session()

    query = request.args.get('q', '').lower().strip()
    results = []

    wizard_routes = [f"/wizard/{key}" for key in WIZARDS.keys()]
    wizard_routes.append("/wizard/person")
    wizard_routes = sorted(set(wizard_routes))

    for route in wizard_routes:
        label = route.replace('/wizard/', '').capitalize()
        if query in label.lower():
            results.append({
                'label': f'🧙 {label}',
                'url': route
            })


    for key, config in AGGREGATE_VIEWS.items():
        title = config.get("title", key).strip()
        if key.startswith(query) or title.lower().startswith(query):
            if not key.endswith("version"):
                results.append({
                    'label': f'📦 {title}',
                    'url': url_for('aggregate_view', aggregate_name=key)  # ✅ Korrekt
                })

    # 🔍 Personensuche nach Name, Email, Telefon, Fax
    people = session.query(Person).options(joinedload(Person.contacts)).all()
    for person in people:
        full_name = f"{person.title or ''} {person.vorname} {person.nachname}".strip().lower()
        matched = False

        if query in full_name:
            matched = True
        else:
            for contact in person.contacts:
                if any(query in (getattr(contact, attr) or '').lower() for attr in ['email', 'phone', 'fax']):
                    matched = True
                    break

        if matched:
            results.append({
                'label': f'👤 {person.vorname} {person.nachname}',
                'url': url_for('aggregate_view', aggregate_name="person", person_id=person.id)
            })

    # Admin-Zeug
    if is_admin_user(session):
        tables = [
            cls.__name__.lower()
            for cls in Base.__subclasses__()
            if hasattr(cls, '__tablename__') and cls.__tablename__ not in ["role", "user"]
        ]

        if 'admin' in query:
            results.append({'label': '🛠️ Admin', 'url': '/admin'})

        for table in tables:
            if query in table.lower():
                results.append({
                    'label': f'📋 {table.capitalize()}',
                    'url': f'/admin/{table}'
                })
        if 'map-editor'.startswith(query):
            results.append({'label': '🗺️ Map-Editor', 'url': '/map-editor'})

    if 'etageplan'.startswith(query):
        results.append({'label': '🗺️ etageplan', 'url': '/etageplan'})

    session.close()

    return jsonify(results)

@app.route('/api/versions')
def get_versions():
    session = Session()
    try:
        transactions = session.query(TransactionTable).order_by(TransactionTable.id.asc()).all()

        versions = []
        for t in transactions:
            timestamp_iso = None
            if hasattr(t, "issued_at") and isinstance(t.issued_at, datetime):
                timestamp_iso = t.issued_at.isoformat()

            versions.append({
                "id": t.id,
                "timestamp": timestamp_iso
            })

        return jsonify(versions)
    except SQLAlchemyError as e:
        app.logger.error("Error fetching versions: %s", e)
        return jsonify([]), 500
    finally:
        session.close()

if __name__ == "__main__":
    with app.app_context():
        db.init_app(app)
        Base.metadata.create_all(db.engine)

    print(f"args.engine_db: {args.engine_db}")

    app.run(debug=args.debug, port=args.port)
