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

parser = argparse.ArgumentParser(description="Starte die Flask-App mit konfigurierbaren Optionen.")
parser.add_argument('--debug', action='store_true', help='Aktiviere den Debug-Modus')
parser.add_argument('--port', type=int, default=5000, help='Port für die App (Standard: 5000)')
parser.add_argument('--secret', type=str, default='geheim', help='SECRET_KEY für Flask (Standard: "geheim")')
parser.add_argument('--engine-db', type=str, default='sqlite:///database.db', help='URI für create_engine()')
args = parser.parse_args()

db_engine_file = "/etc/db_engine"

if os.path.isfile(db_engine_file):
    print(f"[DEBUG] {db_engine_file} ist eine Datei", file=sys.stderr)
    if os.access(db_engine_file, os.R_OK):
        print(f"[DEBUG] {db_engine_file} ist lesbar", file=sys.stderr)
        try:
            with open(db_engine_file, "r", encoding="utf-8") as f:
                file_content = f.read().strip()
                print(f"[DEBUG] Gelesener Inhalt: '{file_content}'", file=sys.stderr)
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
    from flask import Flask, request, redirect, url_for, render_template_string, jsonify, send_from_directory, render_template, abort, send_file, flash, g, has_app_context, Response
    from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user

    from markupsafe import Markup

    from sqlalchemy import create_engine, inspect, Date, DateTime, text, func, event
    from sqlalchemy.orm import sessionmaker, joinedload, Session, Query
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

    from importers import importers_bp
    from auth import admin_required, is_admin_user
    from db import *
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
        print("You cancelled installation")
        sys.exit(0)

app = Flask(__name__)
app.register_blueprint(importers_bp)

def merge_entry_display(entry):
    """
    Versucht eine sinnvolle textuelle Darstellung eines Model-Eintrags zu erzeugen.
    """
    cls = type(entry)
    key_attrs = ["name", "title", "label", "username", "vorname", "nachname", "id"]

    for attr in key_attrs:
        if hasattr(entry, attr):
            val = getattr(entry, attr)
            if val:
                return f"{val} (ID {entry.id})"

    # Fallback: versuche alle Spalten mit Werten zu zeigen
    try:
        col_values = {
            col.name: getattr(entry, col.name)
            for col in entry.__table__.columns
            if getattr(entry, col.name) is not None
        }
        preview = ", ".join(f"{k}={v}" for k, v in col_values.items())
        return f"{cls.__name__}({preview})"
    except Exception:
        return f"{cls.__name__} (ID {entry.id})"

app.jinja_env.globals["entry_display"] = merge_entry_display

app.config['SECRET_KEY'] = args.secret
app.config['SQLALCHEMY_DATABASE_URI'] = args.engine_db

login_manager = LoginManager()
login_manager.init_app(app)

login_manager.login_view = 'login'
login_manager.login_message = "Bitte melde dich an, um fortzufahren."

COLUMN_LABELS = {
    "abteilung.abteilungsleiter_id": "Abteilungsleiter",
    "person.vorname": "Vorname",
    "person.nachname": "Nachname"
}

FK_DISPLAY_COLUMNS = {
    "person": ["title", "vorname", "nachname"]
}

INITIAL_DATA = {
    "kostenstellen": [
        {"name": "Kostenstelle A"},
        {"name": "Kostenstelle B"},
    ],
    "professuren": [
        {"name": "Professur X", "kostenstelle_name": "Kostenstelle A"},
        {"name": "Professur Y", "kostenstelle_name": "Kostenstelle B"},
    ],
    "object_categories": [
        {"name": "Kategorie 1"},
        {"name": "Kategorie 2"},
    ],
    "abteilungen": [
        {"name": "Abteilung Alpha"},
        {"name": "Abteilung Beta"},
    ]
}

LABEL_OVERRIDES = {
    "ausgeber_id": "Ausgeber",
    "besitzer_id": "Besitzer",
    "abteilungsleiter_id": "Abteilungsleiter (Person-ID)",
    "kostenstelle_id": "Kostenstelle-ID",
    "object_id": "Objekt-ID",
    "raum_id": "Raum-ID",
    "person_id": "Person-ID",
    "abteilung_id": "Abteilung-ID",
    "kategorie_id": "Kategorie-ID",
    "professur_id": "Professur-ID",
    "loan_id": "Leihgabe-ID",
}

HIDDEN_FIELD_NAMES = {"version", "Versions", "_version", "guid"}

def get_col_type(col):
    if isinstance(col.type, (Integer, Float)):
        return "number"

    if isinstance(col.type, (Text, String)):
        return "text"

    if isinstance(col.type, Date):
        return "date"

    if isinstance(col.type, Boolean):
        return "checkbox"

    return "text"

def labelize(col_name):
    return LABEL_OVERRIDES.get(
        col_name,
        col_name.replace('_id', '').replace('_', ' ').capitalize()
    )

def is_join_table_model(model):
    cols = list(model.__table__.columns)
    if len(cols) < 2:
        return False
    fk_cols = [col for col in cols if col.foreign_keys]
    # mindestens zwei FKs und alle außer evtl. 'id' sind FKs
    return len(fk_cols) >= 2 and all(col.foreign_keys or col.name == 'id' for col in cols)

def get_subforms(model, exclude_fields=None, exclude_relationships=None):
    exclude_fields = exclude_fields or set()
    exclude_relationships = exclude_relationships or set()
    subforms = []
    mapper = inspect(model)

    for rel in mapper.relationships:
        if not rel.uselist or rel.key in exclude_relationships:
            continue

        related_model = rel.mapper.class_

        if is_continuum_version_class(related_model):
            continue

        if is_join_table_model(related_model):
            continue

        # Versuche FK-Spalte im related_model zu finden, die auf dieses Modell zeigt
        fk_column = None
        fk_count = 0
        for col in related_model.__table__.columns:
            for fk in col.foreign_keys:
                if fk.column.table == model.__table__:
                    fk_column = col.name
                    fk_count += 1

        if fk_count != 1:
            continue

        # Baue Felder für Subform
        fields = []
        for col in related_model.__table__.columns:
            if col.name == "id" or col.name == fk_column or col.name in exclude_fields:
                continue

            field_type = "number" if isinstance(col.type, Integer) else "text"
            fields.append({
                "name": col.name,
                "type": field_type,
                "label": labelize(col.name),
            })

        if len(fields):
            subforms.append({
                "name": rel.key,
                "label": labelize(rel.key),
                "table": related_model,
                "foreign_key": fk_column,
                "fields": fields,
            })

    return subforms

def is_continuum_version_class(klass):
    return klass.__name__.endswith("Version")

def create_wizard_from_model(model, *, title=None, fields_override=None, subforms=None):
    #print(f"🔧 Starte Wizard-Erstellung für: {model.__name__}")
    mapper = class_mapper(model)
    fields = []

    # Wir sammeln FK-Spalten von Beziehungen, damit wir die nicht doppelt als normale Felder behandeln
    fk_columns_of_relationships = set()
    for rel in mapper.relationships:
        if is_continuum_version_class(rel.mapper.class_):
            #print(f"⚠️  Ignoriere Continuum-Beziehung: {rel.key}")
            continue
        fk_cols = [fk.name for fk in rel._calculated_foreign_keys]
        fk_columns_of_relationships.update(fk_cols)

    # Felder aus Columns extrahieren
    for prop in mapper.iterate_properties:
        if isinstance(prop, ColumnProperty):
            col = prop.columns[0]

            if col.primary_key:
                #print(f"⚠️  Ignoriere Primärschlüssel: {col.name}")
                continue
            if col.name in HIDDEN_FIELD_NAMES:
                #print(f"⚠️  Ignoriere verstecktes Feld: {col.name}")
                continue

            # Falls Field überschrieben werden soll
            if fields_override and col.name in fields_override:
                #print(f"✳️  Feld überschrieben durch override: {col.name}")
                field = {
                    "name": col.name,
                    "type": get_col_type(col),
                    "label": labelize(col.name),
                }
                field.update(fields_override[col.name])
                fields.append(field)
                continue

            # Wenn das Feld bereits durch FK abgedeckt ist
            #if col.name in fk_columns_of_relationships:
            #    print(f"⚠️  Ignoriere FK-Feld (wird separat als Relation behandelt): {col.name}")
            #    continue

            # Normales Feld
            field = {
                "name": col.name,
                "type": get_col_type(col),
                "label": labelize(col.name),
            }
            if not col.nullable:
                field["required"] = True
            #print(f"✅ Normales Feld: {col.name}")
            fields.append(field)

    for rel in mapper.relationships:
        if is_continuum_version_class(rel.mapper.class_):
            #print(f"⚠️  Ignoriere Continuum-Beziehung: {rel.key}")
            continue
        fk_cols = [fk.name for fk in rel._calculated_foreign_keys]
        if len(fk_cols) != 1:
            continue
        fk_col_name = fk_cols[0]

        # Wenn die FK-Spalte als Column im Modell existiert → Beziehung redundant → überspringen
        if any(col.name == fk_col_name for col in mapper.columns):
            #print(f"⚠️  Ignoriere Beziehung {rel.key}, da Column {fk_col_name} schon vorhanden ist")
            continue

        if fk_col_name in HIDDEN_FIELD_NAMES:
            continue

        # One-to-many = Subform → ignorieren hier
        if rel.uselist:
            #print(f"⚠️  Ignoriere One-to-Many-Beziehung (kommt als Subform): {rel.key}")
            continue

        field = {
            "name": fk_col_name,
            "type": "relation",
            "label": labelize(rel.key),
            "relation": rel.mapper.class_,
        }

        if fields_override and fk_col_name in fields_override:
            field.update(fields_override[fk_col_name])

        #print(f"✅ Beziehung als Feld: {rel.key} → FK: {fk_col_name}")
        fields.append(field)

    wizard = {
        "title": title or f"{model.__name__} erstellen",
        "table": model,
        "fields": fields,
    }

    # Subforms erzeugen (z.B. Kinder wie Räume zur Person)
    blacklist_fields = HIDDEN_FIELD_NAMES.union(set(f["name"] for f in fields))
    #print(f"\n📦 Subform-Erstellung mit Ausschluss folgender Felder: {blacklist_fields}")
    wizard["subforms"] = get_subforms(model, exclude_fields=blacklist_fields)

    #print(f"🎉 Wizard für {model.__name__} enthält {len(fields)} Felder und {len(wizard['subforms'])} Subforms\n")
    return wizard

WIZARDS = {
    "Abteilung": create_wizard_from_model(
        Abteilung,
        title="Abteilung erstellen",
    ),
    "Transponder": create_wizard_from_model(
        Transponder,
        title="Transponder erstellen",
    ),
    "Professur": create_wizard_from_model(
        Professur,
        title="Professur erstellen",
    ),
    "Kostenstelle": create_wizard_from_model(
        Kostenstelle,
        title="Kostenstelle erstellen",
    ),
    "Inventar": create_wizard_from_model(
        Inventar,
        title="Inventar erstellen",
    ),
    "Person und Abteilung": create_wizard_from_model(
        PersonToAbteilung,
        title="Person zu Abteilung zuordnen",
    ),
    "Objekt": create_wizard_from_model(
        Object,
        title="Objekt erstellen",
    ),
    "Ausleihe": create_wizard_from_model(
        Loan,
        title="Leihgabe erstellen"
    ),
    "Objektkategorie": create_wizard_from_model(
        ObjectKategorie,
        title="Objektkategorie erstellen",
    ),
    "Lager": create_wizard_from_model(
        Lager,
        title="Lager erstellen",
    ),
    "Objekt zu Lager": create_wizard_from_model(
        ObjectToLager,
        title="Objekt zu Lager zuordnen",
    ),
    "Raumlayout": create_wizard_from_model(
        RaumLayout,
        title="Raumlayout definieren",
    ),
    "Person zu Raum": create_wizard_from_model(
        PersonToRaum,
        title="Person zu Raum zuordnen",
    ),
    "Professur zu Person": create_wizard_from_model(
        ProfessurToPerson,
        title="Professur zu Person zuordnen",
    ),
    "Transponder zu Raum": create_wizard_from_model(
        TransponderToRaum,
        title="Transponder zu Raum zuordnen",
    ),
    "Inventar komplett erfassen": create_wizard_from_model(
        Inventar,
        title="Inventar (mit Zuordnungen) erfassen",
    )
}

EMAIL_REGEX = re.compile(r"^[^@]+@[^@]+\.[^@]+$")

def query_with_version(session, model_class):
    # Wenn kein Zeitstempel gesetzt -> aktueller Stand
    if not hasattr(g, 'issued_at') or g.issued_at is None:
        return session.query(model_class).all()

    ModelVersion = version_class(model_class)

    # Versionen abfragen mit filter auf den Zeitstempel aus Cookie (g.issued_at)
    versions = session.query(ModelVersion).\
        join(ModelVersion.transaction).\
        filter(ModelVersion.transaction.issued_at <= g.issued_at).\
        order_by(ModelVersion.transaction.issued_at.desc()).all()

    latest_versions = {}
    for version in versions:
        if version.id not in latest_versions:
            latest_versions[version.id] = version

    return list(latest_versions.values())

@app.before_request
def load_timestamp_from_cookie():
    session = Session()
    version_id_str = request.cookies.get('data_version')
    if version_id_str:
        try:
            version_id = int(version_id_str)
            # Hole timestamp zu version_id aus der DB
            version = session.query(TransactionTable).filter_by(id=version_id).one_or_none()
            if version and version.issued_at:
                g.issued_at = version.issued_at
            else:
                g.issued_at = None
        except Exception as e:
            print(f"[DEBUG] Fehler beim Laden der Version aus Cookie: {e}")
            g.issued_at = None
    else:
        g.issued_at = None

    session.close()

def _data_version_block_check():
    cookie_val = request.cookies.get("data_version")
    if cookie_val is not None and cookie_val.strip() != "":
        raise RuntimeError("Schreiboperationen sind deaktiviert, weil eine ältere Version geladen ist.")

def block_writes_if_data_version_cookie_set(session, flush_context, instances):
    # Alle neuen, geänderten oder gelöschten Objekte
    write_ops = session.new.union(session.dirty).union(session.deleted)
    if not write_ops:
        return  # keine schreibenden Operationen -> alles OK

    # Nur ausführen, wenn Flask-Request-Kontext aktiv ist
    try:
        if request:
            _data_version_block_check()
    except RuntimeError:
        raise
    except Exception:
        # Wenn kein Flask-Kontext (z. B. Migrations), dann ignorieren
        pass

def add_version_filter(query):
    if not has_app_context():
        print("[DEBUG] Kein Flask-App-Kontext aktiv – Versionierung wird übersprungen")
        return query

    if not hasattr(g, 'issued_at') or g.issued_at is None:
        return query

    print(f"[DEBUG] g.issued_at gesetzt auf: {g.issued_at}")

    if not query.column_descriptions:
        print("[DEBUG] Query hat keine column_descriptions – vermutlich leer oder Subquery")
        return query

    model_class = query.column_descriptions[0].get('entity', None)
    if model_class is None:
        print("[DEBUG] Konnte Modellklasse nicht aus column_descriptions extrahieren")
        return query

    if model_class.__name__.endswith('Version'):
        print("[DEBUG] Modell ist bereits eine Version-Klasse – Query bleibt unverändert")
        return query

    try:
        ModelVersion = versioning_manager.version_class_map.get(model_class)
        if ModelVersion is None:
            return query
        print(f"[DEBUG] Zugehörige Version-Klasse: {ModelVersion.__name__}")
    except Exception as e:
        print(f"[DEBUG] Fehler beim Abrufen der Version-Klasse: {e}")
        return query

    TransactionClass = getattr(versioning_manager, 'transaction_cls', None)
    if TransactionClass is None:
        print("[DEBUG] Transaction-Klasse konnte nicht gefunden werden")
        return query
    print(f"[DEBUG] Transaction-Klasse: {TransactionClass}")

    # Prüfe, ob Version-Klasse eine Beziehung zu transaction hat
    if not hasattr(ModelVersion, 'transaction'):
        print("[DEBUG] Version-Klasse hat keine 'transaction'-Beziehung")
        return query

    # Prüfe, ob Transaction-Klasse das Attribut issued_at besitzt
    if not hasattr(TransactionClass, 'issued_at'):
        print("[DEBUG] Transaction-Klasse hat kein Attribut 'issued_at'")
        return query

    try:
        print("[DEBUG] Baue neue Query mit Zeitfilter")

        TransactionAlias = aliased(TransactionClass)

        filtered_query = (
            query.session.query(ModelVersion)
            .join(TransactionAlias, ModelVersion.transaction)
            .filter(TransactionAlias.issued_at <= g.issued_at)
            .order_by(TransactionAlias.issued_at.desc())
        )

        print("[DEBUG] Neue Query erfolgreich erstellt")
        return filtered_query

    except Exception as e:
        print(f"[DEBUG] Fehler beim Erstellen der gefilterten Query: {e}")
        return query

@listens_for(Query, "before_compile", retval=True)
def before_compile_handler(query):
    try:
        new_query = add_version_filter(query)
        return new_query
    except Exception as e:
        return query

def initialize_db_data():
    session = Session()
    try:
        kostenstelle_count = session.query(Kostenstelle).count()
        if kostenstelle_count == 0:
            print("Keine Kostenstellen gefunden, füge neue hinzu...")
            for ks in INITIAL_DATA["kostenstellen"]:
                print(f"  - Füge Kostenstelle hinzu: {ks['name']}")
                obj = Kostenstelle(name=ks["name"])
                session.add(obj)
            session.commit()
            print("Kostenstellen wurden erfolgreich initialisiert.")

        # Professur prüfen und ggf. einfügen
        professur_count = session.query(Professur).count()
        if professur_count == 0:
            print("Keine Professuren gefunden, füge neue hinzu...")
            for prof in INITIAL_DATA["professuren"]:
                print(f"  - Verarbeite Professur: {prof['name']} mit Kostenstelle '{prof['kostenstelle_name']}'")
                kostenstelle_obj = session.query(Kostenstelle).filter_by(name=prof["kostenstelle_name"]).first()
                if kostenstelle_obj is None:
                    session.close()
                    raise ValueError(f"Kostenstelle '{prof['kostenstelle_name']}' nicht gefunden für Professur '{prof['name']}'")
                obj = Professur(name=prof["name"], kostenstelle_id=kostenstelle_obj.id)
                session.add(obj)
            session.commit()
            print("Professuren wurden erfolgreich initialisiert.")

        # ObjectKategorie prüfen und ggf. einfügen
        object_kategorie_count = session.query(ObjectKategorie).count()
        if object_kategorie_count == 0:
            print("Keine ObjectCategories gefunden, füge neue hinzu...")
            for cat in INITIAL_DATA["object_categories"]:
                print(f"  - Füge ObjectKategorie hinzu: {cat['name']}")
                obj = ObjectKategorie(name=cat["name"])
                session.add(obj)
            session.commit()
            print("ObjectCategories wurden erfolgreich initialisiert.")

        # Abteilung prüfen und ggf. einfügen
        abteilung_count = session.query(Abteilung).count()
        if abteilung_count == 0:
            print("Keine Abteilungen gefunden, füge neue hinzu...")
            for abt in INITIAL_DATA["abteilungen"]:
                print(f"  - Füge Abteilung hinzu: {abt['name']}")
                obj = Abteilung(name=abt["name"])
                session.add(obj)
            session.commit()
            print("Abteilungen wurden erfolgreich initialisiert.")

    except SQLAlchemyError as e:
        session.rollback()
        print(f"SQLAlchemy-Fehler beim Initialisieren der DB-Daten: {str(e)}")
    except Exception as e:
        session.rollback()
        print(f"Allgemeiner Fehler beim Initialisieren der DB-Daten: {str(e)}")

    session.close()

def parse_buildings_csv(csv_text):
    session = Session()

    if not isinstance(csv_text, str):
        session.close()
        raise TypeError("csv_text muss ein String sein")
    if not csv_text.strip():
        session.close()
        raise ValueError("csv_text ist leer")

    csv_io = io.StringIO(csv_text)
    reader = csv.reader(csv_io, delimiter=',', quotechar='"')

    header_found = False

    for row in reader:
        if len(row) != 2:
            continue

        if not header_found:
            # Prüfen, ob erste Zeile die Header ist
            if row[0].strip().lower() == "gebaeude_name" and row[1].strip().lower() == "abkürzung":
                header_found = True
                continue  # Header überspringen
            else:
                raise ValueError("Ungültige Header-Zeile: " + str(row))

        gebaeude_name = row[0].strip()
        abkürzung = row[1].strip()

        if not gebaeude_name or not abkürzung:
            continue  # Zeile überspringen, wenn leer

        building_insert = {
            "name": gebaeude_name,
            "abkürzung": abkürzung
        }

        handler = BuildingHandler(session)
        handler.insert_data(building_insert)

    session.close()

def insert_tu_dresden_buildings ():
    csv_input = '''gebaeude_name,abkürzung
"Abstellgeb."," Pienner Str. 38a"
"Andreas-Pfitzmann-Bau","APB"
"Andreas-Schubert-Bau","ASB"
"August-Bebel-Straße","ABS"
"Bamberger Str. 1","B01"
"Barkhausen-Bau","BAR"
"Beamtenhaus"," Pienner Str. 21"
"Bergstr. 69","B69"
"Berndt-Bau","BER"
"Beyer-Bau","BEY"
"Binder-Bau","BIN"
"Bioinnovationszentrum","BIZ"
"Biologie","BIO"
"Boselgarten Coswig","BOS"
"Botanischer Garten","BOT"
"Breitscheidstr. 78-82"," OT Dobritz"
"Bürogebäude Strehlener Str. 22"," 24"
"Bürogebäude Zellescher Weg 17","BZW"
"Chemie","CHE"
"Cotta-Bau","COT"
"Drude-Bau","DRU"
"Dürerstr. 24","DÜR"
"Fahrzeugversuchszentrum","FVZ"
"Falkenbrunnen","FAL"
"Forstbotanischer Garten","FBG"
"Forsttechnik"," Dresdner Str. 24"
"Fraunhofer IWS","FIWS"
"Freital"," Tharandter Str. 7"
"Frenzel-Bau","FRE"
"Fritz-Foerster-Bau","FOE"
"Fritz-Löffler-Str. 10a","L10"
"Georg-Schumann-Bau","SCH"
"Georg-Schumannstr. 7a","S7A"
"Graduiertenakademie","M07"
"GrillCube","GCUB"
"Görges-Bau","GÖR"
"Günther-Landgraf-Bau","GLB"
"Halle Nickern","NIC"
"Hallwachsstr. 3","HAL"
"Hauptgebäude"," Pienner Str. 8"
"Haus 2","U0002"
"Haus 4","U0004"
"Haus 5","U0105"
"Haus 7","U0007"
"Haus 9","U0009"
"Haus 11","U0011"
"Haus 13","U0013"
"Haus 15","U0015"
"Haus 17","U0017"
"Haus 19","U0019"
"Haus 21a","U0021A"
"Haus 22","U0022"
"Haus 25","U0025"
"Haus 27","U0027"
"Haus 29","U0029"
"Haus 31","U0031"
"Haus 33","U0033"
"Haus 38","U0038"
"Haus 41","U0041"
"Haus 44","U0044"
"Haus 47","U0047"
"Haus 50","U0050"
"Haus 53","U0053"
"Haus 58","U0058"
"Haus 60","U0060"
"Haus 62","U0062"
"Haus 66","U0066"
"Haus 69","U0069"
"Haus 71","U0071"
"Haus 81","U0081"
"Haus 83","U0083"
"Haus 90","U0090"
"Haus 97","U0097"
"Haus 111","U0111"
"Heidebroek-Bau","HEI"
"Heinrich-Schütz-Str. 2","AV1"
"Helmholtz-Zentrum Dresden-Rossendorf","FZR"
"Hermann-Krone-Bau","KRO"
"Hohe Str. 53","H53"
"Hörsaalzentrum","HSZ"
"Hülsse-Bau","HÜL"
"Jante-Bau","JAN"
"Judeich-Bau","JUD"
"Kutzbach-Bau","KUT"
"König-Bau","KÖN"
"Leichtbau-Innovationszentrum","LIZ"
"Ludwig-Ermold-Str. 3","E03"
"Marschnerstr. 30"," 32"
"Max-Bergmann-Zentrum","MBZ"
"Mensa","M13"
"Merkel-Bau","MER"
"Mierdel-Bau","MIE"
"Mohr-Bau","MOH"
"Mollier-Bau","MOL"
"Mommsenstr. 5","M05"
"Müller-Bau","MÜL"
"Neuffer-Bau","NEU"
"Nöthnitzer Str. 60a","N60"
"Nöthnitzer Str. 73","N73"
"Nürnberger Ei","NÜR"
"Potthoff-Bau","POT"
"Prozess-Entwicklungszentrum","PEZ"
"Recknagel-Bau","REC"
"Rektorat"," Mommsenstr. 11"
"Rossmässler-Bau","ROS"
"Sachsenberg-Bau","SAC"
"Scharfenberger Str. 152"," OT Kaditz"
"Schweizer Str. 3","SWS"
"Seminargebäude 1","SE1"
"Seminargebäude 2","SE2"
"Semperstr. 14","SEM"
"Stadtgutstr. 10 Fahrbereitschaft","STA"
"Stöckhardt-Bau","STÖ"
"Technische Leitzentrale","TLZ"
"Textilmaschinenhalle","TEX"
"Tillich-Bau","TIL"
"Toepler-Bau","TOE"
"Trefftz-Bau","TRE"
"TUD-Information"," Mommsenstr. 9"
"Verwaltungsgebäude 2 - STURA","VG2"
"Verwaltungsgebäude 3","VG3"
"von-Gerber-Bau","GER"
"von-Mises-Bau","VMB"
"VVT-Halle","VVT"
"Walther-Hempel-Bau","HEM"
"Walther-Pauer-Bau","PAU"
"Weberplatz","WEB"
"Weißbachstr. 7","W07"
"Werner-Hartmann-Bau","WHB"
"Wiener Str. 48","W48"
"Willers-Bau","WIL"
"Windkanal Marschnerstraße 28","WIK"
"Wohnheim"," Pienner Str. 9"
"Würzburger Str. 46","WÜR"
"Zellescher Weg 21","Z21"
"Zellescher Weg 41c","Z41"
"Zeltschlösschen","NMEN"
"Zeuner-Bau","ZEU"
"Zeunerstr. 1a","ZS1"
"Übergabestation Nöthnitzer Str. 62a","NOE"
"ÜS+Trafo Bergstr.","BRG"
"Bürogebäude Strehlener Str. 14","STR"
'''

    parse_buildings_csv(csv_input)

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

@app.route('/dashboard')
@login_required
def dashboard():
    return f'Hello, {current_user.username}!'

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

def is_valid_email(email):
    return bool(EMAIL_REGEX.match(email.strip()))

def column_label(table, col):
    return COLUMN_LABELS.get(f"{table}.{col}", col.replace("_id", "").replace("_", " ").capitalize())

@app.context_processor
def inject_sidebar_data():
    session = Session()

    tables = [
        cls.__tablename__
        for cls in Base.__subclasses__()
        if hasattr(cls, '__tablename__') and cls.__tablename__ not in IGNORED_TABLES
    ]

    wizard_routes = [f"/wizard/{key}" for key in WIZARDS.keys()]
    wizard_routes.append("/wizard/person")
    wizard_routes = sorted(set(wizard_routes))

    is_authenticated = current_user.is_authenticated
    is_admin = False

    if is_authenticated:
        try:
            # User nochmal frisch aus DB laden mit Rollen eager
            user = session.query(User).options(
                joinedload(User.roles)
            ).filter(User.id == current_user.id).one_or_none()

            if user is not None:
                is_admin = any(role.name == 'admin' for role in user.roles)
            else:
                print(f"User mit ID {current_user.id} nicht in DB gefunden")
        except DetachedInstanceError:
            print("DetachedInstanceError: current_user is not bound to session")
        except Exception as e:
            print(f"Unbekannter Fehler beim Laden des Users: {e}")

    replace_conf = get_replace_config_dict()

    replace_configs = json.dumps(replace_conf, ensure_ascii=False)

    theme_cookie = request.cookies.get('theme')
    theme = theme_cookie if theme_cookie in ['dark', 'light'] else 'light'

    session.close()

    return dict(
        tables=tables,
        wizard_routes=wizard_routes,
        is_authenticated=is_authenticated,
        is_admin=is_admin,
        replace_configs=Markup(replace_configs),
        theme=theme
    )

@app.route("/")
@login_required
def index():
    insert_tu_dresden_buildings()
    initialize_db_data()

    tables = [
        cls.__tablename__
        for cls in Base.__subclasses__()
        if hasattr(cls, '__tablename__') and cls.__tablename__ not in ["role", "user"]
    ]

    # wizard_routes aus den keys von WIZARDS + eventuell "person"
    wizard_routes = [f"/wizard/{key}" for key in WIZARDS.keys()]
    wizard_routes.append("/wizard/person")
    wizard_routes = sorted(set(wizard_routes))

    return render_template("index.html", tables=tables, wizard_routes=wizard_routes, user=current_user)

@app.route('/admin', methods=['GET', 'POST'])
@login_required
@admin_required
def admin_panel():
    session = Session()

    if request.method == 'POST' and 'new_username' in request.form:
        username = request.form['new_username']
        password = request.form['new_password']
        role_id = request.form.get('new_role')

        if session.query(User).filter_by(username=username).first():
            flash('Benutzername existiert bereits.')
        else:
            hashed = generate_password_hash(password)
            user = User(username=username, password=hashed, is_active=False)  # NEU: standardmäßig inaktiv
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

    session.close()
    return render_template('admin_panel.html', users=users, roles=roles)

@app.route('/admin/delete/<int:user_id>')
@login_required
@admin_required
def delete_user(user_id):
    session = Session()
    user = session.query(User).get(user_id)

    if not user:
        flash("Benutzer nicht gefunden.")
    else:
        session.delete(user)
        session.commit()
        flash("Benutzer gelöscht.")

    session.close()

    return redirect(url_for('admin_panel'))

@app.route('/admin/update/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def update_user(user_id):
    session = Session()

    user = session.query(User).get(user_id)
    if not user:
        flash("Benutzer nicht gefunden.")
        session.close()
        return redirect(url_for('admin_panel'))

    # Aktivieren (falls angefragt und noch nicht aktiv)
    if 'activate_user' in request.form and not user.is_active:
        user.is_active = True
        flash(f"Benutzer {user.username} wurde aktiviert.")

    # Benutzername ändern
    new_username = request.form.get('username')
    if new_username and new_username != user.username:
        if session.query(User).filter(User.username == new_username, User.id != user.id).first():
            flash("Benutzername existiert bereits.")
            session.close()
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
    session.close()
    return redirect(url_for('admin_panel'))

@app.route('/admin/activate/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def activate_user(user_id):
    session = Session()
    user = session.query(User).get(user_id)

    if not user:
        session.close()
        return jsonify(success=False, error="Benutzer nicht gefunden"), 404

    if user.is_active:
        session.close()
        return jsonify(success=False, error="Benutzer ist bereits aktiviert"), 400

    user.is_active = True
    session.commit()
    session.close()

    return jsonify(success=True)

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(app.static_folder, 'favicon.ico')

def get_model_class_by_tablename(table_name):
    try:
        return next((c for c in Base.__subclasses__() if c.__tablename__ == table_name), None)
    except Exception as e:
        app.logger.error(f"Fehler beim Abrufen der Modelklasse für Tabelle {table_name}: {e}")
        return None

def get_relevant_columns(cls):
    try:
        inspector = inspect(cls)
        return [c for c in inspector.columns if not c.primary_key and c.name not in ("created_at", "updated_at")]
    except Exception as e:
        app.logger.error(f"Fehler beim Inspektieren der Spalten für Klasse {cls}: {e}")
        return []

def get_foreign_key_columns(columns):
    try:
        return {c.name: list(c.foreign_keys)[0] for c in columns if c.foreign_keys}
    except Exception as e:
        app.logger.error(f"Fehler beim Extrahieren der Fremdschlüssel aus Spalten: {e}")
        return {}

def get_fk_options(session, fk_columns):
    fk_options = {}
    try:
        for col_name, fk in fk_columns.items():
            ref_table = fk.column.table.name
            ref_cls = get_model_class_by_tablename(ref_table)
            if ref_cls:
                display_cols = FK_DISPLAY_COLUMNS.get(ref_table, "name")
                records = session.query(ref_cls).all()
                options = []
                for r in records:
                    # key: Wert des FK (z.B. id)
                    key = getattr(r, fk.column.name, None)
                    # label: zusammengesetzter Name
                    if isinstance(display_cols, list):
                        # Alle Spaltenwerte auslesen und verbinden
                        parts = []
                        for col in display_cols:
                            val = getattr(r, col, None)
                            if val is not None:
                                parts.append(str(val))
                        label_text = " ".join(parts) if parts else "???"
                    else:
                        # Nur ein einzelner Spaltenname als String
                        label_text = getattr(r, display_cols, "???")
                    label = f"{label_text} ({key})"
                    options.append((key, label))
                fk_options[col_name] = options
    except Exception as e:
        app.logger.error(f"Fehler beim Abrufen der FK-Optionen: {e}")
    return fk_options

def generate_input_field(col, value=None, row_id=None, fk_options=None, table_name=""):
    col_after_dot = col.name

    try:
        input_name = f"{table_name}_{row_id or 'new'}_{col.name}"
        val = "" if value is None else html.escape(str(value))

        if fk_options and col.name in fk_options:
            options_list = fk_options[col.name]
            if not options_list:
                return "", False
            options_html = ""
            for opt_value, opt_label in options_list:
                selected = "selected" if str(opt_value) == val else ""
                options_html += f'<option value="{html.escape(str(opt_value))}" {selected}>{html.escape(opt_label)}</option>'
            return f'<select name="{html.escape(input_name)}" class="cell-input">{options_html}</select>', True

        col_type_str = str(col.type).upper()

        if "INTEGER" in col_type_str:
            return f'<input placeholder="{col_after_dot}" type="number" name="{html.escape(input_name)}" value="{val}" class="cell-input">', True
        if "FLOAT" in col_type_str or "DECIMAL" in col_type_str or "NUMERIC" in col_type_str:
            return f'<input placeholder="{col_after_dot}" type="number" step="any" name="{html.escape(input_name)}" value="{val}" class="cell-input">', True
        if "TEXT" in col_type_str or "VARCHAR" in col_type_str or "CHAR" in col_type_str:
            return f'<input placeholder="{col_after_dot}" type="text" name="{html.escape(input_name)}" value="{val}" class="cell-input">', True
        if "DATE" in col_type_str:
            return f'<input placeholder="{col_after_dot}" type="date" name="{html.escape(input_name)}" value="{val}" class="cell-input">', True

        return f'<input placeholder="{col_after_dot}" type="text" name="{html.escape(input_name)}" value="{val}" class="cell-input">', True
    except Exception as e:
        app.logger.error(f"Fehler beim Generieren des Input-Feldes für Spalte {col.name}: {e}")
        return f'<input placeholder="{col_after_dot}" type="text" name="{html.escape(input_name)}" value="" class="cell-input">', True

def get_column_label(table_name, column_name):
    # Hier deine Logik für die Label-Erzeugung
    # Einfacher Platzhalter:
    try:
        return column_label(table_name, column_name)
    except Exception as e:
        app.logger.error(f"Fehler beim Abrufen des Labels für {table_name}.{column_name}: {e}")
        return column_name

def prepare_table_data(session, cls, table_name):
    columns = get_relevant_columns(cls)
    fk_columns = get_foreign_key_columns(columns)
    fk_options = get_fk_options(session, fk_columns)

    try:
        rows = session.query(cls).all()
    except Exception as e:
        app.logger.error(f"Fehler bei der Abfrage der Tabelle {table_name}: {e}")
        rows = []

    row_html = []
    row_ids = []
    table_has_missing_inputs = False
    missing_input_info = {}  # NEU

    for row in rows:
        row_inputs = []
        try:
            row_id = getattr(row, "id", None)
            if row_id is None:
                first_col_name = columns[0].name if columns else None
                row_id = getattr(row, first_col_name, None) if first_col_name else None
        except Exception as e:
            app.logger.error(f"Fehler beim Zugriff auf ID der Zeile: {e}")
            row_id = None

        row_ids.append(row_id)

        for col in columns:
            col_name = col.name
            if col_name == "return":
                col_name = "return_"

            try:
                value = getattr(row, col_name)
            except Exception as e:
                app.logger.error(f"Fehler beim Zugriff auf Spalte {col_name}: {e}")
                value = None

            label = get_column_label(table_name, col.name)
            try:
                input_html, valid = generate_input_field(
                    col,
                    value,
                    row_id=row_id,
                    fk_options=fk_options,
                    table_name=table_name
                )
                if not valid:
                    table_has_missing_inputs = True
                    missing_input_info.setdefault(col.name, []).append(row_id)
            except Exception as e:
                app.logger.error(f"Fehler bei der Generierung des Input-Felds für {col.name}: {e}")
                input_html = '<input value="Error">'
                table_has_missing_inputs = True
                missing_input_info.setdefault(col.name, []).append(row_id)

            row_inputs.append((input_html, label))
        row_html.append(row_inputs)

    new_entry_inputs = []
    for col in columns:
        try:
            input_html, valid = generate_input_field(
                col,
                fk_options=fk_options,
                table_name=table_name
            )
            if not valid:
                table_has_missing_inputs = True
                missing_input_info.setdefault(col.name, []).append("new_entry")
        except Exception as e:
            app.logger.error(f"Fehler bei der Generierung des neuen Input-Felds für {col.name}: {e}")
            input_html = '<input value="Error">'
            table_has_missing_inputs = True
            missing_input_info.setdefault(col.name, []).append("new_entry")

        label = get_column_label(table_name, col.name)
        new_entry_inputs.append((input_html, label))

    column_labels = [get_column_label(table_name, col.name) for col in columns]

    return column_labels, row_html, new_entry_inputs, row_ids, table_has_missing_inputs, missing_input_info

def load_static_file(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        app.logger.error(f"Fehler beim Laden der Datei {path}: {e}")
        return ""

@app.route("/table/<table_name>")
@login_required
def table_view(table_name):
    if table_name in IGNORED_TABLES:
        abort(404, description="Tabelle darf nicht angezeigt werden")

    session = Session()
    cls = get_model_class_by_tablename(table_name)
    if cls is None:
        session.close()
        abort(404, description="Tabelle nicht gefunden")

    # Erweiterte Rückgabe mit missing_input_info
    column_labels, row_html, new_entry_inputs, row_ids, table_has_missing_inputs, missing_input_info = prepare_table_data(session, cls, table_name)

    javascript_code = load_static_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "table_scripts.js")).replace("{{ table_name }}", table_name)

    row_data = list(zip(row_html, row_ids))

    missing_data_messages = []
    if table_has_missing_inputs and missing_input_info:
        missing_data_messages.append('<div class="warning">⚠️ Fehlende Eingaben:</div><ul>')
        for col_name, problem_ids in missing_input_info.items():
            pretty_ids = ", ".join(str(i) for i in problem_ids)
            missing_data_messages.append(f"<li><strong>{col_name}</strong>: fehlend in {pretty_ids}</li>")
        missing_data_messages.append("</ul>")

    session.close()

    return render_template(
        "table_view.html",
        table_name=table_name,
        column_labels=column_labels,
        row_data=row_data,
        new_entry_inputs=new_entry_inputs,
        javascript_code=javascript_code,
        missing_data_messages=missing_data_messages
    )

@app.route("/add/<table_name>", methods=["POST"])
@login_required
def add_entry(table_name):
    session = Session()
    cls = next((c for c in Base.__subclasses__() if c.__tablename__ == table_name), None)
    if not cls:
        session.close()
        return jsonify(success=False, error="Tabelle nicht gefunden")
    try:
        obj = cls()
        for key, val in request.form.items():
            _, _, field = key.partition(f"{table_name}_new_")
            if not hasattr(obj, field):
                continue
            col_type = getattr(cls, field).property.columns[0].type

            if val == "":
                setattr(obj, field, None)
            elif isinstance(col_type, Date):
                setattr(obj, field, datetime.datetime.strptime(val, "%Y-%m-%d").date())
            elif isinstance(col_type, DateTime):
                setattr(obj, field, datetime.datetime.fromisoformat(val))
            else:
                setattr(obj, field, val)

        session.add(obj)
        session.commit()
        session.close()
        return jsonify(success=True)
    except IntegrityError as e:
        session.rollback()
        session.close()
        # Du kannst die Fehlermeldung hier anpassen, z.B. auf Deutsch:
        return jsonify(success=False, error="Ein Eintrag mit diesen Werten existiert bereits oder eine Einschränkung wurde verletzt.")
    except Exception as e:
        session.rollback()
        session.close()
        return jsonify(success=False, error=str(e))

@app.route("/update/<table_name>", methods=["POST"])
@login_required
def update_entry(table_name):
    session = Session()
    cls = next((c for c in Base.__subclasses__() if c.__tablename__ == table_name), None)
    if not cls:
        session.close()
        return jsonify(success=False, error="Tabelle nicht gefunden")
    try:
        name = request.form.get("name")
        value = request.form.get("value")
        prefix = f"{table_name}_"
        if not name.startswith(prefix):
            return jsonify(success=False, error="Ungültiger Feldname")
        parts = name[len(prefix):].split("_", 1)
        if len(parts) != 2:
            return jsonify(success=False, error="Ungültiger Feldname")
        row_id_str, field = parts
        row_id = int(row_id_str)
        obj = session.query(cls).get(row_id)
        if not obj:
            return jsonify(success=False, error="Datensatz nicht gefunden")

        col_type = getattr(cls, field).property.columns[0].type
        if value == "":
            setattr(obj, field, None)
        elif isinstance(col_type, Date):
            setattr(obj, field, datetime.datetime.strptime(value, "%Y-%m-%d").date())
        elif isinstance(col_type, DateTime):
            setattr(obj, field, datetime.datetime.fromisoformat(value))
        else:
            setattr(obj, field, value)
        session.commit()
        session.close()
        return jsonify(success=True)
    except Exception as e:
        session.rollback()
        session.close()
        return jsonify(success=False, error=str(e))

@app.route("/delete/<table_name>", methods=["POST"])
@login_required
def delete_entry(table_name):
    session = Session()
    cls = next((c for c in Base.__subclasses__() if c.__tablename__ == table_name), None)
    if not cls:
        session.close()
        return jsonify(success=False, error="Tabelle nicht gefunden")

    try:
        json_data = request.get_json()

        if not json_data or "id" not in json_data:
            session.close()
            return jsonify(success=False, error="Keine ID angegeben")

        row_id = json_data["id"]
        if not isinstance(row_id, int):
            try:
                row_id = int(row_id)
            except ValueError:
                session.close()
                return jsonify(success=False, error="Ungültige ID")

        obj = session.query(cls).get(row_id)
        if not obj:
            session.close()
            return jsonify(success=False, error="Datensatz nicht gefunden")

        session.delete(obj)
        session.commit()
        session.close()
        return jsonify(success=True)

    except Exception as e:
        session.rollback()
        session.close()
        return jsonify(success=False, error=str(e))

def generate_aggregate_views(base, model_config={}):
    views = {}
    mapper_registry = base.registry if hasattr(base, 'registry') else registry()

    for mapper in mapper_registry.mappers:
        cls = mapper.class_
        model_name = cls.__name__.lower()
        config = model_config.get(model_name, {})
        insp = inspect(cls)

        # Title
        title = config.get("title") or (
            f"{inflect_engine.plural(cls.__name__)}übersicht"
            if cls.__name__.endswith("r") else f"{cls.__name__}übersicht"
        )

        # joinedload options
        options = config.get("options")
        if options is None:
            options = [joinedload(getattr(cls, rel.key)) for rel in insp.relationships]

        # Filters
        filters = config.get("filters", {})

        # Filter Function
        filter_func = config.get("filter_func", lambda q, f: q)

        # Map Function
        def make_map_func(model):
            def map_func(obj):
                result = {}
                for col in inspect(model).columns:
                    val = getattr(obj, col.key)
                    if val is None:
                        result[col.key] = "-"
                    elif hasattr(val, "isoformat"):
                        result[col.key] = val.isoformat()
                    elif isinstance(val, float):
                        result[col.key] = f"{val:.2f}"
                    else:
                        result[col.key] = str(val)
                return result
            return map_func

        map_func = config.get("map_func", make_map_func(cls))

        # Add Columns
        add_columns = config.get("add_columns", [])

        # Extra Context
        extra_context_func = config.get("extra_context_func")

        # Entry zusammenbauen
        entry = {
            "model": cls,
            "title": title,
            "options": options,
            "filters": filters,
            "filter_func": filter_func,
            "map_func": map_func
        }

        if add_columns:
            entry["add_columns"] = add_columns

        if extra_context_func:
            entry["extra_context_func"] = extra_context_func

        views[model_name] = entry

    return views

AGGREGATE_VIEWS = generate_aggregate_views(Base, {
    "inventar": {
        "title": "Inventarübersicht",
        "filters": {
            "show_only_unreturned": (bool, "unreturned"),
            "besitzer_filter": (int, "besitzer"),
            "ausgeber_filter": (int, "ausgeber"),
        },
        "filter_func": lambda q, f: (
            q.filter(Inventar.rückgabedatum.is_(None)) if f.get("show_only_unreturned") else q
        ).filter(
            Inventar.besitzer_id == f["besitzer_filter"]
        ) if f.get("besitzer_filter") else q.filter(
            Inventar.ausgeber_id == f["ausgeber_filter"]
        ) if f.get("ausgeber_filter") else q,
        "map_func": lambda inv: {
            "ID": inv.id,
            "Seriennummer": inv.seriennummer or "-",
            "Objekt": inv.object.name if inv.object else "-",
            "Kategorie": _get_kategorie_name(inv.object.kategorie) if inv.object else "-",
            "Anlagennummer": inv.anlagennummer or "-",
            "Ausgegeben an": _get_person_name(inv.besitzer),
            "Ausgegeben durch": _get_person_name(inv.ausgeber),
            "Ausgabedatum": inv.erhaltungsdatum.isoformat() if inv.erhaltungsdatum else "-",
            "Rückgabedatum": inv.rückgabedatum.isoformat() if inv.rückgabedatum else "Nicht zurückgegeben",
            "Raum": _create_room_name(inv.room),
            "Abteilung": _get_abteilung_name(inv.abteilung),
            "Professur": _get_professur_name(inv.professur),
            "Kostenstelle": _get_kostenstelle_name(inv.kostenstelle),
            "Preis": f"{inv.preis:.2f} €" if inv.preis is not None else "-",
            "Kommentar": inv.kommentar or "-"
        }
    },

    "transponder": {
        "title": "Ausgegebene Transponder",
        "add_columns": ["PDF"],
        "filters": {
            "show_only_unreturned": (bool, "unreturned"),
            "besitzer_id_filter": (int, "besitzer_id"),
            "ausgeber_id_filter": (int, "ausgeber_id")
        },
        "filter_func": lambda q, f: (
            q.filter(Transponder.rückgabedatum.is_(None)) if f.get("show_only_unreturned") else q
        ).filter(
            Transponder.besitzer_id == f["besitzer_id_filter"]
        ) if f.get("besitzer_id_filter") else q.filter(
            Transponder.ausgeber_id == f["ausgeber_id_filter"]
        ) if f.get("ausgeber_id_filter") else q,
        "map_func": lambda t: {
            "ID": t.id,
            "Seriennummer": t.seriennummer or "-",
            "Ausgegeben an": f'<input type="text" name="besitzer_id" data-update_info="transponder_{t.id}" value="{html.escape(str(t.besitzer_id))}" />',
            "Ausgegeben durch": f'<input type="text" name="ausgeber_id" data-update_info="transponder_{t.id}" value="{html.escape(str(t.ausgeber_id))}" />',
            "Ausgabedatum": t.erhaltungsdatum.isoformat() if t.erhaltungsdatum else "-",
            "Rückgabedatum": t.rückgabedatum.isoformat() if t.rückgabedatum else "Nicht zurückgegeben",
            "Gebäude": ", ".join(sorted({r.building.name if r.building else "?" for link in t.room_links if (r := link.room)})) or "-",
            "Räume": ", ".join(sorted({f"{r.name} ({r.etage}.OG)" for link in t.room_links if (r := link.room)})) or "-",
            "Kommentar": t.kommentar or "-"
        },
        "extra_context_func": lambda: {
            "toggle_url": url_for(
                "aggregate_view",
                aggregate_name="transponder",
                unreturned="0" if request.args.get("unreturned") == "1" else "1",
                besitzer_id=request.args.get("besitzer_id", ""),
                ausgeber_id=request.args.get("ausgeber_id", "")
            )
        }
    },

    "person": {
        "title": "Personenübersicht",
        "map_func": lambda p: {
            "ID": p.id,
            "Titel": p.title or "-",
            "Vorname": p.vorname or "-",
            "Nachname": p.nachname or "-",
            "Email": next((c.email for c in p.contacts if c.email), "-"),
            "Telefon": next((c.phone for c in p.contacts if c.phone), "-"),
            "Fax": next((c.fax for c in p.contacts if c.fax), "-"),
            "Kommentar": p.kommentar or "-",
            "Abteilungen": ", ".join([a.abteilung.name for a in p.person_abteilungen]) or "-",
            "Leitet Abteilungen": ", ".join([a.name for a in p.departments]) or "-",
            "Professuren": ", ".join([pp.professur.name for pp in p.professuren]) or "-",
            "Räume": ", ".join(
                [f"{r.room.building.abkürzung or r.room.building.name or '?'}-{r.room.name}"
                 for r in p.räume if r.room and r.room.building]
            ) or "-",
            "Transponder-Besitz": ", ".join(
                [f"{t.seriennummer or '?'}" for t in p.transponders_owned]
            ) or "-",
            "Transponder-Ausgaben": ", ".join(
                [f"{t.seriennummer or '?'}" for t in p.transponders_issued]
            ) or "-",
        }
    },

    "abteilung": {
        "title": "Abteilungsübersicht",
        "filters": {
            "leiter_filter": (int, "abteilungsleiter_id"),
            "name_filter": (str, "name")
        },
        "filter_func": lambda q, f: (
            q.filter(Abteilung.abteilungsleiter_id == f["leiter_filter"]) if f.get("leiter_filter") else q
        ).filter(
            Abteilung.name.ilike(f"%{f['name_filter']}%")
        ) if f.get("name_filter") else q,
        "map_func": lambda a: {
            "ID": a.id,
            "Name": a.name or "-",
            "Abteilungsleiter": f"{a.leiter.vorname} {a.leiter.nachname}" if a.leiter else "-",
            "Personen": ", ".join(f"{p.person.vorname} {p.person.nachname}" for p in a.persons) or "-"
        }
    },

    "kostenstelle": {
        "title": "Kostenstellenübersicht",
        "filters": {
            "name_filter": (str, "name"),
        },
        "filter_func": lambda q, f: (
            q.filter(Kostenstelle.name.ilike(f"%{f['name_filter']}%")) if f.get("name_filter") else q
        ),
        "map_func": lambda k: {
            "ID": k.id,
            "Name": k.name or "-",
            # Alle zugehörigen Professurnamen als kommaseparierte Liste
            "Professur": ", ".join(p.name for p in k.professuren) if k.professuren else "-",
            # Alle Personen aller Professuren als Vorname Nachname, kommasepariert
            "Personen": ", ".join(
                f"{pp.person.vorname} {pp.person.nachname}"
                for prof in k.professuren for pp in prof.persons
            ) or "-"
        }
    },

    "raum": {
        "title": "Raumübersicht",
        "filters": {
            "building_filter": (int, "building_id"),
            "floor_filter": (int, "etage")
        },
        "filter_func": lambda q, f: (
            q.filter(Raum.building_id == f["building_filter"]) if f.get("building_filter") else q
        ).filter(
            Raum.etage == f["floor_filter"]
        ) if f.get("floor_filter") else q,
        "map_func": lambda r: {
            "ID": r.id,
            "Gebäude": r.building.name if r.building else "-",
            "Raum": r.name or "-",
            "Etage": r.etage if r.etage is not None else "-",
            "Personen": ", ".join(f"{pr.person.vorname} {pr.person.nachname}" for pr in r.person_links) or "-",
            "Transponder": ", ".join(t.transponder.seriennummer or "-" for t in r.transponder_links) or "-"
        }
    },
})

def is_load_on_versions(opt):
    try:
        path = getattr(opt, '_path', [])
        keys = [step.key for step in path if hasattr(step, 'key')]
        if "versions" in keys:
            return True
    except Exception:
        pass
    return False

def path_contains_versions(opt):
    try:
        path = getattr(opt, '_path', [])
        for step in path:
            if getattr(step, 'key', None) == "versions":
                return True
        to_bind = getattr(opt, '_to_bind', None)
        if to_bind and path_contains_versions(to_bind):
            return True
    except Exception:
        pass
    return False

def is_option_on_versions(opt):
    """
    Prüft, ob eine SQLAlchemy Ladeoption (z.B. joinedload, subqueryload, selectinload, Load)
    auf den 'versions' Relationship-Pfad zielt.

    Funktioniert für joinedload('versions'), joinedload(Model.versions), Load(...).
    """
    # Prüfe den Pfad (path) der Option, falls vorhanden
    if isinstance(opt, Load):
        # .path ist ein tuple von Attributen/Strings, z.B. ('versions',) oder ('parent', 'versions')
        for element in opt.path:
            # element kann Attribut oder String sein
            if hasattr(element, 'key'):
                if element.key == "versions":
                    return True
            elif element == "versions":
                return True
        return False
    # joinedload etc. können unterschiedliche Klassen sein, wir versuchen .path zu lesen
    if hasattr(opt, 'path'):
        for element in opt.path:
            if hasattr(element, 'key'):
                if element.key == "versions":
                    return True
            elif element == "versions":
                return True
    # Falls es ein string ist oder anderes, prüfen wir nicht
    return False

def create_aggregate_view(view_id):
    config = AGGREGATE_VIEWS[view_id]

    def view_func():
        session = Session()

        raw_options = config.get("options", [])
        print(f"[DEBUG] Loaded raw options for view_id={view_id}: {raw_options}")

        # Filter out eager loading options that load 'versions' relationship robustly
        options = []
        for opt in raw_options:
            if is_option_on_versions(opt):
                print("[DEBUG] Skipping eager load on 'versions' relationship due to SQLAlchemy-Continuum limitation")
                continue
            options.append(opt)

        print(f"[DEBUG] Final options applied (after filtering): {options}")

        try:
            query = session.query(config["model"]).options(*options)
            print("[DEBUG] Query successfully created with options")
        except Exception as e:
            print(f"[ERROR] Exception during query creation with options: {e}")
            session.close()
            raise

        # Filter aus Request-Parametern parsen
        filters = {}
        for key, (typ, param) in config.get("filters", {}).items():
            val = request.args.get(param)
            if typ == bool:
                if val == "1":
                    filters[key] = True
                else:
                    filters[key] = False
            elif typ == int:
                try:
                    filters[key] = int(val)
                except (ValueError, TypeError):
                    filters[key] = None
            else:
                filters[key] = val if val is not None else None
        print(f"[DEBUG] Parsed filters: {filters}")

        try:
            query = config["filter_func"](query, filters)
            print("[DEBUG] Filter function applied successfully")
        except Exception as e:
            print(f"[ERROR] Exception during filter_func application: {e}")
            session.close()
            raise

        try:
            data_list = query.all()
            print(f"[DEBUG] Query executed, {len(data_list)} rows fetched")
        except sqlalchemy.exc.InvalidRequestError as e:
            if "versions" in str(e):
                print("[ERROR] Query failed due to eager loading of 'versions' relationship. Please remove such options.")
            session.close()
            raise
        except Exception as e:
            print(f"[ERROR] Exception during query execution: {e}")
            session.close()
            raise

        rows = []
        try:
            rows = [config["map_func"](obj) for obj in data_list]
            print("[DEBUG] map_func applied to all rows")
        except Exception as e:
            print(f"[ERROR] Exception during map_func application: {e}")
            session.close()
            raise

        column_labels = list(rows[0].keys()) if rows else []
        if "add_columns" in config:
            column_labels += config["add_columns"]
        print(f"[DEBUG] Column labels: {column_labels}")

        row_data = []
        for obj, row in zip(data_list, rows):
            row_cells = []
            for col in column_labels:
                val = row.get(col, "-")
                if isinstance(val, str) and val.startswith("<input"):
                    row_cells.append(val)
                else:
                    row_cells.append(escape(str(val)))
            row_data.append(row_cells)

        ctx = {
            "title": config["title"],
            "column_labels": column_labels,
            "row_data": row_data,
            "filters": filters,
            "url_for_view": request.endpoint and url_for(request.endpoint, aggregate_name=view_id),
            "filter_config": config.get("filters", {}),  # ← DAS HAT GEFEHLT
        }

        if "extra_context_func" in config:
            try:
                extra = config["extra_context_func"]()
                ctx.update(extra)
                print("[DEBUG] extra_context_func applied")
            except Exception as e:
                print(f"[ERROR] Exception during extra_context_func: {e}")
                session.close()
                raise

        session.close()

        return render_template("aggregate_view.html", **ctx)

    return view_func

@app.route("/aggregate/<string:aggregate_name>")
@login_required
def aggregate_view(aggregate_name):
    allowed_aggregates = {
        "inventar",
        "transponder",
        "person",
        "abteilung",
        "kostenstelle",
        "raum",
        "object_kategorie",
        "lager",
    }
    if aggregate_name not in allowed_aggregates:
        # Optional: 404 oder Fehlerseite
        abort(404)

    return create_aggregate_view(aggregate_name)()

def _create_room_name(r):
    if r:
        etage_str = f"{r.etage}.OG" if r.etage is not None else "?"
        return f"{r.name} ({etage_str})"
    return "-"

def _get_professur_name(pf):
    return pf.name if pf else "-"

def _get_abteilung_name(a):
    return a.name if a else "-"

def _get_kostenstelle_name(k):
    return k.name if k else "-"

def _get_person_name(p):
    if p:
        return f"{p.vorname} {p.nachname}"
    return "Unbekannt"

def _get_kategorie_name(c):
    return c.name if c else "-"

@app.route("/wizard/person", methods=["GET", "POST"])
@login_required
def wizard_person():
    session = Session()
    error = None
    success = False

    form_data = {
        "title": "",
        "vorname": "",
        "nachname": "",
        "kommentar": "",
        "image_url": "",
        "contacts": [],
        "transponders": [],
        "räume": []
    }

    try:
        if request.method == "POST":
            # Grunddaten
            form_data["title"] = request.form.get("title", "").strip()
            form_data["vorname"] = request.form.get("vorname", "").strip()
            form_data["nachname"] = request.form.get("nachname", "").strip()
            form_data["kommentar"] = request.form.get("kommentar", "").strip()
            form_data["image_url"] = request.form.get("image_url", "").strip()

            if not form_data["vorname"] or not form_data["nachname"]:
                raise ValueError("Vorname und Nachname sind Pflichtfelder.")

            # Kontakte aus Formular lesen
            emails = request.form.getlist("email[]")
            phones = request.form.getlist("phone[]")
            faxes = request.form.getlist("fax[]")
            kommentars = request.form.getlist("contact_kommentar[]")

            contacts = []
            valid_emails = []
            max_len = max(len(emails), len(phones), len(faxes), len(kommentars))

            for i in range(max_len):
                email_val = emails[i].strip() if i < len(emails) else ""
                phone_val = phones[i].strip() if i < len(phones) else ""
                fax_val = faxes[i].strip() if i < len(faxes) else ""
                kommentar_val = kommentars[i].strip() if i < len(kommentars) else ""

                form_data["contacts"].append({
                    "email": email_val,
                    "phone": phone_val,
                    "fax": fax_val,
                    "kommentar": kommentar_val
                })

                if email_val:
                    if not is_valid_email(email_val):
                        raise ValueError(f"Ungültige Email-Adresse: {email_val}")
                    valid_emails.append(email_val)

                if any([email_val, phone_val, fax_val, kommentar_val]):
                    contacts.append({
                        "email": email_val or None,
                        "phone": phone_val or None,
                        "fax": fax_val or None,
                        "kommentar": kommentar_val or None
                    })

            if not valid_emails:
                raise ValueError("Mindestens eine gültige Email muss eingegeben werden.")

            # Transponder: Verschachtelte Arrays korrekt auslesen
            def extract_multiindex_form_data(prefix: str) -> list[list[str]]:
                pattern = re.compile(rf"{re.escape(prefix)}\[(\d+)\]\[\]")
                grouped_data = defaultdict(list)

                for key in request.form.keys():
                    match = pattern.match(key)
                    if match:
                        index = int(match.group(1))
                        values = request.form.getlist(key)
                        grouped_data[index].extend(values)

                return [grouped_data[i] for i in sorted(grouped_data.keys())]

            serials_grouped = extract_multiindex_form_data("transponder_serial")
            kommentars_grouped = extract_multiindex_form_data("transponder_kommentar")

            # Falls keine verschachtelten Arrays vorliegen, alternativ einfache Liste verwenden
            if not serials_grouped:
                serials_grouped = [request.form.getlist("transponder_serial[]")]
            if not kommentars_grouped:
                kommentars_grouped = [request.form.getlist("transponder_kommentar[]")]

            transponders = []
            form_data["transponders"] = []

            # Alle Transpondergruppen iterieren
            for group_index in range(max(len(serials_grouped), len(kommentars_grouped))):
                serials = serials_grouped[group_index] if group_index < len(serials_grouped) else []
                tp_kommentars = kommentars_grouped[group_index] if group_index < len(kommentars_grouped) else []

                max_tp = max(len(serials), len(tp_kommentars))

                for i in range(max_tp):
                    serial = serials[i].strip() if i < len(serials) else ""
                    kommentar = tp_kommentars[i].strip() if i < len(tp_kommentars) else ""

                    form_data["transponders"].append({
                        "serial": serial,
                        "kommentar": kommentar
                    })

                    if serial:
                        transponders.append({
                            "serial": serial,
                            "kommentar": kommentar or None
                        })

            # Räume aus Formular (z.B. raum_id[] oder room_guid[])
            raum_ids = request.form.getlist("raum_id[]") or request.form.getlist("room_guid[]")
            räume = []
            form_data["räume"] = []

            for rid in raum_ids:
                rid = rid.strip()
                form_data["räume"].append({"id": rid})
                if rid:
                    # Typumwandlung zu int, falls raum_id Integer ist
                    try:
                        rid_int = int(rid)
                    except ValueError:
                        raise ValueError(f"Ungültige Raum-ID: {rid}")

                    room = session.query(Raum).filter_by(id=rid_int).first()
                    if not room:
                        raise ValueError(f"Unbekannte Raum-ID: {rid}")
                    räume.append(room)

            # Person anlegen
            new_person = Person(
                title=form_data["title"] or None,
                vorname=form_data["vorname"],
                nachname=form_data["nachname"],
                kommentar=form_data["kommentar"] or None,
                image_url=form_data["image_url"] or None
            )
            session.add(new_person)
            session.flush()  # um ID zu bekommen

            # Kontakte speichern
            for contact in contacts:
                pc = PersonContact(
                    person_id=new_person.id,
                    email=contact["email"],
                    phone=contact["phone"],
                    fax=contact["fax"],
                    kommentar=contact["kommentar"]
                )
                session.add(pc)

            # Transponder speichern mit besitzer_id auf new_person.id
            for tp in transponders:
                t = Transponder(
                    besitzer_id=new_person.id,
                    seriennummer=tp["serial"],
                    kommentar=tp["kommentar"]
                )
                session.add(t)

            # Räume verknüpfen (PersonToRaum)
            for room in räume:
                ptr = PersonToRaum(
                    person_id=new_person.id,
                    raum_id=room.id
                )
                session.add(ptr)

            session.commit()
            success = True

            # Formular zurücksetzen
            form_data = {
                "title": "",
                "vorname": "",
                "nachname": "",
                "kommentar": "",
                "image_url": "",
                "contacts": [],
                "transponders": [],
                "räume": []
            }

    except (ValueError, SQLAlchemyError) as e:
        session.rollback()
        error = str(e)

    except Exception as e:
        session.rollback()
        error = f"Unbekannter Fehler: {e}"

    finally:
        session.close()

    return render_template(
        "person_wizard.html",
        success=success,
        error=error,
        form_data=form_data
    )

@app.route("/map-editor")
@login_required
def map_editor():
    session = Session()

    building_id_param = request.args.get("building_id")
    etage_param = request.args.get("etage")

    etageplan_dir = os.path.join("static", "floorplans")

    # floorplan als Struktur: { building_id: [etage1, etage2, ...] }
    building_map = {}

    for filename in os.listdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), etageplan_dir)):
        if filename.startswith("b") and "_f" in filename and filename.endswith(".png"):
            try:
                parts = filename.removeprefix("b").removesuffix(".png").split("_f")
                b_id = int(parts[0])
                _f = int(parts[1])
                if b_id not in building_map:
                    building_map[b_id] = []
                building_map[b_id].append(_f)
            except Exception:
                continue

    building_names = {}
    try:
        building_ids = list(building_map.keys())
        if building_ids:
            buildings = session.query(Building).filter(Building.id.in_(building_ids)).all()
            for building in buildings:
                building_names[building.id] = building.name
    except Exception as e:
        session.close()
        return f"Error loading building names: {str(e)}", 500

    # Kein Gebäude oder etage gewählt → Auswahlseite rendern
    if building_id_param is None or etage_param is None:
        session.close()
        return render_template(
            "map_editor.html",
            floorplan={},
            image_url=None,
            image_width=None,
            image_height=None,
            building_id=None,
            building_names=building_names,
            etage=None,
            building_map=building_map
        )

    try:
        building_id = int(building_id_param)
        etage = int(etage_param)
    except ValueError:
        return "Invalid 'building_id' or 'etage' – must be integers", 400

    filename = f"b{building_id}_f{etage}.png"
    image_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "floorplans", filename)

    if not os.path.exists(image_path):
        session.close()
        return f"Image not found: {filename}", 404

    try:
        with Image.open(image_path) as img:
            width, height = img.size
    except Exception as e:
        session.close()
        return f"Error opening image: {str(e)}", 500

    image_url = f"static/floorplans/b{building_id}_f{etage}.png"

    image_width = 1
    image_height = 1

    try:
        with Image.open(image_path) as img:
            image_width, image_height = img.size
    except Exception as e:
        print(f"Error trying to get image width and height for {image_url}")

    session.close()

    return render_template(
        "map_editor.html",
        floorplan={},
        image_url=image_url,
        image_width=image_width,
        image_height=image_height,
        building_id=building_id,
        etage=etage,
        building_map=building_map,
        building_names=building_names
    )

@app.route("/wizard/<wizard_name>", methods=["GET", "POST"])
@login_required
def run_wizard(wizard_name):
    allowed = set(WIZARDS.keys())

    allowed.add("person")

    if wizard_name not in allowed:
        abort(404)

    return _wizard_internal(wizard_name)

def convert_datetime_value(field, value):
    if value is None:
        return None
    if field.get("type") == "date":
        return datetime.datetime.strptime(value, "%Y-%m-%d").date()
    if field.get("type") == "datetime-local":
        return datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M")
    return value

def get_json_safe_config(wizard):
    def strip(obj):
        if isinstance(obj, dict):
            return {k: strip(v) for k, v in obj.items() if k != "table"}

        if isinstance(obj, list):
            return [strip(i) for i in obj]

        return obj

    return strip(wizard)

def _wizard_internal(name):
    session = Session()

    config = WIZARDS.get(name)
    if not config:
        session.close()
        abort(404)

    success = False
    error = None
    form_data = {}

    if request.method == "POST":
        try:
            main_model = config["table"]
            # Hauptdaten aus dem Formular lesen
            main_data = {
                f["name"]: convert_datetime_value(f, request.form.get(f["name"], "").strip() or None)
                for f in config["fields"]
            }

            # Pflichtfelder prüfen
            missing = [f['name'] for f in config['fields'] if f.get('required') and not main_data[f['name']]]
            if missing:
                session.close()
                raise ValueError(f"Pflichtfelder fehlen: {', '.join(missing)}")

            main_instance = main_model(**main_data)
            session.add(main_instance)
            session.flush()

            for sub in config.get("subforms", []):
                table = sub["table"]
                foreign_key = sub["foreign_key"]
                field_names = [f["name"] for f in sub["fields"]]
                data_lists = {f: request.form.getlist(f + "[]") for f in field_names}

                for i in range(max(len(l) for l in data_lists.values())):
                    entry = {
                        f: data_lists[f][i].strip() if i < len(data_lists[f]) else None
                        for f in field_names
                    }
                    if any(entry.values()):
                        entry[foreign_key] = main_instance.id
                        session.add(table(**entry))

            session.commit()
            success = True

        except IntegrityError as e:
            session.rollback()
            # Hier kannst du die eigentliche Fehlermeldung aus e.orig oder e.args parsen, je nach DB-Backend
            error = "Ein Datenbank-Integritätsfehler ist aufgetreten: " + str(e.orig)

            # Formulardaten zum Wiederbefüllen speichern
            form_data = request.form.to_dict(flat=False)

        except Exception as e:
            session.rollback()
            error = str(e)
            form_data = request.form.to_dict(flat=False)

    session.close()

    return render_template(
        "wizard.html",
        config=config,
        config_json=get_json_safe_config(config) or [],
        success=success,
        error=error,
        form_data=form_data or {}
    )

def get_abteilung_metadata(abteilung_id: int) -> dict:
    session = Session()
    try:
        abteilung = session.query(Abteilung).filter(Abteilung.id == abteilung_id).one_or_none()
        if abteilung is None:
            session.close()
            return None

        metadata = {
            "id": abteilung.id,
            "name": abteilung.name,
            "abteilungsleiter": None,
            "personen": []
        }

        if abteilung.leiter is not None:
            metadata["abteilungsleiter"] = {
                "id": abteilung.leiter.id,
                "vorname": abteilung.leiter.vorname,
                "nachname": abteilung.leiter.nachname,
                "title": abteilung.leiter.title
            }

        # Falls du die Personen mit drin haben willst
        for person_to_abteilung in abteilung.persons:
            person = person_to_abteilung.person
            if person:
                metadata["personen"].append({
                    "id": person.id,
                    "vorname": person.vorname,
                    "nachname": person.nachname,
                    "title": person.title
                })

        session.close()
        return metadata

    except SQLAlchemyError as e:
        session.close()
        return {"error": str(e)}

def generate_fields_for_schluesselausgabe_from_metadata(
    ausgeber: dict,
    besitzer: dict,
    transponder: dict,
    abteilung: dict = None
) -> dict:
    data = {}

    FIELD_NAMES = [
        'Text1', 'Text3', 'Text4', 'Text5', 'Text7', 'Text8',
        'GebäudeRow1', 'RaumRow1', 'SerienNrSchlüsselNrRow1', 'AnzahlRow1',
        'GebäudeRow2', 'RaumRow2', 'SerienNrSchlüsselNrRow2', 'AnzahlRow2',
        'GebäudeRow3', 'RaumRow3', 'SerienNrSchlüsselNrRow3', 'AnzahlRow3',
        'GebäudeRow4', 'RaumRow4', 'SerienNrSchlüsselNrRow4', 'AnzahlRow4',
        'GebäudeRow5', 'RaumRow5', 'SerienNrSchlüsselNrRow5', 'AnzahlRow5',
        'Datum Übergebende:r', 'Datum Übernehmende:r', 'Weitere Anmerkungen'
    ]

    def extract_contact_string(person_dict):
        if not person_dict:
            return ""
        contacts = person_dict.get("contacts", [])
        if not contacts:
            return ""
        contact = contacts[0]  # nur erster Eintrag
        phone = ""
        phone = contact.get("phone", "")
        if phone:
            phone = phone.strip()

        email = ""
        email = contact.get("email", "")
        if email:
            email = email.strip()

        if phone and email:
            return f"{phone} / {email}"

        if email:
            return email

        if phone:
            return phone

        return ""

    for name in FIELD_NAMES:
        value = ""

        if name == "Text1":
            if abteilung and "name" in abteilung:
                value = abteilung["name"]

        elif name == "Text3":
            vorname = ausgeber.get("vorname", "")
            nachname = ausgeber.get("nachname", "")
            if nachname and vorname:
                value = f"{nachname}, {vorname}"
            elif nachname:
                value = f"{nachname}"
            elif vorname:
                value = f"{vorname}"

        elif name == "Text4":
            value = extract_contact_string(ausgeber)

        elif name == "Text5":
            value = abteilung.get("name", "") if abteilung else ""

        elif name == "Text7":
            value = besitzer.get("nachname", "") + ", " + besitzer.get("vorname", "") if besitzer else ""

        elif name == "Text8":
            value = extract_contact_string(besitzer)

        elif name.startswith("GebäudeRow"):
            index = int(name.replace("GebäudeRow", "")) - 1
            räume = transponder.get("räume", [])
            if 0 <= index < len(räume):
                building = räume[index].get("building")
                if building:
                    value = building.get("name", "")

        elif name.startswith("RaumRow"):
            index = int(name.replace("RaumRow", "")) - 1
            räume = transponder.get("räume", [])
            if 0 <= index < len(räume):
                value = räume[index].get("name", "")

        elif name.startswith("SerienNrSchlüsselNrRow"):
            index = int(name.replace("SerienNrSchlüsselNrRow", "")) - 1
            räume = transponder.get("räume", [])
            if 0 <= index < len(räume):
                room = räume[index]
                has_building = room.get("building", {}).get("name")
                has_room = room.get("name")
                if has_building and has_room and transponder.get("seriennummer"):
                    value = transponder["seriennummer"]

        elif name.startswith("AnzahlRow"):
            index = int(name.replace("AnzahlRow", "")) - 1
            räume = transponder.get("räume", [])
            if 0 <= index < len(räume):
                room = räume[index]
                has_building = room.get("building", {}).get("name")
                has_room = room.get("name")
                if has_building and has_room:
                    value = "1"

        elif name == "Datum Übergebende:r":
            if transponder.get("erhaltungsdatum"):
                value = transponder["erhaltungsdatum"].strftime("%d.%m.%Y")

        elif name == "Datum Übernehmende:r":
            if transponder.get("rückgabedatum"):
                value = transponder["rückgabedatum"].strftime("%d.%m.%Y")

        elif name == "Weitere Anmerkungen":
            if transponder.get("kommentar"):
                value = transponder["kommentar"]

        data[name] = value

    return data

def get_transponder_metadata(transponder_id: int) -> Optional[dict]:
    session = Session()

    try:
        transponder = session.query(Transponder).filter(Transponder.id == transponder_id).one_or_none()

        if transponder is None:
            session.close()
            return None

        metadata = {
            "id": transponder.id,
            "seriennummer": transponder.seriennummer,
            "erhaltungsdatum": transponder.erhaltungsdatum,
            "rückgabedatum": transponder.rückgabedatum,
            "kommentar": transponder.kommentar,

            "ausgeber": None,
            "besitzer": None,
            "räume": []
        }

        if transponder.ausgeber is not None:
            metadata["ausgeber"] = {
                "id": transponder.ausgeber.id,
                "vorname": transponder.ausgeber.vorname,
                "nachname": transponder.ausgeber.nachname,
                "title": transponder.ausgeber.title
            }

        if transponder.besitzer is not None:
            metadata["besitzer"] = {
                "id": transponder.besitzer.id,
                "vorname": transponder.besitzer.vorname,
                "nachname": transponder.besitzer.nachname,
                "title": transponder.besitzer.title
            }

        for link in transponder.room_links:
            room = link.room

            room_data = {
                "id": room.id,
                "name": room.name,
                "etage": room.etage,
                "building": None
            }

            if room.building is not None:
                room_data["building"] = {
                    "id": room.building.id,
                    "name": room.building.name,
                    "building_number": room.building.building_number,
                    "abkürzung": room.building.abkürzung
                }

            metadata["räume"].append(room_data)

        session.close()

        return metadata

    except SQLAlchemyError as e:
        session.close()

        return {"error": str(e)}

def get_person_metadata(person_id: int) -> dict:
    session = Session()

    try:
        person = session.query(Person).filter(Person.id == person_id).one_or_none()

        if person is None:
            session.close()
            return {"error": f"No person found with id {person_id}"}

        metadata = {
            "id": person.id,
            "title": person.title,
            "vorname": person.vorname,
            "nachname": person.nachname,
            "kommentar": person.kommentar,
            "image_url": person.image_url,

            "contacts": [],
            "räume": [],
            "transponders_issued": [],
            "transponders_owned": [],
            "departments": [],
            "person_abteilungen": [],
            "professuren": []
        }

        for contact in person.contacts:
            metadata["contacts"].append({
                "id": contact.id,
                "phone": contact.phone,
                "fax": contact.fax,
                "email": contact.email,
                "kommentar": contact.kommentar
            })

        for room in person.räume:
            metadata["räume"].append({
                "id": room.id,
                "raum_id": getattr(room, "raum_id", None),  # adapt if necessary
                "kommentar": getattr(room, "kommentar", None)
            })

        for transponder in person.transponders_issued:
            metadata["transponders_issued"].append({
                "id": transponder.id,
                "number": getattr(transponder, "number", None),
                "besitzer_id": transponder.besitzer_id
            })

        for transponder in person.transponders_owned:
            metadata["transponders_owned"].append({
                "id": transponder.id,
                "number": getattr(transponder, "number", None),
                "ausgeber_id": transponder.ausgeber_id
            })

        for dept in person.departments:
            metadata["departments"].append({
                "id": dept.id,
                "name": getattr(dept, "name", None)
            })

        for pa in person.person_abteilungen:
            metadata["person_abteilungen"].append({
                "id": pa.id,
                "abteilung_id": getattr(pa, "abteilung_id", None),
                "funktion": getattr(pa, "funktion", None)
            })

        for prof in person.professuren:
            metadata["professuren"].append({
                "id": prof.id,
                "professur_id": getattr(prof, "professur_id", None),
                "title": getattr(prof, "title", None)
            })

        session.close()
        return metadata

    except SQLAlchemyError as e:
        session.close()
        return {"error": str(e)}

def fill_pdf_form(template_path, data_dict):
    reader = PdfReader(template_path)
    writer = PdfWriter()

    # Alle Seiten übernehmen
    writer.append_pages_from_reader(reader)

    # 🛠️ AcroForm vom Original-PDF übernehmen
    if "/AcroForm" in reader.trailer["/Root"]:
        writer._root_object.update({
            NameObject("/AcroForm"): reader.trailer["/Root"]["/AcroForm"]
        })

    # Feldwerte vorbereiten
    fields = reader.get_fields()
    filled_fields = {}

    for field_name in data_dict:
        if field_name in fields:
            filled_fields[field_name] = data_dict[field_name]

    # 📝 Formularfelder auf erster Seite aktualisieren
    writer.update_page_form_field_values(writer.pages[0], filled_fields)

    # Ergebnis zurückgeben
    output_io = io.BytesIO()
    writer.write(output_io)
    output_io.seek(0)
    return output_io

@app.route('/generate_pdf/schliessmedien/')
@login_required
def generate_pdf():
    TEMPLATE_PATH = 'pdfs/ausgabe_schliessmedien.pdf'

    ausgeber_id = request.args.get('ausgeber_id')
    besitzer_id = request.args.get('besitzer_id')
    transponder_id = request.args.get('transponder_id')

    missing = []
    if not transponder_id:
        missing.append("transponder_id")

    if missing:
        return render_template_string(
            "<h1>Fehlende Parameter</h1><ul>{% for m in missing %}<li>{{ m }}</li>{% endfor %}</ul>",
            missing=missing
        ), 400

    ausgeber = get_person_metadata(ausgeber_id)
    besitzer = get_person_metadata(besitzer_id)
    transponder = get_transponder_metadata(transponder_id)

    not_found = []
    if ausgeber is None:
        not_found.append(f"Keine Person mit ausgeber_id: {ausgeber_id}")
    if besitzer is None:
        not_found.append(f"Keine Person mit besitzer_id: {besitzer_id}")
    if transponder is None:
        not_found.append(f"Kein Transponder mit transponder_id: {transponder_id}")

    if not_found:
        return render_template_string(
            "<h1>Nicht Gefunden</h1><ul>{% for msg in not_found %}<li>{{ msg }}</li>{% endfor %}</ul>",
            not_found=not_found
        ), 404

    field_data = generate_fields_for_schluesselausgabe_from_metadata(ausgeber, besitzer, transponder, )

    filled_pdf = fill_pdf_form(TEMPLATE_PATH, field_data)
    if filled_pdf is None:
        return render_template_string("<h1>Fehler</h1><p>Das PDF-Formular konnte nicht generiert werden.</p>"), 500

    return send_file(
        filled_pdf,
        mimetype='application/pdf',
        as_attachment=True,
        download_name='ausgabe_schliessmedien_filled.pdf'
    )

@app.route("/transponder", methods=["GET"])
@login_required
def transponder_form():
    session = Session()
    persons = session.query(Person).order_by(Person.nachname).all()
    transponders = session.query(Transponder).options(
        joinedload(Transponder.besitzer)
    ).order_by(Transponder.seriennummer).all()

    session.close()
    return render_template("transponder_form.html",
        config={"title": "Transponder-Ausgabe / Rückgabe"},
        persons=persons,
        transponders=transponders,
        current_date=date.today().isoformat()
    )

@app.route("/transponder/ausgabe", methods=["POST"])
@login_required
def transponder_ausgabe():
    person_id = request.form.get("person_id")
    transponder_id = request.form.get("transponder_id")
    erhaltungsdatum_str = request.form.get("erhaltungsdatum")

    session = Session()

    try:
        transponder = session.get(Transponder, int(transponder_id))
        transponder.besitzer_id = int(person_id)
        transponder.erhaltungsdatum = date.fromisoformat(erhaltungsdatum_str)
        session.session.commit()
        flash("Transponder erfolgreich ausgegeben.", "success")
    except Exception as e:
        session.session.rollback()
        flash(f"Fehler bei Ausgabe: {str(e)}", "danger")

    session.close()
    return redirect(url_for("transponder.transponder_form"))

@app.route("/transponder/rueckgabe", methods=["POST"])
@login_required
def transponder_rueckgabe():
    transponder_id = request.form.get("transponder_id")
    rückgabedatum_str = request.form.get("rückgabedatum")

    session = Session()

    try:
        transponder = session.session.get(Transponder, int(transponder_id))
        transponder.rückgabedatum = date.fromisoformat(rückgabedatum_str)
        transponder.besitzer_id = None
        session.commit()
        flash("Transponder erfolgreich zurückgenommen.", "success")
    except Exception as e:
        session.rollback()
        flash(f"Fehler bei Rückgabe: {str(e)}", "danger")

    session.close()
    return redirect(url_for("transponder.transponder_form"))

@app.route("/user_edit/<handler_name>", methods=["GET", "POST"])
@login_required
def gui_edit(handler_name):
    handler, error = get_handler_instance(handler_name)
    if error:
        return f"<h1>{error}</h1>", 404

    message = None
    try:
        if request.method == "POST":
            form_data = dict(request.form)
            obj_id = form_data.pop("id", None)
            try:
                if "delete" in form_data and form_data["delete"] == "1":
                    # Lösch-Request
                    if obj_id is not None:
                        try:
                            success = handler.delete_by_id(int(obj_id))
                            if success:
                                message = f"Eintrag {obj_id} gelöscht."
                            else:
                                message = "Löschen fehlgeschlagen."
                        except Exception as e:
                            message = f"Fehler beim Löschen: {e}"
                    else:
                        message = "Keine ID angegeben zum Löschen."
                else:
                    # Update oder Insert
                    if obj_id:
                        success = handler.update_by_id(int(obj_id), form_data)
                        message = f"Eintrag {obj_id} aktualisiert." if success else "Update fehlgeschlagen."
                    else:
                        inserted_id = handler.insert_data(form_data)
                        message = f"Neuer Eintrag eingefügt mit ID {inserted_id}"
            except Exception as e:
                message = f"Fehler: {e}"

        if not hasattr(handler, "get_all"):
            return f"<h1>Handler {handler_name} unterstützt kein get_all()</h1>", 400

        rows = handler.get_all()
        if not rows:
            if hasattr(handler, "get_columns"):
                columns = handler.get_columns()
            elif hasattr(handler, "model"):
                columns = [col.name for col in handler.model.__table__.columns]
            else:
                columns = []
        else:
            columns = list(handler.to_dict(rows[0]).keys())

        return render_template(
            "edit.html",
            handler=handler_name,
            rows=rows,
            columns=columns,
            message=message,
        )
    finally:
        handler.session.close()

@app.route('/floorplan')
@login_required
def etageplan():
    session = Session()

    building_id_param = request.args.get("building_id")
    etage_param = request.args.get("etage")

    building_id = None
    etage = None

    # Versuche ints aus Parametern zu machen
    try:
        if building_id_param is not None:
            building_id = int(building_id_param)
        if etage_param is not None:
            etage = int(etage_param)
    except ValueError:
        session.close()
        return "Invalid 'building_id' or 'etage' – must be integers", 400

    # Lade alle verfügbaren Gebäude & Etagen
    etageplan_dir = os.path.join("static", "floorplans")
    building_map = {}

    for filename in os.listdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), etageplan_dir)):
        if filename.startswith("b") and "_f" in filename and filename.endswith(".png"):
            try:
                parts = filename.removeprefix("b").removesuffix(".png").split("_f")
                b_id = int(parts[0])
                f = int(parts[1])
                if b_id not in building_map:
                    building_map[b_id] = []
                building_map[b_id].append(f)
            except Exception:
                continue

    # Gebäude-Namen aus DB laden
    building_names = {}
    try:
        building_ids = list(building_map.keys())
        if building_ids:
            buildings = session.query(Building).filter(Building.id.in_(building_ids)).all()
            for building in buildings:
                building_names[building.id] = building.name
    except Exception as e:
        session.close()
        return f"Error loading building names: {str(e)}", 500

    # Wenn Gebäude oder Etage fehlen, einfach das Template mit Auswahlfeldern rendern (ohne etageplan-Bild)
    if building_id is None or etage is None:
        session.close()
        return render_template(
            "floorplan.html",
            image_url=None,
            image_width=None,
            image_height=None,
            building_id=building_id,
            etage=etage,
            building_map=building_map,
            building_names=building_names
        )

    # Prüfe, ob Bild existiert
    filename = f"b{building_id}_f{etage}.png"
    image_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "floorplans", filename)

    if not os.path.exists(image_path):
        return f"Image not found: {filename}", 404

    # Bildgröße ermitteln
    try:
        with Image.open(image_path) as img:
            width, height = img.size
    except Exception as e:
        return f"Error opening image: {str(e)}", 500

    # Template mit Bild rendern
    return render_template(
        "floorplan.html",
        image_url=f"/static/floorplans/{filename}",
        image_width=width,
        image_height=height,
        building_id=building_id,
        etage=etage,
        building_map=building_map,
        building_names=building_names
    )

@app.route("/api/save_or_update_room", methods=["POST"])
@login_required
def save_or_update_room():
    session = Session()
    data = request.get_json()

    if not isinstance(data, dict):
        session.close()
        return jsonify({"error": "Expected a JSON object"}), 400

    try:
        validated = _save_room_validate_input(data)
    except ValueError as e:
        session.close()
        return jsonify({"error": str(e)}), 400

    with session.no_autoflush:
        try:
            room = _save_room_find_existing(session, validated)

            if room is None:
                room = _save_room_create(session, validated)
            else:
                _save_room_update_fields(room, validated)

            _save_room_set_layout(session, room, validated)
            session.commit()

            ret = jsonify({
                "status": "success",
                "raum_id": room.id,
                "room_name": room.name,
                "guid": room.guid,
                "etage": room.etage,
                "layout": {
                    "x": validated["x"],
                    "y": validated["y"],
                    "width": validated["width"],
                    "height": validated["height"]
                }
            })

            session.close()

            return ret, 200

        except IntegrityError as e:
            session.rollback()
            session.close()
            return jsonify({"error": "Database integrity error", "details": str(e)}), 409
        except SQLAlchemyError as e:
            session.rollback()
            session.close()
            return jsonify({"error": str(e)}), 500

def _save_room_validate_input(data):
    name = data.get("name")
    if not name or not isinstance(name, str):
        raise ValueError("Missing or invalid 'name'")

    for key in ["x", "y", "width", "height"]:
        if not isinstance(data.get(key), (int, float)):
            raise ValueError("Invalid or missing layout data")

    etage = data.get("etage")
    if etage is not None and not isinstance(etage, int):
        raise ValueError("Invalid etage – must be an integer")

    guid = data.get("guid")
    if guid is not None and not isinstance(guid, str):
        raise ValueError("Invalid 'guid' – must be a string")

    return {
        "name": name,
        "building_id": data.get("building_id"),
        "x": data["x"],
        "y": data["y"],
        "width": data["width"],
        "height": data["height"],
        "id": data.get("id"),
        "old_name": data.get("old_name"),
        "etage": etage,
        "guid": guid
    }

def _save_room_find_existing(session, v):
    for lookup in [
        lambda: session.query(Raum).filter_by(guid=v["guid"]).one_or_none() if v["guid"] else None,
        lambda: session.query(Raum).filter_by(id=v["id"]).one_or_none() if v["id"] else None,
        lambda: _save_room_query_by_name(session, v["old_name"], v["building_id"]) if v["old_name"] else None,
        lambda: _save_room_query_by_name(session, v["name"], v["building_id"])
    ]:
        room = lookup()
        if room:
            return room
    return None

def _save_room_query_by_name(session, name, building_id):
    q = session.query(Raum).filter(Raum.name == name)
    if building_id is not None:
        q = q.filter(Raum.building_id == building_id)
    return q.one_or_none()

def _save_room_create(session, v):
    if v["building_id"] is None:
        raise ValueError("Cannot create room without 'building_id'")
    room = Raum(
        name=v["name"],
        building_id=v["building_id"],
        etage=v["etage"],
        guid=v["guid"] or str(uuid.uuid4())
    )
    session.add(room)
    try:
        session.flush()
    except IntegrityError:
        session.rollback()
        room = _save_room_query_by_name(session, v["name"], v["building_id"])
        if not room:
            raise
    return room

def _save_room_update_fields(room, v):
    if v["name"] != room.name:
        room.name = v["name"]
    if v["etage"] is not None:
        room.etage = v["etage"]
    if v["guid"] and room.guid != v["guid"]:
        room.guid = v["guid"]

def _save_room_set_layout(session, room, v):
    if room.layout:
        room.layout.x = v["x"]
        room.layout.y = v["y"]
        room.layout.width = v["width"]
        room.layout.height = v["height"]
    else:
        layout = RaumLayout(
            raum_id=room.id,
            x=v["x"],
            y=v["y"],
            width=v["width"],
            height=v["height"]
        )
        session.add(layout)

@app.route("/get_floorplan", methods=["GET"])
@login_required
def get_floorplan():
    session = Session()

    building_id_param = request.args.get("building_id")
    etage = request.args.get("etage")

    try:
        building_id = int(building_id_param) if building_id_param is not None else None
        etage = int(etage) if etage is not None else None
    except ValueError:
        session.close()
        return jsonify({"error": "Invalid 'building_id' or 'etage' – must be integers"}), 400

    if building_id is None or etage is None:
        session.close()
        return jsonify({"error": "Both 'building_id' and 'etage' parameters are required"}), 400
    try:
        query = session.query(Raum).join(RaumLayout).filter(
            Raum.building_id == building_id,
            Raum.etage == etage
        )

        räume = query.all()

        result = []
        for room in räume:
            layout = room.layout
            if layout is None:
                continue  # skip räume without layout

            result.append({
                "id": room.id,
                "name": room.name,
                "x": layout.x,
                "y": layout.y,
                "width": layout.width,
                "height": layout.height,
                "guid": room.guid,
                "building_id": room.building_id,
                "etage": room.etage
            })

        session.close()
        return jsonify(result), 200

    except SQLAlchemyError as e:
        session.close()
        return jsonify({"error": str(e)}), 500

@app.route("/api/delete_person_from_raum", methods=["GET"])
@login_required
def delete_person_from_raum():
    # Query-Parameter auslesen
    person_id = request.args.get("person_id", type=int)
    raum_id = request.args.get("raum_id", type=int)

    # Validierung der Parameter
    if person_id is None:
        session.close()
        return jsonify({"error": "Missing or invalid 'person_id' parameter"}), 400
    if raum_id is None:
        session.close()
        return jsonify({"error": "Missing or invalid 'raum_id' parameter"}), 400

    session = Session()

    try:
        # Verknüpfungseintrag suchen
        link = session.query(PersonToRaum).filter(
            PersonToRaum.person_id == person_id,
            PersonToRaum.raum_id == raum_id
        ).one_or_none()

        if link is None:
            session.close()
            return jsonify({"error": f"Link between person_id '{person_id}' and raum_id '{raum_id}' not found"}), 200

        session.delete(link)
        session.commit()
        session.close()

        return jsonify({"status": f"Link between person_id '{person_id}' and raum_id '{raum_id}' deleted successfully"}), 200

    except SQLAlchemyError as e:
        session.rollback()
        session.close()
        return jsonify({"error": str(e)}), 500


@app.route("/api/delete_person_id_to_object_id", methods=["GET"])
@login_required
def delete_person_id_to_object_id():
    session = Session()
    # Parameter auslesen
    person_id = request.args.get("person_id", type=int)
    object_id = request.args.get("object_id", type=int)

    # Validierung
    if person_id is None:
        session.close()
        return jsonify({"error": "Missing or invalid 'person_id' parameter"}), 400
    if object_id is None:
        session.close()
        return jsonify({"error": "Missing or invalid 'object_id' parameter"}), 400

    try:
        # Eintrag suchen mit passender besitzer_id und object_id
        eintrag = session.query(Inventar).filter(
            Inventar.besitzer_id == person_id,
            Inventar.object_id == object_id
        ).one_or_none()

        if eintrag is None:
            session.close()
            return jsonify({"status": f"Kein Inventar-Eintrag mit person_id '{person_id}' und object_id '{object_id}' gefunden"}), 200

        session.delete(eintrag)
        session.commit()
        session.close()

        return jsonify({"status": f"Eintrag mit person_id '{person_id}' und object_id '{object_id}' erfolgreich gelöscht"}), 200

    except SQLAlchemyError as e:
        session.rollback()
        session.close()
        return jsonify({"error": str(e)}), 500

@app.route("/api/delete_room", methods=["POST"])
@login_required
def delete_room():
    data = request.get_json()

    if not isinstance(data, dict):
        session.close()
        return jsonify({"error": "Expected JSON object"}), 400

    name = data.get("name")
    building_id = data.get("building_id")

    if not isinstance(name, str):
        session.close()
        return jsonify({"error": "Missing or invalid 'name' field"}), 400

    session = Session()

    try:
        query = session.query(Raum).filter(Raum.name == name)

        if building_id is not None:
            query = query.filter(Raum.building_id == building_id)

        room = query.one_or_none()

        if room is None:
            session.close()
            return jsonify({"error": f"Raum with name '{name}' not found"}), 404

        session.delete(room)
        session.commit()

        session.close()
        return jsonify({"status": f"Raum '{name}' deleted successfully"}), 200

    except SQLAlchemyError as e:
        session.rollback()
        session.close()
        return jsonify({"error": str(e)}), 500

@app.route('/api/add_or_update_person', methods=['POST'])
@login_required
def add_or_update_person():
    data = request.json
    vorname = data.get('vorname')
    nachname = data.get('nachname')
    alter = data.get('alter')
    rolle = data.get('rolle')

    if not vorname or not nachname or alter is None:
        return jsonify({'error': 'Vorname, Nachname und Alter sind Pflichtfelder'}), 400

    db = get_db()
    cursor = db.cursor()

    cursor.execute('SELECT id FROM person WHERE vorname=? AND nachname=?', (vorname, nachname))
    row = cursor.fetchone()

    if row:
        person_id = row['id']
        cursor.execute('UPDATE person SET alter=?, rolle=? WHERE id=?', (alter, rolle, person_id))
    else:
        cursor.execute('INSERT INTO person (vorname, nachname, alter, rolle) VALUES (?, ?, ?, ?)', (vorname, nachname, alter, rolle))

    db.commit()
    return jsonify({'message': 'Person gespeichert'}), 200

@app.route("/api/add_person", methods=["POST"])
@login_required
def add_person():
    data = request.get_json()
    required_fields = ["vorname", "nachname", "title", "kommentar", "image_url"]
    for field in required_fields:
        if field not in data:
            session.close()
            return jsonify({"error": f"Missing field: {field}"}), 400

    session = Session()
    try:
        person = Person(
            vorname=data["vorname"],
            nachname=data["nachname"],
            title=data["title"],
            kommentar=data["kommentar"],
            image_url=data["image_url"]
        )
        session.add(person)
        session.commit()
        session.close()
        return jsonify({"status": "success", "person_id": person.id}), 200

    except sqlalchemy.exc.IntegrityError as e:
        session.rollback()
        # Wenn UNIQUE constraint verletzt wurde, Person suchen und ID zurückgeben
        if "UNIQUE constraint failed" in str(e.orig):
            existing_person = session.query(Person).filter_by(
                vorname=data["vorname"],
                nachname=data["nachname"],
                title=data["title"]
            ).first()
            session.close()
            if existing_person:
                return jsonify({"status": "exists", "person_id": existing_person.id}), 200
            else:
                # Falls keine Person gefunden wird, obwohl Constraint Fehler da war
                return jsonify({"error": "Integrity error, but person not found"}), 500
        else:
            session.close()
            return jsonify({"error": str(e)}), 500

    except Exception as e:
        session.rollback()
        session.close()
        return jsonify({"error": str(e)}), 500

@app.route("/api/save_person_to_raum", methods=["POST"])
@login_required
def save_person_to_raum():
    session = Session()
    try:
        data = request.get_json()
        if not data or "raum" not in data or "person" not in data:
            session.close()
            return jsonify({"error": f"Request body must include both 'raum' and 'person' fields, got: {data}"}), 400

        raum_id = data["raum"]
        person_data = data["person"]
        required_fields = ["vorname", "nachname", "image_url"]

        for field in required_fields:
            if field not in person_data:
                session.close()
                return jsonify({"error": f"Missing required field in person data: '{field}'"}), 400

        x = data.get("x")
        y = data.get("y")

        title = person_data.get("title")
        kommentar = person_data.get("kommentar")

        person = session.query(Person).filter_by(
            vorname=person_data["vorname"],
            nachname=person_data["nachname"],
            title=title
        ).first()

        if not person:
            person = Person(
                vorname=person_data["vorname"],
                nachname=person_data["nachname"],
                title=title,
                kommentar=kommentar,
                image_url=person_data["image_url"]
            )
            session.add(person)
            session.flush()

        room = session.query(Raum).filter_by(id=raum_id).first()
        if not room:
            session.close()
            return jsonify({"error": f"Raum '{raum_id}' not found"}), 404

        session.query(PersonToRaum).filter_by(person_id=person.id).delete()

        link = PersonToRaum(
            person_id=person.id,
            raum_id=room.id,
            x=x,
            y=y
        )
        session.add(link)

        session.commit()

        struct = {
            "status": "updated",
            "person_id": person.id,
            "raum_id": room.id,
            "link_id": link.id,
            "x": x,
            "y": y
        }

        session.close()
        return jsonify(struct), 200

    except IntegrityError as e:
        session.rollback()
        session.close()
        return jsonify({
            "error": "Database integrity error",
            "details": str(e)
        }), 500

    except Exception as e:
        session.rollback()
        session.close()
        return jsonify({
            "error": f"Unexpected server error: {e}",
            "details": str(e)
        }), 500

@app.route("/api/save_object_to_person", methods=["POST"])
@login_required
def save_object_to_person():
    session = Session()
    try:
        data = request.get_json()

        if not data or "object_id" not in data or "besitzer_id" not in data:
            session.close()
            return jsonify({"error": "Request must include 'object_id' and 'besitzer_id'"}), 400

        object_id = data["object_id"]
        besitzer_id = data["besitzer_id"]

        # Optionalfelder
        raum_id = data.get("raum_id")
        kommentar = data.get("kommentar")
        preis = data.get("preis")
        anschaffungsdatum = data.get("anschaffungsdatum")
        erhaltungsdatum = data.get("erhaltungsdatum")
        rückgabedatum = data.get("rückgabedatum")
        kostenstelle_id = data.get("kostenstelle_id")
        anlagennummer = data.get("anlagennummer")
        professur_id = data.get("professur_id")
        abteilung_id = data.get("abteilung_id")
        ausgeber_id = data.get("ausgeber_id")

        # Validierung: Existieren object/person?
        obj = session.query(Object).filter_by(id=object_id).first()
        person = session.query(Person).filter_by(id=besitzer_id).first()

        if not obj:
            session.close()
            return jsonify({"error": f"Object with ID {object_id} not found"}), 404
        if not person:
            session.close()
            return jsonify({"error": f"Person with ID {besitzer_id} not found"}), 404

        # Ein Inventar-Eintrag mit diesem Objekt existiert?
        inventar = session.query(Inventar).filter_by(object_id=object_id).first()

        if inventar:
            # Update
            inventar.besitzer_id = besitzer_id
            inventar.raum_id = raum_id
            inventar.kommentar = kommentar
            inventar.preis = preis
            inventar.anschaffungsdatum = anschaffungsdatum
            inventar.erhaltungsdatum = erhaltungsdatum
            inventar.rückgabedatum = rückgabedatum
            inventar.kostenstelle_id = kostenstelle_id
            inventar.anlagennummer = anlagennummer
            inventar.professur_id = professur_id
            inventar.abteilung_id = abteilung_id
            inventar.ausgeber_id = ausgeber_id
            status = "updated"
        else:
            # Neuanlage
            inventar = Inventar(
                object_id=object_id,
                besitzer_id=besitzer_id,
                raum_id=raum_id,
                kommentar=kommentar,
                preis=preis,
                anschaffungsdatum=anschaffungsdatum,
                erhaltungsdatum=erhaltungsdatum,
                rückgabedatum=rückgabedatum,
                kostenstelle_id=kostenstelle_id,
                anlagennummer=anlagennummer,
                professur_id=professur_id,
                abteilung_id=abteilung_id,
                ausgeber_id=ausgeber_id
            )
            session.add(inventar)
            status = "created"

        session.commit()

        response = {
            "status": status,
            "inventar_id": inventar.id,
            "object_id": object_id,
            "besitzer_id": besitzer_id
        }

        session.close()
        return jsonify(response), 200

    except IntegrityError as e:
        session.rollback()
        session.close()
        return jsonify({
            "error": "Database integrity error",
            "details": str(e)
        }), 500

    except Exception as e:
        session.rollback()
        session.close()
        return jsonify({
            "error": "Unexpected server error",
            "details": str(e)
        }), 500

@app.route("/api/get_person_database", methods=["GET"])
def get_person_database():
    try:
        session = Session()
        persons = session.query(Person).all()

        result = []
        for person in persons:
            result.append({
                "vorname": person.vorname or "",
                "nachname": person.nachname or "",
                "title": person.title or "",
                "etage": 0,
                "kommentar": person.kommentar or "",
                "id": person.id,
                "image_url": person.image_url or ""
            })


        session.close()
        return jsonify(result), 200
    except Exception as e:
        print(f"❌ Fehler bei /api/get_person_database: {e}")
        session.close()
        return jsonify({"error": "Fehler beim Abrufen der Personen"}), 500

@app.route("/api/get_object_database", methods=["GET"])
def get_object_database():
    try:
        session = Session()
        objects = session.query(Object).all()

        result = []
        for obj in objects:
            result.append({
                "id": obj.id,
                "name": obj.name or "",
                "Preis": obj.preis or 0,
                "category": obj.kategorie.name if obj.kategorie else ""
            })

        session.close()
        return jsonify(result), 200

    except Exception as e:
        print(f"❌ Fehler bei /api/get_object_database: {e}")
        session.close()
        return jsonify({"error": "Fehler beim Abrufen der Objekte"}), 500

@app.route("/api/get_person_id_object_id_database", methods=["GET"])
def get_person_id_object_id_database():
    session = Session()
    try:
        inventars = session.query(Inventar).all()

        result = []
        for inventar in inventars:
            result.append({
                "person_id": inventar.besitzer_id or "",
                "object_id": inventar.object_id or "",
            })

        return jsonify(result), 200
    except Exception as e:
        print(f"❌ Fehler bei /api/get_person_database: {e}")
        return jsonify({"error": "Fehler beim Abrufen der Personen"}), 500
    finally:
        session.close()


@app.route("/api/get_raum_id")
def get_raum_id():
    session = Session()
    building_name = request.args.get("gebäudename")
    room_name = request.args.get("raumname")

    if not building_name and not room_name:
        session.close()
        return jsonify({"error": "Missing parameters: building_name and room_name"}), 400

    if not building_name:
        session.close()
        return jsonify({"error": "Missing parameter: building_name"}), 400

    if not room_name:
        session.close()
        return jsonify({"error": "Missing parameter: room_name"}), 400

    try:
        # Gebäude suchen oder anlegen
        building = session.query(Building).filter_by(name=building_name).first()
        if not building:
            building = Building(name=building_name, building_number="", abkürzung="")
            session.add(building)
            session.commit()

        # Raum suchen oder anlegen
        room = (
            session.query(Raum)
            .filter_by(building_id=building.id, name=room_name)
            .first()
        )
        if not room:
            new_guid = str(uuid.uuid4())
            room = Raum(
                building_id=building.id,
                name=room_name,
                etage=0,
                guid=new_guid,
            )
            session.add(room)
            session.commit()

        ret = jsonify({"raum_id": room.id})

        session.close()

        return ret

    except SQLAlchemyError as e:
        # Optionale Logging-Ausgabe
        print(f"DB error: {e}")
        session.close()
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/get_building_names', methods=['GET'])
def get_building_names():
    session = Session()

    buildings = session.query(Building.name).order_by(Building.name).all()
    names = [b.name for b in buildings if b.name]

    session.close()

    return jsonify(names)

def get_names(session, model, id_field, name_fields):
    """
    Generische Funktion, um Namen aus einem Modell abzufragen.

    :param session: DB-Session
    :param model: SQLAlchemy-Modellklasse (z.B. Person, Object)
    :param id_field: Feld für die ID (z.B. Person.id)
    :param name_fields: Liste von Feldern, aus denen der Name zusammengesetzt wird (z.B. [Person.vorname, Person.nachname])
    :return: dict {id: name}
    """
    query_fields = [id_field] + name_fields
    records = session.query(*query_fields).all()

    result = {}
    for r in records:
        id_val = getattr(r, id_field.key)
        # Namen zusammensetzen und whitespace trimmen
        name_parts = [getattr(r, f.key) or '' for f in name_fields]
        name = " ".join(name_parts).strip()

        if name:  # nur mit Namen hinzufügen
            result[id_val] = name

    return result

@app.route('/schema')
@login_required
@admin_required
def schema():
    show_versions = request.args.get('show_versions', 'false').lower() == 'true'

    if show_versions:
        # Alle Tabellen anzeigen
        tables = list(Base.metadata.tables.values())
    else:
        # Tabellen filtern, die nicht auf '_version' enden
        tables = [t for t in Base.metadata.tables.values() if not t.name.endswith('_version') and not t.name in IGNORED_TABLES]

    graph = create_schema_graph(
        metadata=Base.metadata,
        tables=tables,
        engine=engine
    )
    graph.write_png('/tmp/schema.png', encoding='utf-8')
    return send_file('/tmp/schema.png', mimetype='image/png')

@app.route('/api/get_names/<table_name>', methods=['GET'])
@login_required
def get_names_dynamic(table_name):
    TABLE_NAME_OVERRIDES = {
        'transponder': ['seriennummer'],
    }

    if table_name in IGNORED_TABLES:
        return abort(404, "Table not found")

    model = None
    for mapper in Base.registry.mappers:
        cls = mapper.class_
        if hasattr(cls, '__tablename__') and cls.__tablename__ == table_name:
            model = cls
            break

    if model is None:
        return abort(404, "Table not found")

    id_col = getattr(model, 'id', None)
    if id_col is None:
        return abort(500, "No id column found")

    name_cols = []
    override = TABLE_NAME_OVERRIDES.get(table_name)
    if override:
        name_cols = [getattr(model, col) for col in override if hasattr(model, col)]
    else:
        for col in class_mapper(model).columns:
            col_name = col.key.lower()
            if any(x in col_name for x in ('name', 'first', 'last')):
                name_cols.append(getattr(model, col.key))

    if not name_cols:
        return abort(500, "No name columns found")

    session = Session()
    try:
        result = get_names(session, model, id_col, name_cols)
    finally:
        session.close()

    return jsonify(result)

@app.route('/db_info')
@login_required
@admin_required
def db_info():
    inspector = inspect(engine)
    conn = engine.connect()

    tables = []
    relationships = []

    for table_name in inspector.get_table_names():
        columns = inspector.get_columns(table_name)
        indexes = inspector.get_indexes(table_name)
        pks = inspector.get_pk_constraint(table_name).get('constrained_columns', [])
        fks = inspector.get_foreign_keys(table_name)
        count = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar()

        tables.append({
            'name': table_name,
            'columns': columns,
            'indexes': indexes,
            'pks': pks,
            'fks': fks,
            'num_columns': len(columns),
            'num_rows': count
        })

        for fk in fks:
            relationships.append((table_name, fk['referred_table']))

    # Mermaid ER Diagram
    def make_mermaid(tables, relationships):
        lines = ['erDiagram']
        for t in tables:
            lines.append(f'  {t["name"]} {{')
            for col in t['columns']:
                col_type = str(col["type"]).split('(')[0]
                nullable = '' if col["nullable"] else ' NOT NULL'
                lines.append(f'    {col_type} {col["name"]}{nullable}')
            lines.append('  }')

        for src, target in relationships:
            lines.append('  ' + src + ' }|--|| ' + target + ' : FK')

        return "\n".join(lines)

    mermaid = make_mermaid(tables, relationships)

    return render_template('db_info.html', tables=tables, mermaid=mermaid)

@app.route("/api/get_person_raum_data", methods=["GET"])
@login_required
def get_person_raum_data():
    session = None
    try:
        session = Session()

        building_id = request.args.get("building_id", type=int)
        etage = request.args.get("etage", type=int)

        if building_id is None or etage is None:
            if session:
                session.close()
            return jsonify({"error": "Missing building_id or etage parameter"}), 400

        räume = session.query(Raum).options(
            joinedload(Raum.layout),
            joinedload(Raum.person_links)
                .joinedload(PersonToRaum.person)
                .joinedload(Person.contacts)
        ).filter(
            Raum.building_id == building_id,
            Raum.etage == etage
        ).all()

        person_dict_map = {}

        for room in räume:
            room_info = room.to_dict()
            layout_info = room.layout.to_dict() if room.layout else {}

            for ptr in room.person_links:
                person = ptr.person
                person_id = person.id

                person_dict = person.to_dict()

                x_value = ptr.x
                y_value = ptr.y

                person_dict["x"] = x_value
                person_dict["y"] = y_value

                if person_id not in person_dict_map:
                    person_dict_map[person_id] = {
                        "person": person_dict,
                        "contacts": [c.to_dict() for c in person.contacts],
                        "räume": []
                    }

                if x_value is None or y_value is None:
                    print(f"⚠ Warnung: PersonToRaum id={ptr.id} hat x oder y = None")

                # Sicherheit: Stelle sicher, dass x/y NICHT in room_info vorkommen
                if "x" in room_info:
                    del room_info["x"]
                if "y" in room_info:
                    del room_info["y"]

                person_dict_map[person_id]["räume"].append({
                    "raum": room_info,
                    "layout": layout_info
                })

        if session:
            session.close()

        result = list(person_dict_map.values())
        return jsonify(result)

    except SQLAlchemyError as e:
        if session:
            session.rollback()
            session.close()
        print(f"❌ SQLAlchemy Fehler in /api/get_person_raum_data: {e}")
        return jsonify({"error": "Internal server error"}), 500

    except Exception as e:
        if session:
            session.close()
        print(f"❌ Fehler in /api/get_person_raum_data: {e}")
        return jsonify({"error": "Internal server error"}), 500

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
                'url': url_for('aggregate_persons_view', person_id=person.id)
            })

    # Admin-Zeug
    if is_admin_user(session):
        tables = [
            cls.__tablename__
            for cls in Base.__subclasses__()
            if hasattr(cls, '__tablename__') and cls.__tablename__ not in ["role", "user"]
        ]

        if 'admin' in query:
            results.append({'label': '🛠️ Admin', 'url': '/admin'})

        for table in tables:
            if query in table.lower():
                results.append({
                    'label': f'📋 {table.capitalize()}',
                    'url': url_for('table_view', table_name=table)
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
            if hasattr(t, "issued_at") and isinstance(t.issued_at, datetime.datetime):
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

@app.route("/api/auto_update/transponder", methods=["GET"])
def update_transponder_field():
    session = Session()

    try:
        element_name = request.args.get("name", type=str)
        update_id = request.args.get("id", type=int)
        new_val = request.args.get("val", type=str)

        if not element_name or update_id is None or new_val is None:
            return jsonify({
                "error": "Missing one or more required parameters",
                "required": ["name", "id", "val"]
            }), 400

        # Column-Definitionen korrekt extrahieren
        mapper = inspect(Transponder)
        column_info = {}
        for attr in mapper.attrs:
            if hasattr(attr, 'columns'):
                column_info[attr.key] = attr.columns[0].type

        if element_name not in column_info:
            return jsonify({"error": f"Invalid column name: '{element_name}'"}), 400

        column_type = column_info[element_name]

        # Objekt holen
        transponder = session.get(Transponder, update_id)
        if transponder is None:
            return jsonify({"error": f"No transponder found with id={update_id}"}), 404

        # Typbasierte Wert-Konvertierung
        parsed_value = None
        column_type_name = column_type.__class__.__name__

        if column_type_name in ["Integer", "SmallInteger", "BigInteger"]:
            try:
                parsed_value = int(new_val)
            except ValueError:
                parsed_value = None

        elif column_type_name in ["Text", "String", "Unicode", "UnicodeText"]:
            parsed_value = new_val

        elif column_type_name == "Date":
            try:
                parsed_value = datetime.strptime(new_val, "%Y-%m-%d").date()
            except ValueError:
                return jsonify({
                    "error": f"Invalid date format for '{element_name}'",
                    "expected_format": "YYYY-MM-DD",
                    "input": new_val
                }), 400

        else:
            return jsonify({
                "error": f"Unsupported column type '{column_type_name}' for field '{element_name}'"
            }), 400

        # Wert setzen
        setattr(transponder, element_name, parsed_value)
        session.commit()

        return jsonify({
            "message": f"Feld '{element_name}' für die ID {update_id} erfolgreich geupdatet",
            "id": update_id,
            "new_value": parsed_value
        }), 200

    except SQLAlchemyError as e:
        session.rollback()
        return jsonify({
            "error": "Database error",
            "details": str(e)
        }), 500

    except Exception as e:
        session.rollback()
        return jsonify({
            "error": "Unhandled exception",
            "details": str(e),
            "type": type(e).__name__
        }), 500

    finally:
        session.close()

def _readonly_block_check():
    session = Session()

    is_admin = is_admin_user(session)

    session.close()

    if is_admin:
        return

    if getattr(current_user, 'readonly', False):
        raise RuntimeError("Schreiboperationen sind deaktiviert: Benutzer ist im Readonly-Modus.")

def block_writes_if_user_readonly(session, flush_context, instances):
    write_ops = session.new.union(session.dirty).union(session.deleted)

    if not write_ops:
        return  # nichts zu tun

    try:
        if request:
            _readonly_block_check()
    except RuntimeError:
        raise
    except Exception:
        # Kein Flask-Kontext (z. B. bei Alembic), dann ignorieren
        pass

def get_replace_config_dict():
    def make_api_url(name): return f"/api/get_names/{name}"

    SPECIAL_CASES = {
        "room": {
            "key": "raum_id",
            "label": "Gebäude+Etage+Raum",
            "fields": [
                {
                    "gebäudename": {
                        "name": "Gebäudename",
                        "type": "select",
                        "options_url": "/api/get_names/building"
                    }
                },
                {
                    "floor": {
                        "name": "Etage",
                        "type": "text"
                    }
                },
                {
                    "raumname": {
                        "name": "Raumname",
                        "type": "text"
                    }
                }
            ],
            "url": "/api/get_raum_id?building_name={building_name}&room_name={room_name}"
        }
    }

    def extract_foreign_keys(model):
        result = {}
        inspected = inspect(model)

        for rel in inspected.relationships:
            target = rel.mapper.class_
            fk_column = list(rel.local_columns)[0]
            fk_name = fk_column.name

            if hasattr(target, "__tablename__"):
                target_table = target.__tablename__

                if target_table in SPECIAL_CASES:
                    special = SPECIAL_CASES[target_table]
                    result[special["key"]] = special
                    continue

                # Einheitliches Format: fields als Liste von Dicts
                result[fk_name] = {
                    "fields": [
                        {
                            fk_name: {
                                "name": target_table.capitalize(),
                                "type": "select",
                                "options_url_id_dict": make_api_url(target_table)
                            }
                        }
                    ],
                    "label": target_table.capitalize()
                }

        return result

    def is_valid_model(cls):
        return (
            isinstance(cls, type)
            and hasattr(cls, "__tablename__")
            and not cls.__name__.endswith("Version")
        )

    MODELS = [
        mapper.class_
        for mapper in Base.registry.mappers
        if is_valid_model(mapper.class_)
    ]

    names = {}
    for model in MODELS:
        names.update(extract_foreign_keys(model))

    if "person_id" in names:
        for alias in ["ausgeber_id", "besitzer_id", "abteilungsleiter_id"]:
            names[alias] = names["person_id"]

    return names

def get_model_by_tablename(name):
    for mapper in Base.registry.mappers:
        cls = mapper.class_
        if hasattr(cls, "__tablename__") and cls.__tablename__ == name:
            return cls
    return None

def merge_model_entries(session, model, ids_to_merge, target_id):
    if target_id in ids_to_merge:
        ids_to_merge.remove(target_id)
    if not ids_to_merge:
        return

    model_name = model.__tablename__
    related_models = Base.registry.mappers

    for related_mapper in related_models:
        for prop in related_mapper.attrs:
            if hasattr(prop, "columns"):
                for col in prop.columns:
                    if col.foreign_keys:
                        for fk in col.foreign_keys:
                            if fk.column.table.name == model_name:
                                related_cls = related_mapper.class_
                                fk_col = col.name
                                q = session.query(related_cls).filter(getattr(related_cls, fk_col).in_(ids_to_merge))
                                for obj in q.all():
                                    setattr(obj, fk_col, target_id)
                                    flag_modified(obj, fk_col)
                                break

    for id_ in ids_to_merge:
        obj = session.get(model, id_)
        if obj:
            session.delete(obj)

    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        raise

def get_referenced_tablenames():
    referenced = set()
    for mapper in Base.registry.mappers:
        for prop in mapper.attrs:
            if hasattr(prop, "columns"):
                for col in prop.columns:
                    for fk in col.foreign_keys:
                        referenced.add(fk.column.table.name)
    return referenced


def merge_entry_display(entry):
    if entry is None:
        return ""

    # Versuche bevorzugte Attribute
    preferred_attrs = ["name", "title", "label"]
    for attr in preferred_attrs:
        val = getattr(entry, attr, None)
        if val is not None and isinstance(val, str) and val.strip():
            return val.strip()

    # Wenn keine bevorzugten Attribute, suche erste String-Spalte im Modell
    try:
        # entry.__mapper__ ist SQLAlchemy Mapper für das Modell
        for attr_key, attr_val in entry.__mapper__.c.items():
            # attr_val ist Column-Objekt
            # Prüfe ob Column vom Typ String/Text ist
            col_type = attr_val.type
            if isinstance(col_type, (String, Unicode, Text)):
                val = getattr(entry, attr_key, None)
                if val is not None and isinstance(val, str) and val.strip():
                    return val.strip()
    except Exception:
        pass

    # Fallback: Versuche id auszugeben
    if hasattr(entry, "id"):
        return f"ID {entry.id}"

    # Sonst repr
    return repr(entry)

@app.context_processor
def utility_processor():
    return dict(entry_display=merge_entry_display)

@app.route("/merge", methods=["GET", "POST"])
@login_required
@admin_required
def merge_interface():
    session = Session()

    referenced_tables = get_referenced_tablenames()

    all_tables = [
        mapper.class_.__tablename__
        for mapper in Base.registry.mappers
        if hasattr(mapper.class_, "__tablename__")
        and mapper.class_.__tablename__ in referenced_tables
        and mapper.class_.__tablename__ not in IGNORED_TABLES
        and not mapper.class_.__name__.endswith("Version")
    ]

    selected_table = request.args.get("table")
    Model = get_model_by_tablename(selected_table) if selected_table else None
    entries = session.query(Model).all() if Model else []

    if request.method == "POST":
        selected_ids = list(map(int, request.form.getlist("merge_ids")))
        target_id = int(request.form["target_id"])

        if target_id in selected_ids:
            selected_ids.remove(target_id)

        if not selected_ids:
            session.close()
            flash("Bitte mindestens einen Eintrag zum Zusammenführen auswählen.", "error")
        else:
            try:
                merge_model_entries(session, Model, selected_ids, target_id)
                session.close()
                flash("Merge erfolgreich durchgeführt.", "success")
                return redirect(url_for("merge_interface", table=selected_table))
            except Exception as e:
                session.rollback()
                session.close()
                flash(f"Fehler beim Mergen: {e}", "error")

    session.close()
    
    print(all_tables)
    print(entries)

    return render_template(
        "merge_generic.html",
        tables=all_tables,
        selected_table=selected_table,
        entries=entries,
    )

event.listen(Session, "before_flush", block_writes_if_user_readonly)
event.listen(Session, "before_flush", block_writes_if_data_version_cookie_set)

if __name__ == "__main__":
    app.run(debug=args.debug, port=args.port)
