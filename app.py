import sys
import re
import platform
import shutil
import os
import subprocess
from datetime import date
from copy import deepcopy
import csv

try:
    import venv
except ModuleNotFoundError:
    print("venv not found. Is python3-venv installed?")
    sys.exit(1)

from pathlib import Path

VENV_PATH = Path.home() / ".verwaltung_venv"
PYTHON_BIN = VENV_PATH / ("Scripts" if platform.system() == "Windows" else "bin") / ("python.exe" if platform.system() == "Windows" else "python")

def create_and_setup_venv():
    print(f"Creating virtualenv at {VENV_PATH}")
    venv.create(VENV_PATH, with_pip=True)
    subprocess.check_call([PYTHON_BIN, "-m", "pip", "install", "--upgrade", "pip"])
    subprocess.check_call([PYTHON_BIN, "-m", "pip", "install", "--upgrade", "flask", "sqlalchemy", "pypdf", "cryptography", "aiosqlite"])

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
    from flask import Flask, request, redirect, url_for, render_template_string, jsonify, send_from_directory, render_template, abort, send_file, flash
    from sqlalchemy import create_engine, inspect
    from sqlalchemy.orm import sessionmaker, joinedload, Session
    from sqlalchemy.exc import SQLAlchemyError
    from db_defs import *
    from pypdf import PdfReader, PdfWriter
    from pypdf.generic import NameObject
    import io
    from markupsafe import escape
    import html
    from sqlalchemy import Date, DateTime
    import cryptography
    import aiosqlite
    import datetime

    from db_interface import *
except ModuleNotFoundError:
    if not VENV_PATH.exists():
        create_and_setup_venv()
    else:
        try:
            subprocess.check_call([PYTHON_BIN, "-m", "pip", "install", "-q", "--upgrade", "flask", "sqlalchemy", "pypdf", "cryptography", "aiosqlite"])
        except subprocess.CalledProcessError:
            shutil.rmtree(VENV_PATH)
            create_and_setup_venv()
            restart_with_venv()
    try:
        restart_with_venv()
    except KeyboardInterrupt:
        print("You cancelled installation")
        sys.exit(0)

app = Flask(__name__)
engine = create_engine("sqlite:///database.db")
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

COLUMN_LABELS = {
    "abteilung.abteilungsleiter_id": "Abteilungsleiter",
    "person.first_name": "Vorname",
    "person.last_name": "Nachname"
}

FK_DISPLAY_COLUMNS = {
    "person": ["title", "first_name", "last_name"]
}

WIZARDS = {}

WIZARDS["transponder"] = {
    "title": "Transponder erstellen",
    "model": Transponder,
    "fields": [
        {"name": "issuer_id", "type": "number", "label": "Ausgeber-ID", "required": True},
        {"name": "owner_id", "type": "number", "label": "Besitzer-ID"},
        {"name": "serial_number", "type": "text", "label": "Seriennummer"},
        {"name": "got_date", "type": "date", "label": "Ausgabedatum"},
    ],
    "subforms": [
        {
            "name": "room_links",
            "label": "Zugeordnete Räume",
            "model": TransponderToRoom,
            "foreign_key": "transponder_id",
            "fields": [
                {"name": "room_id", "type": "number", "label": "Raum-ID"},
            ]
        }
    ]
}

HANDLER_MAP = {
    "person": PersonWithContactHandler,
    "abteilung": AbteilungHandler,
    "person_abteilung": PersonToAbteilungHandler,
    "building": BuildingHandler,
    "room": RoomHandler,
    "person_room": PersonToRoomHandler,
    "transponder": TransponderHandler,
    "transponder_room": TransponderToRoomHandler,
}

EMAIL_REGEX = re.compile(r"^[^@]+@[^@]+\.[^@]+$")

def parse_buildings_csv(csv_text):
    session = Session()

    if not isinstance(csv_text, str):
        raise TypeError("csv_text muss ein String sein")
    if not csv_text.strip():
        raise ValueError("csv_text ist leer")

    csv_io = io.StringIO(csv_text)
    reader = csv.reader(csv_io, delimiter=',', quotechar='"')

    header_found = False

    for row in reader:
        if len(row) != 2:
            continue

        if not header_found:
            # Prüfen, ob erste Zeile die Header ist
            if row[0].strip().lower() == "gebaeude_name" and row[1].strip().lower() == "abkuerzung":
                header_found = True
                continue  # Header überspringen
            else:
                raise ValueError("Ungültige Header-Zeile: " + str(row))

        gebaeude_name = row[0].strip()
        abkuerzung = row[1].strip()

        if not gebaeude_name or not abkuerzung:
            continue  # Zeile überspringen, wenn leer

        building_insert = {
            "name": gebaeude_name,
            "abkuerzung": abkuerzung
        }

        handler = BuildingHandler(session)
        handler.insert_data(building_insert)

def insert_tu_dresden_buildings ():
    csv_input = '''gebaeude_name,abkuerzung
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


def is_valid_email(email):
    return bool(EMAIL_REGEX.match(email.strip()))

def column_label(table, col):
    return COLUMN_LABELS.get(f"{table}.{col}", col.replace("_id", "").replace("_", " ").capitalize())

@app.route("/")
def index():
    tables = [cls.__tablename__ for cls in Base.__subclasses__()]

    wizard_routes = []
    for rule in app.url_map.iter_rules():
        if rule.rule.startswith("/wizard") and rule.rule != "/wizard":
            wizard_routes.append(rule.rule)
    wizard_routes = sorted(wizard_routes)

    return render_template("index.html", tables=tables, wizard_routes=wizard_routes)

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
            return f'<input type="number" name="{html.escape(input_name)}" value="{val}" class="cell-input">', True
        if "FLOAT" in col_type_str or "DECIMAL" in col_type_str or "NUMERIC" in col_type_str:
            return f'<input type="number" step="any" name="{html.escape(input_name)}" value="{val}" class="cell-input">', True
        if "TEXT" in col_type_str or "VARCHAR" in col_type_str or "CHAR" in col_type_str:
            return f'<input type="text" name="{html.escape(input_name)}" value="{val}" class="cell-input">', True
        if "DATE" in col_type_str:
            return f'<input type="date" name="{html.escape(input_name)}" value="{val}" class="cell-input">', True

        return f'<input type="text" name="{html.escape(input_name)}" value="{val}" class="cell-input">', True
    except Exception as e:
        app.logger.error(f"Fehler beim Generieren des Input-Feldes für Spalte {col.name}: {e}")
        return f'<input type="text" name="{html.escape(input_name)}" value="" class="cell-input">', True

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
            except AttributeError:
                value = None
            except Exception as e:
                app.logger.error(f"Fehler beim Zugriff auf Spalte {col_name} der Tabelle {table_name}: {e}")
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
            except Exception as e:
                app.logger.error(f"Fehler bei der Generierung des Input-Felds für {col.name}: {e}")
                input_html = '<input value="Error">'
                valid = True

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
        except Exception as e:
            app.logger.error(f"Fehler bei der Generierung des neuen Input-Felds für {col.name}: {e}")
            input_html = '<input value="Error">'
        label = get_column_label(table_name, col.name)
        new_entry_inputs.append((input_html, label))

    column_labels = [get_column_label(table_name, col.name) for col in columns]

    return column_labels, row_html, new_entry_inputs, row_ids, table_has_missing_inputs

def load_static_file(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        app.logger.error(f"Fehler beim Laden der Datei {path}: {e}")
        return ""

@app.route("/table/<table_name>")
def table_view(table_name):
    session = Session()
    cls = get_model_class_by_tablename(table_name)
    if cls is None:
        abort(404, description="Tabelle nicht gefunden")

    column_labels, row_html, new_entry_inputs, row_ids, table_has_missing_inputs = prepare_table_data(session, cls, table_name)

    javascript_code = load_static_file("static/table_scripts.js").replace("{{ table_name }}", table_name)

    row_data = list(zip(row_html, row_ids))

    missing_data_messages = []
    if table_has_missing_inputs:
        link = url_for("table_view", table_name=table_name)
        missing_data_messages.append(
            '<div class="warning">⚠️ Fehlende Eingabeoptionen für Tabelle</div>'
        )

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
def add_entry(table_name):
    session = Session()
    cls = next((c for c in Base.__subclasses__() if c.__tablename__ == table_name), None)
    if not cls:
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
        return jsonify(success=True)
    except Exception as e:
        session.rollback()
        return jsonify(success=False, error=str(e))

@app.route("/update/<table_name>", methods=["POST"])
def update_entry(table_name):
    session = Session()
    cls = next((c for c in Base.__subclasses__() if c.__tablename__ == table_name), None)
    if not cls:
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
        return jsonify(success=True)
    except Exception as e:
        session.rollback()
        return jsonify(success=False, error=str(e))

@app.route("/delete/<table_name>", methods=["POST"])
def delete_entry(table_name):
    session = Session()
    cls = next((c for c in Base.__subclasses__() if c.__tablename__ == table_name), None)
    if not cls:
        return jsonify(success=False, error="Tabelle nicht gefunden")

    try:
        json_data = request.get_json()

        if not json_data or "id" not in json_data:
            return jsonify(success=False, error="Keine ID angegeben")

        row_id = json_data["id"]
        if not isinstance(row_id, int):
            try:
                row_id = int(row_id)
            except ValueError:
                return jsonify(success=False, error="Ungültige ID")

        obj = session.query(cls).get(row_id)
        if not obj:
            return jsonify(success=False, error="Datensatz nicht gefunden")

        session.delete(obj)
        session.commit()
        return jsonify(success=True)

    except Exception as e:
        session.rollback()
        return jsonify(success=False, error=str(e))


@app.route("/aggregate/")
def aggregate_index():
    return render_template("aggregate_index.html")  # Optional – nur als Startseite für Aggregates


@app.route("/aggregate/transponder")
def aggregate_transponder_view():
    session = Session()

    # Filter aus Query-Params
    show_only_unreturned = request.args.get("unreturned") == "1"
    owner_filter = request.args.get("owner", "").strip()
    issuer_filter = request.args.get("issuer", "").strip()

    try:
        query = session.query(Transponder) \
            .options(
                joinedload(Transponder.owner),
                joinedload(Transponder.issuer),
                joinedload(Transponder.room_links).joinedload(TransponderToRoom.room).joinedload(Room.building)
            )

        # Filter nur nicht zurückgegebene
        if show_only_unreturned:
            query = query.filter(Transponder.return_date.is_(None))

        # Filter für owner (besitzer)
        if owner_filter:
            # Hier gehen wir davon aus, dass owner_filter eine Teilstring-Suche auf Vor- oder Nachname erlaubt
            query = query.join(Transponder.owner).filter(
                (Transponder.owner.first_name.ilike(f"%{owner_filter}%")) |
                (Transponder.owner.last_name.ilike(f"%{owner_filter}%"))
            )

        # Filter für issuer (ausgeber)
        if issuer_filter:
            query = query.join(Transponder.issuer).filter(
                (Transponder.issuer.first_name.ilike(f"%{issuer_filter}%")) |
                (Transponder.issuer.last_name.ilike(f"%{issuer_filter}%"))
            )

        transponder_list = query.all()

        rows = []
        for t in transponder_list:
            owner = t.owner
            issuer = t.issuer
            rooms = [link.room for link in t.room_links if link.room]
            buildings = list({r.building.name if r.building else "?" for r in rooms})

            row = {
                "ID": t.id,
                "Seriennummer": t.serial_number or "-",
                "Ausgegeben an": f"{owner.first_name} {owner.last_name}" if owner else "Unbekannt",
                "Ausgegeben durch": f"{issuer.first_name} {issuer.last_name}" if issuer else "Unbekannt",
                "Ausgabedatum": t.got_date.isoformat() if t.got_date else "-",
                "Rückgabedatum": t.return_date.isoformat() if t.return_date else "Nicht zurückgegeben",
                "Gebäude": ", ".join(sorted(buildings)) if buildings else "-",
                "Räume": ", ".join(sorted(set(f"{r.name} ({r.floor}.OG)" for r in rooms))) if rooms else "-",
                "Kommentar": t.comment or "-",
            }
            rows.append(row)

        column_labels = list(rows[0].keys()) if rows else []
        column_labels.append("PDF")


        row_data = [
            [html.escape(str(row[col])) for col in column_labels if col != "PDF"] +
            [f"<a href='http://localhost:5000/generate_pdf/schliessmedien/?issuer_id={issuer.id if issuer else ''}&owner_id={owner.id if owner else ''}&transponder_id={t.id}'><img src='../static/pdf.svg' height=32 width=32></a>"]
            for t, row, owner, issuer in [(t, row, t.owner, t.issuer) for t, row in zip(transponder_list, rows)]
        ]


        # Filter-Dict zum dynamischen Befüllen des Formulars und Anzeige des Status
        filters = {
            "Nur nicht zurückgegebene anzeigen": show_only_unreturned,
            "Besitzer (Ausgegeben an)": owner_filter,
            "Ausgeber (Ausgegeben durch)": issuer_filter
        }

        return render_template(
            "aggregate_view.html",
            title="Ausgegebene Transponder",
            column_labels=column_labels,
            row_data=row_data,
            filters=filters,
            # toggle_url nicht mehr hart codiert, hier auf Basis aktueller Filter mit geändertem "unreturned"
            toggle_url=url_for(
                "aggregate_transponder_view",
                unreturned="0" if show_only_unreturned else "1",
                owner=owner_filter,
                issuer=issuer_filter
            )
        )

    except Exception as e:
        app.logger.error(f"Fehler beim Laden der Transponder-Aggregatsansicht: {e}")
        return render_template("error.html", message="Fehler beim Laden der Daten.")

@app.route("/aggregate/inventory")
def aggregate_inventory_view():
    session = None
    try:
        session = Session()

        # Query-Parameter auslesen
        show_only_unreturned = request.args.get("unreturned") == "1"
        owner_filter = request.args.get("owner", type=int)
        issuer_filter = request.args.get("issuer", type=int)

        # Grundquery mit Joins
        query = session.query(Inventory) \
            .options(
                joinedload(Inventory.owner),
                joinedload(Inventory.issuer),
                joinedload(Inventory.object).joinedload(Object.category),
                joinedload(Inventory.kostenstelle),
                joinedload(Inventory.abteilung),
                joinedload(Inventory.professorship),
                joinedload(Inventory.room)
            )

        # Filter anwenden
        if show_only_unreturned:
            query = query.filter(Inventory.return_date.is_(None))

        if owner_filter:
            query = query.filter(Inventory.owner_id == owner_filter)

        if issuer_filter:
            query = query.filter(Inventory.issuer_id == issuer_filter)

        inventory_list = query.all()

        # Hilfsfunktionen
        def person_name(p):
            if p:
                return f"{p.first_name} {p.last_name}"
            return "Unbekannt"

        def category_name(c):
            return c.name if c else "-"

        def kostenstelle_name(k):
            return k.name if k else "-"

        def abteilung_name(a):
            return a.name if a else "-"

        def professorship_name(pf):
            return pf.name if pf else "-"

        def room_name(r):
            if r:
                floor_str = f"{r.floor}.OG" if r.floor is not None else "?"
                return f"{r.name} ({floor_str})"
            return "-"

        rows = []
        for inv in inventory_list:
            row = {
                "ID": inv.id,
                "Seriennummer": inv.serial_number or "-",
                "Objekt": inv.object.name if inv.object else "-",
                "Kategorie": category_name(inv.object.category) if inv.object else "-",
                "Anlagennummer": inv.anlagennummer or "-",
                "Ausgegeben an": person_name(inv.owner),
                "Ausgegeben durch": person_name(inv.issuer),
                "Ausgabedatum": inv.got_date.isoformat() if inv.got_date else "-",
                "Rückgabedatum": inv.return_date.isoformat() if inv.return_date else "Nicht zurückgegeben",
                "Raum": room_name(inv.room),
                "Abteilung": abteilung_name(inv.abteilung),
                "Professur": professorship_name(inv.professorship),
                "Kostenstelle": kostenstelle_name(inv.kostenstelle),
                "Preis": f"{inv.price:.2f} €" if inv.price is not None else "-",
                "Kommentar": inv.comment or "-"
            }
            rows.append(row)

        # Für Filter: Alle User (Owner und Issuer) holen (vereinfachend hier alle Personen)
        # Du kannst ggf. nur Owner oder Issuer spezifisch holen, falls nötig
        people_query = session.query(Person).order_by(Person.last_name, Person.first_name).all()
        people = [{"id": p.id, "name": f"{p.first_name} {p.last_name}"} for p in people_query]

        column_labels = list(rows[0].keys()) if rows else []
        row_data = [[escape(str(row[col])) for col in column_labels] for row in rows]

        return render_template(
            "aggregate_view.html",
            title="Inventarübersicht",
            column_labels=column_labels,
            row_data=row_data,
            filters={
                "unreturned": show_only_unreturned,
                "owner": owner_filter,
                "issuer": issuer_filter,
            },
            people=people,
            url_for_view=url_for("aggregate_inventory_view")
        )
    except Exception as e:
        app.logger.error(f"Fehler beim Laden der Inventar-Aggregatsansicht: {e}")
        return render_template("error.html", message="Fehler beim Laden der Daten.")
    finally:
        if session:
            session.close()


@app.route("/wizard")
def wizard_index():
    wizard_routes = []
    for rule in app.url_map.iter_rules():
        if rule.rule.startswith("/wizard") and rule.rule != "/wizard":
            wizard_routes.append(rule.rule)
    wizard_routes = sorted(wizard_routes)
    return render_template("wizard_index.html", wizard_routes=wizard_routes)

@app.route("/wizard/person", methods=["GET", "POST"])
def wizard_person():
    session = Session()
    error = None
    success = False

    if request.method == "POST":
        try:
            title = request.form.get("title", "").strip() or None
            first_name = request.form.get("first_name", "").strip()
            last_name = request.form.get("last_name", "").strip()
            comment = request.form.get("comment", "").strip() or None
            image_url = request.form.get("image_url", "").strip() or None

            if not first_name or not last_name:
                raise ValueError("Vorname und Nachname sind Pflichtfelder.")

            emails = request.form.getlist("email[]")
            phones = request.form.getlist("phone[]")
            faxes = request.form.getlist("fax[]")
            comments = request.form.getlist("contact_comment[]")

            valid_emails = [e.strip() for e in emails if e.strip() != ""]
            if len(valid_emails) == 0:
                raise ValueError("Mindestens eine Email muss eingegeben werden.")

            for email in valid_emails:
                if not is_valid_email(email):
                    raise ValueError(f"Ungültige Email-Adresse: {email}")

            new_person = Person(
                title=title,
                first_name=first_name,
                last_name=last_name,
                comment=comment,
                image_url=image_url
            )
            session.add(new_person)
            session.flush()

            max_len = max(len(emails), len(phones), len(faxes), len(comments))
            for i in range(max_len):
                email_val = emails[i].strip() if i < len(emails) else None
                phone_val = phones[i].strip() if i < len(phones) else None
                fax_val = faxes[i].strip() if i < len(faxes) else None
                comment_val = comments[i].strip() if i < len(comments) else None

                if any([email_val, phone_val, fax_val, comment_val]):
                    if email_val and not is_valid_email(email_val):
                        raise ValueError(f"Ungültige Email-Adresse in Kontakt: {email_val}")

                    contact = PersonContact(
                        person_id=new_person.id,
                        email=email_val,
                        phone=phone_val,
                        fax=fax_val,
                        comment=comment_val
                    )
                    session.add(contact)

            session.commit()
            success = True

        except Exception as e:
            session.rollback()
            error = str(e) + "\n" + traceback.format_exc()

        finally:
            session.close()

    return render_template("person_wizard.html", success=success, error=error)

@app.route("/map-editor")
def map_editor():
    return render_template("map_editor.html")

@app.route("/wizard/transponder", methods=["GET", "POST"])
def run_wizard():
    return _wizard_internal("transponder")

def _wizard_internal(name):
    config = WIZARDS.get(name)
    if not config:
        abort(404)

    # JSON-sichere Version ohne nicht-serialisierbare Objekte
    def get_json_safe_config(config):
        safe = deepcopy(config)
        for sub in safe.get("subforms", []):
            sub.pop("model", None)
            sub.pop("foreign_key", None)
        safe.pop("model", None)
        return safe

    session = Session()
    success = False
    error = None

    if request.method == "POST":
        try:
            main_model = config["model"]
            main_data = {
                f["name"]: request.form.get(f["name"], "").strip() or None
                for f in config["fields"]
            }

            if any(f.get("required") and not main_data[f["name"]] for f in config["fields"]):
                raise ValueError("Pflichtfelder fehlen.")

            main_instance = main_model(**main_data)
            session.add(main_instance)
            session.flush()

            for sub in config.get("subforms", []):
                model = sub["model"]
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
                        session.add(model(**entry))

            session.commit()
            success = True

        except Exception as e:
            session.rollback()
            error = str(e)
        finally:
            session.close()

    return render_template("wizard.html", config=config, config_json=get_json_safe_config(config), success=success, error=error)

def get_abteilung_metadata(abteilung_id: int) -> dict:
    session = Session()
    try:
        abteilung = session.query(Abteilung).filter(Abteilung.id == abteilung_id).one_or_none()
        if abteilung is None:
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
                "first_name": abteilung.leiter.first_name,
                "last_name": abteilung.leiter.last_name,
                "title": abteilung.leiter.title
            }

        # Falls du die Personen mit drin haben willst
        for person_to_abteilung in abteilung.persons:
            person = person_to_abteilung.person
            if person:
                metadata["personen"].append({
                    "id": person.id,
                    "first_name": person.first_name,
                    "last_name": person.last_name,
                    "title": person.title
                })

        return metadata

    except SQLAlchemyError as e:
        return {"error": str(e)}
    finally:
        session.close()


def generate_fields_for_schluesselausgabe_from_metadata(
    issuer: dict,
    owner: dict,
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
        phone = contact.get("phone", "").strip()
        email = contact.get("email", "").strip()

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
            # Hier wird die Abteilung eingetragen, wenn vorhanden,
            # ansonsten wie gehabt der Vorname des Issuers
            if abteilung and "name" in abteilung:
                value = abteilung["name"]

        elif name == "Text3":
            first_name = issuer.get("first_name", "")
            last_name = issuer.get("last_name", "")
            value = f"{last_name}, {first_name}"

        elif name == "Text4":
            value = extract_contact_string(issuer)
        elif name == "Text5":
            value = abteilung.get("name", "") if abteilung else ""
        elif name == "Text7":
            value = owner.get("last_name", "") + ", " + owner.get("first_name", "") if owner else ""
        elif name == "Text8":
            value = extract_contact_string(owner)

        elif name.startswith("GebäudeRow"):
            index = int(name.replace("GebäudeRow", "")) - 1
            if 0 <= index < len(transponder.get("rooms", [])):
                building = transponder["rooms"][index].get("building")
                if building:
                    value = building.get("name", "")
        elif name.startswith("RaumRow"):
            index = int(name.replace("RaumRow", "")) - 1
            if 0 <= index < len(transponder.get("rooms", [])):
                value = transponder["rooms"][index].get("name", "")
        elif name.startswith("SerienNrSchlüsselNrRow1"):
            if transponder.get("serial_number"):
                value = transponder["serial_number"]
        elif name.startswith("AnzahlRow1"):
            value = "1"

        elif name == "Datum Übergebende:r":
            if transponder.get("got_date"):
                value = transponder["got_date"].strftime("%d.%m.%Y")
        elif name == "Datum Übernehmende:r":
            if transponder.get("return_date"):
                value = transponder["return_date"].strftime("%d.%m.%Y")
        elif name == "Weitere Anmerkungen":
            if transponder.get("comment"):
                value = transponder["comment"]

        data[name] = value

    return data


def get_transponder_metadata(transponder_id: int) -> dict:
    session = Session()

    try:
        transponder = session.query(Transponder).filter(Transponder.id == transponder_id).one_or_none()

        if transponder is None:
            return None

        metadata = {
            "id": transponder.id,
            "serial_number": transponder.serial_number,
            "got_date": transponder.got_date,
            "return_date": transponder.return_date,
            "comment": transponder.comment,

            "issuer": None,
            "owner": None,
            "rooms": []
        }

        if transponder.issuer is not None:
            metadata["issuer"] = {
                "id": transponder.issuer.id,
                "first_name": transponder.issuer.first_name,
                "last_name": transponder.issuer.last_name,
                "title": transponder.issuer.title
            }

        if transponder.owner is not None:
            metadata["owner"] = {
                "id": transponder.owner.id,
                "first_name": transponder.owner.first_name,
                "last_name": transponder.owner.last_name,
                "title": transponder.owner.title
            }

        for link in transponder.room_links:
            room = link.room

            room_data = {
                "id": room.id,
                "name": room.name,
                "floor": room.floor,
                "building": None
            }

            if room.building is not None:
                room_data["building"] = {
                    "id": room.building.id,
                    "name": room.building.name,
                    "building_number": room.building.building_number,
                    "abkuerzung": room.building.abkuerzung
                }

            metadata["rooms"].append(room_data)

        return metadata

    except SQLAlchemyError as e:
        return {"error": str(e)}

def get_person_metadata(person_id: int) -> dict:
    session = Session()

    try:
        person = session.query(Person).filter(Person.id == person_id).one_or_none()

        if person is None:
            return {"error": f"No person found with id {person_id}"}

        metadata = {
            "id": person.id,
            "title": person.title,
            "first_name": person.first_name,
            "last_name": person.last_name,
            "created_at": person.created_at,
            "comment": person.comment,
            "image_url": person.image_url,

            "contacts": [],
            "rooms": [],
            "transponders_issued": [],
            "transponders_owned": [],
            "departments": [],
            "person_abteilungen": [],
            "professorships": []
        }

        for contact in person.contacts:
            metadata["contacts"].append({
                "id": contact.id,
                "phone": contact.phone,
                "fax": contact.fax,
                "email": contact.email,
                "comment": contact.comment
            })

        for room in person.rooms:
            metadata["rooms"].append({
                "id": room.id,
                "room_id": getattr(room, "room_id", None),  # adapt if necessary
                "comment": getattr(room, "comment", None)
            })

        for transponder in person.transponders_issued:
            metadata["transponders_issued"].append({
                "id": transponder.id,
                "number": getattr(transponder, "number", None),
                "owner_id": transponder.owner_id
            })

        for transponder in person.transponders_owned:
            metadata["transponders_owned"].append({
                "id": transponder.id,
                "number": getattr(transponder, "number", None),
                "issuer_id": transponder.issuer_id
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

        for prof in person.professorships:
            metadata["professorships"].append({
                "id": prof.id,
                "professorship_id": getattr(prof, "professorship_id", None),
                "title": getattr(prof, "title", None)
            })

        return metadata

    except SQLAlchemyError as e:
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
def generate_pdf():
    TEMPLATE_PATH = 'pdfs/ausgabe_schliessmedien.pdf'

    issuer_id = request.args.get('issuer_id')
    owner_id = request.args.get('owner_id')
    transponder_id = request.args.get('transponder_id')

    missing = []
    if not issuer_id:
        missing.append("issuer_id (Ausgeber-ID)")
    if not owner_id:
        missing.append("owner_id (Besitzer-ID)")
    if not transponder_id:
        missing.append("transponder_id")

    if missing:
        return render_template_string(
            "<h1>Fehlende Parameter</h1><ul>{% for m in missing %}<li>{{ m }}</li>{% endfor %}</ul>",
            missing=missing
        ), 400

    issuer = get_person_metadata(issuer_id)
    owner = get_person_metadata(owner_id)
    transponder = get_transponder_metadata(transponder_id)

    not_found = []
    if issuer is None:
        not_found.append(f"Keine Person mit issuer_id: {issuer_id}")
    if owner is None:
        not_found.append(f"Keine Person mit owner_id: {owner_id}")
    if transponder is None:
        not_found.append(f"Kein Transponder mit transponder_id: {transponder_id}")

    if not_found:
        return render_template_string(
            "<h1>Nicht Gefunden</h1><ul>{% for msg in not_found %}<li>{{ msg }}</li>{% endfor %}</ul>",
            not_found=not_found
        ), 404

    field_data = generate_fields_for_schluesselausgabe_from_metadata(issuer, owner, transponder, )

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
def transponder_form():
    session = Session()
    persons = session.query(Person).order_by(Person.last_name).all()
    transponders = session.query(Transponder).options(
        joinedload(Transponder.owner)
    ).order_by(Transponder.serial_number).all()

    return render_template("transponder_form.html",
        config={"title": "Transponder-Ausgabe / Rückgabe"},
        persons=persons,
        transponders=transponders,
        current_date=date.today().isoformat()
    )

@app.route("/transponder/ausgabe", methods=["POST"])
def transponder_ausgabe():
    person_id = request.form.get("person_id")
    transponder_id = request.form.get("transponder_id")
    got_date_str = request.form.get("got_date")

    session = Session()

    try:
        transponder = session.get(Transponder, int(transponder_id))
        transponder.owner_id = int(person_id)
        transponder.got_date = date.fromisoformat(got_date_str)
        session.session.commit()
        flash("Transponder erfolgreich ausgegeben.", "success")
    except Exception as e:
        session.session.rollback()
        flash(f"Fehler bei Ausgabe: {str(e)}", "danger")

    return redirect(url_for("transponder.transponder_form"))

@app.route("/transponder/rueckgabe", methods=["POST"])
def transponder_rueckgabe():
    transponder_id = request.form.get("transponder_id")
    return_date_str = request.form.get("return_date")

    session = Session()

    try:
        transponder = session.session.get(Transponder, int(transponder_id))
        transponder.return_date = date.fromisoformat(return_date_str)
        transponder.owner_id = None
        session.commit()
        flash("Transponder erfolgreich zurückgenommen.", "success")
    except Exception as e:
        session.rollback()
        flash(f"Fehler bei Rückgabe: {str(e)}", "danger")

    return redirect(url_for("transponder.transponder_form"))

def get_handler_instance(handler_name):
    handler_class = HANDLER_MAP.get(handler_name)
    if not handler_class:
        return None, f"Unbekannter Handler: {handler_name}"
    session = Session()
    return handler_class(session), None

@app.route("/user_edit/")
def user_edit_index():
    # Liste aller Handlernamen als Links anzeigen
    handler_names = list(HANDLER_MAP.keys())

    # Einfaches HTML mit Links generieren
    html = "<h1>Verfügbare Handler</h1><ul>"
    for name in handler_names:
        html += f'<li><a href="/user_edit/{name}">{name}</a></li>'
    html += "</ul>"
    return html

@app.route("/user_edit/<handler_name>", methods=["GET", "POST"])
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
def home():
    return render_template('floorplan.html')


@app.route("/api/save_map", methods=["POST"])
def save_map():
    data = request.get_json()

    if not isinstance(data, list):
        return jsonify({"error": "Expected list of room layout objects"}), 400

    session = Session()

    try:
        for item in data:
            name = item.get("name")
            x = item.get("x")
            y = item.get("y")
            width = item.get("width")
            height = item.get("height")

            if not all(isinstance(v, (int, float)) for v in [x, y, width, height]) or not isinstance(name, str):
                return jsonify({"error": f"Invalid data format in item: {item}"}), 400

            room = session.query(Room).filter(Room.name == name).one_or_none()

            if room is None:
                return jsonify({"error": f"Room with name '{name}' not found"}), 404

            if room.layout:
                room.layout.x = x
                room.layout.y = y
                room.layout.width = width
                room.layout.height = height
            else:
                layout = RoomLayout(
                    room=room,
                    x=x,
                    y=y,
                    width=width,
                    height=height
                )
                session.add(layout)

        session.commit()
        return jsonify({"status": "success"}), 200

    except SQLAlchemyError as e:
        session.rollback()
        return jsonify({"error": str(e)}), 500

    finally:
        session.close()


@app.route("/api/delete_room", methods=["POST"])
def delete_room():
    data = request.get_json()

    if not isinstance(data, dict):
        return jsonify({"error": "Expected JSON object"}), 400

    name = data.get("name")
    building_id = data.get("building_id")

    if not isinstance(name, str):
        return jsonify({"error": "Missing or invalid 'name' field"}), 400

    session = Session()

    try:
        query = session.query(Room).filter(Room.name == name)

        if building_id is not None:
            query = query.filter(Room.building_id == building_id)

        room = query.one_or_none()

        if room is None:
            return jsonify({"error": f"Room with name '{name}' not found"}), 404

        session.delete(room)
        session.commit()

        return jsonify({"status": f"Room '{name}' deleted successfully"}), 200

    except SQLAlchemyError as e:
        session.rollback()
        return jsonify({"error": str(e)}), 500

    finally:
        session.close()

@app.route("/api/update_room_name", methods=["POST"])
def update_room_name():
    data = request.get_json()

    old_name = data.get("old_name")
    new_name = data.get("new_name")
    room_id = data.get("id")
    building_id = data.get("building_id")

    if not new_name or (not old_name and not room_id):
        return jsonify({"error": "Missing 'new_name' and 'id' or 'old_name'"}), 400

    session = Session()

    try:
        query = session.query(Room)

        if room_id:
            query = query.filter(Room.id == room_id)
        elif old_name:
            query = query.filter(Room.name == old_name)
            if building_id is not None:
                query = query.filter(Room.building_id == building_id)

        room = query.one_or_none()

        if room is None:
            return jsonify({"error": "Room not found"}), 404

        room.name = new_name
        session.commit()

        return jsonify({"status": "success", "room_id": room.id, "new_name": room.name}), 200

    except SQLAlchemyError as e:
        session.rollback()
        return jsonify({"error": str(e)}), 500

    finally:
        session.close()

if __name__ == "__main__":
    insert_tu_dresden_buildings()

    app.run(debug=True, port=5000)
