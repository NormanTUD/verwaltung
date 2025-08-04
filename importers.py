from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from flask import Blueprint, request, jsonify, render_template
from auth import admin_required
import pandas as pd
import io
import json
import re
from db import *

importers_bp = Blueprint('importers', __name__)

ALIAS_MAPPING = {
    "Person.vorname": ["Vorname", "first_name", "Vorname"],
    "Person.nachname": ["Nachname", "last_name", "surname", "Name"],  # bleibt wegen "Name"!
    "Person.title": ["Titel", "Title"],
    "Person.kommentar": ["Kommentar", "Note", "Bemerkung"],
    "PersonContact.email": ["Email", "E-Mail", "email"],
    "PersonContact.phone": ["Telefon", "Phone", "Telefonnummer", "Mobil", "Dienstlich"],
    "Abteilung.name": ["Abteilung", "Department", "Abteilungsname", "Struktureinheit"],
    "Abteilung.abteilungsleiter": ["AL", "Abteilungsleiter", "Leiter"],
    "Abteilung.vertretung": ["Vertretung", "Vertreter", "Stellvertretung"],
    "Building.name": ["Gebäude", "Building", "Gebäudename"],
    "Raum.name": ["Raum", "Room", "Raumname"],
    "PrincipalInvestigatorToAbteilung.person": ["PI", "Principal Investigator"],
}

def get_or_create(model_class, filter_data: dict, create_data: dict, session=None):
    """
    Holt ein existierendes Objekt anhand der Filterdaten oder erstellt es neu.
    
    :param model_class: SQLAlchemy Modelklasse
    :param filter_data: dict mit Attributen zur Identifikation
    :param create_data: dict mit Attributen zur Erstellung
    :param session: optional, SQLAlchemy-Session. Wenn None, wird über das Objekt eine gezogen.
    :return: (Objekt, True wenn erstellt, sonst False)
    """
    try:
        if session is None:
            # Versuche eine Session aus einem der Filter-Werte zu bekommen
            for value in filter_data.values():
                if hasattr(value, '__class__'):
                    session = Session.object_session(value)
                    break
        if session is None:
            raise RuntimeError("Session muss übergeben werden oder aus Objekten ableitbar sein")

        instance = session.query(model_class).filter_by(**filter_data).first()
        if instance:
            # Optional aktualisieren mit zusätzlichen Daten
            for key, value in create_data.items():
                setattr(instance, key, value)
            return instance, False
        else:
            instance = model_class(**create_data)
            session.add(instance)
            return instance, True

    except Exception as e:
        raise RuntimeError(f"get_or_create fehlgeschlagen für {model_class.__name__}: {str(e)}")

def split_name(name: str) -> dict:
    """
    Erwartet ein Format wie 'Nachname, Vorname' oder 'Vorname Nachname'.
    Gibt ein dict mit {'vorname': ..., 'nachname': ...} zurück.
    """
    if not name or not isinstance(name, str):
        return {"vorname": None, "nachname": None}
    
    parts = name.strip().split(",", 1)
    if len(parts) == 2:
        return {"nachname": parts[0].strip(), "vorname": parts[1].strip()}
    
    parts = name.strip().split()
    if len(parts) >= 2:
        return {"vorname": parts[0].strip(), "nachname": " ".join(parts[1:]).strip()}
    
    return {"vorname": None, "nachname": name.strip()}

def resolve_person_by_name(name_str):
    """
    Sucht oder erstellt eine Person anhand des Namens (Nachname, Vorname)
    """
    name_parts = split_name(name_str)
    if not name_parts["vorname"] or not name_parts["nachname"]:
        return None
    person_filter = {
        "vorname": name_parts["vorname"],
        "nachname": name_parts["nachname"]
    }
    return get_or_create(Person, person_filter, person_filter)[0]


# Hilfsfunktion, um Vorwahl aus Spaltennamen zu extrahieren
def extract_area_code(column_name: str) -> str:
    """
    Extrahiert eine Vorwahl aus Spaltennamen wie "Telefon 463-" oder "Telefon 1234-"
    Gibt die Vorwahl als String zurück oder "" wenn keine gefunden wurde.
    """
    match = re.search(r'(\d+)[-\s]*$', column_name)
    if match:
        return match.group(1)
    return ""

def match_column(col_name):
    if not isinstance(col_name, str):
        return None

    col_name_clean = col_name.strip().lower()
    for key, aliases in ALIAS_MAPPING.items():
        # exakte Übereinstimmung (case-insensitive)
        aliases_lower = [a.lower() for a in aliases]
        if col_name_clean in aliases_lower:
            return key

        # startswith Check (case-insensitive)
        for alias_lower in aliases_lower:
            if col_name_clean.startswith(alias_lower):
                return key

    return None

@importers_bp.route("/import/", methods=["GET", "POST"])
@login_required
@admin_required
def import_upload():
    if request.method == "POST":
        file = request.files.get("file")
        csvtext = request.form.get("csvtext")

        df = None
        filename = None

        try:
            if file and file.filename:
                filename = secure_filename(file.filename)
                ext = os.path.splitext(filename)[1].lower()
                if ext == ".csv":
                    df = pd.read_csv(file)
                elif ext in [".xls", ".xlsx"]:
                    df = pd.read_excel(file)
                else:
                    return "Nur CSV, XLS, XLSX-Dateien erlaubt.", 400
            elif csvtext:
                try:
                    df = pd.read_csv(io.StringIO(csvtext), sep="\t")
                except pd.errors.ParserError:
                    df = pd.read_csv(io.StringIO(csvtext), sep=",")
        except Exception as e:
            return f"Fehler beim Einlesen der Datei: {str(e)}", 400

        if df is None or df.empty:
            return "Fehler beim Einlesen der Datei oder Datei leer", 400

        column_map = {}
        for col in df.columns:
            column_map[col] = match_column(col)

        return render_template(
            "import_preview.html",
            columns=df.columns,
            rows=df.to_dict(orient="records"),
            column_map=column_map,
            possible_targets=list(ALIAS_MAPPING.keys()),
            data_json=df.to_dict(orient="records")
        )

    return render_template("import_upload.html")



@importers_bp.route("/import/commit", methods=["POST"])
@login_required
@admin_required
def import_commit():
    session = Session()
    log = []
    errors = []

    try:
        raw_json = request.form.get("data_json")
        if not raw_json:
            return __import_render_error("Fehler: Kein JSON übergeben!", log, errors)

        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError as e:
            return __import_render_error(f"Fehler beim Parsen von JSON: {str(e)}", log, errors)

        structured_map, area_code_map = __import_extract_mappings(request.form)

        for row_index, row in enumerate(data):
            __import_process_row(row_index, row, session, structured_map, area_code_map, log, errors)

        if errors:
            session.rollback()
            return __import_render_error("Fehler beim Import", log, errors)

        session.commit()
        return render_template("import_result.html", success=True, message="Import erfolgreich", log=log, errors=errors)

    except Exception as e:
        session.rollback()
        return __import_render_error(f"Unbekannter Fehler: {str(e)}", log, errors)

    finally:
        session.close()

def __import_render_error(message, log, errors):
    return render_template("import_result.html", success=False, message=message, log=log, errors=errors), 400

def __import_split_name(name):
    if not name or not isinstance(name, str):
        return {"vorname": None, "nachname": None}
    parts = name.strip().split(",", 1)
    if len(parts) == 2:
        return {"nachname": parts[0].strip(), "vorname": parts[1].strip()}
    parts = name.strip().split()
    if len(parts) >= 2:
        return {"vorname": parts[0].strip(), "nachname": " ".join(parts[1:]).strip()}
    return {"vorname": None, "nachname": name.strip()}

def __import_resolve_person_by_name(name_str, session):
    name_parts = __import_split_name(name_str)
    if not name_parts["vorname"] or not name_parts["nachname"]:
        return None
    return __import_get_or_create(Person, name_parts, name_parts, session)[0]

def __import_extract_mappings(form_data):
    structured_map = {}
    area_code_map = {}
    for key, val in form_data.items():
        if key.startswith("column_map["):
            colname = key[len("column_map["):-1]
            structured_map[colname] = val.strip() if val.strip() else ""
            area_code_map[colname] = extract_area_code(colname) if structured_map[colname] else ""
    return structured_map, area_code_map

def __import_get_or_create(model_class, filter_data, create_data, session):
    """
    Holt ein existierendes Objekt anhand der Filterdaten oder erstellt es neu.
    
    :param model_class: SQLAlchemy-Modelklasse
    :param filter_data: dict mit Attributen zur Identifikation
    :param create_data: dict mit Attributen zur Erstellung/Aktualisierung
    :param session: SQLAlchemy-Session (muss übergeben werden!)
    :return: (Objekt, True wenn neu erstellt, sonst False)
    """
    try:
        instance = session.query(model_class).filter_by(**filter_data).first()
        if instance:
            for k, v in create_data.items():
                setattr(instance, k, v)
            return instance, False
        else:
            instance = model_class(**create_data)
            session.add(instance)
            return instance, True
    except Exception as e:
        raise RuntimeError(f"Fehler bei get_or_create für {model_class.__name__}: {str(e)}")

def __import_process_row(index, row, session, structured_map, area_code_map, log, errors):
    try:
        person_data = {}
        person_related = {"contacts": [], "abteilungen": [], "professuren": [], "räume": []}

        for colname, target in structured_map.items():
            if not target or '.' not in target:
                if target:
                    errors.append(f"Ungültiges Mapping '{target}' für Spalte '{colname}'")
                continue

            model_name, attr = target.split('.', 1)
            value = row.get(colname)

            if model_name == "PersonContact" and attr == "phone":
                area_code = area_code_map.get(colname, "")
                value = __import_process_phone(value, area_code)

            if model_name == "Person" and attr == "nachname" and value and "," in str(value):
                parts = __import_split_name(value)
                if parts["vorname"]:
                    person_data["vorname"] = parts["vorname"]
                if parts["nachname"]:
                    person_data["nachname"] = parts["nachname"]
                continue

            if model_name == "Abteilung" and attr in ["abteilungsleiter", "vertretung"]:
                __import_handle_abteilung_special(index, row, attr, value, session, log, errors)
                continue

            if model_name == "Person":
                person_data[attr] = value
            else:
                person_related_key = __import_model_name_to_key(model_name)
                if person_related_key:
                    person_related[person_related_key].append({attr: value})

        filter_person = {k: v for k, v in person_data.items() if k in ["title", "vorname", "nachname"] and v}
        if not filter_person:
            errors.append(f"Zeile {index + 1}: Keine eindeutigen Felder für Person vorhanden.")
            return

        person, created = __import_get_or_create(Person, filter_person, person_data, session)
        if not person:
            errors.append(f"Zeile {index + 1}: Person konnte nicht erstellt werden.")
            return

        log.append(f"Zeile {index + 1}: {'Neue Person erstellt' if created else 'Person aktualisiert'}: {filter_person}")

        session.flush()  # Flush hier, um person.id sicher zu haben!

        __import_save_contacts(person, person_related["contacts"], index, session, log)
        __import_link_abteilungen(person, person_related["abteilungen"], index, session, log, errors)

        session.flush()

    except IntegrityError as ie:
        session.rollback()
        errors.append(f"Zeile {index + 1}: Datenbank-Fehler: {str(ie)}")
    except Exception as ex:
        session.rollback()
        errors.append(f"Zeile {index + 1}: Allgemeiner Fehler: {str(ex)}")


def __import_model_name_to_key(name):
    mapping = {
        "PersonContact": "contacts",
        "Abteilung": "abteilungen",
        "Professur": "professuren",
        "Raum": "räume"
    }
    return mapping.get(name)

def __import_process_phone(value, area_code):
    if value is None or value == "":
        return None
    raw_number = str(value).strip().replace(" ", "")
    if area_code and not raw_number.startswith(area_code):
        return area_code + raw_number
    return raw_number

def __import_handle_abteilung_special(index, row, attr, value, session, log, errors):
    abt_name = row.get("Abteilung") or row.get("Department") or row.get("Struktureinheit")
    if abt_name and value:
        person = __import_resolve_person_by_name(value, session)
        if person:
            abt, _ = __import_get_or_create(Abteilung, {"name": abt_name}, {"name": abt_name}, session)
            if attr == "abteilungsleiter":
                abt.abteilungsleiter_id = person.id
                log.append(f"Zeile {index + 1}: Abteilungsleiter für '{abt_name}' gesetzt: {person.vorname} {person.nachname}")
            elif attr == "vertretung":
                abt.vertretungs_id = person.id
                log.append(f"Zeile {index + 1}: Vertretung für '{abt_name}' gesetzt: {person.vorname} {person.nachname}")
            session.add(abt)
        else:
            role_label = "Abteilungsleiter" if attr == "abteilungsleiter" else "Vertretung"
            errors.append(f"Zeile {index + 1}: {role_label} '{value}' konnte nicht erstellt werden.")

def __import_save_contacts(person, contacts, index, session, log):
    for contact_data in contacts:
        if not contact_data:
            continue
        filter_contact = {"person_id": person.id}
        if "email" in contact_data and contact_data["email"]:
            filter_contact["email"] = contact_data["email"]
        elif "phone" in contact_data and contact_data["phone"]:
            filter_contact["phone"] = contact_data["phone"]
        else:
            continue
        contact_data["person_id"] = person.id
        contact, c_created = __import_get_or_create(PersonContact, filter_contact, contact_data, session)
        log.append(f"Zeile {index + 1}: {'Neuer Kontakt hinzugefügt' if c_created else 'Kontakt aktualisiert'}: {contact_data}")

def __import_link_abteilungen(person, abteilungen, index, session, log, errors):
    for abt_data in abteilungen:
        if not abt_data or "name" not in abt_data or not abt_data["name"]:
            continue
        abt, a_created = __import_get_or_create(Abteilung, {"name": abt_data["name"]}, abt_data, session)
        if abt:
            exists = any(pta.abteilung_id == abt.id for pta in person.person_abteilungen)
            if not exists:
                pta = PersonToAbteilung(person=person, abteilung=abt)
                session.add(pta)
                log.append(f"Zeile {index + 1}: Abteilung '{abt.name}' mit Person verknüpft.")
        else:
            errors.append(f"Zeile {index + 1}: Abteilung konnte nicht erstellt/verknüpft werden.")

