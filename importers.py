import sys
from typing import Optional, Tuple
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from flask import Blueprint, request, jsonify, render_template
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import io
import json
import re
from sqlalchemy.exc import IntegrityError
from pprint import pprint
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
    "Inventar.rückgabedatum": ["Rückgabe"],
    "Inventar.erhaltungsdatum": ["Ausgabedatum"],
    "Inventar.inventarnummer": ["Inventar-Nr.", "Inventarnummer"],
    "PrincipalInvestigatorToAbteilung.person": ["PI", "Principal Investigator"],
    
    "Inventar.anlagennummer": ["Anlagennummer", "Anlagenummer", "Anlagennr", "Anlagen Nr"],
    "Inventar.anschaffungsdatum": ["Aktivierung am", "AnschDatum", "Anschaffungsdatum"],
    "Inventar.seriennummer": ["Serialnummer", "Seriennummer", "Serien-Nr"],
    "Inventar.preis": ["AnschWert", "Preis", "Wert"],
    "Inventar.kommentar": ["Inventurhinweis", "Inventurhinweis 2", "Inventur-Info", "Inventurkommentar"],
    "Inventar.raum_id": ["Raum", "Raumnummer", "Room"],
    "Inventar.kostenstelle_id": ["Verantw.Kostenstelle", "Kostenstelle", "Kostenstellen-ID"],
    "Object.bezeichnung": ["Anlagenbezeichnung", "Objektbezeichnung", "Bezeichnung"],
}

def get_or_create_object_and_kategorie(
    session: Session,
    object_name: str,
    kategorie_name: str,
    preis: Optional[float] = None
) -> Tuple[Column[int], Column[int]]:
    print(f"object_name: {object_name}, kategorie_name: {kategorie_name}, preis: {preis}")
    try:
        # Kategorie prüfen oder erstellen
        kategorie = session.query(ObjectKategorie).filter_by(name=kategorie_name).first()
        if kategorie is None:
            kategorie = ObjectKategorie(name=kategorie_name)
            session.add(kategorie)
            session.flush()  # um ID zu erhalten, ohne commit

        # Objekt prüfen oder erstellen
        query = session.query(Object).filter_by(name=object_name, kategorie_id=kategorie.id)
        if preis is None:
            query = query.filter(Object.preis == None)
        else:
            query = query.filter_by(preis=preis)

        obj = query.first()
        if obj is None:
            obj = Object(name=object_name, preis=preis, kategorie_id=kategorie.id)
            session.add(obj)
            session.flush()  # um ID zu erhalten

        return (obj.id, kategorie.id)

    except SQLAlchemyError as e:
        session.rollback()
        raise RuntimeError(f"Fehler beim Einfügen oder Abrufen: {e}")

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
        person_related = {
            "contacts": [],
            "abteilungen": [],
            "professuren": [],
            "räume": [],
            "inventar": []
        }

        ignored_col_names = []

        for colname, target in structured_map.items():
            if not target or '.' not in target:
                if target:
                    errors.append(f"Ungültiges Mapping '{target}' für Spalte '{colname}'")
                continue

            model_name, attr = target.split('.', 1)
            value = row.get(colname)

            done = False

            if model_name == "PersonContact" and attr == "phone":
                area_code = area_code_map.get(colname, "")
                value = __import_process_phone(value, area_code)
                done = True

            if model_name == "Person" and attr == "nachname" and value and "," in str(value):
                parts = __import_split_name(value)
                if parts["vorname"]:
                    person_data["vorname"] = parts["vorname"]
                    done = True
                if parts["nachname"]:
                    person_data["nachname"] = parts["nachname"]
                    done = True
                continue

            if model_name == "Objekt" and attr in ["bezeichnung"]:
                price = 12321
                category_name = "AUTO_INSERTED_CATEGORY"
                
                person_related["inventar"].append(get_or_create_object_and_kategorie(session, value, category_name, price))
                done = True
                continue

            if model_name == "Abteilung" and attr in ["abteilungsleiter", "vertretung"]:
                __import_handle_abteilung_special(index, row, attr, value, session, log, errors)
                done = True
                continue

            if model_name == "Person":
                person_data[attr] = value
                done = True
            else:
                related_key = __import_model_name_to_key(model_name)
                if related_key:
                    person_related[related_key].append({attr: value})
                    done = True

            if not done:
                if colname not in ignored_col_names:
                    ignored_col_names.append(colname)

        if len(ignored_col_names):
            for err in ignored_col_names:
                errors.append(f"Ignorierte Spalte: {err}")

        # Filter für eindeutige Identifikation
        filter_person = {k: v for k, v in person_data.items() if k in ["title", "vorname", "nachname"] and v}
        if not filter_person:
            errors.append(f"Zeile {index + 1}: Keine eindeutigen Felder für Person vorhanden.")
            return

        # Person erstellen oder holen
        person, created = __import_get_or_create(Person, filter_person, person_data, session)
        if not person:
            errors.append(f"Zeile {index + 1}: Person konnte nicht erstellt werden.")
            return

        log.append(f"Zeile {index + 1}: {'Neue Person erstellt' if created else 'Person aktualisiert'}: {filter_person}")
        session.flush()  # für person.id

        # Verwandte Entitäten verarbeiten
        __import_save_contacts(person, person_related["contacts"], index, session, log)
        __import_link_abteilungen(person, person_related["abteilungen"], index, session, log, errors)
        __import_link_professuren(person, person_related["professuren"], index, session, log, errors)
        __import_link_raeume(person, person_related["räume"], index, session, log, errors)
        __import_save_inventar(person, person_related["inventar"], index, session, log, errors)

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
    """
    Setzt Abteilungsleiter, Vertretung oder Principal Investigator (PI) für eine Abteilung.
    Falls die Person nicht existiert, wird sie neu erstellt.
    
    :param index: Zeilenindex (für Log)
    :param row: Zeilendaten (dict)
    :param attr: Attribut, z.B. 'abteilungsleiter', 'vertretung', 'principal_investigator'
    :param value: Personenname als String (z.B. "Vorname Nachname")
    :param session: SQLAlchemy-Session
    :param log: Liste zur Aufnahme von Logeinträgen
    :param errors: Liste zur Aufnahme von Fehlern
    """
    abt_name = row.get("Abteilung") or row.get("Department") or row.get("Struktureinheit")
    if not abt_name or not value:
        return

    # Hilfsfunktion zum Aufsplitten des Namens (einfacher Ansatz)
    def split_name(full_name):
        parts = full_name.strip().split()
        if len(parts) == 0:
            return None, None
        elif len(parts) == 1:
            return parts[0], ""
        else:
            return parts[0], " ".join(parts[1:])

    # Versuche Person anhand Name zu laden oder neu zu erstellen
    vorname, nachname = split_name(value)
    if vorname is None:
        errors.append(f"Zeile {index + 1}: Ungültiger Personenname '{value}'.")
        return

    filter_person = {"vorname": vorname, "nachname": nachname}
    person_data = {"vorname": vorname, "nachname": nachname}
    try:
        person, created = __import_get_or_create(Person, filter_person, person_data, session)
        if created:
            log.append(f"Zeile {index + 1}: Neue Person erstellt: {vorname} {nachname}")
    except Exception as e:
        errors.append(f"Zeile {index + 1}: Fehler beim Erstellen/Laden der Person '{value}': {str(e)}")
        return

    # Hole oder erstelle Abteilung
    try:
        abt, _ = __import_get_or_create(Abteilung, {"name": abt_name}, {"name": abt_name}, session)
    except Exception as e:
        errors.append(f"Zeile {index + 1}: Fehler beim Erstellen/Laden der Abteilung '{abt_name}': {str(e)}")
        return

    # Je nach attr den passenden Fremdschlüssel setzen
    if attr == "abteilungsleiter":
        abt.abteilungsleiter_id = person.id
        log.append(f"Zeile {index + 1}: Abteilungsleiter für '{abt_name}' gesetzt: {vorname} {nachname}")
    elif attr == "vertretung":
        abt.vertretungs_id = person.id
        log.append(f"Zeile {index + 1}: Vertretung für '{abt_name}' gesetzt: {vorname} {nachname}")
    elif attr == "principal_investigator":
        # Principal Investigator wird über Zuordnungstabelle gepflegt
        # Prüfen, ob Zuordnung bereits existiert
        pi_assoc = session.query(PrincipalInvestigatorToAbteilung).filter_by(
            person_id=person.id, abteilung_id=abt.id).first()
        if not pi_assoc:
            pi_assoc = PrincipalInvestigatorToAbteilung(person_id=person.id, abteilung_id=abt.id)
            session.add(pi_assoc)
            log.append(f"Zeile {index + 1}: Principal Investigator für '{abt_name}' hinzugefügt: {vorname} {nachname}")
        else:
            log.append(f"Zeile {index + 1}: Principal Investigator für '{abt_name}' bereits vorhanden: {vorname} {nachname}")
    else:
        errors.append(f"Zeile {index + 1}: Unbekanntes Attribut '{attr}'.")

    session.add(abt)

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

def __import_link_professuren(person, professuren, index, session, log, errors):
    for prof_data in professuren:
        if not prof_data or "name" not in prof_data or not prof_data["name"]:
            continue

        prof, created = __import_get_or_create(Professur, {"name": prof_data["name"]}, prof_data, session)
        if not prof:
            errors.append(f"Zeile {index + 1}: Professur '{prof_data.get('name')}' konnte nicht erstellt/verknüpft werden.")
            continue

        exists = any(ptp.professur_id == prof.id for ptp in person.professuren)
        if not exists:
            ptp = ProfessurToPerson(person=person, professur=prof)
            session.add(ptp)
            log.append(f"Zeile {index + 1}: Professur '{prof.name}' mit Person verknüpft.")
            
def __import_link_raeume(person, raeume, index, session, log, errors):
    for raum_data in raeume:
        if not raum_data or "name" not in raum_data or not raum_data["name"]:
            continue

        raum, created = __import_get_or_create(Raum, {"name": raum_data["name"]}, raum_data, session)
        if not raum:
            errors.append(f"Zeile {index + 1}: Raum '{raum_data.get('name')}' konnte nicht erstellt/verknüpft werden.")
            continue

        exists = any(ptr.raum_id == raum.id for ptr in person.räume)
        if not exists:
            ptr = PersonToRaum(person=person, room=raum)
            session.add(ptr)
            log.append(f"Zeile {index + 1}: Raum '{raum.name}' mit Person verknüpft.")

def __import_save_inventar(person, inventar_list, index, session, log, errors):
    for inv_data in inventar_list:
        if not inv_data:
            continue

        inv_data["besitzer_id"] = person.id

        filter_inv = {}
        if "inventarnummer" in inv_data and inv_data["inventarnummer"]:
            filter_inv["inventarnummer"] = inv_data["inventarnummer"]
        elif "anlagennummer" in inv_data and inv_data["anlagennummer"]:
            filter_inv["anlagennummer"] = inv_data["anlagennummer"]
        else:
            errors.append(f"Zeile {index + 1}: Kein eindeutiger Inventar-Identifier vorhanden.")
            continue

        inventar, created = __import_get_or_create(Inventar, filter_inv, inv_data, session)
        if inventar:
            log.append(f"Zeile {index + 1}: Inventar {'erstellt' if created else 'aktualisiert'}: {filter_inv}")
        else:
            errors.append(f"Zeile {index + 1}: Inventar konnte nicht erstellt/aktualisiert werden.")

