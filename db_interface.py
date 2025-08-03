import datetime
from typing import Optional, Dict, Any, Type, List
from sqlalchemy.orm import Session
from sqlalchemy import select, delete
from db_defs import (
    Person, PersonContact, Abteilung, PersonToAbteilung,
    Building, Raum, PersonToRaum, Transponder, TransponderToRaum
)
from sqlalchemy.exc import IntegrityError

class AbstractDBHandler:
    def __init__(self, session: Session, model: Type):
        self.session = session
        self.model = model

    def get_row(self, id: int) -> Optional[Any]:
        try:
            return self.session.get(self.model, id)
        except Exception as e:
            print(f"❌ Fehler bei get_row: {e}")
            return None

    def _get_row_by_values(self, data: Dict[str, Any]) -> Optional[Any]:
        try:
            query = select(self.model)
            for k, v in data.items():
                if v is not None:
                    query = query.where(getattr(self.model, k) == v)
            result = self.session.execute(query).scalars().first()
            return result
        except Exception as e:
            print(f"❌ Fehler bei _get_row_by_values: {e}")
            return None

    def _safe_insert(self, data: Dict[str, Any]) -> Optional[int]:
        try:
            existing_row = self._get_row_by_values(data)
            if existing_row is not None:
                return existing_row.id
            row = self.model(**data)
            self.session.add(row)
            self.session.commit()
            self.session.refresh(row)
            return row.id
        except IntegrityError as e:
            self.session.rollback()
            existing_row = self._get_row_by_values(data)
            if existing_row is not None:
                return existing_row.id
            print(f"❌ IntegrityError beim _safe_insert: {e}")
            return None
        except Exception as e:
            self.session.rollback()
            print(f"❌ Fehler bei _safe_insert: {e}")
            return None

    def insert_data(self, data: Dict[str, Any]) -> Optional[int]:
        try:
            return self._safe_insert(data)
        except Exception as e:
            print(f"❌ Fehler bei insert_data: {e}")
            return None

    def delete_by_id(self, id: int) -> bool:
        try:
            stmt = delete(self.model).where(self.model.id == id)
            self.session.execute(stmt)
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            print(f"❌ Fehler beim Löschen: {e}")
            return False

    def get_id(self, data: Dict[str, Any]) -> Optional[int]:
        try:
            query = select(self.model)
            for k, v in data.items():
                if v is not None:
                    query = query.where(getattr(self.model, k) == v)
            result = self.session.execute(query).scalars().first()
            if result:
                return result.id
            return None
        except Exception as e:
            print(f"❌ Fehler bei get_id: {e}")
            return None

    def insert_into_db(self, data: Dict[str, Any]) -> Optional[int]:
        existing_id = self.get_id(data)
        if existing_id is not None:
            return existing_id
        try:
            row = self.model(**data)
            self.session.add(row)
            self.session.commit()
            self.session.refresh(row)
            return row.id
        except IntegrityError as e:
            self.session.rollback()
            print(f"❌ IntegrityError bei insert_into_db: {e}")
            # Versuche nochmal existing_id zu holen (Race-Condition möglich)
            return self.get_id(data)
        except Exception as e:
            self.session.rollback()
            print(f"❌ Fehler bei insert_into_db: {e}")
            return None

    def bulk_insert(self, data_list: List[Dict[str, Any]]) -> List[int]:
        ids = []
        try:
            for data in data_list:
                id_ = self.insert_into_db(data)
                if id_ is not None:
                    ids.append(id_)
            return ids
        except Exception as e:
            self.session.rollback()
            print(f"❌ Fehler bei bulk_insert: {e}")
            return []

    def set_column(self, id_: int, column: str, value: Any) -> bool:
        try:
            row = self.get_row(id_)
            if row is None or not hasattr(row, column):
                return False
            setattr(row, column, value)
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            print(f"❌ Fehler bei set_column: {e}")
            return False

    def set_row(self, id_: int, new_values: Dict[str, Any]) -> bool:
        try:
            row = self.get_row(id_)
            if row is None:
                return False
            for k, v in new_values.items():
                if hasattr(row, k):
                    setattr(row, k, v)
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            print(f"❌ Fehler bei set_row: {e}")
            return False

    def get_all(self, filters: Optional[Dict[str, Any]] = None) -> List[Any]:
        try:
            query = select(self.model)
            if filters:
                for k, v in filters.items():
                    query = query.where(getattr(self.model, k) == v)
            result = self.session.execute(query).scalars().all()
            return result
        except Exception as e:
            print(f"❌ Fehler bei get_all: {e}")
            return []

    def update(self, filters: Dict[str, Any], new_values: Dict[str, Any]) -> int:
        # return Anzahl der geänderten Zeilen
        try:
            stmt = update(self.model)
            for k, v in filters.items():
                stmt = stmt.where(getattr(self.model, k) == v)
            stmt = stmt.values(**new_values)
            result = self.session.execute(stmt)
            self.session.commit()
            return result.rowcount
        except Exception as e:
            self.session.rollback()
            print(f"❌ Fehler bei update: {e}")
            return 0

    def delete(self, id_: int) -> bool:
        try:
            stmt = delete(self.model).where(self.model.id == id_)
            result = self.session.execute(stmt)
            self.session.commit()
            return result.rowcount > 0
        except Exception as e:
            self.session.rollback()
            print(f"❌ Fehler bei delete: {e}")
            return False

    def to_dict(self, instance: Any) -> Dict[str, Any]:
        try:
            return {col.name: getattr(instance, col.name) for col in instance.__table__.columns}
        except Exception as e:
            print(f"❌ Fehler bei to_dict: {e}")
            return {}

    def update_by_id(self, id_: int, new_values: Dict[str, Any]) -> bool:
        print(new_values)
        """
        Aktualisiert eine Zeile anhand der ID mit neuen Werten.
        Gibt True zurück, wenn erfolgreich, sonst False.
        """
        try:
            row = self.get_row(id_)
            if row is None:
                print(f"❌ update_by_id: Kein Eintrag mit id={id_} gefunden.")
                return False
            for key, value in new_values.items():
                if hasattr(row, key):
                    setattr(row, key, value)
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            print(f"❌ Fehler bei update_by_id: {e}")
            return False

# Spezifische Klassen

class PersonHandler(AbstractDBHandler):
    def __init__(self, session: Session):
        super().__init__(session, Person)

    def insert_person(self, data: Dict[str, Any]) -> int:
        if "created_at" not in data:
            data["created_at"] = datetime.datetime.utcnow()
        return self.insert_into_db(data)

class AbteilungHandler(AbstractDBHandler):
    def __init__(self, session: Session):
        self.session = session
        self.model = Abteilung

class PersonToAbteilungHandler(AbstractDBHandler):
    def __init__(self, session: Session):
        super().__init__(session, PersonToAbteilung)

class BuildingHandler(AbstractDBHandler):
    def __init__(self, session: Session):
        super().__init__(session, Building)

class RaumHandler(AbstractDBHandler):
    def __init__(self, session: Session):
        super().__init__(session, Raum)

class PersonToRaumHandler(AbstractDBHandler):
    def __init__(self, session: Session):
        super().__init__(session, PersonToRaum)

class TransponderHandler(AbstractDBHandler):
    def __init__(self, session: Session):
        super().__init__(session, TransponderToRaum)
        self.session = session
        self.model = Transponder  # Dein SQLAlchemy-Modell

class TransponderToRaumHandler(AbstractDBHandler):
    def __init__(self, session: Session):
        super().__init__(session, TransponderToRaum)

class PersonWithContactHandler(AbstractDBHandler):
    def __init__(self, session: Session):
        super().__init__(session, Person)

    def insert_person_with_contacts(self, person_data: dict, contacts: List[dict]) -> Optional[int]:
        try:
            stmt = select(Person).where(
                Person.title == person_data.get("title"),
                Person.vorname == person_data.get("vorname"),
                Person.nachname == person_data.get("nachname")
            )
            existing_person = self.session.execute(stmt).scalars().first()
            if existing_person:
                print(f"ℹ️ Person existiert bereits mit ID {existing_person.id}")
                return existing_person.id

            new_person = Person(
                title=person_data.get("title"),
                vorname=person_data.get("vorname"),
                nachname=person_data.get("nachname"),
                created_at=person_data.get("created_at") or datetime.datetime.utcnow(),
                comment=person_data.get("comment"),
                image_url=person_data.get("image_url")
            )

            self.session.add(new_person)

            if contacts:
                for c in contacts:
                    contact = PersonContact(
                        phone=c.get("phone"),
                        fax=c.get("fax"),
                        email=c.get("email"),
                        comment=c.get("comment"),
                        person=new_person
                    )
                    self.session.add(contact)

            self.session.commit()
            return new_person.id
        except IntegrityError as e:
            self.session.rollback()
            print(f"❌ IntegrityError beim insert_person_with_contacts: {e}")
            return None
        except Exception as e:
            self.session.rollback()
            print(f"❌ Fehler beim insert_person_with_contacts: {e}")
            return None

    def update_person(self, person_id: int, new_values: Dict[str, Any]) -> bool:
        try:
            person = self.session.get(Person, person_id)
            if person is None:
                return False
            for k, v in new_values.items():
                if hasattr(person, k):
                    setattr(person, k, v)
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            print(f"❌ Fehler bei update_person: {e}")
            return False

    def update_person_column(self, person_id: int, column: str, value: Any) -> bool:
        try:
            person = self.session.get(Person, person_id)
            if person is None or not hasattr(person, column):
                return False

            new_title = person.title
            new_vorname = person.vorname
            new_nachname = person.nachname

            if column == "title":
                new_title = value
            elif column == "vorname":
                new_vorname = value
            elif column == "nachname":
                new_nachname = value

            stmt = select(Person).where(
                Person.id != person_id,
                Person.title == new_title,
                Person.vorname == new_vorname,
                Person.nachname == new_nachname
            )
            existing = self.session.execute(stmt).scalars().first()
            if existing:
                print(f"❌ Abgelehnt: Person mit (title={new_title}, vorname={new_vorname}, nachname={new_nachname}) existiert bereits (ID={existing.id})")
                return False

            setattr(person, column, value)
            self.session.commit()
            return True
        except IntegrityError as e:
            self.session.rollback()
            print(f"❌ IntegrityError beim Commit: {e}")
            return False
        except Exception as e:
            self.session.rollback()
            print(f"❌ Fehler bei update_person_column: {e}")
            return False

    def get_person_contacts(self, person_id: int) -> List[PersonContact]:
        try:
            query = select(PersonContact).where(PersonContact.person_id == person_id)
            results = self.session.execute(query).scalars().all()
            return results
        except Exception as e:
            print(f"❌ Fehler bei get_person_contacts: {e}")
            return []

    def add_contact_to_person(self, person_id: int, contact_data: Dict[str, Any]) -> Optional[int]:
        try:
            person = self.session.get(Person, person_id)
            if person is None:
                print(f"❌ Person mit ID {person_id} nicht gefunden")
                return None

            contact = PersonContact(**contact_data, person=person)
            self.session.add(contact)
            self.session.commit()
            self.session.refresh(contact)
            return contact.id
        except Exception as e:
            self.session.rollback()
            print(f"❌ Fehler bei add_contact_to_person: {e}")
            return None
