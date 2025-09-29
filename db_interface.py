import datetime
from typing import Optional, Dict, Any, Type, List
from sqlalchemy.orm import Session
from sqlalchemy import select, delete
from sqlalchemy.exc import IntegrityError
from sqlalchemy import Column

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

