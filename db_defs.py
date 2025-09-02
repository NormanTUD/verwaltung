from datetime import datetime
from typing import Optional, Dict, Any, Type, List
from sqlalchemy import (create_engine, Column, Integer, String, Text, ForeignKey, Date, Float, TIMESTAMP, UniqueConstraint, Table, Boolean, Index)
from sqlalchemy.orm.util import AliasedClass
from sqlalchemy.inspection import inspect
from sqlalchemy.exc import NoInspectionAvailable
from sqlalchemy.orm import declarative_base, relationship, Session, class_mapper, RelationshipProperty, aliased, joinedload
from flask_login import UserMixin
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy_continuum import make_versioned, TransactionFactory, version_class
from sqlalchemy.orm import configure_mappers
from sqlalchemy.orm import DeclarativeBase

make_versioned(user_cls=None)

class CustomBase:
    def to_dict(self, recursive=False):
        try:
            result = {col.name: getattr(self, col.name) for col in self.__table__.columns}
            if recursive:
                for rel in self.__mapper__.relationships:
                    val = getattr(self, rel.key)
                    if isinstance(val, list):
                        result[rel.key] = [x.to_dict(recursive=False) if hasattr(x, 'to_dict') else {} for x in val]
                    elif val is not None:
                        result[rel.key] = val.to_dict(recursive=False) if hasattr(val, 'to_dict') else {}
            return result
        except Exception as e:
            print(f"❌ Fehler bei to_dict: {e}")
            return {}

class Base(DeclarativeBase, CustomBase):
    pass

class User(UserMixin, Base):
    __tablename__ = "user"
    id = Column(Integer, primary_key=True)
    username = Column(String(150), unique=True)
    password = Column(String(180))
    role = Column(String(50))
    is_active = Column(Boolean, default=False)
    person_id = Column(Integer, ForeignKey("person.id", ondelete="CASCADE"))
    readonly = Column(Boolean, default=False)

    user_roles = Table(
        'user_roles', Base.metadata,
        Column('user_id', Integer, ForeignKey('user.id')),
        Column('role_id', Integer, ForeignKey('role.id'))
    )

    roles = relationship(
        "Role",
        secondary=user_roles,
        back_populates="users"
    )

    def __repr__(self):
        status = "active" if self.is_active else "inactive"
        readonly_flag = "readonly" if self.readonly else "editable"
        roles = ", ".join([role.name for role in self.roles]) if self.roles else "no roles"
        return f"User: {self.username} ({status}, {readonly_flag}, Roles: {roles})"

class Role(Base):
    __tablename__ = 'role'
    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True)

    users = relationship(
        "User",
        secondary=User.user_roles,  # oder: 'user_roles' falls global definiert
        back_populates="roles"
    )

    def __repr__(self):
        user_list = ", ".join([user.username for user in self.users]) if self.users else "no users"
        return f"Role: {self.name} (Users: {user_list})"

class Person(Base):
    __tablename__ = "person"
    __versioned__: dict = {}
    id = Column(Integer, primary_key=True)
    title = Column(Text)
    vorname = Column(Text)
    nachname = Column(Text)
    kommentar = Column(Text)
    image_url = Column(Text)

    contacts = relationship("PersonContact", back_populates="person", cascade="all, delete")
    räume = relationship("PersonToRaum", back_populates="person", cascade="all, delete")
    transponders_issued = relationship("Transponder", foreign_keys="[Transponder.ausgeber_id]", back_populates="ausgeber")
    transponders_owned = relationship("Transponder", foreign_keys="[Transponder.besitzer_id]", back_populates="besitzer")

    departments = relationship(
        "Abteilung",
        foreign_keys="[Abteilung.abteilungsleiter_id]",
        back_populates="leiter"
    )

    vertretende_abteilungen = relationship(
        "Abteilung",
        foreign_keys="[Abteilung.vertretungs_id]",
        back_populates="vertretung"
    )

    principal_investigator_abteilungen = relationship(
        "PrincipalInvestigatorToAbteilung",
        back_populates="person",
        cascade="all, delete"
    )

    person_abteilungen = relationship("PersonToAbteilung", back_populates="person", cascade="all, delete")
    professuren = relationship("ProfessurToPerson", back_populates="person", cascade="all, delete")

    __table_args__ = (
        UniqueConstraint("title", "vorname", "nachname", name="uq_person_name_title"),
    )

    def __repr__(self):
        return (
            f"{self.title or ''} {self.vorname or ''} {self.nachname or ''}".strip() +
            (f" – {self.kommentar}" if self.kommentar else "")
        )

class PersonContact(Base):
    __tablename__ = "person_contact"
    __versioned__: dict = {}
    id = Column(Integer, primary_key=True)
    person_id = Column(Integer, ForeignKey("person.id", ondelete="CASCADE"))
    phone = Column(Text)
    fax = Column(Text)
    email = Column(Text)
    kommentar = Column(Text)
    person = relationship("Person", back_populates="contacts")
    
    __table_args__ = (
        UniqueConstraint("person_id", "email", name="uq_contact_person_email"),
        UniqueConstraint("person_id", "phone", name="uq_contact_person_phone"),
        UniqueConstraint("person_id", "fax", name="uq_contact_person_fax"),
        Index("ix_person_contact_person_id", "person_id"),
        Index("ix_person_contact_email", "email"),
    )

    def __repr__(self):
        parts = []
        if self.phone:
            parts.append(f"Phone: {self.phone}")
        if self.fax:
            parts.append(f"Fax: {self.fax}")
        if self.email:
            parts.append(f"Email: {self.email}")
        contact_info = ", ".join(parts) if parts else "No contact info"
        return f"PersonContact(id={self.id}, {contact_info})"

class Abteilung(Base):
    __tablename__ = "abteilung"
    __versioned__: dict = {}
    id = Column(Integer, primary_key=True)
    name = Column(Text)
    abteilungsleiter_id = Column(Integer, ForeignKey("person.id", ondelete="SET NULL"))
    vertretungs_id = Column(Integer, ForeignKey("person.id", ondelete="SET NULL"))

    leiter = relationship("Person", foreign_keys=[abteilungsleiter_id], back_populates="departments")
    vertretung = relationship("Person", foreign_keys=[vertretungs_id], back_populates="vertretende_abteilungen")

    persons = relationship("PersonToAbteilung", back_populates="abteilung", cascade="all, delete")
    
    principal_investigators = relationship(
        "PrincipalInvestigatorToAbteilung",
        back_populates="abteilung",
        cascade="all, delete"
    )
    
    __table_args__ = (
        UniqueConstraint("name", name="uq_abteilung_name"),
    )

    def __repr__(self):
        leiter_name = f"{self.leiter.vorname} {self.leiter.nachname}" if self.leiter else "None"
        vertreter_name = f"{self.vertretung.vorname} {self.vertretung.nachname}" if self.vertretung else "None"
        return f"Abteilung(id={self.id}, name='{self.name}', leiter={leiter_name}, vertreter={vertreter_name})"

class PrincipalInvestigatorToAbteilung(Base):
    __tablename__ = "principal_investigator_to_abteilung"
    __versioned__: dict = {}
    id = Column(Integer, primary_key=True)
    person_id = Column(Integer, ForeignKey("person.id", ondelete="CASCADE"), nullable=False)
    abteilung_id = Column(Integer, ForeignKey("abteilung.id", ondelete="CASCADE"), nullable=False)

    person = relationship("Person", back_populates="principal_investigator_abteilungen")
    abteilung = relationship("Abteilung", back_populates="principal_investigators")

    __table_args__ = (
        UniqueConstraint("person_id", "abteilung_id", name="uq_pi_to_abteilung"),
    )

    def __repr__(self):
        person_name = f"{self.person.vorname} {self.person.nachname}" if self.person else "None"
        abteilung_name = self.abteilung.name if self.abteilung else "None"
        return f"PrincipalInvestigatorToAbteilung(id={self.id}, person={person_name}, abteilung={abteilung_name})"


class PersonToAbteilung(Base):
    __tablename__ = "person_to_abteilung"
    __versioned__: dict = {}
    id = Column(Integer, primary_key=True)
    person_id = Column(Integer, ForeignKey("person.id", ondelete="CASCADE"))
    abteilung_id = Column(Integer, ForeignKey("abteilung.id", ondelete="CASCADE"))

    person = relationship("Person", back_populates="person_abteilungen")
    abteilung = relationship("Abteilung", back_populates="persons")
    
    __table_args__ = (
        UniqueConstraint("person_id", "abteilung_id", name="uq_person_to_abteilung"),
    )

    def __repr__(self):
        person_name = f"{self.person.vorname} {self.person.nachname}" if self.person else "None"
        abteilung_name = self.abteilung.name if self.abteilung else "None"
        return f"PersonToAbteilung(id={self.id}, person={person_name}, abteilung={abteilung_name})"

class Kostenstelle(Base):
    __tablename__ = "kostenstelle"
    __versioned__: dict = {}
    id = Column(Integer, primary_key=True)
    name = Column(Text)
    professur_id = Column(Integer, ForeignKey("professur.id", ondelete="CASCADE"))

    professuren = relationship(
        "Professur",
        back_populates="kostenstelle",
        foreign_keys="[Professur.kostenstelle_id]"
    )

    __table_args__ = (
        UniqueConstraint("name", name="uq_kostenstelle_name"),
        UniqueConstraint("professur_id", name="uq_professur"),
    )

    def __repr__(self):
        professur_name = self.professuren[0].name if self.professuren else "None"
        return f"Kostenstelle(id={self.id}, name={self.name}, professur={professur_name})"


class Professur(Base):
    __tablename__ = "professur"
    __versioned__: dict = {}
    id = Column(Integer, primary_key=True)
    kostenstelle_id = Column(Integer, ForeignKey("kostenstelle.id", ondelete="SET NULL"))
    name = Column(Text)

    kostenstelle = relationship(
        "Kostenstelle",
        back_populates="professuren",
        foreign_keys=[kostenstelle_id]
    )

    persons = relationship(
        "ProfessurToPerson",
        back_populates="professur",
        cascade="all, delete"
    )

    __table_args__ = (
        UniqueConstraint("kostenstelle_id", "name", name="uq_professur_per_kostenstelle"),
    )

    def __repr__(self):
        kostenstelle_name = self.kostenstelle.name if self.kostenstelle else "None"
        return f"Professur(id={self.id}, name={self.name}, kostenstelle={kostenstelle_name})"

class ProfessurToPerson(Base):
    __tablename__ = "professur_to_person"
    __versioned__: dict = {}
    id = Column(Integer, primary_key=True)
    professur_id = Column(Integer, ForeignKey("professur.id", ondelete="CASCADE"))
    person_id = Column(Integer, ForeignKey("person.id", ondelete="CASCADE"))
    professur = relationship("Professur", back_populates="persons")
    person = relationship("Person", back_populates="professuren")
    
    __table_args__ = (
        UniqueConstraint("person_id", "professur_id", name="uq_professur_to_person"),
    )

    def __repr__(self):
        professur_name = self.professur.name if self.professur else "None"
        person_name = f"{self.person.vorname} {self.person.nachname}" if self.person else "None"
        return f"ProfessurToPerson(id={self.id}, professur={professur_name}, person={person_name})"

class Building(Base):
    __tablename__ = "building"
    __versioned__: dict = {}
    id = Column(Integer, primary_key=True)
    name = Column(Text)
    gebäudenummer = Column(Text)
    abkürzung = Column(Text)
    räume = relationship("Raum", back_populates="building")

    def __repr__(self):
        return f"Building(id={self.id}, name='{self.name}', gebäudenummer='{self.gebäudenummer}', abkürzung='{self.abkürzung}')"

class Raum(Base):
    __tablename__ = "room"
    __versioned__: dict = {}
    id = Column(Integer, primary_key=True)
    building_id = Column(Integer, ForeignKey("building.id", ondelete="SET NULL"))
    name = Column(Text)
    etage = Column(Integer)
    building = relationship("Building", back_populates="räume")
    person_links = relationship("PersonToRaum", back_populates="room", cascade="all, delete")
    transponder_links = relationship("TransponderToRaum", back_populates="room", cascade="all, delete")
    layout = relationship("RaumLayout", back_populates="room", uselist=False, cascade="all, delete")
    guid = Column(Text)

    __table_args__ = (
        UniqueConstraint("building_id", "name", name="uq_room_per_building"),
        UniqueConstraint("guid", name="uq_guid"),
        Index("ix_room_building_id", "building_id"),
        Index("ix_room_etage", "etage"),
        Index("ix_room_guid", "guid"),
    )

    def __repr__(self):
        building_name = self.building.name if self.building else None
        return f"Raum(id={self.id}, name='{self.name}', etage={self.etage}, building='{building_name}', guid='{self.guid}')"

class PersonToRaum(Base):
    __tablename__ = "person_to_room"
    __versioned__: dict = {}
    id = Column(Integer, primary_key=True)
    person_id = Column(Integer, ForeignKey("person.id", ondelete="CASCADE"))
    raum_id = Column(Integer, ForeignKey("room.id", ondelete="CASCADE"))
    x = Column(Integer)
    y = Column(Integer)
    person = relationship("Person", back_populates="räume")
    room = relationship("Raum", back_populates="person_links")
    
    __table_args__ = (
        UniqueConstraint("person_id", "raum_id", name="uq_person_to_room"),
        Index("ix_person_to_room_person_id", "person_id"),
        Index("ix_person_to_room_raum_id", "raum_id"),
    )

    def __repr__(self):
        person_name = f"{self.person.vorname} {self.person.nachname}" if self.person else None
        room_name = self.room.name if self.room else None
        return f"PersonToRaum(id={self.id}, person='{person_name}', room='{room_name}', x={self.x}, y={self.y})"

class Transponder(Base):
    __tablename__ = "transponder"
    __versioned__: dict = {}
    id = Column(Integer, primary_key=True)
    ausgeber_id = Column(Integer, ForeignKey("person.id", ondelete="SET NULL"))
    besitzer_id = Column(Integer, ForeignKey("person.id", ondelete="SET NULL"))
    erhaltungsdatum = Column(Date)
    rückgabedatum = Column("rückgabedatum", Date)
    seriennummer = Column(Text)
    kommentar = Column(Text)
    ausgeber = relationship("Person", foreign_keys=[ausgeber_id], back_populates="transponders_issued")
    besitzer = relationship("Person", foreign_keys=[besitzer_id], back_populates="transponders_owned")
    room_links = relationship("TransponderToRaum", back_populates="transponder", cascade="all, delete")
    
    __table_args__ = (
        UniqueConstraint("seriennummer", name="uq_transponder_serial"),
        Index("ix_transponder_besitzer_id", "besitzer_id"),
        Index("ix_transponder_ausgeber_id", "ausgeber_id"),
    )

    def __repr__(self):
        ausgeber_name = f"{self.ausgeber.vorname} {self.ausgeber.nachname}" if self.ausgeber else None
        besitzer_name = f"{self.besitzer.vorname} {self.besitzer.nachname}" if self.besitzer else None
        return (
            f"Transponder(id={self.id}, seriennummer='{self.seriennummer}', "
            f"ausgeber='{ausgeber_name}', besitzer='{besitzer_name}', "
            f"erhaltungsdatum={self.erhaltungsdatum}, rückgabedatum={self.rückgabedatum})"
        )

class TransponderToRaum(Base):
    __tablename__ = "transponder_to_room"
    __versioned__: dict = {}
    id = Column(Integer, primary_key=True)
    transponder_id = Column(Integer, ForeignKey("transponder.id", ondelete="CASCADE"))
    raum_id = Column(Integer, ForeignKey("room.id", ondelete="CASCADE"))
    transponder = relationship("Transponder", back_populates="room_links")
    room = relationship("Raum", back_populates="transponder_links")

    __table_args__ = (
        UniqueConstraint("transponder_id", "raum_id", name="uq_transponder_to_room"),
        Index("ix_transponder_to_room_transponder_id", "transponder_id"),
        Index("ix_transponder_to_room_raum_id", "raum_id"),
    )

    def __repr__(self):
        transponder_sn = self.transponder.seriennummer if self.transponder else None
        raum_name = self.room.name if self.room else None
        return f"TransponderToRaum(id={self.id}, transponder='{transponder_sn}', raum='{raum_name}')"


class ObjectKategorie(Base):
    __tablename__ = "object_kategorie"
    id = Column(Integer, primary_key=True)
    name = Column(Text)

    def __repr__(self):
        return f"ObjectKategorie(id={self.id}, name='{self.name}')"

class Object(Base):
    __tablename__ = "object"
    id = Column(Integer, primary_key=True)
    name = Column(Text)
    preis = Column(Float)
    kategorie_id = Column(Integer, ForeignKey("object_kategorie.id", ondelete="SET NULL"))
    kategorie = relationship("ObjectKategorie")

    def __repr__(self):
        kategorie_name = self.kategorie.name if self.kategorie else None
        return f"Object(id={self.id}, name='{self.name}', preis={self.preis}, kategorie='{kategorie_name}')"


class Lager(Base):
    __tablename__ = "lager"
    __versioned__: dict = {}

    id = Column(Integer, primary_key=True)
    name = Column(Text)
    raum_id = Column(Integer, ForeignKey("room.id", ondelete="SET NULL"))

    __table_args__ = (
        UniqueConstraint("raum_id", name="uq_lager_raum"),
    )

    def __repr__(self):
        return f"Lager(id={self.id}, name='{self.name}', raum_id={self.raum_id})"

class ObjectToLager(Base):
    __tablename__ = "object_to_lager"
    __versioned__: dict = {}
    id = Column(Integer, primary_key=True)
    object_id = Column(Integer, ForeignKey("object.id", ondelete="CASCADE"))
    lager_id = Column(Integer, ForeignKey("lager.id", ondelete="CASCADE"))

    __table_args__ = (
        UniqueConstraint("object_id", "lager_id", name="uq_object_to_lager"),
    )

    def __repr__(self):
        return f"ObjectToLager(id={self.id}, object_id={self.object_id}, lager_id={self.lager_id})"

class Inventar(Base):
    __tablename__ = "inventory"
    __versioned__: dict = {}
    id = Column(Integer, primary_key=True)
    besitzer_id = Column(Integer, ForeignKey("person.id", ondelete="SET NULL"))
    object_id = Column(Integer, ForeignKey("object.id", ondelete="SET NULL"))
    ausgeber_id = Column(Integer, ForeignKey("person.id", ondelete="SET NULL"))
    anschaffungsdatum = Column(Date)
    erhaltungsdatum = Column(Date)
    rückgabedatum = Column("rückgabedatum", Date)
    seriennummer = Column(Text)
    kostenstelle_id = Column(Integer, ForeignKey("kostenstelle.id", ondelete="SET NULL"))
    inventarnummer = Column(Text)
    anlagennummer = Column(Text)
    kommentar = Column(Text)
    preis = Column(Float)
    raum_id = Column(Integer, ForeignKey("room.id", ondelete="SET NULL"))
    professur_id = Column(Integer, ForeignKey("professur.id", ondelete="SET NULL"))
    abteilung_id = Column(Integer, ForeignKey("abteilung.id", ondelete="SET NULL"))

    besitzer = relationship("Person", foreign_keys=[besitzer_id], lazy="joined")
    ausgeber = relationship("Person", foreign_keys=[ausgeber_id], lazy="joined")
    object = relationship("Object", lazy="joined")
    kostenstelle = relationship("Kostenstelle", lazy="joined")
    abteilung = relationship("Abteilung", lazy="joined")
    professur = relationship("Professur", lazy="joined")
    room = relationship("Raum", foreign_keys=[raum_id], lazy="joined")

    __table_args__ = (
        Index("ix_inventory_besitzer_id", "besitzer_id"),
        Index("ix_inventory_ausgeber_id", "ausgeber_id"),
        Index("ix_inventory_object_id", "object_id"),
        Index("ix_inventory_raum_id", "raum_id"),
        Index("ix_inventory_kostenstelle_id", "kostenstelle_id"),
        Index("ix_inventory_professur_id", "professur_id"),
        Index("ix_inventory_abteilung_id", "abteilung_id"),
    )

    def __repr__(self):
        return (
            f"Inventar(id={self.id}, inventarnummer={self.inventarnummer!r}, anlagennummer={self.anlagennummer!r}, "
            f"object={self.object.name if self.object else None!r}, besitzer={self.besitzer.nachname if self.besitzer else None!r}, "
            f"raum={self.room.name if self.room else None!r}, preis={self.preis})"
        )

class RaumLayout(Base):
    __tablename__ = "room_layout"
    __versioned__: dict = {}
    id = Column(Integer, primary_key=True)
    raum_id = Column(Integer, ForeignKey("room.id", ondelete="CASCADE"), nullable=False)
    x = Column(Integer, nullable=False)
    y = Column(Integer, nullable=False)
    width = Column(Integer, nullable=False)
    height = Column(Integer, nullable=False)

    room = relationship("Raum", back_populates="layout")

    __table_args__ = (
        UniqueConstraint("raum_id", name="uq_raum_id"),
        UniqueConstraint("raum_id", "x", "y", "width", "height", name="uq_raum_id_x_y_width_height"),
    )

    def __repr__(self):
        return (
            f"RaumLayout(id={self.id}, raum={self.room.name if self.room else None!r}, "
            f"x={self.x}, y={self.y}, width={self.width}, height={self.height})"
        )


class Loan(Base):
    __tablename__ = "loan"
    __versioned__: dict = {}
    id = Column(Integer, primary_key=True)
    besitzer_id = Column(Integer, ForeignKey("person.id", ondelete="SET NULL"))
    ausgeber_id = Column(Integer, ForeignKey("person.id", ondelete="SET NULL"))
    leihdatum = Column(Date)
    rückgabedatum = Column(Date)
    kommentar = Column(Text)

    person = relationship("Person", foreign_keys=[besitzer_id], lazy="joined")
    ausgeber = relationship("Person", foreign_keys=[ausgeber_id], lazy="joined")
    objekte = relationship("ObjectToLoan", back_populates="loan", cascade="all, delete")

    __table_args__ = (
        Index("ix_loan_person_id", "besitzer_id"),
        Index("ix_loan_ausgeber_id", "ausgeber_id"),
        Index("ix_loan_leihdatum", "leihdatum"),
        Index("ix_loan_rückgabedatum", "rückgabedatum"),
    )

    def __repr__(self):
        return (
            f"Loan(id={self.id}, besitzer={self.person.nachname if self.person else None}, "
            f"ausgeber={self.ausgeber.nachname if self.ausgeber else None}, "
            f"leihdatum={self.leihdatum}, rückgabedatum={self.rückgabedatum}, "
            f"kommentar={self.kommentar!r})"
        )


class ObjectToLoan(Base):
    __tablename__ = "object_to_loan"
    __versioned__: dict = {}
    id = Column(Integer, primary_key=True)
    loan_id = Column(Integer, ForeignKey("loan.id", ondelete="CASCADE"))
    object_id = Column(Integer, ForeignKey("object.id", ondelete="SET NULL"))
    object = relationship("Object", lazy="joined")
    loan = relationship("Loan", back_populates="objekte")

    __table_args__ = (
        UniqueConstraint("loan_id", "object_id", name="uq_loan_object"),
        Index("ix_object_to_loan_loan_id", "loan_id"),
        Index("ix_object_to_loan_object_id", "object_id"),
    )

    def __repr__(self):
        return (
            f"ObjectToLoan(id={self.id}, "
            f"loan_id={self.loan_id}, "
            f"object_name={self.object.name if self.object else None})"
        )

db = SQLAlchemy(model_class=Base)
