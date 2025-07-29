from typing import Optional, Dict, Any, Type, List
from sqlalchemy import (create_engine, Column, Integer, String, Text, ForeignKey, Date, Float, TIMESTAMP, UniqueConstraint, Table)
from sqlalchemy.orm.util import AliasedClass
from sqlalchemy.inspection import inspect
from sqlalchemy.exc import NoInspectionAvailable
from sqlalchemy.orm import declarative_base, relationship, Session, class_mapper, RelationshipProperty, aliased, joinedload
from flask_login import UserMixin
from flask_sqlalchemy import SQLAlchemy

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

Base = declarative_base(cls=CustomBase)

class User(UserMixin, Base):
    __tablename__ = "user"
    id = Column(Integer, primary_key=True)
    username = Column(String(150), unique=True)
    password = Column(String(150))
    role = Column(String(50))

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

class Role(Base):
    __tablename__ = 'role'
    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True)

    users = relationship(
        "User",
        secondary=User.user_roles,  # oder: 'user_roles' falls global definiert
        back_populates="roles"
    )

class Person(Base):
    __tablename__ = "person"
    id = Column(Integer, primary_key=True)
    title = Column(Text)
    first_name = Column(Text)
    last_name = Column(Text)
    comment = Column(Text)
    image_url = Column(Text)

    contacts = relationship("PersonContact", back_populates="person", cascade="all, delete")
    rooms = relationship("PersonToRoom", back_populates="person", cascade="all, delete")
    transponders_issued = relationship("Transponder", foreign_keys="[Transponder.issuer_id]", back_populates="issuer")
    transponders_owned = relationship("Transponder", foreign_keys="[Transponder.owner_id]", back_populates="owner")
    departments = relationship("Abteilung", back_populates="leiter")
    person_abteilungen = relationship("PersonToAbteilung", back_populates="person", cascade="all, delete")
    professorships = relationship("ProfessorshipToPerson", back_populates="person", cascade="all, delete")
    
    __table_args__ = (
        UniqueConstraint("title", "first_name", "last_name", name="uq_person_name_title"),
    )

    def get_all(self) -> List:
        try:
            query = select(Person)
            result = self.session.execute(query).scalars().all()
            return result
        except Exception as e:
            print(f"❌ Fehler bei get_all in PersonHandler: {e}")
            return []

    def to_dict(self) -> Dict[str, Any]:
        try:
            return {col.name: getattr(self, col.name) for col in self.__table__.columns}
        except Exception as e:
            print(f"❌ Fehler bei to_dict in PersonHandler: {e}")
            return {}


class PersonContact(Base):
    __tablename__ = "person_contact"
    id = Column(Integer, primary_key=True)
    person_id = Column(Integer, ForeignKey("person.id", ondelete="CASCADE"))
    phone = Column(Text)
    fax = Column(Text)
    email = Column(Text)
    comment = Column(Text)
    person = relationship("Person", back_populates="contacts")
    
    __table_args__ = (
        UniqueConstraint("person_id", "email", name="uq_contact_person_email"),
        UniqueConstraint("person_id", "phone", name="uq_contact_person_phone"),
       UniqueConstraint("person_id", "fax", name="uq_contact_person_fax"),
    )

class Abteilung(Base):
    __tablename__ = "abteilung"
    id = Column(Integer, primary_key=True)
    name = Column(Text)
    abteilungsleiter_id = Column(Integer, ForeignKey("person.id", ondelete="SET NULL"))
    leiter = relationship("Person", back_populates="departments")
    persons = relationship("PersonToAbteilung", back_populates="abteilung", cascade="all, delete")
    
    __table_args__ = (
        UniqueConstraint("name", name="uq_abteilung_name"),
    )

class PersonToAbteilung(Base):
    __tablename__ = "person_to_abteilung"
    id = Column(Integer, primary_key=True)
    person_id = Column(Integer, ForeignKey("person.id", ondelete="CASCADE"))
    abteilung_id = Column(Integer, ForeignKey("abteilung.id", ondelete="CASCADE"))
    person = relationship("Person", back_populates="person_abteilungen")
    abteilung = relationship("Abteilung", back_populates="persons")
    
    __table_args__ = (
        UniqueConstraint("person_id", "abteilung_id", name="uq_person_to_abteilung"),
    )

class Kostenstelle(Base):
    __tablename__ = "kostenstelle"
    id = Column(Integer, primary_key=True)
    name = Column(Text)
    professorships = relationship("Professorship", back_populates="kostenstelle")
    
    __table_args__ = (
        UniqueConstraint("name", name="uq_kostenstelle_name"),
    )

class Professorship(Base):
    __tablename__ = "professorship"
    id = Column(Integer, primary_key=True)
    kostenstelle_id = Column(Integer, ForeignKey("kostenstelle.id", ondelete="SET NULL"))
    name = Column(Text)
    kostenstelle = relationship("Kostenstelle", back_populates="professorships")
    persons = relationship("ProfessorshipToPerson", back_populates="professorship", cascade="all, delete")
    
    __table_args__ = (
        UniqueConstraint("kostenstelle_id", "name", name="uq_professorship_per_kostenstelle"),
    )

class ProfessorshipToPerson(Base):
    __tablename__ = "professorship_to_person"
    id = Column(Integer, primary_key=True)
    professorship_id = Column(Integer, ForeignKey("professorship.id", ondelete="CASCADE"))
    person_id = Column(Integer, ForeignKey("person.id", ondelete="CASCADE"))
    professorship = relationship("Professorship", back_populates="persons")
    person = relationship("Person", back_populates="professorships")
    
    __table_args__ = (
        UniqueConstraint("person_id", "professorship_id", name="uq_professorship_to_person"),
    )

class Building(Base):
    __tablename__ = "building"
    id = Column(Integer, primary_key=True)
    name = Column(Text)
    building_number = Column(Text)
    abkuerzung = Column(Text)
    rooms = relationship("Room", back_populates="building")

class Room(Base):
    __tablename__ = "room"
    id = Column(Integer, primary_key=True)
    building_id = Column(Integer, ForeignKey("building.id", ondelete="SET NULL"))
    name = Column(Text)
    floor = Column(Integer)
    building = relationship("Building", back_populates="rooms")
    person_links = relationship("PersonToRoom", back_populates="room", cascade="all, delete")
    transponder_links = relationship("TransponderToRoom", back_populates="room", cascade="all, delete")
    layout = relationship("RoomLayout", back_populates="room", uselist=False, cascade="all, delete")
    guid = Column(Text)

    __table_args__ = (
        UniqueConstraint("building_id", "name", name="uq_room_per_building"),
        UniqueConstraint("guid", name="uq_guid"),
    )

class PersonToRoom(Base):
    __tablename__ = "person_to_room"
    id = Column(Integer, primary_key=True)
    person_id = Column(Integer, ForeignKey("person.id", ondelete="CASCADE"))
    room_id = Column(Integer, ForeignKey("room.id", ondelete="CASCADE"))
    person = relationship("Person", back_populates="rooms")
    room = relationship("Room", back_populates="person_links")
    
    __table_args__ = (
        UniqueConstraint("person_id", "room_id", name="uq_person_to_room"),
    )

class Transponder(Base):
    __tablename__ = "transponder"
    id = Column(Integer, primary_key=True)
    issuer_id = Column(Integer, ForeignKey("person.id", ondelete="SET NULL"))
    owner_id = Column(Integer, ForeignKey("person.id", ondelete="SET NULL"))
    got_date = Column(Date)
    return_date = Column("return_date", Date)
    serial_number = Column(Text)
    comment = Column(Text)
    issuer = relationship("Person", foreign_keys=[issuer_id], back_populates="transponders_issued")
    owner = relationship("Person", foreign_keys=[owner_id], back_populates="transponders_owned")
    room_links = relationship("TransponderToRoom", back_populates="transponder", cascade="all, delete")
    
    __table_args__ = (
        UniqueConstraint("serial_number", name="uq_transponder_serial"),
    )

class TransponderToRoom(Base):
    __tablename__ = "transponder_to_room"
    id = Column(Integer, primary_key=True)
    transponder_id = Column(Integer, ForeignKey("transponder.id", ondelete="CASCADE"))
    room_id = Column(Integer, ForeignKey("room.id", ondelete="CASCADE"))
    transponder = relationship("Transponder", back_populates="room_links")
    room = relationship("Room", back_populates="transponder_links")

    __table_args__ = (
        UniqueConstraint("transponder_id", "room_id", name="uq_transponder_to_room"),
    )

class ObjectCategory(Base):
    __tablename__ = "object_category"
    id = Column(Integer, primary_key=True)
    name = Column(Text)
    objects = relationship("Object", back_populates="category")
    
    __table_args__ = (
        UniqueConstraint("name", name="uq_object_category_name"),
    )

class Object(Base):
    __tablename__ = "object"
    id = Column(Integer, primary_key=True)
    name = Column(Text)
    price = Column(Float)
    category_id = Column(Integer, ForeignKey("object_category.id", ondelete="SET NULL"))
    category = relationship("ObjectCategory", back_populates="objects")
    
    __table_args__ = (
        UniqueConstraint("name", "category_id", name="uq_object_per_category"),
    )

class Lager(Base):
    __tablename__ = "lager"
    id = Column(Integer, primary_key=True)
    raum_id = Column(Integer, ForeignKey("room.id", ondelete="SET NULL"))

    __table_args__ = (
        UniqueConstraint("raum_id", name="uq_lager_raum"),
    )

class ObjectToLager(Base):
    __tablename__ = "object_to_lager"
    id = Column(Integer, primary_key=True)
    object_id = Column(Integer, ForeignKey("object.id", ondelete="CASCADE"))
    lager_id = Column(Integer, ForeignKey("lager.id", ondelete="CASCADE"))

    __table_args__ = (
        UniqueConstraint("object_id", "lager_id", name="uq_object_to_lager"),
    )

class Inventory(Base):
    __tablename__ = "inventory"
    id = Column(Integer, primary_key=True)
    owner_id = Column(Integer, ForeignKey("person.id", ondelete="SET NULL"))
    object_id = Column(Integer, ForeignKey("object.id", ondelete="SET NULL"))
    issuer_id = Column(Integer, ForeignKey("person.id", ondelete="SET NULL"))
    acquisition_date = Column(Date)
    got_date = Column(Date)
    return_date = Column("return_date", Date)
    serial_number = Column(Text)
    kostenstelle_id = Column(Integer, ForeignKey("kostenstelle.id", ondelete="SET NULL"))
    anlagennummer = Column(Text)
    comment = Column(Text)
    price = Column(Float)
    raum_id = Column(Integer, ForeignKey("room.id", ondelete="SET NULL"))
    professorship_id = Column(Integer, ForeignKey("professorship.id", ondelete="SET NULL"))
    abteilung_id = Column(Integer, ForeignKey("abteilung.id", ondelete="SET NULL"))

    owner = relationship("Person", foreign_keys=[owner_id], lazy="joined")
    issuer = relationship("Person", foreign_keys=[issuer_id], lazy="joined")
    object = relationship("Object", lazy="joined")
    kostenstelle = relationship("Kostenstelle", lazy="joined")
    abteilung = relationship("Abteilung", lazy="joined")
    professorship = relationship("Professorship", lazy="joined")
    room = relationship("Room", foreign_keys=[raum_id], lazy="joined")

class RoomLayout(Base):
    __tablename__ = "room_layout"
    id = Column(Integer, primary_key=True)
    room_id = Column(Integer, ForeignKey("room.id", ondelete="CASCADE"), nullable=False)
    x = Column(Integer, nullable=False)
    y = Column(Integer, nullable=False)
    width = Column(Integer, nullable=False)
    height = Column(Integer, nullable=False)

    room = relationship("Room", back_populates="layout")

    __table_args__ = (
        UniqueConstraint("room_id", name="uq_room_id"),
        UniqueConstraint("room_id", "x", "y", "width", "height", name="uq_room_id_x_y_width_height"),
    )
