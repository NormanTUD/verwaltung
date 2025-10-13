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

db = SQLAlchemy(model_class=Base)
