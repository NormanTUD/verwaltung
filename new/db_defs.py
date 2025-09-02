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


db = SQLAlchemy()

class Client(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    email = db.Column(db.String(120))
    phone = db.Column(db.String(50))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    projects = db.relationship("Project", backref="client", lazy=True)
    invoices = db.relationship("Invoice", backref="client", lazy=True)

    def __repr__(self):
        return f"{self.name}"


class Project(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    description = db.Column(db.String(300))
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    
    client_id = db.Column(db.Integer, db.ForeignKey("client.id"), nullable=True)
    tasks = db.relationship("Task", backref="project", lazy=True)
    milestones = db.relationship("Milestone", backref="project", lazy=True)

    def __repr__(self):
        return f"{self.name}"


class Task(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    description = db.Column(db.String(200))
    status = db.Column(db.String(50))
    due_date = db.Column(db.Date)
    
    project_id = db.Column(db.Integer, db.ForeignKey("project.id"), nullable=True)
    assigned_to = db.Column(db.String(100))

    def __repr__(self):
        return f"{self.description} ({self.status})"


class Milestone(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120))
    target_date = db.Column(db.Date)
    project_id = db.Column(db.Integer, db.ForeignKey("project.id"), nullable=False)

    def __repr__(self):
        return f"{self.name}"


class Invoice(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    amount = db.Column(db.Float)
    paid = db.Column(db.Boolean, default=False)
    client_id = db.Column(db.Integer, db.ForeignKey("client.id"), nullable=False)

    def __repr__(self):
        return f"Invoice {self.id} - {self.amount}€"
