import os
import random
from datetime import datetime
from flask import Flask, render_template
from flask_sqlalchemy import SQLAlchemy
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView
from sqlalchemy.orm import class_mapper
from wtforms import IntegerField, FloatField
from wtforms_sqlalchemy.fields import QuerySelectField
from flask_admin.form import Select2Widget
from wtforms.validators import Optional as OptionalValidator
from wtforms_sqlalchemy.fields import QuerySelectField, QuerySelectMultipleField

# -------------------------
# Flask Setup
# -------------------------
app = Flask(__name__)
app.secret_key = "irgendein_langes_geheimes_string_passwort"
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///test.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# -------------------------
# Models
# -------------------------
from db_defs import *

from flask_admin.contrib.sqla import ModelView
from sqlalchemy.orm import class_mapper
from wtforms import IntegerField, FloatField
from wtforms.validators import Optional as OptionalValidator
from wtforms_sqlalchemy.fields import QuerySelectField, QuerySelectMultipleField
from flask_admin.form import Select2Widget

from flask_admin.contrib.sqla import ModelView
from sqlalchemy.orm import class_mapper
from wtforms import IntegerField, FloatField
from wtforms.validators import Optional as OptionalValidator
from wtforms_sqlalchemy.fields import QuerySelectField, QuerySelectMultipleField
from flask_admin.form import Select2Widget
from flask_admin.contrib.sqla import ModelView
from sqlalchemy.orm import class_mapper
from wtforms import IntegerField, FloatField
from wtforms.validators import Optional as OptionalValidator
from wtforms_sqlalchemy.fields import QuerySelectField, QuerySelectMultipleField
from flask_admin.form import Select2Widget

class AutoModelView(ModelView):
    """Automatisch alle Spalten + FK Dropdowns via QuerySelectField / QuerySelectMultipleField"""

    can_create = True
    can_edit = True
    can_delete = True
    column_display_pk = True
    form_excluded_columns = ["id"]

    def __init__(self, model, session, **kwargs):
        print(f"[DEBUG] Initializing AutoModelView for model: {model.__name__}")

        self.form_columns = [
            c.key for c in class_mapper(model).columns
            if c.key != "id" and not c.key.endswith("_id")
        ]
        self.form_extra_fields = {}

        # -----------------------------
        # Beziehungen debug
        # -----------------------------
        for rel in class_mapper(model).relationships:
            print(f"[DEBUG] Processing relationship: {rel.key}, direction={rel.direction.name}")
            name = rel.key

            factory = self._make_query_factory(rel, session)
            print(f"[DEBUG] Query factory for {name}: {factory}")

            if rel.direction.name == "MANYTOONE":
                if name not in self.form_columns:
                    self.form_columns.append(name)
                try:
                    self.form_extra_fields[name] = QuerySelectField(
                        label=name.capitalize(),
                        query_factory=factory,
                        get_label=lambda obj: str(obj),
                        allow_blank=True,
                        blank_text="-- Keine --",
                        widget=Select2Widget()
                    )
                    print(f"[DEBUG] Created MANYTOONE QuerySelectField for {name}")
                except Exception as e:
                    print(f"[ERROR] Failed to create MANYTOONE field {name}: {e}")
            elif rel.direction.name == "MANYTOMANY":
                if name not in self.form_columns:
                    self.form_columns.append(name)
                try:
                    self.form_extra_fields[name] = QuerySelectMultipleField(
                        label=name.capitalize(),
                        query_factory=factory,
                        get_label=lambda obj: str(obj),
                        widget=Select2Widget(multiple=True)
                    )
                    print(f"[DEBUG] Created MANYTOMANY QuerySelectMultipleField for {name}")
                except Exception as e:
                    print(f"[ERROR] Failed to create MANYTOMANY field {name}: {type(e).__name__}: {e}")


        # -----------------------------
        # Numerische Spalten debug
        # -----------------------------
        for col in class_mapper(model).columns:
            try:
                col_type = col.type.python_type
            except NotImplementedError:
                print(f"[DEBUG] Column {col.key} has no python_type, skipping")
                continue

            if col_type == int and col.key != "id":
                print(f"[DEBUG] Adding IntegerField for {col.key}")
                self.form_extra_fields[col.key] = IntegerField(
                    col.key.capitalize(),
                    validators=[OptionalValidator()]
                )
            elif col_type == float:
                print(f"[DEBUG] Adding FloatField for {col.key}")
                self.form_extra_fields[col.key] = FloatField(
                    col.key.capitalize(),
                    validators=[OptionalValidator()]
                )

        # -----------------------------
        # LIST VIEW
        # -----------------------------
        self.column_list = []
        self.column_formatters = {}

        for col in class_mapper(model).columns:
            if col.key.endswith("_id"):
                rel_name = col.key[:-3]
                if hasattr(model, rel_name):
                    self.column_list.append(rel_name)
                    self.column_formatters[rel_name] = self._format_fk
            else:
                self.column_list.append(col.key)

        # Boolean-Felder
        for col in class_mapper(model).columns:
            try:
                if col.type.python_type == bool:
                    self.column_formatters[col.key] = lambda v, c, m, n: "✔" if getattr(m, n) else "✘"
            except NotImplementedError:
                continue

        super().__init__(model, session, **kwargs)

    @staticmethod
    def _format_fk(view, context, model, name):
        val = getattr(model, name)
        return str(val) if val else ""

    @staticmethod
    def _make_query_factory(rel, session):
        """Return a function for query_factory to avoid WTForms 'tuple' bug"""
        def factory():
            result = session.query(rel.mapper.class_).all()
            print(f"[DEBUG] Query factory result for {rel.key}: {result}")
            # Sicherheit: falls Tuple, in Liste umwandeln
            if isinstance(result, tuple):
                print(f"[WARN] result is tuple, converting to list")
                result = list(result)
            return result
        return factory


# -------------------------
# Admin Setup
# -------------------------
admin = Admin(app, name="DB Verwaltung", template_mode="bootstrap4", base_template="admin_base.html")

for mapper in db.Model.registry.mappers:
    model = mapper.class_
    admin.add_view(AutoModelView(model, db.session))

# -------------------------
# Dynamische Startseite
# -------------------------
@app.route("/")
def index():
    tables = [{"name": view.name, "url": view.url} for view in admin._views]
    return render_template("index.html", tables=tables)


# -------------------------
# Run App
# -------------------------
if __name__ == "__main__":
    with app.app_context():
        db.init_app(app)
        Base.metadata.create_all(db.engine)

    app.run(debug=True)
