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

class AutoModelView(ModelView):
    """Automatisch alle Spalten + FK Dropdowns via QuerySelectField"""

    can_create = True
    can_edit = True   # <-- Damit Edit-Button angezeigt wird
    can_delete = True
    column_display_pk = True  # ID in List View optional sichtbar
    form_excluded_columns = ["id"]  # ID im Formular nicht bearbeiten

    def __init__(self, model, session, **kwargs):
        print(f"[DEBUG] Initializing AutoModelView for model: {model.__name__}")

        # -----------------------------
        # FORM
        # -----------------------------
        self.form_columns = [
            c.key for c in class_mapper(model).columns
            if c.key != "id" and not c.key.endswith("_id")
        ]
        self.form_extra_fields = {}

        # Many-to-One Beziehungen → Dropdowns
        for rel in class_mapper(model).relationships:
            if rel.direction.name == "MANYTOONE":
                name = rel.key
                if name not in self.form_columns:
                    self.form_columns.append(name)
                self.form_extra_fields[name] = QuerySelectField(
                    label=name.capitalize(),
                    query_factory=self._make_query_factory(rel, session),
                    get_label=lambda obj: str(obj),
                    allow_blank=True,
                    blank_text="-- Keine --",
                    widget=Select2Widget()
                )

        # Numerische Spalten (Integer & Float)
        for col in class_mapper(model).columns:
            if col.type.python_type == int and col.key != "id":
                self.form_extra_fields[col.key] = IntegerField(
                    label=col.key.capitalize()
                )
            elif col.type.python_type == float:
                self.form_extra_fields[col.key] = FloatField(
                    label=col.key.capitalize()
                )

        # -----------------------------
        # LIST VIEW
        # -----------------------------
        self.column_list = []
        self.column_formatters = {}

        for col in class_mapper(model).columns:
            if col.key.endswith("_id"):
                # FK wird ersetzt durch Relation
                rel_name = col.key[:-3]
                if hasattr(model, rel_name):
                    self.column_list.append(rel_name)
                    self.column_formatters[rel_name] = self._format_fk
            else:
                self.column_list.append(col.key)

        # Boolean-Felder als Checkbox anzeigen
        for col in class_mapper(model).columns:
            if col.type.python_type == bool:
                self.column_formatters[col.key] = lambda v, c, m, n: "✔" if getattr(m, n) else "✘"

        super().__init__(model, session, **kwargs)

    @staticmethod
    def _format_fk(view, context, model, name):
        val = getattr(model, name)
        return str(val) if val else ""

    @staticmethod
    def _make_query_factory(rel, session):
        return lambda rel=rel: session.query(rel.mapper.class_).all()

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
