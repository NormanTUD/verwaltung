import random
from flask import Flask
from flask_appbuilder import AppBuilder, SQLA
from flask_appbuilder.models.sqla.interface import SQLAInterface
from flask_appbuilder.views import ModelView
from sqlalchemy import Column, Integer, String, ForeignKey, Float, Date
from sqlalchemy.orm import relationship
from datetime import datetime, timedelta

# -------------------------
# Flask + SQLAlchemy Setup
# -------------------------
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///test.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = 'mysecret'
db = SQLA(app)
appbuilder = AppBuilder(app, db.session)


# -------------------------
# Models
# -------------------------
class Department(db.Model):
    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)
    def __repr__(self):
        return self.name


class Person(db.Model):
    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)
    age = Column(Integer, nullable=False)
    department_id = Column(Integer, ForeignKey('department.id'), nullable=False)
    department = relationship("Department", backref="persons")
    def __repr__(self):
        return self.name


class Client(db.Model):
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    email = Column(String(120), nullable=False)
    def __repr__(self):
        return self.name


class Project(db.Model):
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    client_id = Column(Integer, ForeignKey("client.id"))
    owner_id = Column(Integer, ForeignKey("person.id"))
    client = relationship("Client", backref="projects")
    owner = relationship("Person", backref="projects")
    def __repr__(self):
        return self.name


class Task(db.Model):
    id = Column(Integer, primary_key=True)
    title = Column(String(100), nullable=False)
    description = Column(String(500))
    project_id = Column(Integer, ForeignKey("project.id"))
    assignee_id = Column(Integer, ForeignKey("person.id"))
    project = relationship("Project", backref="tasks")
    assignee = relationship("Person", backref="tasks_assigned")
    def __repr__(self):
        return self.title


class Invoice(db.Model):
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("project.id"))
    client_id = Column(Integer, ForeignKey("client.id"))
    amount = Column(Float)
    due_date = Column(Date)
    project = relationship("Project", backref="invoices")
    client = relationship("Client", backref="invoices")
    def __repr__(self):
        return f"Invoice {self.id}"


class TimeEntry(db.Model):
    id = Column(Integer, primary_key=True)
    task_id = Column(Integer, ForeignKey("task.id"))
    person_id = Column(Integer, ForeignKey("person.id"))
    hours = Column(Float)
    date = Column(Date)
    task = relationship("Task", backref="time_entries")
    person = relationship("Person", backref="time_entries")
    def __repr__(self):
        return f"{self.hours}h on {self.date}"


# -------------------------
# ModelViews
# -------------------------
class DepartmentModelView(ModelView):
    datamodel = SQLAInterface(Department)
    list_columns = ['name']


class PersonModelView(ModelView):
    datamodel = SQLAInterface(Person)
    list_columns = ['name', 'age', 'department']


class ClientModelView(ModelView):
    datamodel = SQLAInterface(Client)
    list_columns = ['name', 'email']


class ProjectModelView(ModelView):
    datamodel = SQLAInterface(Project)
    list_columns = ['name', 'client', 'owner']


class TaskModelView(ModelView):
    datamodel = SQLAInterface(Task)
    list_columns = ["title", "project", "assignee"]
    search_columns = ["title", "description"]


class InvoiceModelView(ModelView):
    datamodel = SQLAInterface(Invoice)
    list_columns = ["id", "project", "client", "amount", "due_date"]


class TimeEntryModelView(ModelView):
    datamodel = SQLAInterface(TimeEntry)
    list_columns = ["task", "person", "hours", "date"]


# -------------------------
# Sidebar
# -------------------------
appbuilder.add_view(DepartmentModelView, "Departments", icon="fa-folder", category="Management")
appbuilder.add_view(PersonModelView, "Persons", icon="fa-user", category="Management")
appbuilder.add_view(ClientModelView, "Clients", icon="fa-users", category="Management")
appbuilder.add_view(ProjectModelView, "Projects", icon="fa-briefcase", category="Management")
appbuilder.add_view(TaskModelView, "Tasks", icon="fa-tasks", category="Management")
appbuilder.add_view(InvoiceModelView, "Invoices", icon="fa-file-invoice", category="Management")
appbuilder.add_view(TimeEntryModelView, "Time Entries", icon="fa-clock", category="Management")


# -------------------------
# DB Init + Random Data
# -------------------------
def init_db():
    db.create_all()

    # Departments
    if not db.session.query(Department).count():
        deps = [Department(name="IT"), Department(name="HR"), Department(name="Finance")]
        db.session.add_all(deps)
        db.session.commit()

    # Persons
    if not db.session.query(Person).count():
        deps = db.session.query(Department).all()
        for i in range(10):
            db.session.add(Person(
                name=f"Person {i}",
                age=random.randint(20, 60),
                department=random.choice(deps)
            ))
        db.session.commit()

    # Clients
    if not db.session.query(Client).count():
        for i in range(5):
            db.session.add(Client(
                name=f"Client {i}",
                email=f"client{i}@example.com"
            ))
        db.session.commit()

    # Projects
    if not db.session.query(Project).count():
        persons = db.session.query(Person).all()
        clients = db.session.query(Client).all()
        for i in range(8):
            db.session.add(Project(
                name=f"Project {i}",
                owner=random.choice(persons),
                client=random.choice(clients)
            ))
        db.session.commit()

    # Tasks
    if not db.session.query(Task).count():
        projects = db.session.query(Project).all()
        persons = db.session.query(Person).all()
        for i in range(20):
            db.session.add(Task(
                title=f"Task {i}",
                description="Lorem ipsum " + str(random.randint(1000, 9999)),
                project=random.choice(projects),
                assignee=random.choice(persons)
            ))
        db.session.commit()

    # Invoices
    if not db.session.query(Invoice).count():
        projects = db.session.query(Project).all()
        clients = db.session.query(Client).all()
        for i in range(10):
            db.session.add(Invoice(
                project=random.choice(projects),
                client=random.choice(clients),
                amount=random.uniform(1000, 10000),
                due_date=datetime.now().date()
            ))
        db.session.commit()

    # TimeEntries
    if not db.session.query(TimeEntry).count():
        tasks = db.session.query(Task).all()
        persons = db.session.query(Person).all()
        for i in range(30):
            db.session.add(TimeEntry(
                task=random.choice(tasks),
                person=random.choice(persons),
                hours=random.uniform(1, 8),
                date=(datetime.now() - timedelta(days=random.randint(0, 30))).date()
            ))
        db.session.commit()


# Init DB direkt beim Start
init_db()


# -------------------------
# Run App
# -------------------------
if __name__ == "__main__":
    app.run(debug=True)
