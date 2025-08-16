from flask import Flask, render_template, request, redirect
from flask_arango_orm import ArangoORM, Document

app = Flask(__name__)
db = ArangoORM(app, url="http://localhost:8529", username="root", password="root", database="test_db")

class Person(Document):
    __collection__ = "persons"
    name = str
    age = int

db.register(Person)
db.create_all()  # erstellt die Collection, falls noch nicht vorhanden

@app.route("/")
def index():
    persons = Person.all()
    return render_template("index.html", persons=persons)

@app.route("/add", methods=["POST"])
def add_person():
    name = request.form["name"]
    age = int(request.form["age"])
    Person(name=name, age=age).save()
    return redirect("/")

@app.route("/delete/<key>")
def delete_person(key):
    person = Person.get(key)
    if person:
        person.delete()
    return redirect("/")

if __name__ == "__main__":
    app.run(debug=True)

