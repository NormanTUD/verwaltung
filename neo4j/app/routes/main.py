from flask import Blueprint, render_template, current_app, url_for

bp = Blueprint("main", __name__, template_folder="../../templates", static_folder="../../static")

@bp.route("/")
def index():
    links = [
        ("CSV Import", url_for("importer.upload")),
        ("Views (saved queries)", url_for("views.index")),
        ("Potential merges", url_for("merge.index")),
        ("API docs", url_for("api.index"))
    ]
    return render_template("index.html", links=links)
