from flask import Blueprint, render_template, request, current_app, redirect, url_for, flash, jsonify
from typing import List, Dict

bp = Blueprint("views", __name__, template_folder="../../templates", static_folder="../../static")


def _safe_run_cypher(neo, cypher: str, params: dict = None) -> List[Dict]:
    try:
        return neo.run_cypher(cypher, params or {})
    except Exception:
        return []


@bp.route("/", methods=["GET"])
def index():
    neo = current_app.neo
    try:
        rows = neo.run_cypher(
            "MATCH (v:View) RETURN v.name as name, v.cypher as cypher, elementId(v) as id ORDER BY v.name"
        )
    except Exception as e:
        rows = []
        flash(f"Fehler beim Laden der Views: {e}", "danger")
    return render_template("views.html", views=rows)


@bp.route("/delete/<vid>", methods=["POST"])
def delete_view(vid):
    neo = current_app.neo
    try:
        neo.run_cypher("MATCH (v:View) WHERE elementId(v)=$id DETACH DELETE v", {"id": vid})
        flash("View erfolgreich gelöscht", "success")
    except Exception as e:
        flash(f"Fehler beim Löschen der View: {e}", "danger")
    return redirect(url_for("views.index"))


@bp.route("/create", methods=["GET", "POST"])
def create():
    neo = current_app.neo

    if request.method == "GET":
        try:
            result = neo.run_cypher(
                "CALL db.schema.nodeTypeProperties() "
                "YIELD nodeType, propertyName "
                "RETURN nodeType, collect(DISTINCT propertyName) as props"
            )
            filters = []
            for rec in result:
                label = rec["nodeType"]
                for prop in rec["props"]:
                    filters.append({
                        "id": f"{label}.{prop}",
                        "label": f"{label}.{prop}",
                        "type": "string",
                        "operators": ["equal", "not_equal", "contains", "begins_with", "ends_with"]
                    })
        except Exception as e:
            filters = []
            flash(f"Fehler beim Laden der Properties: {e}", "danger")

        return render_template("create_view.html", filters=filters)

    # POST: View speichern
    name = request.form.get("name")
    cypher = request.form.get("cypher")
    query_json = request.form.get("query_json")
    if not name or not cypher:
        flash("Name und Cypher sind notwendig", "danger")
        return redirect(url_for("views.create"))

    try:
        neo.run_cypher(
            "CREATE (v:View {name:$name, cypher:$cypher, query_json:$query_json}) RETURN elementId(v) as id",
            {"name": name, "cypher": cypher, "query_json": query_json}
        )
    except Exception as e:
        flash(f"Fehler beim Speichern der View: {e}", "danger")
        return redirect(url_for("views.create"))

    return redirect(url_for("views.index"))

@bp.route("/run/<vid>", methods=["GET"])
def run_view(vid):
    neo = current_app.neo
    try:
        print("==================== DEBUG ====================")
        print(f"Requested view id: {vid}")

        # View abfragen
        rec = neo.run_cypher(
            "MATCH (v:View) WHERE elementId(v)=$vid RETURN v.cypher as cypher, v.name as name",
            {"vid": vid}
        )
        print("==================== DEBUG ====================")
        print(f"Raw view query result: {rec}")

        if not rec:
            print("No view found!")
            return "View not found", 404

        cypher = rec[0]["cypher"]


        # Prüfen, ob 'elementId' schon drin ist
        if "elementId(" not in cypher:
            # automatisch elementId(n) als node_id ergänzen
            cypher = cypher.replace("RETURN n", "RETURN n, elementId(n) AS node_id")

        print("==================== DEBUG ====================")
        print(f"Cypher to execute:\n{cypher}")

        # Cypher-Query ausführen
        raw_results = neo.run_cypher(cypher)
        print("==================== DEBUG ====================")
        print(f"Raw results from view cypher ({len(raw_results)} rows):")
        for idx, r in enumerate(raw_results):
            print(f"Row {idx}: {r}")

        results = []

        for row_idx, row in enumerate(raw_results):
            print("==================== DEBUG ====================")
            print(f"Processing row {row_idx}: {row}")

            flat_row = {}
            node_id = None

            for k, v in row.items():
                if isinstance(v, dict):
                    flat_row.update(v)
                else:
                    flat_row[k] = v

            # node_id direkt aus der row
            if "node_id" in row:
                node_id = row["node_id"]


            # Node-ID direkt aus row, falls elementId() zurückgegeben wurde
            if "node_id" in row:
                node_id = row["node_id"]
                print(f"Row {row_idx}, found node_id in row: {node_id}")

            # Falls alles fehlschlägt: node_id = None
            if not node_id:
                print(f"Row {row_idx} has no node_id, will be None in output")

            flat_row["__node_id"] = node_id
            print(f"Final __node_id for row {row_idx}: {node_id}")
            print(f"Flat row {row_idx} contents: {flat_row}")

            results.append(flat_row)

        print("==================== DEBUG ====================")
        print("Prepared results to render:")
        for r in results:
            print(r)

    except Exception as e:
        print("==================== EXCEPTION ====================")
        print(e)
        return f"Fehler beim Ausführen der View: {e}", 400

    return render_template("view_run.html", results=results, view_id=vid)

@bp.route("/metadata", methods=["GET"])
def metadata():
    """
    Returns labels, relationship types and property keys discovered in the DB.
    The client uses this to populate the QueryBuilder filters and pattern selects.
    """
    neo = current_app.neo
    labels = _safe_run_cypher(neo, "CALL db.labels() YIELD label RETURN label")
    rels = _safe_run_cypher(neo, "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType")
    props = _safe_run_cypher(neo, "CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey")
    return jsonify({
        "labels": [r.get("label") for r in labels if r.get("label")],
        "relationshipTypes": [r.get("relationshipType") for r in rels if r.get("relationshipType")],
        "propertyKeys": [r.get("propertyKey") for r in props if r.get("propertyKey")]
    })


# -----------------------------
# FastAPI-kompatibler Update Endpoint für editable Tabellen
# -----------------------------
@bp.route("/api/node/<node_id>", methods=["PATCH"])
def update_node(node_id):
    neo = current_app.neo
    try:
        data = request.get_json(force=True)
        if not data:
            return jsonify({"error": "No data provided"}), 400

        set_clauses = []
        params = {"nid": node_id}
        for k, v in data.items():
            set_clauses.append(f"n.{k} = $param_{k}")
            params[f"param_{k}"] = v

        set_query = ", ".join(set_clauses)
        cypher = f"MATCH (n) WHERE elementId(n)=$nid SET {set_query} RETURN n"
        rec = neo.run_cypher(cypher, params)
        if not rec:
            return jsonify({"error": "Node not found"}), 404
        return jsonify({"status": "ok", "node": rec[0]["n"]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
