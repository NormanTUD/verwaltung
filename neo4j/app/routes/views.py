from flask import Blueprint, render_template, request, current_app, redirect, url_for, flash, jsonify
from typing import List, Dict
from py2neo import Node

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
        rec = neo.run_cypher(
            "MATCH (v:View) WHERE elementId(v)=$vid RETURN v.cypher as cypher, v.name as name",
            {"vid": vid}
        )
        
        if not rec:
            return "View not found", 404

        cypher = rec[0]["cypher"]
        
        if "RETURN" not in cypher:
            return "Cypher-Query muss eine RETURN-Klausel enthalten.", 400

        raw_results = neo.run_cypher(cypher)
        
        # Leere Liste für die aufbereiteten Ergebnisse
        results = []
        
        # Liste für die Spaltennamen
        columns = []
        
        if raw_results:
            # Spaltennamen aus den Keys der ersten Zeile extrahieren
            # Wir nehmen die Keys aus dem Result-Set, die von Neo4j zurückgegeben wurden
            columns = raw_results[0].keys()

            for row in raw_results:
                flat_row = {}
                for k in columns:
                    v = row[k]
                    
                    if isinstance(v, Node):
                        # Wenn der Wert ein Knoten ist, extrahieren wir seine Eigenschaften
                        # und fügen die elementId als '__node_id' hinzu
                        flat_row.update(dict(v))
                        flat_row['__node_id'] = v.element_id
                    else:
                        flat_row[k] = v
                
                results.append(flat_row)
        
    except Exception as e:
        return f"Fehler beim Ausführen der View: {e}", 400

    return render_template("view_run.html", results=results, columns=columns, view_id=vid)

@bp.route("/add_column", methods=["POST"])
def add_column():
    data = request.json
    view_id = data.get("view_id")
    property_name = data.get("property_name")
    
    neo = current_app.neo
    try:
        # Cypher-Query der View abrufen
        rec = neo.run_cypher(
            "MATCH (v:View) WHERE elementId(v)=$vid RETURN v.cypher as cypher",
            {"vid": view_id}
        )
        if not rec:
            return jsonify({"success": False, "message": "View not found"}), 404

        cypher = rec[0]["cypher"]
        
        # Cypher-Query ausführen, um alle Knoten-IDs zu erhalten
        result_nodes = neo.run_cypher(cypher.replace("RETURN n", "RETURN n, elementId(n) AS __node_id"))
        
        node_ids = [row["__node_id"] for row in result_nodes]

        # Alle Knoten mit der neuen Eigenschaft aktualisieren
        neo.run_cypher(
            f"UNWIND $ids AS nodeId MATCH (n) WHERE elementId(n) = nodeId SET n.{property_name} = ''",
            {"ids": node_ids}
        )
        return jsonify({"success": True, "message": f"Spalte '{property_name}' erfolgreich hinzugefügt."})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 400

@bp.route("/add_row", methods=["POST"])
def add_row():
    data = request.json
    label = data.get("label")
    properties = data.get("properties", {})
    
    neo = current_app.neo
    try:
        cypher = f"CREATE (n:{label} $props) RETURN elementId(n) as __node_id"
        result = neo.run_cypher(cypher, {"props": properties})
        
        new_node_id = result[0]["__node_id"]
        
        return jsonify({"success": True, "node_id": new_node_id, "label": label, "properties": properties})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 400

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
