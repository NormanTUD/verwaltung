import io
import pandas as pd
from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app, jsonify
from werkzeug.utils import secure_filename
import csv

bp = Blueprint("importer", __name__, template_folder="../../templates", static_folder="../../static")

def normalize_colname(name: str) -> str:
    return "".join(c.lower() if c.isalnum() else "_" for c in name.strip())

@bp.route("/upload", methods=["GET", "POST"])
def upload():
    if request.method == "GET":
        return render_template("upload.html")

    # POST: entweder Datei oder Textbereich
    text = request.form.get("csv_text", None)
    file = request.files.get("csv_file", None)

    df = None
    csv_raw = None
    try:
        if file and file.filename:
            data = file.stream.read().decode("utf-8", errors="replace")
            csv_raw = data
        elif text:
            data = text
            csv_raw = text
        else:
            flash("Keine CSV/TSV Daten gefunden", "danger")
            return redirect(url_for("importer.upload"))

        # delimiter automatisch erkennen
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(data.splitlines()[0])
        delimiter = dialect.delimiter

        df = pd.read_csv(io.StringIO(data), delimiter=delimiter, dtype=str).fillna("")

    except Exception as e:
        flash(f"Fehler beim Einlesen der CSV/TSV: {e}", "danger")
        return redirect(url_for("importer.upload"))

    # Spalten normalisieren
    original_cols = list(df.columns)
    normalized = {c: normalize_colname(c) for c in original_cols}
    df.rename(columns=normalized, inplace=True)

    # Vorschau anzeigen, User kann Label/Typ wählen
    preview = df.head(20).to_dict(orient="records")
    columns = list(df.columns)

    try:
        labels = [row["label"] for row in neo.run_cypher("CALL db.labels() YIELD label RETURN label")]
        all_nodes = {}
        for label in labels:
            cypher = f"MATCH (n:{label}) RETURN n, elementId(n) AS node_id LIMIT 100"
            raw_results = neo.run_cypher(cypher)
            nodes = []
            for row in raw_results:
                node = dict(row["n"]) if "n" in row else {}
                node["__node_id"] = row.get("node_id")
                nodes.append(node)
            all_nodes[label] = nodes
    except Exception as e:
        all_nodes = {}

    return render_template(
        "upload_preview.html",
        preview=preview,
        columns=columns,
        all_nodes=all_nodes,
        normalized=normalized,
        rowcount=len(df),
        csv_text=csv_raw  # Originaltext weitergeben
    )

@bp.route("/import_preview", methods=["POST"])
def import_preview():
    """
    Zeigt die CSV an, bevor sie importiert wird.
    """
    import io
    import pandas as pd
    from flask import request, render_template, current_app

    csv_text = request.form.get("csv_text")
    file = request.files.get("csv_file", None)

    # CSV einlesen
    try:
        if file and file.filename:
            data = file.stream.read().decode("utf-8", errors="replace")
            df = pd.read_csv(io.StringIO(data), dtype=str).fillna("")
        elif csv_text:
            df = pd.read_csv(io.StringIO(csv_text), dtype=str).fillna("")
        else:
            return "No CSV supplied", 400
    except Exception as e:
        return f"CSV parse error: {e}", 400

    # normalize columns
    def normalize_colname(name: str) -> str:
        return "".join(c.lower() if c.isalnum() else "_" for c in name.strip())
    df.rename(columns={c: normalize_colname(c) for c in df.columns}, inplace=True)

    neo = current_app.neo

    # alle existierenden Nodes abrufen (wie in all_nodes)
    try:
        labels = [row["label"] for row in neo.run_cypher("CALL db.labels() YIELD label RETURN label")]
        all_nodes = {}
        for label in labels:
            cypher = f"MATCH (n:{label}) RETURN n, elementId(n) AS node_id LIMIT 100"
            raw_results = neo.run_cypher(cypher)
            nodes = []
            for row in raw_results:
                node = dict(row["n"]) if "n" in row else {}
                node["__node_id"] = row.get("node_id")
                nodes.append(node)
            all_nodes[label] = nodes
    except Exception as e:
        all_nodes = {}

    # Preview vorbereiten (z.B. die ersten 5 Zeilen)
    preview = df.head(5).to_dict(orient="records")
    return render_template(
        "import_confirm.html",
        csv_text=csv_text,
        columns=list(df.columns),
        preview=preview,
        rowcount=len(df),
        all_nodes=all_nodes
    )


@bp.route("/import_confirm", methods=["POST"])
def import_confirm():
    """
    Import CSV into Neo4j with flexible column-to-entity mapping.
    Expects:
    - csv_text: raw CSV content (hidden field)
    - column_entity_<colname>: target entity for each column
    - new_entity_<colname>: optional new entity name for each column
    - primary_keys: list of column names used as primary key for MERGE
    """
    import io
    import pandas as pd
    from flask import request, jsonify, current_app

    csv_text = request.form.get("csv_text")
    primary_keys = request.form.getlist("primary_keys")
    file = request.files.get("csv_file", None)

    # reload CSV
    try:
        if file and file.filename:
            data = file.stream.read().decode("utf-8", errors="replace")
            df = pd.read_csv(io.StringIO(data), dtype=str).fillna("")
        elif csv_text:
            df = pd.read_csv(io.StringIO(csv_text), dtype=str).fillna("")
        else:
            return "No CSV supplied", 400
    except Exception as e:
        return f"CSV parse error: {e}", 400

    # normalize columns
    def normalize_colname(name: str) -> str:
        return "".join(c.lower() if c.isalnum() else "_" for c in name.strip())
    df.rename(columns={c: normalize_colname(c) for c in df.columns}, inplace=True)

    neo = current_app.neo

    # Build column -> entity mapping
    for col in df.columns:
        entity = request.form.get(f"column_entity_{col}", "Entity")
        map_field = request.form.get(f"map_field_{col}", col)
        new_field = request.form.get(f"new_field_{col}", "").strip()
        if new_field:
            map_field = new_field
        column_entity_map[col] = entity
        column_rename_map[col] = map_field


    created = 0
    updated = 0
    errors = []

    # Process each row
    for idx, row in df.iterrows():
        # Group properties by entity
        entity_props = {}
        for col, value in row.items():
            ent = column_entity_map.get(col, "Entity")
            entity_props.setdefault(ent, {})[col] = value if value and str(value) != "nan" else ""

        # Insert/Merge nodes per entity
        for ent, props in entity_props.items():
            # Determine merge keys
            keys = [k for k in primary_keys if k in props and props[k] != ""]

            if keys:
                # MERGE on composite key
                merge_map = "{" + ", ".join([f"{k}: $props.{k}" for k in keys]) + "}"
                cypher = f"MERGE (n:{ent} {merge_map}) SET n += $props RETURN elementId(n) as id"
            else:
                # No primary keys -> CREATE
                cypher = f"CREATE (n:{ent} $props) RETURN elementId(n) as id"

            params = {"props": props}

            try:
                res = neo.run_cypher(cypher, params)
                if res:
                    created += 1
            except Exception as e:
                errors.append(f"Row {idx}, Entity {ent}: {str(e)}")

    return jsonify({"created": created, "updated": updated, "errors": errors})

@bp.route("/columns/normalize_preview", methods=["POST"])
def normalize_preview():
    # returns normalized names for client preview
    import json
    data = request.get_json() or {}
    cols = data.get("columns", [])
    def normalize_colname(name: str) -> str:
        return "".join(c.lower() if c.isalnum() else "_" for c in name.strip())
    mapping = {c: normalize_colname(c) for c in cols}
    return mapping
