from flask import Blueprint, request, jsonify, render_template
import time

def create_index_bp(graph):
    bp = Blueprint("index_bp", __name__)

    class GraphAPI:
        def __init__(self, driver):
            self.driver = driver

        def get_node_labels(self):
            try:
                result = self.driver.run("CALL db.labels()").data()
                return [r["label"] for r in result]
            except Exception as e:
                raise RuntimeError(f"Neo4j-Fehler bei get_node_labels: {e}")

        def get_properties_for_label(self, label):
            try:
                result = self.driver.run(
                    f"MATCH (n:`{label}`) UNWIND keys(n) AS k RETURN DISTINCT k AS prop"
                ).data()
                return [r["prop"] for r in result]
            except Exception as e:
                raise RuntimeError(f"Neo4j-Fehler bei get_properties_for_label({label}): {e}")

        def get_existing_indexes(self):
            try:
                result = self.driver.run("SHOW INDEXES").data()
                indexes = []
                for r in result:
                    # labelsOrTypes existiert in Neo4j 4.x/5.x, fallback falls Key fehlt
                    labels = r.get("labelsOrTypes") or r.get("entityType") or []
                    props = r.get("properties") or []
                    indexes.append({"labels": labels, "properties": props})
                return indexes
            except Exception as e:
                print(f"Warnung: Fehler beim Abrufen der Indizes: {e}")
                return []  # niemals None zurückgeben

        def create_index(self, label, prop):
            try:
                query = f"CREATE INDEX IF NOT EXISTS FOR (n:`{label}`) ON (n.`{prop}`)"
                self.driver.run(query)

                # systematisch prüfen, bis der Index ONLINE ist
                while True:
                    indexes = self.driver.run("SHOW INDEXES").data()
                    found = False
                    for idx in indexes:
                        labels = idx.get("labelsOrTypes") or idx.get("entityType") or []
                        props = idx.get("properties") or []
                        state = idx.get("state") or ""
                        if label in labels and prop in props:
                            found = True
                            if state.upper() == "ONLINE":
                                return  # Index ist fertig
                            else:
                                break  # Index existiert, aber nicht ONLINE
                    if not found:
                        raise RuntimeError(f"Index für {label}.{prop} wurde nicht gefunden")

    api = GraphAPI(graph)

    # === GET-Route: GUI-Seite ===
    @bp.route("/index_manager", methods=["GET"])
    def index_manager():
        try:
            node_labels = api.get_node_labels()
            label_props = {label: api.get_properties_for_label(label) for label in node_labels}
            existing_indexes = api.get_existing_indexes()
            return render_template(
                "index_manager.html",
                labels=node_labels,
                label_props=label_props,
                existing_indexes=existing_indexes,
            )
        except Exception as e:
            return f"Fehler beim Laden der Index-Seite: {e}", 500

    # === POST-Route: Index erstellen ===
    @bp.route("/create_indices", methods=["POST"])
    def create_indices():
        try:
            data = request.get_json()
            selections = data.get("indices", [])
            created = []

            for sel in selections:
                label = sel.get("label")
                prop = sel.get("property")
                if label and prop:
                    api.create_index(label, prop)
                    created.append({"label": label, "property": prop})

            return jsonify({"status": "success", "created": created})
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 500

    return bp
