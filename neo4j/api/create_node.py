from flask import Blueprint, request, jsonify

def create_create_node_bp(graph):
    bp = Blueprint("create_node", __name__)

    def fn_validate_request_body(data):
        if not data:
            return False, "Request-Body leer oder ungültig."

        # Validierung für den 'create_node' Request
        if "props" in data:
            props = data["props"]
            if not isinstance(props, dict) or not props:
                return False, "Props-Objekt ist leer oder ungültig."
            # Da props ein Wörterbuch ist, überprüfen wir, ob es mindestens ein property/value-Paar hat.
            # Wir können davon ausgehen, dass der erste Schlüssel der property-Name ist.
            property_name = next(iter(props), None)
            if not property_name or not str(property_name).isidentifier():
                return False, f"Ungültiger Property-Name in props: {property_name}"
            return True, None
        
        # Validierung für den 'update_nodes' Request
        if "property" in data and "value" in data:
            if not str(data["property"]).isidentifier():
                return False, f"Ungültiger Property-Name: {data['property']}"
            return True, None

        return False, "Unbekannter Request-Typ. Weder 'props' noch 'property' und 'value' gefunden."

    def fn_create_node(node_label, prop_name, value):
        print(f"Determined node label: {node_label}")
        
        # 1. Backticks für den Node-Label hinzufügen
        # Dies ist erforderlich, um Leerzeichen oder Sonderzeichen im Label zu behandeln.
        safe_node_label = f"`{node_label}`"
        
        # 2. Die Cypher-Abfrage anpassen, um den sicheren Label zu verwenden
        query = f"CREATE (n:{safe_node_label}) SET n.{prop_name}=$value RETURN ID(n) AS id"
        
        result = graph.run(query, value=value).data()
        
        if result and result[0]['id'] is not None:
            return result[0]['id']
        else:
            raise Exception("Failed to create new node in the database.")

    def fn_create_relationships(new_node_id, connect_data):
        if not connect_data:
            return

        for item in connect_data:
            if "id" in item:
                existing_node_id = item["id"]
                
                # Die gesamte Logik, um den Beziehungstyp zu bestimmen,
                # wird direkt in die Cypher-Abfrage verschoben.
                # Das Backend muss keine Annahmen mehr treffen.
                query_rel = f"""
                    MATCH (from_node) WHERE ID(from_node) = $from_id
                    MATCH (to_node) WHERE ID(to_node) = $to_id
                    
                    MERGE (from_node)-[rel:TYPE]->(to_node)
                    
                    ON CREATE SET rel.type = CASE
                        WHEN 'Person' IN labels(from_node) AND 'Buch' IN labels(to_node) THEN 'HAT_GESCHRIEBEN'
                        WHEN 'Person' IN labels(from_node) AND 'Ort' IN labels(to_node) THEN 'WOHNT_IN'
                        ELSE 'CONNECTED_TO'
                    END
                    RETURN rel
                """
                
                graph.run(query_rel, from_id=existing_node_id, to_id=new_node_id)

    @bp.route('/create_node', methods=['POST'])
    def api_create_node():
        try:
            data = request.get_json(silent=True)
            print(f"Incoming request data: {data}")

            is_valid, error_msg = fn_validate_request_body(data)
            if not is_valid:
                return jsonify({"status": "error", "message": error_msg}), 400

            props = data.get("props", {})
            node_label = data.get("node_label")
            connect_data = data.get("connectTo", [])
            
            prop_name = next(iter(props), None)
            value = props.get(prop_name)

            new_node_id = fn_create_node(node_label, prop_name, value)
            
            # Correct the function call here. 
            # Pass only the two expected arguments.
            fn_create_relationships(new_node_id, connect_data)

            print(f"Node creation process completed: {new_node_id}")
            return jsonify({
                "status": "success",
                "message": f"Neuer Node erstellt mit ID {new_node_id}",
                "newNodeId": new_node_id
            })

        except Exception as e:
            print(f"Exception in api_create_node: {e}")
            return jsonify({"status": "error", "message": str(e)}), 500

    return bp
