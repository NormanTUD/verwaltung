from flask import Blueprint, request, jsonify

def create_graph_data_bp(graph):
    bp = Blueprint("graph_data", __name__)

    @bp.route('/graph-data')
    def get_graph_data():
        if graph is None:
            return jsonify({"error": "Neo4j connection not available"}), 500

        query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        RETURN n, m, r, ID(n) AS n_id, ID(m) AS m_id, ID(r) AS r_id
        """

        try:
            result = graph.run(query)

            nodes = {}
            links = []
            seen_links = set()

            for record in result:
                n = record['n']
                r = record['r']
                m = record['m']
                n_id = record['n_id']

                if n_id not in nodes:
                    nodes[n_id] = {
                        'id': n_id,
                        'label': next(iter(n.labels), None),
                        'properties': dict(n)
                    }

                if r is not None and m is not None:
                    m_id = record['m_id']
                    r_id = record['r_id']

                    if m_id not in nodes:
                        nodes[m_id] = {
                            'id': m_id,
                            'label': next(iter(m.labels), None),
                            'properties': dict(m)
                        }

                    if r_id not in seen_links:
                        rel_type = type(r)  # <- Hier liegt der Trick
                        if not isinstance(rel_type, str):
                            rel_type = r.__class__.__name__

                        links.append({
                            'source': n_id,
                            'target': m_id,
                            'type': rel_type,
                            'properties': dict(r)
                        })
                        seen_links.add(r_id)

            return jsonify({'nodes': list(nodes.values()), 'links': links})

        except Exception as e:
            return jsonify({"error": str(e)}), 500

    return bp
