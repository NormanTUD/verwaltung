
def register_blueprints(app, graph):
    from api.get_data_as_table import create_get_data_bp
    from api.dump_database import create_dump_database_bp
    from api.reset_and_load_data import create_reset_and_load_data_bp
    from api.delete_node import create_delete_node_bp
    from api.delete_nodes import create_delete_nodes_bp
    from api.create_node import create_create_node_bp
    from api.add_property_to_nodes import create_add_property_to_nodes_bp
    from api.delete_all import create_delete_all_bp
    from api.graph_data import create_graph_data_bp
    from api.update_node import create_update_node_bp
    from api.add_row import create_add_row_bp
    from api.add_column import create_add_column_bp
    from api.update_nodes import create_update_nodes_bp
    from api.add_relationship import create_add_relationship_bp
    from api.reset_and_load_complex_data import create_complex_data_bp
    from api.labels import create_labels_bp
    from api.properties import create_properties_bp
    from api.relationships import create_relationships_bp
    from api.query_overview import create_query_overview
    from api.save_queries import create_save_queries
    from index_manager import create_index_bp

    app.register_blueprint(create_get_data_bp(graph), url_prefix='/api')
    app.register_blueprint(create_dump_database_bp(graph), url_prefix='/api')
    app.register_blueprint(create_reset_and_load_data_bp(graph), url_prefix='/api')
    app.register_blueprint(create_delete_node_bp(graph), url_prefix='/api')
    app.register_blueprint(create_add_property_to_nodes_bp(graph), url_prefix='/api')
    app.register_blueprint(create_delete_nodes_bp(graph), url_prefix='/api')
    app.register_blueprint(create_create_node_bp(graph), url_prefix='/api')
    app.register_blueprint(create_delete_all_bp(graph), url_prefix='/api')
    app.register_blueprint(create_graph_data_bp(graph), url_prefix='/api')
    app.register_blueprint(create_update_node_bp(graph), url_prefix='/api')
    app.register_blueprint(create_add_row_bp(graph), url_prefix='/api')
    app.register_blueprint(create_add_column_bp(graph), url_prefix='/api')
    app.register_blueprint(create_save_queries(), url_prefix='/api')
    app.register_blueprint(create_update_nodes_bp(graph), url_prefix='/api')
    app.register_blueprint(create_add_relationship_bp(graph), url_prefix='/api')
    app.register_blueprint(create_complex_data_bp(graph), url_prefix='/api')
    app.register_blueprint(create_labels_bp(graph), url_prefix='/api')
    app.register_blueprint(create_properties_bp(graph), url_prefix='/api')
    app.register_blueprint(create_relationships_bp(graph), url_prefix='/api')

    app.register_blueprint(create_index_bp(graph), url_prefix='/')
    app.register_blueprint(create_query_overview(), url_prefix='/')
