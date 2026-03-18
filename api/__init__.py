def initialize_api(app):
    from api.read_as_table.routes import create_get_data_bp

    app.register_blueprint(create_get_data_bp(), url_prefix="/api")
