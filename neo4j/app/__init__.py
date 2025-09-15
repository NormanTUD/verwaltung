import os
from flask import Flask
from .config import Config
from .neo4j_wrapper import Neo4jWrapper
from dotenv import load_dotenv

load_dotenv()

def create_app():
    app = Flask(__name__, template_folder="templates", static_folder="../static")
    app.config.from_object(Config)

    # init neo4j wrapper and attach to app
    neo = Neo4jWrapper(
        uri=app.config["NEO4J_URI"],
        user=app.config["NEO4J_USER"],
        password=app.config["NEO4J_PASS"]
    )
    app.neo = neo

    # register blueprints
    from .routes.main import bp as main_bp
    from .routes.importer import bp as importer_bp
    from .routes.views import bp as views_bp
    from .routes.merge import bp as merge_bp
    from .routes.api import bp as api_bp

    app.register_blueprint(main_bp)
    app.register_blueprint(importer_bp, url_prefix="/importer")
    app.register_blueprint(views_bp, url_prefix="/views")
    app.register_blueprint(merge_bp, url_prefix="/merge")
    app.register_blueprint(api_bp, url_prefix="/api")

    return app
