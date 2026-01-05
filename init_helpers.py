import argparse
from urllib.parse import urlparse
from pathlib import Path

def parsing():
    parser = argparse.ArgumentParser(description="Starte die Flask-App mit konfigurierbaren Optionen.")
    parser.add_argument('--debug', action='store_true', help='Aktiviere den Debug-Modus')
    parser.add_argument('--disable_login', action='store_true', help='Deaktivier den Login')
    parser.add_argument('--port', type=int, default=5000, help='Port für die App (Standard: 5000)')
    parser.add_argument('--engine-db', type=str, default='sqlite:///instance/database.db', help='URI für create_engine()')
    return parser.parse_args()



def normalize_sqlite_uri(uri: str) -> str:
    """
    Wenn es sich um eine SQLite URI handelt, konvertiere sie zu einem absoluten Pfad
    in ./instance/, behalte den Dateinamen bei.
    """
    parsed = urlparse(uri)

    if parsed.scheme != 'sqlite':
        # Keine SQLite URI, return original
        return uri

    # Hole den Datenbank-Dateinamen
    db_name = Path(parsed.path).name

    # Absoluter Pfad: $(pwd)/instance/<db_name>
    abs_path = Path.cwd() / 'instance' / db_name
    abs_path.parent.mkdir(parents=True, exist_ok=True)  # ensure ./instance exists

    # Neue URI zusammenbauen
    new_uri = f'sqlite:///{abs_path}'
    return new_uri
