import argparse
from urllib.parse import urlparse
from pathlib import Path
import os, sys

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

def read_db_engine_file(abs_path) -> str | None:
    # TODO: this function should not print in the long run
    if os.path.isfile(abs_path):
        print(f"[DEBUG] {abs_path} ist eine Datei", file=sys.stderr)
        if os.access(abs_path, os.R_OK):
            print(f"[DEBUG] {abs_path} ist lesbar", file=sys.stderr)
            try:
                with open(abs_path, "r", encoding="utf-8") as f:
                    file_content = f.read().strip()
                    print(f"[DEBUG] Gelesener Inhalt: '{file_content}'", file=sys.stderr)
                    if file_content:
                        print(f"[DEBUG] args.engine_db auf '{file_content}' gesetzt", file=sys.stderr)
                        return file_content
                    else:
                        print(f"[WARN] {abs_path} ist leer", file=sys.stderr)
            except Exception as e:
                print(f"[ERROR] Fehler beim Lesen von {abs_path}: {str(e)}", file=sys.stderr)
        else:
            print(f"[ERROR] Keine Leserechte für {abs_path}", file=sys.stderr)
    return None
