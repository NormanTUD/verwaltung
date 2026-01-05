import os
import sys
import inflect
from db_defs import *
from sqlalchemy.orm import sessionmaker, joinedload, Session, Query
from sqlalchemy_continuum import TransactionFactory, versioning_manager


Transaction = TransactionFactory(Base)

configure_mappers()
from init_helpers import normalize_sqlite_uri, read_db_engine_file

full_url = 'sqlite:///database.db'
from constants import DB_ENGINE_FILE
# either keep full url or set it to contents of DB_ENGINE_FILE
engine_file_contents = read_db_engine_file(DB_ENGINE_FILE)
if engine_file_contents:
    full_url = engine_file_contents


"""
if os.path.isfile(DB_ENGINE_FILE):
    print(f"[DEBUG] {DB_ENGINE_FILE} ist eine Datei", file=sys.stderr)
    if os.access(DB_ENGINE_FILE, os.R_OK):
        print(f"[DEBUG] {DB_ENGINE_FILE} ist lesbar", file=sys.stderr)
        try:
            with open(DB_ENGINE_FILE, "r", encoding="utf-8") as f:
                file_content = f.read().strip()
                print(f"[DEBUG] Gelesener Inhalt: '{file_content}'", file=sys.stderr)
                if file_content:
                    full_url = file_content
                    print(f"[DEBUG] args.engine_db auf '{full_url}' gesetzt", file=sys.stderr)
                else:
                    print(f"[WARN] {DB_ENGINE_FILE} ist leer", file=sys.stderr)
        except Exception as e:
            print(f"[ERROR] Fehler beim Lesen von {DB_ENGINE_FILE}: {str(e)}", file=sys.stderr)
    else:
        print(f"[ERROR] Keine Leserechte für {DB_ENGINE_FILE}", file=sys.stderr)
"""
full_url = normalize_sqlite_uri(full_url)

engine = create_engine(full_url)

try:
    Base.metadata.create_all(engine, checkfirst=True)
except AssertionError as e:
    print(f"Error trying to create all tables. Did you forget to specify the database, which is needed for MySQL, but not SQLite? Error: {e}")
    sys.exit(1)

Session = sessionmaker(bind=engine)

TransactionTable = versioning_manager.transaction_cls

inflect_engine = inflect.engine()
