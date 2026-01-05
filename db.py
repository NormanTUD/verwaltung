import os
import sys
import inflect
from db_defs import *
from sqlalchemy.orm import sessionmaker, joinedload, Session, Query
from sqlalchemy_continuum import TransactionFactory, versioning_manager
from pathlib import Path
from urllib.parse import urlparse, urlunparse

Transaction = TransactionFactory(Base)

configure_mappers()
from init_helpers import normalize_sqlite_uri

full_url = 'sqlite:///database.db'

db_engine_file = "/etc/db_engine"

if os.path.isfile(db_engine_file):
    print(f"[DEBUG] {db_engine_file} ist eine Datei", file=sys.stderr)
    if os.access(db_engine_file, os.R_OK):
        print(f"[DEBUG] {db_engine_file} ist lesbar", file=sys.stderr)
        try:
            with open(db_engine_file, "r", encoding="utf-8") as f:
                file_content = f.read().strip()
                print(f"[DEBUG] Gelesener Inhalt: '{file_content}'", file=sys.stderr)
                if file_content:
                    full_url = file_content
                    print(f"[DEBUG] args.engine_db auf '{full_url}' gesetzt", file=sys.stderr)
                else:
                    print(f"[WARN] {db_engine_file} ist leer", file=sys.stderr)
        except Exception as e:
            print(f"[ERROR] Fehler beim Lesen von {db_engine_file}: {str(e)}", file=sys.stderr)
    else:
        print(f"[ERROR] Keine Leserechte für {db_engine_file}", file=sys.stderr)

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
