import inflect
from db_defs import *
from sqlalchemy.orm import sessionmaker, joinedload, Session, Query
from sqlalchemy_continuum import TransactionFactory, versioning_manager

Transaction = TransactionFactory(Base)

configure_mappers()

full_url = "sqlite:///database.db"
engine = create_engine(full_url)

try:
    Base.metadata.create_all(engine, checkfirst=True)
except AssertionError as e:
    print(f"Error trying to create all tables. Did you forget to specify the database, which is needed for MySQL, but not SQLite? Error: {e}")
    sys.exit(1)

Session = sessionmaker(bind=engine)

TransactionTable = versioning_manager.transaction_cls

inflect_engine = inflect.engine()
