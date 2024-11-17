from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def configure_database(connection_string):
    """Create a database engine and session."""
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)
    return engine, Session
