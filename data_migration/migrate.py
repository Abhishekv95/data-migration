import logging
from sqlalchemy import create_engine, Table, MetaData, insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from .config import configure_database

# Set up structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)

def migrate_data(mysql_conn_string, postgres_conn_string, table_name):
    """Migrate data from MySQL to PostgreSQL."""
    mysql_session = postgres_session = None

    try:
        # Configure MySQL connection
        mysql_engine, mysql_session_factory = configure_database(mysql_conn_string)
        mysql_session = mysql_session_factory()
        logger.info("Connected to MySQL.")

        # Configure PostgreSQL connection
        postgres_engine, postgres_session_factory = configure_database(postgres_conn_string)
        postgres_session = postgres_session_factory()
        logger.info("Connected to PostgreSQL.")

        # Fetch metadata for table schema
        logger.info(f"Fetching metadata for table: {table_name}")
        metadata = MetaData()
        metadata.reflect(bind=mysql_engine, only=[table_name])
        mysql_table = metadata.tables[table_name]

        # Fetch data from MySQL
        logger.info(f"Fetching data from MySQL table: {table_name}")
        result = mysql_session.execute(mysql_table.select())
        rows = result.fetchall()
        column_names = mysql_table.columns.keys()
        logger.info(f"Fetched {len(rows)} rows from {table_name}.")

        # Create table in PostgreSQL if it doesn't exist
        metadata.reflect(bind=postgres_engine)
        if table_name not in metadata.tables:
            logger.info(f"Table {table_name} does not exist in PostgreSQL. Creating it.")
            mysql_table.metadata.create_all(postgres_engine)

        # Insert data into PostgreSQL
        logger.info(f"Inserting data into PostgreSQL table: {table_name}")
        postgres_table = metadata.tables[table_name]
        insert_query = insert(postgres_table).values(rows)
        postgres_session.execute(insert_query)
        postgres_session.commit()
        logger.info(f"Data migration for {table_name} completed successfully.")

    except SQLAlchemyError as sql_err:
        logger.error(f"SQLAlchemy error during data migration: {sql_err}")
        if postgres_session:
            postgres_session.rollback()
    except Exception as e:
        logger.error(f"Unexpected error during data migration: {e}")
    finally:
        if mysql_session:
            mysql_session.close()
            logger.info("Closed MySQL session.")
        if postgres_session:
            postgres_session.close()
            logger.info("Closed PostgreSQL session.")
