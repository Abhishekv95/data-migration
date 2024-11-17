import logging
from .config import configure_database

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def migrate_data(mysql_conn_string, postgres_conn_string, table_name):
    """Migrate data from MySQL to PostgreSQL."""
    try:
        # Configure MySQL
        mysql_engine, mysql_session_factory = configure_database(mysql_conn_string)
        mysql_session = mysql_session_factory()

        # Configure PostgreSQL
        postgres_engine, postgres_session_factory = configure_database(postgres_conn_string)
        postgres_session = postgres_session_factory()

        # Read data from MySQL
        logging.info(f"Fetching data from MySQL table: {table_name}")
        result = mysql_session.execute(f"SELECT * FROM {table_name}")
        rows = result.fetchall()
        column_names = result.keys()

        # Insert data into PostgreSQL
        logging.info(f"Inserting data into PostgreSQL table: {table_name}")
        insert_query = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({', '.join(['%s'] * len(column_names))})"
        postgres_session.execute(insert_query, rows)
        postgres_session.commit()

        logging.info(f"Data migration from {table_name} completed successfully.")

    except Exception as e:
        logging.error(f"Error during data migration: {e}")
    finally:
        mysql_session.close()
        postgres_session.close()
