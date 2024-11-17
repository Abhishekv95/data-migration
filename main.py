from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection strings
MYSQL_CONN_STRING = "mysql+pymysql://<user>:<password>@<host>:<port>/<database>"
POSTGRES_CONN_STRING = "postgresql+psycopg2://<user>:<password>@<host>:<port>/<database>"

def migrate_data(table_name):
    try:
        # Connect to MySQL
        mysql_engine = create_engine(MYSQL_CONN_STRING)
        SessionMySQL = sessionmaker(bind=mysql_engine)
        mysql_session = SessionMySQL()
        
        # Connect to PostgreSQL
        postgres_engine = create_engine(POSTGRES_CONN_STRING)
        SessionPostgres = sessionmaker(bind=postgres_engine)
        postgres_session = SessionPostgres()

        # Read data from MySQL
        logging.info(f"Fetching data from MySQL table: {table_name}")
        result = mysql_session.execute(f"SELECT * FROM {table_name}")
        rows = result.fetchall()
        column_names = result.keys()

        # Insert data into PostgreSQL
        logging.info(f"Inserting data into PostgreSQL table: {table_name}")
        postgres_session.execute(
            f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({', '.join(['%s'] * len(column_names))})",
            rows
        )
        postgres_session.commit()

        logging.info(f"Data migration from {table_name} completed successfully.")

    except Exception as e:
        logging.error(f"Error during data migration: {e}")
    finally:
        mysql_session.close()
        postgres_session.close()

if __name__ == "__main__":
    table_name = input("Enter the table name to migrate: ")
    migrate_data(table_name)
