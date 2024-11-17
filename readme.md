# Data Migration Utility

A Python-based tool to migrate data from MySQL to PostgreSQL using SQLAlchemy, ensuring scalable and efficient data transfer for any dataset.

pip install .

from data_migration_utility import migrate_data

MYSQL_CONN_STRING = "mysql+pymysql://<user>:<password>@<host>:<port>/<database>"
POSTGRES_CONN_STRING = "postgresql+psycopg2://<user>:<password>@<host>:<port>/<database>"
TABLE_NAME = "your_table_name"

migrate_data(MYSQL_CONN_STRING, POSTGRES_CONN_STRING, TABLE_NAME)
