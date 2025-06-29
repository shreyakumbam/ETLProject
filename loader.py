import os
import logging
import psycopg2
from dotenv import load_dotenv
from config_paths import config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

# Load environment variables from .env file
load_dotenv()

def get_pg_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            user=os.getenv("DB_USERNAME"),
            password=os.getenv("DB_PASSWORD"),
            dbname=os.getenv("DB_NAME")
        )
        logging.info("PostgreSQL connection established.")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to PostgreSQL: {e}")
        raise

def load_csv_to_postgres(csv_path, table_name):
    conn = None
    cursor = None
    try:
        conn = get_pg_connection()
        cursor = conn.cursor()
        with open(csv_path, 'r') as f:
            cursor.copy_expert(f"COPY \"{table_name}\" FROM STDIN WITH CSV HEADER", f)
        conn.commit()
        logging.info(f"Data successfully loaded into '{table_name}'.")
    except Exception as e:
        logging.error(f"Failed to load data: {e}")
        raise
    finally:
        if cursor:
            cursor.close()

def main():
    csv_path = config["transformation_path"]
    table_name = os.getenv("DEST_TABLE")
    
    if not table_name:
        logging.error("DEST_TABLE not defined in environment variables.")
        return

    load_csv_to_postgres(csv_path, table_name)

if __name__ == "__main__":
    main()
