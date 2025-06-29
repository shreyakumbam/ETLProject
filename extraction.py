import jaydebeapi
import pandas as pd
import os
import logging
from csv_utils import update_etl_config
from config_paths import config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

def load_config(path):
    try:
        df = pd.read_csv(path)
        return df
    except Exception as e:
        logging.error(f"Failed to load config file: {e}")
        raise

def get_requested_fields(config_df, limit=9):
    try:
        config_df.columns = config_df.columns.str.strip().str.lower()
        fields = (
            config_df['source fieldname']
            .dropna()
            .str.strip()
            .str.lower()
            .head(limit)
            .tolist()
        )
        logging.info(f"Fields from config: {fields}")
        return fields
    except KeyError:
        logging.error(f"Missing 'source fieldname' in config file. Columns: {config_df.columns.tolist()}")
        raise
    except Exception as e:
        logging.error(f"Error parsing fields from config: {e}")
        raise

def extract_data(fields, config):
    jdbc_driver = os.getenv("JDBC_DRIVER")
    jdbc_url = os.getenv("JDBC_URL")
    username = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    jar_file = os.getenv("JDBC_JAR_PATH")
    table_name = os.getenv("DB_TABLE")

    # Quote for PostgreSQL case-sensitive fields
    field_str = ', '.join([f.lower() for f in fields])
    logging.info(f"Executing query: SELECT {field_str} FROM {table_name} LIMIT 9")

    try:
        conn = jaydebeapi.connect(jdbc_driver, jdbc_url, [username, password], jar_file)
        cursor = conn.cursor()
        query = f"SELECT {field_str} FROM {table_name} LIMIT 9"
        cursor.execute(query)
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=fields)
        df.to_csv(config["extracted_path"], index=False)
        logging.info("Extraction complete.")
    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        raise
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass

def main():
    update_etl_config(config["config_file"])
    config_df = load_config(config["config_file"])
    requested_fields = get_requested_fields(config_df)
    extract_data(requested_fields, config)

if __name__ == "__main__":
    main()
