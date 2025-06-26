import jaydebeapi
import pandas as pd
import os
import logging
from config_paths import config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        logging.FileHandler("etl_extract.log"),
        logging.StreamHandler()
    ]
)

def load_config(path):
    try:
        df = pd.read_csv(path)
        return df
    except Exception as e:
        logging.error(f"Failed to load config file: {e}")
        raise

def get_requested_fields(config_df, limit=8):
    try:
        return (
            config_df['Source FieldName']
            .dropna()
            .str.strip()
            .str.lower()
            .head(limit)
            .tolist()
        )
    except KeyError as e:
        logging.error("Missing 'Source FieldName' in config file")
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

    field_str = ', '.join(fields)

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
    config_df = load_config(config["config_file"])
    requested_fields = get_requested_fields(config_df)
    extract_data(requested_fields, config)

if __name__ == "__main__":
    main()
