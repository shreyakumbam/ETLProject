from dotenv import load_dotenv
import os

load_dotenv()

config = {
    "config_file": os.getenv("config_file"),
    "extracted_path": os.getenv("extracted_file"),
    "transformation_path": os.getenv("transformation_file"),
    "output_path": os.getenv("output_file"),
    "db_host": os.getenv("DB_HOST"),
    "db_port": os.getenv("DB_PORT"),
    "db_user": os.getenv("DB_USER"),
    "db_pass": os.getenv("DB_PASS"),
    "db_name": os.getenv("DB_NAME"),
    "jdbc_jar_path": os.getenv("JDBC_JAR_PATH"),
    "db_table": os.getenv("DB_TABLE")
}