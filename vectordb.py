import os
import psycopg2
import numpy as np
import logging
from dotenv import load_dotenv
from config_paths import config

load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def generate_embeddings():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            user=os.getenv("DB_USERNAME"),
            password=os.getenv("DB_PASSWORD"),
            dbname=os.getenv("DB_NAME")
        )
        cur = conn.cursor()
        logging.info("Successfully connected to PostgreSQL.")
    except Exception as e:
        logging.error("Database connection failed.", exc_info=True)
        return

    try:
        cur.execute("SELECT SourceDocumentID, DataDescription FROM etl_data")
        rows = cur.fetchall()
    except Exception as e:
        logging.error("Failed to fetch data from etl_data.", exc_info=True)
        cur.close()
        conn.close()
        return

    if not rows:
        logging.warning("No rows found in etl_data table.")
        cur.close()
        conn.close()
        return

    logging.info(f"Generating embeddings for {len(rows)} rows...")

    for source_doc_id, description in rows:
        try:
            description = description or ""

            # Generate a fake embedding (1536-dim) for testing
            embedding = np.random.rand(1536).tolist()
            vector_str = "[" + ",".join(map(str, embedding)) + "]"

            # Insert with conflict handling to avoid duplicates
            cur.execute("""
                INSERT INTO etl_embeddings (SourceDocumentID, DataDescription, Embedding)
                VALUES (%s, %s, %s::vector)
                ON CONFLICT (SourceDocumentID) DO NOTHING
            """, (source_doc_id, description, vector_str))

            logging.info(f"Inserted embedding for SourceDocumentID {source_doc_id}")

        except Exception as e:
            logging.error(f"Failed to insert embedding for SourceDocumentID {source_doc_id}", exc_info=True)
            continue

    try:
        conn.commit()
        logging.info("All embeddings committed successfully.")
    except Exception as e:
        logging.error("Failed to commit transaction.", exc_info=True)

    cur.close()
    conn.close()
    logging.info("Database connection closed.")

if __name__ == "__main__":
    generate_embeddings()
