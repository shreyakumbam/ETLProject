import os
import logging
import psycopg2
import pandas as pd
import json
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer
from config_paths import config
from bookembeddings import generate_book_embeddings

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

# Load environment variables
load_dotenv()

# Load embedding model (768-dimension)
model = SentenceTransformer('sentence-transformers/all-mpnet-base-v2')

def get_pg_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD"),
        dbname=os.getenv("DB_NAME")
    )

def load_csv_to_postgres(csv_path, table_name):
    df_columns = pd.read_csv(csv_path, nrows=0).columns
    normalized_columns = [col.lower() for col in df_columns]

    column_list = ', '.join([f'"{col}"' for col in normalized_columns])
    copy_command = f'COPY "{table_name}" ({column_list}) FROM STDIN WITH CSV HEADER'

    conn = None
    cursor = None
    try:
        conn = get_pg_connection()
        cursor = conn.cursor()

        cursor.execute(f'DELETE FROM "{table_name}"')
        logging.info(f"Cleared existing data from '{table_name}'.")

        with open(csv_path, 'r') as f:
            cursor.copy_expert(copy_command, f)

        conn.commit()
        logging.info(f"CSV data loaded into '{table_name}'.")
    except Exception as e:
        logging.error(f"Failed to load data: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def ensure_embedding_column(conn, table_name):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s AND column_name = 'embedding'
        """, (table_name.lower(),))
        if not cur.fetchone():
            logging.info("Adding 'embedding' column to table...")
            cur.execute(f'ALTER TABLE "{table_name}" ADD COLUMN embedding vector(768)')
            conn.commit()
        else:
            logging.info("'embedding' column already exists in the table.")

def generate_embeddings(table_name):
    conn = get_pg_connection()
    ensure_embedding_column(conn, table_name)

    with conn.cursor() as cur:
        cur.execute(f'SELECT * FROM "{table_name}" WHERE embedding IS NULL')
        rows = cur.fetchall()
        colnames = [desc[0].lower() for desc in cur.description]

        if not rows:
            logging.info("No rows found that need embeddings.")
            return

        if 'description' not in colnames:
            logging.error("Required column 'description' not found.")
            return

        id_col = 'sysdocid' if 'sysdocid' in colnames else colnames[0]
        id_idx = colnames.index(id_col)
        desc_idx = colnames.index('description')

        logging.info(f"Generating embeddings for {len(rows)} rows...")

        for row in rows:
            doc_id = row[id_idx]
            description = row[desc_idx] or ""

            try:
                embedding = model.encode(description, normalize_embeddings=True).tolist()
                embedding_str = "[" + ",".join(map(str, embedding)) + "]"

                cur.execute(f"""
                    UPDATE "{table_name}"
                    SET embedding = %s::vector
                    WHERE "{id_col}" = %s
                """, (embedding_str, doc_id))

                logging.info(f"Updated embedding for {id_col}: {doc_id}")
            except Exception as e:
                logging.error(f"Failed to update embedding for {id_col} {doc_id}: {e}")

    conn.commit()
    conn.close()
    logging.info("All embeddings updated successfully.")

def export_table_to_csv(table_name, output_file):
    conn = get_pg_connection()
    try:
        df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)
        df.to_csv(output_file, index=False)
        logging.info(f"Exported table '{table_name}' to CSV: {output_file}")
    except Exception as e:
        logging.error(f"Failed to export table to CSV: {e}")
    finally:
        conn.close()

def ensure_bookembedding_column(conn, table_name):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s AND column_name = 'bookembeddings'
        """, (table_name.lower(),))
        if not cur.fetchone():
            logging.info("Adding 'bookembeddings' column to table...")
            cur.execute(f'ALTER TABLE "{table_name}" ADD COLUMN bookembeddings jsonb')
            conn.commit()
        else:
            logging.info("'bookembeddings' column already exists in the table.")

def update_book_embeddings(table_name, book_embedding_df):
    conn = get_pg_connection()
    ensure_bookembedding_column(conn, table_name)

    book_embeddings = book_embedding_df[['id', 'page', 'chunk_idx', 'text', 'embedding']].to_dict(orient='records')

    with conn.cursor() as cur:
        # Assuming single row to update or adapt with condition
        cur.execute(f"""
            UPDATE "{table_name}"
            SET bookembeddings = %s
            WHERE bookembeddings IS NULL
        """, (json.dumps(book_embeddings),))

    conn.commit()
    conn.close()
    logging.info("Book embeddings updated in table.")

def main():
    csv_path = config["transformation_path"]
    table_name = os.getenv("DEST_TABLE") or "loader_table"
    output_csv = "loader_file.csv"

    load_csv_to_postgres(csv_path, table_name)
    generate_embeddings(table_name)

    # Generate book embeddings
    book_embedding_df = generate_book_embeddings()
    update_book_embeddings(table_name, book_embedding_df)

    export_table_to_csv(table_name, output_csv)

if __name__ == "__main__":
    main()
