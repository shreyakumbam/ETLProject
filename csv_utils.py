import pandas as pd
import logging
import re


# Adds the field DataDescription to the table 
def append_new_config_row(path):
    try:
        df = pd.read_csv(path)
        logging.info(f"Existing config columns: {df.columns.tolist()}")

        first_col = df.columns[0]
        if (df[first_col].astype(str).str.strip() == "DataDescription").any():
            logging.info("Row with DataDescription already exists. No action taken.")
            return

        new_row = {
            df.columns[0]: "DataDescription",
            df.columns[1]: "Text",
            df.columns[2]: "Description"
        }

        df = pd.concat([pd.DataFrame([new_row]), df], ignore_index=True)
        df.to_csv(path, index=False)
        logging.info("New row prepended to config.")

    except Exception as e:
        logging.error(f"Failed to append new row: {e}")
        raise


# Removes 'im_' or 'ironmountain_im' prefixes from the Target FieldName column.
def clean_target_fieldname_column(path):
    try:
        df = pd.read_csv(path)
        logging.info(f"Loaded CSV with columns: {df.columns.tolist()}")

        # Find target fieldname column
        target_col = None
        for col in df.columns:
            if col.strip().lower() == "target fieldname":
                target_col = col
                break

        if target_col is None:
            logging.warning("Target FieldName column not found. No changes made.")
            return

        # Remove prefixes
        def remove_prefix(val):
            if pd.isna(val):
                return val
            val = str(val)
            return re.sub(r'^(ironmountain_im|im_)', '', val, flags=re.IGNORECASE)

        df[target_col] = df[target_col].apply(remove_prefix)
        df.to_csv(path, index=False)
        logging.info(f"Cleaned prefixes from '{target_col}' column and saved CSV.")

    except Exception as e:
        logging.error(f"Failed to clean Target FieldName column: {e}")
        raise


def update_etl_config(path):
    append_new_config_row(path)
    clean_target_fieldname_column(path)
