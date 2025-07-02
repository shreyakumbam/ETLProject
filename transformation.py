import pandas as pd
import logging
from config_paths import config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

def load_config(path):
    try:
        df = pd.read_csv(path)

        # Drop duplicates
        df = df.drop_duplicates(subset=["Target FieldName"], keep="first")

         # Fill NaN in "Target Default Value" with empty strings
        df["Target Default Value"] = df["Target Default Value"].fillna("").astype(str)

        # Strip spaces from all relevant columns
        df["Source FieldName"] = df["Source FieldName"].str.strip().str.lower()
        df["Target FieldName"] = df["Target FieldName"].astype(str).str.strip()
        df["Target DataType"] = df["Target DataType"].astype(str).str.strip().str.lower()

        # Replace empty Target FieldNames with the corresponding Target Default Values
        df["Target FieldName"] = df.apply(
            lambda row: row["Target Default Value"] if row["Target FieldName"] == "" else row["Target FieldName"],
            axis=1
        )

        # Drop rows where Target FieldName or DataType is still missing after fallback
        df = df.dropna(subset=["Target FieldName", "Target DataType"])
        df = df[df["Target FieldName"] != ""]  # In case both were empty

        return df

    except Exception as e:
        logging.error(f"Failed to load config file: {e}")
        raise

def load_extracted_data(path):
    try:
        df = pd.read_csv(path)
        df.columns = df.columns.str.strip().str.lower()
        return df
    except Exception as e:
        logging.error(f"Failed to load extracted data: {e}")
        raise

def transform_data(df, config_df):
    try:
        source_to_target = dict(zip(config_df["Source FieldName"], config_df["Target FieldName"]))
        data_type_map = dict(zip(config_df["Source FieldName"], config_df["Target DataType"]))
        default_value_map = dict(zip(config_df["Source FieldName"], config_df["Target Default Value"]))

        for source_col, target_col in source_to_target.items():
            if source_col not in df.columns:
                logging.warning(f"Column '{source_col}' not found in data. Skipping.")
                continue

            # Fill missing values if default is provided
            default_val = default_value_map.get(source_col)
            if default_val != "":
                df[source_col] = df[source_col].fillna(default_val)

            # Type conversion
            dtype = data_type_map.get(source_col, "")
            if dtype == "date":
                df[source_col] = pd.to_datetime(df[source_col], errors='coerce')
            elif dtype == "string":
                df[source_col] = df[source_col].astype(str)

        # Rename columns that exist in the data
        existing_cols = [col for col in source_to_target if col in df.columns]
        rename_map = {col: source_to_target[col] for col in existing_cols}
        df = df.rename(columns=rename_map)

        return df
    except Exception as e:
        logging.error(f"Error during transformation: {e}")
        raise

def main():
    config_df = load_config(config["config_file"])
    df = load_extracted_data(config["extracted_path"])
    transformed_df = transform_data(df, config_df)
    transformed_df.to_csv("transformed_data.csv", index=False)
    logging.info("Transformation complete.")

if __name__ == "__main__":
    main()