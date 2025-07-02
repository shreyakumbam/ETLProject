import unittest
import pandas as pd
from transformation import load_config, load_extracted_data, transform_data

class TestETLTransformationRealFiles(unittest.TestCase):

    def test_load_config_file(self):
        # Test that the config CSV loads and cleans invalid rows
        config_df = load_config("etl_config.csv")
        self.assertGreater(len(config_df), 0, "Config file is empty")
        self.assertIn("Source FieldName", config_df.columns, "Missing 'Source FieldName' column")
        self.assertIn("Target FieldName", config_df.columns, "Missing 'Target FieldName' column")
        self.assertFalse(config_df["Target FieldName"].isnull().any(), "Nulls found in 'Target FieldName'")
        self.assertEqual(config_df["Target FieldName"].duplicated().sum(), 0, "Duplicates found in 'Target FieldName'")

    def test_load_extracted_data_file(self):
        # Test that extracted CSV loads and columns are normalized (lowercase, stripped)
        df = load_extracted_data("extracted_data.csv")
        self.assertGreater(len(df), 0, "Extracted data is empty")
        self.assertIn("sourcedocumentid", df.columns, "'sourcedocumentid' column missing")
        self.assertIn("title", df.columns, "'title' column missing")

    def test_transform_data_output(self):
        # Load config and extracted data
        config_df = load_config("etl_config.csv")
        df = load_extracted_data("extracted_data.csv")

        # Filter config to only include mappings where source field is present in data
        valid_configs = config_df[config_df["Source FieldName"].str.lower().isin(df.columns)]
        expected_columns = valid_configs["Target FieldName"]
        expected_columns = expected_columns[expected_columns.notnull() & (expected_columns != "nan") & (expected_columns != "")]
        expected_columns = expected_columns.tolist()

        # Apply transformation
        transformed_df = transform_data(df, config_df)

        # Debug print to verify
        print("Expected columns:", expected_columns)
        print("Transformed DataFrame columns:", transformed_df.columns.tolist())

        # Check all expected columns exist in transformed data
        missing_columns = [col for col in expected_columns if col not in transformed_df.columns]
        if missing_columns:
            self.fail(f"Missing expected columns in transformed data: {missing_columns}")

        # Check required fields (with defaults) are not null
        required_fields = valid_configs[valid_configs["Target Default Value"] != ""]["Target FieldName"].tolist()
        for col in required_fields:
            if col in transformed_df.columns:
                self.assertFalse(
                    transformed_df[col].isnull().any(),
                    f"Column '{col}' has null values despite having default values"
                )

        # Check date fields have datetime dtype
        date_fields = valid_configs[valid_configs["Target DataType"].str.lower() == "date"]["Target FieldName"].tolist()
        for col in date_fields:
            if col in transformed_df.columns:
                self.assertTrue(
                    pd.api.types.is_datetime64_any_dtype(transformed_df[col]),
                    f"Column '{col}' is not datetime dtype"
                )

        # Check string fields have object dtype
        string_fields = valid_configs[valid_configs["Target DataType"].str.lower() == "string"]["Target FieldName"].tolist()
        for col in string_fields:
            if col in transformed_df.columns:
                self.assertTrue(
                    pd.api.types.is_object_dtype(transformed_df[col]),
                    f"Column '{col}' is not string dtype"
                )

if __name__ == "__main__":
    unittest.main()
