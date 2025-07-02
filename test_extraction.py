import unittest
import pandas as pd
from extraction import load_config, get_requested_fields

class TestETLExtraction(unittest.TestCase):

    def test_load_config_file(self):
        # Test that the ETL config file loads successfully with expected columns.
        config_df = load_config("etl_config.csv")
        self.assertGreater(len(config_df), 0)
        self.assertIn("Source FieldName", config_df.columns)

    def test_get_requested_fields_from_config(self):
        # Test the list of requested fields extracted from the config file.
        config_df = load_config("etl_config.csv")
        fields = get_requested_fields(config_df)
        expected_fields = [
            'datadescription', 'sourcedocumentid', 'filename', 'title', 'qcscandate', 
            'cmodloaddate', 'scandate', 'documentid', 'batchreferenceid'
        ]
        self.assertEqual(fields, expected_fields)


class TestExtractedData(unittest.TestCase):

    def setUp(self):
        # Load the extracted data once for reuse in all tests.
        self.df = pd.read_csv("extracted_data.csv")
        self.expected_columns = [
            'datadescription', 'sourcedocumentid', 'filename', 'title', 'qcscandate', 
            'cmodloaddate', 'scandate', 'documentid', 'batchreferenceid'
        ]

    def test_column_names(self):
        # Ensure all expected columns are present in the extracted data.
        df_columns = [col.strip().lower() for col in self.df.columns]
        for col in self.expected_columns:
            self.assertIn(col, df_columns)

    def test_required_fields_not_null(self):
        # Ensure key fields are not null.
        required_fields = ['sourcedocumentid', 'filename', 'title']
        for col in required_fields:
            self.assertIn(col, self.df.columns)
            self.assertFalse(self.df[col].isnull().any(), f"Null values found in {col}")

if __name__ == "__main__":
    unittest.main()


