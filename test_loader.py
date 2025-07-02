import unittest
from unittest.mock import patch, mock_open, MagicMock
import loader

class TestPostgresCSVLoader(unittest.TestCase):

    @patch("loader.psycopg2.connect")
    @patch.dict("os.environ", {
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_USERNAME": "user",
        "DB_PASSWORD": "pass",
        "DB_NAME": "testdb"
    })
    def test_get_pg_connection_success(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        conn = loader.get_pg_connection()
        self.assertEqual(conn, mock_conn)
        mock_connect.assert_called_once_with(
            host="localhost",
            port="5432",
            user="user",
            password="pass",
            dbname="testdb"
        )

    @patch("loader.psycopg2.connect", side_effect=Exception("Connection failed"))
    @patch.dict("os.environ", {
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_USERNAME": "user",
        "DB_PASSWORD": "pass",
        "DB_NAME": "testdb"
    })
    def test_get_pg_connection_failure(self, _):
        with self.assertRaises(Exception) as context:
            loader.get_pg_connection()
        self.assertIn("Connection failed", str(context.exception))

    @patch("loader.get_pg_connection")
    @patch("builtins.open", new_callable=mock_open, read_data="id,name\n1,Alice\n2,Bob\n")
    def test_load_csv_to_postgres_success(self, mock_file, mock_get_conn):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        loader.load_csv_to_postgres("dummy.csv", "test_table")

        mock_file.assert_called_once_with("dummy.csv", 'r')
        mock_cursor.copy_expert.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()

    @patch("loader.get_pg_connection", side_effect=Exception("DB error"))
    def test_load_csv_to_postgres_connection_failure(self, _):
        with self.assertRaises(Exception) as context:
            loader.load_csv_to_postgres("dummy.csv", "test_table")
        self.assertIn("DB error", str(context.exception))

    @patch("loader.load_csv_to_postgres")
    @patch.dict("os.environ", {"DEST_TABLE": "test_table"})
    @patch("loader.config", {"transformation_path": "dummy.csv"})
    def test_main_success(self, mock_loader):
        loader.main()
        mock_loader.assert_called_once_with("dummy.csv", "test_table")

    @patch("loader.load_csv_to_postgres")
    @patch.dict("os.environ", {}, clear=True)
    @patch("loader.config", {"transformation_path": "dummy.csv"})
    def test_main_missing_dest_table(self, mock_loader):
        with self.assertLogs(level="ERROR") as log_output:
            loader.main()
            self.assertIn("DEST_TABLE not defined in environment variables.", log_output.output[0])
            mock_loader.assert_not_called()

if __name__ == "__main__":
    unittest.main()
