import unittest
import sqlite3
import tempfile
import os
from subset_isolates import connect_to_database, close_database_connection, subset_isolates_by_metadata

class TestDatabaseFunctions(unittest.TestCase):
    def setUp(self):
        # Create a temporary database for testing
        self.db_fd, self.db_path = tempfile.mkstemp()
        self.conn, self.cursor = self.connect_to_temporary_database()

        # Initialize the database with a sample table
        self.initialize_database()

    def tearDown(self):
        # Close the temporary database and remove the temporary file
        self.close_database_connection()
        os.close(self.db_fd)
        os.remove(self.db_path)

    def connect_to_temporary_database(self):
        # Connect to the temporary database
        conn, cursor = connect_to_database(self.db_path)
        return conn, cursor

    def initialize_database(self):
        # Create a sample table in the database for testing
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS metadata (
                filename TEXT,
                filepath TEXT,
                bioproject TEXT,
                organism_name TEXT,
                submitter TEXT
            )
        ''')
        # Insert some sample data
        self.cursor.execute("INSERT INTO metadata (filename, filepath, bioproject, organism_name, submitter) VALUES (?, ?, ?, ?, ?)",
                            ("file1.fasta", "/path/to/file1", "project1", "organism1", "submitter1"))
        self.cursor.execute("INSERT INTO metadata (filename, filepath, bioproject, organism_name, submitter) VALUES (?, ?, ?, ?, ?)",
                            ("file2.fasta", "/path/to/file2", "project1", "organism1", "submitter2"))
        self.cursor.execute("INSERT INTO metadata (filename, filepath, bioproject, organism_name, submitter) VALUES (?, ?, ?, ?, ?)",
                            ("file3.fasta", "/path/to/file3", "project2", "organism1", "submitter3"))
        self.cursor.execute("INSERT INTO metadata (filename, filepath, bioproject, organism_name, submitter) VALUES (?, ?, ?, ?, ?)",
                            ("file4.fasta", "/path/to/file4", "project2", "organism1", "submitter1"))
        self.cursor.execute("INSERT INTO metadata (filename, filepath, bioproject, organism_name, submitter) VALUES (?, ?, ?, ?, ?)",
                            ("file5.fasta", "/path/to/file5", "project2", "organism1", "submitter2"))
        self.conn.commit()

    def close_database_connection(self):
        # Close the temporary database connection
        close_database_connection(self.conn)

    def test_subset_isolates_by_metadata(self):
        # Test the subset_isolates_by_metadata function with the temporary database
        tests = [
            (("project2", "bioproject"), [("file3.fasta", "/path/to/file3"), ("file4.fasta", "/path/to/file4"), ("file5.fasta", "/path/to/file5")]),
            (("nonexisiting", "fakecolumn"), None),
            (("submitter3", "submitter"), [("file3.fasta", "/path/to/file3")])
        ]
        for test, expected_result in tests:
            result = subset_isolates_by_metadata(self.cursor, *test)
            self.assertEqual(result, expected_result)


if __name__ == '__main__':
    unittest.main()
