import unittest
import sqlite3
import tempfile
import os
from ..db_creation/database_operations import create_databases, insert_bakta, insert_software, insert_run

class TestDatabaseFunctions(unittest.TestCase):

    def setUp(self):
        # Create a temporary SQLite database for testing
        self.db_file = tempfile.mktemp()
        create_databases(self.db_file)

    def tearDown(self):
        # Clean up the temporary database file
        os.remove(self.db_file)

    def test_insert_bakta(self):
        # Test the insert_bakta function
        bakta_info = ("filename1", "contig_id1", "gene_id1", "source1", "type1", 1, 100, "+", "phase1", "gene_name1", "locus_tag1", "product1", "dbxref1")
        insert_bakta(self.db_file, bakta_info)

        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM bakta WHERE filename=?", ("filename1",))
        result = cursor.fetchone()
        conn.close()

        self.assertIsNotNone(result)
        self.assertEqual(result[0], 1)  # Check if the ID is correct

    def test_insert_software(self):
        # Test the insert_software function
        software_info = ("Bakta", "1.0", "--arguments", "Description of Bakta")
        insert_software(self.db_file, software_info)

        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM software WHERE software=?", ("Bakta",))
        result = cursor.fetchone()
        conn.close()

        self.assertIsNotNone(result)
        self.assertEqual(result[0], 1)  # Check if the ID is correct

    def test_insert_run(self):
        # Test the insert_run function
        run_info = ("filename1", "12345", "2023-09-01 10:00:00", 1)
        insert_run(self.db_file, run_info)

        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM run WHERE filename=?", ("filename1",))
        result = cursor.fetchone()
        conn.close()

        self.assertIsNotNone(result)
        self.assertEqual(result[0], "filename1")  # Check if the run_accession is correct

if __name__ == '__main__':
    unittest.main()
