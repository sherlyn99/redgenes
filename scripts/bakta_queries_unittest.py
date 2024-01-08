import unittest
import sqlite3
import io
from tempfile import TemporaryDirectory
from bakta_queries import (
    connect_to_database,
    close_database_connection,
    get_gene_info_by_name,
    extract_gene_sequence_from_fasta,
    write_gene_sequences_to_fasta,
    extract_gene_sequence_to_fasta,
)

class TestYourScript(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for test output files
        self.temp_dir = TemporaryDirectory()
        self.db_connection = sqlite3.connect(':memory:')  # In-memory database
        self.db_cursor = self.db_connection.cursor()

        # Create the necessary tables for testing
        self.create_test_tables()

    def tearDown(self):
        # Clean up temporary files and directories
        self.temp_dir.cleanup()
        # Close the in-memory database connection
        self.db_connection.close()

    def test_connect_to_database(self):
        conn, cursor = connect_to_database(self.db_connection)
        self.assertIsInstance(conn, sqlite3.Connection)
        self.assertIsInstance(cursor, sqlite3.Cursor)

    def test_close_database_connection(self):
        conn, _ = connect_to_database(self.db_connection)  # Fixed this line
        close_database_connection(conn)
        with self.assertRaises(sqlite3.ProgrammingError):
            conn.execute("SELECT 1")

    def create_test_tables(self):
        # Create the 'bakta' and 'identifier' tables for testing
        self.db_cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS bakta (
                contig_id TEXT,
                start INT,
                end INT,
                strand TEXT,
                entity_id TEXT,
                gene_id TEXT,
                filename TEXT,
                filepath TEXT
            )
            """
        )

        self.db_cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS identifier (
                entity_id TEXT,
                filename TEXT,
                filepath TEXT
            )
            """
        )

        # Insert test data into 'bakta' table
        self.db_cursor.execute(
            """
            INSERT INTO bakta VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("Contig1", 1, 10, "1", "Entity1", "Gene1", "File1", "/path/to/files"),
        )

    def test_get_gene_info_by_name(self):
        conn, cursor = connect_to_database(self.db_connection)  # Fixed this line
        # Replace with your test data
        test_gene_name = "Gene1"
        test_column = "gene_id"
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS bakta (contig_id TEXT, start INT, end INT, strand TEXT, entity_id TEXT, gene_id TEXT, filename TEXT, filepath TEXT)"
        )
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS identifier (entity_id TEXT, filename TEXT, filepath TEXT)"
        )
        cursor.execute(
            "INSERT INTO bakta VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("Contig1", 1, 10, "1", "Entity1", "Gene1", "File1", "/path/to/files"),
        )
        gene_info = get_gene_info_by_name(cursor, test_gene_name, test_column)
        self.assertEqual(len(gene_info), 1)
        conn.close()

    def test_extract_gene_sequence_from_fasta(self):
        fasta_file_path = os.path.join(self.temp_dir.name, "test.fasta")
        # Create a test FASTA file
        with open(fasta_file_path, "w") as fasta_file:
            fasta_file.write(">Contig1\nACGT")

        gene_sequence = extract_gene_sequence_from_fasta(fasta_file_path, "Contig1", 1, 4, "1")
        self.assertEqual(gene_sequence, "CGT")

    def test_write_gene_sequences_to_fasta(self):
        outfile = io.StringIO()
        write_gene_sequences_to_fasta(outfile, gene_sequences)
        exp = '\n'.join(['>foo', 'ATGC', 
                     '>bar', 'TGCA', ''])  # empty string at the end to get an ending newline
        outfile.seek(0)
        obs = outfile.read()
        self.assertEqual(obs, exp)
        
    def test_extract_gene_sequence_to_fasta(self):
        output_file_path = os.path.join(self.temp_dir.name, "test_output.fasta")
        extract_gene_sequence_to_fasta(self.db_connection, "Gene1", "gene_id", output_file_path)  # Fixed this line
        self.assertTrue(os.path.isfile(output_file_path))
        # Add more assertions to check the content of the generated file if needed

if __name__ == "__main__":
    unittest.main()
