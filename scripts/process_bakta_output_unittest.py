import unittest
import tempfile
import os
import shutil
from unittest.mock import patch
from process_bakta_output import read_gff_file, process_qualifiers, process_dbxref, insert_bakta, parse_bakta, delete_bakta_output_files
from database_operations import create_databases

class BaseTestCase(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for testing
        self.temp_dir = tempfile.mkdtemp()

        self.temp_db = os.path.join(self.temp_dir, "mock.db")
        create_databases(self.temp_db)

        # Mock data for testing
        self.mock_gff_content = """
        ##gff-version 3
        contig_1\tsource\tgene\t1\t100\t.\t+\t.\tID=gene1
        contig_1\tsource\tgene\t150\t300\t.\t-\t.\tID=gene2
        """
        self.mock_gff_file = os.path.join(self.temp_dir, "mock.gff3")

        with open(self.mock_gff_file, "w") as mock_gff:
            mock_gff.write(self.mock_gff_content)

    def tearDown(self):
        # Delete the temporary directory and its contents
        shutil.rmtree(self.temp_dir)

class TestReadGFFFile(BaseTestCase):
    def test_read_gff_file(self):
        # Call the function with mock data
        gff_records = read_gff_file(self.mock_gff_file)

        # Assert that the generator is not None
        self.assertIsNotNone(gff_records)

class TestProcessQualifiers(BaseTestCase):
    def test_process_qualifiers(self):
        # Mock data for testing
        qualifiers = {"ID": "gene1", "Name": "GeneA", "product": "ProteinA"}
        qualifier_keys = ["ID", "Name", "product"]

        # Call the function with mock data
        processed_qualifiers = process_qualifiers(qualifiers, qualifier_keys)

        # Assert that the processed qualifiers match the expected result
        expected_result = {"ID": "gene1", "Name": "GeneA", "product": "ProteinA"}
        self.assertEqual(processed_qualifiers, expected_result)

class TestProcessDbxref(BaseTestCase):
    def test_process_dbxref(self):
        # Mock data for testing
        dbxref_entry = "RefSeq:123, UniParc:456, UniRef:789"

        # Call the function with mock data
        processed_dbxref = process_dbxref(dbxref_entry)

        # Assert that the processed dbxref entries match the expected result
        expected_result = {'RefSeq': '123', ' UniParc': '456', ' UniRef': '789'}
        self.assertEqual(processed_dbxref, expected_result)

class TestInsertBakta(BaseTestCase):
    @patch('process_bakta_output.insert_bakta')
    def test_insert_bakta(self, mock_insert_bakta):
        db = self.temp_db
        entity_id = "mock_entity"
        contig_id = "contig_1"
        position = 1
        gene_id = "gene1"
        source = "source"
        type = "gene"
        start = 1
        end = 100
        strand = "+"
        phase = None
        name = "GeneA"
        product = "ProteinA"
        refseq = "123"
        so = "456"
        uniparc = "789"
        uniref = "012"
        kegg = "345"
        pfam = "678"
        run_accession = 12345

        # Call the function with mock data
        result = insert_bakta(db, entity_id, contig_id, position, gene_id, source, type, start, end, strand, phase,
                     name, product, refseq, so, uniparc, uniref, kegg, pfam, run_accession)

        self.assertEqual(result, 0)

class TestParseBakta(BaseTestCase):
    @patch('process_bakta_output.insert_bakta')
    def test_parse_bakta(self, mock_insert_bakta):
        # Mock data for testing
        db = self.temp_db
        entity_id = "mock"
        temp_dir = self.temp_dir
        run_accession = 12345

        # Call the function with mock data
        parse_bakta(db, entity_id, temp_dir, run_accession)

        # Assert that the insert_bakta function was called with the expected arguments
        mock_insert_bakta.assert_called()

class TestDeleteBaktaOutputFiles(BaseTestCase):
    def test_delete_bakta_output_files(self):
        # Mock Bakta output directory
        bakta_output = "mock_output"
        bakta_output_path = os.path.join(self.temp_dir, bakta_output)

        # Create a file in the mock output directory
        os.makedirs(bakta_output_path, exist_ok=True)
        mock_file_path = os.path.join(bakta_output_path, "mock_file.txt")
        with open(mock_file_path, "w") as mock_file:
            mock_file.write("Mock content")

        # Call the function with the mock data
        delete_bakta_output_files(bakta_output, self.temp_dir)

        # Assert that the Bakta output directory and its contents were deleted
        self.assertFalse(os.path.exists(bakta_output_path))
        self.assertFalse(os.path.exists(mock_file_path))

if __name__ == '__main__':
    unittest.main()
