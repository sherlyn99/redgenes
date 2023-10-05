import os
import pandas as pd
import sqlite3
from tempfile import TemporaryDirectory
import unittest

from md_main import check_input, insert_into_identifier, insert_into_metadata


class TestMdMain(unittest.TestCase):

    def setUp(self):
        # Create a temporary directory for test output files
        self.temp_dir = TemporaryDirectory()
        self.db_connection = sqlite3.connect(':memory:')  # In-memory database
        self.db_cursor = self.db_connection.cursor()

        # Create the necessary tables for testing
        self.db_cursor.execute('''
            CREATE TABLE IF NOT EXISTS identifier(
                entity_id integer primary key autoincrement,
                filename varchar not null,
                filepath varchar not null,
                external_accession varchar,
                external_source varchar,
                active int default 1,
                created_at timestamp default current_timestamp,
                unique(filename, filepath),
                unique(external_accession, external_source)
            )
        ''')

        self.db_cursor.execute('''
            CREATE TABLE IF NOT EXISTS metadata (
                md_id integer primary key autoincrement,
                entity_id integer,
                external_source varchar,
                external_accession varchar,
                bioproject varchar,
                biosample varchar,
                wgs_master varchar,
                refseq_category varchar,
                taxid integer,
                species_taxid varchar,
                organism_name varchar,
                infraspecific_name varchar,
                isolate varchar,
                version_status varchar,
                assembly_level varchar,
                release_type varchar,
                genome_rep varchar,
                seq_rel_date varchar,
                asm_name varchar,
                submitter varchar,
                gbrs_paired_asm varchar,
                paired_asm_comp varchar,
                ftp_path varchar,
                excluded_from_refseq varchar,
                relation_to_type_material varchar,
                asm_not_live_date varchar,
                created_at timestamp default current_timestamp,
                foreign key (entity_id) references identifier (entity_id)
            )
        ''')

    def tearDown(self):
        # Clean up temporary files and directories
        self.temp_dir.cleanup()
        # Close the in-memory database connection
        self.db_connection.close()

    def test_check_input(self):
        # Test the check_input function with a valid metadata file
        metadata_file = './test/data/gordon_db_ready.tsv'
        md = check_input(metadata_file)
        self.assertIsInstance(md, pd.DataFrame)
        self.assertEqual(md.shape, (91,26))

    def test_insert_into_identifier(self):
        # Test the insert_into_identifier function
        md = pd.DataFrame({
            'filename': ['file1', 'file2'],
            'filepath': ['path1', 'path2'],
            'external_accession': ['acc1', 'acc2'],
            'external_source': ['source1', 'source2']
        })
        entity_ids = insert_into_identifier(self.db_connection, md)
        self.assertEqual(len(entity_ids), 2)

    def test_insert_into_metadata(self):
        # Test the insert_into_metadata function
        input_md = pd.DataFrame({
            "external_source": ["source1"],
            "external_accession": ["aaaa"],
            "filename": ["aaaa"],
            "filepath": ["aaaa"],
            "bioproject": ["aaaa"],
            "biosample": ["aaaa"],
            "wgs_master": ["aaaa"],
            "refseq_category": ["aaaa"],
            "taxid": ["aaaa"],
            "species_taxid": ["aaaa"],
            "organism_name": ["aaaa"],
            "infraspecific_name": ["aaaa"],
            "isolate": ["aaaa"],
            "version_status": ["aaaa"],
            "assembly_level": ["aaaa"],
            "release_type": ["aaaa"],
            "genome_rep": ["aaaa"], 
            "seq_rel_date": ["aaaa"],
            "asm_name": ["aaaa"],
            "submitter": ["aaaa"],
            "gbrs_paired_asm": ["aaaa"],
            "paired_asm_comp": ["aaaa"],
            "ftp_path": ["aaaa"],
            "excluded_from_refseq": ["aaaa"],
            "relation_to_type_material": ["aaaa"],
            "asm_not_live_date": ["aaaa"]
        })
        insert_into_metadata(self.db_connection, input_md, [2])
        sql_query = 'SELECT * FROM metadata'
        output_md = pd.read_sql_query(sql_query, self.db_connection)
        _entity_ids_df = pd.DataFrame({'entity_id': [2]})
        _md_ids_df = pd.DataFrame({'md_id': [1]})
        input_md = pd.concat([_md_ids_df, _entity_ids_df, input_md], axis=1)
        input_md = input_md.drop(['filename', 'filepath'], axis=1)
        output_md = output_md.drop('created_at', axis=1)
        print(input_md)
        print(output_md)
        self.assertTrue(output_md.equals(input_md))

if __name__ == '__main__':
    unittest.main()

