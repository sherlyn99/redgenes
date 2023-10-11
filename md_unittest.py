import os
import pandas as pd
import sqlite3
import unittest
from click.testing import CliRunner

from scripts.database_operations import create_databases
from md_main import check_input, metadata

class TestMdMain(unittest.TestCase):

    def test_check_input(self):
        '''Test the check_input function with a valid metadata file'''
        metadata_file = './test/data/gordon_db_ready.tsv'
        md = check_input(metadata_file)
        self.assertIsInstance(md, pd.DataFrame)
        self.assertEqual(md.shape, (91,26))

    def test_metadata_command(self):
        runner = CliRunner()
        metadata_file = './test/data/gordon_db_ready.tsv'
        db_path = 'test.db'
        create_databases(db_path)
        result = runner.invoke(metadata, ['--md', metadata_file, '--db', db_path])
        print(result.output)
        print(result.exc_info)
        self.assertEqual(result.exit_code, 0)

        test_db_connection = sqlite3.connect(db_path)
        cursor = test_db_connection.cursor()
        cursor.execute('select * from identifier')
        rows = cursor.fetchall()
        self.assertEqual(len(rows), 91)

        cursor.execute('select * from metadata')
        rows = cursor.fetchall()
        self.assertEqual(len(rows), 91)

        test_db_connection.close()
        os.remove('test.db')
        os.remove('md_main.log')
    
if __name__ == '__main__':
    unittest.main()
