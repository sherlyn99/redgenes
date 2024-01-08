# ----------------------------------------------------------------------------
# Copyright (c) 2023--, GG2 Database development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file COPYING.txt, distributed with this software.
# ----------------------------------------------------------------------------

import os
import logging
import sys
import pkg_resources
import re
from Bio import SeqIO
import gzip

class Test(object):

    def __init__(self, ap):

        args = ap.parse_args()

        for k, v in args.__dict__.items():
            setattr(self, k, v)

    def check_fasta(self):
        """
        Checks input fasta file

        Raises
        ------
        Exception
            'Exception: Fasta files not found'
        
        ValueError
            'ValueError: Fasta file not in fasta format'
        """
        logging.debug('Checking for fasta file')
        try:
            with open(self.fasta, "r") as handle:
                fasta = SeqIO.parse(handle, "fasta")
        except Exception:
            logging.error('Fasta file not found')
            sys.exit()
        if not (fasta):
            logging.error('Fasta file not in fasta format')
            sys.exit()

    def check_db(self):
        """
        Check database 

        Raises
        ------
        Exception
            'Exception: Database not found'

        ValueError
            'ValueError: Database not a valid database. Valid db name ends with .db'
        """
        logging.debug('Checking database')

        # Check database has been established
        if len(self.db) < 3 or self.db[-3:] != ".db":
            logging.error('Database not a valid database. Valid db name ends with .db')
            raise ValueError('Database not a valid database. Valid db name ends with .db')
            sys.exit()

        # Check database has been established
        if not os.path.exists(self.db):
            logging.error('Database not found')
            raise Exception('Database not found')
            sys.exit()

        # Check if all expected tables have been created
        expected_tables = [
            'bakta', 'metadata', 'run_info',
            'identifier','quast','software_info']
        conn = sqlite3.connect(self.db)
        cursor = conn.cursor()
        cursor.execute("select name from sqlite_master where type = 'table'")
        actual_tables = [row[0] for row in cursor.fetchall()]

        for table in expected_tables:
            assert table in actual_tables, f'Table {table} not found in the database.'

        conn.close()
        return
