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
    """
    This class evaluates all user input to prepare for snakemake run.
    """

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
            'Exception: Database  not found'

        ValueError
            'ValueError: Database not a valid database'
        """
        logging.debug('Checking database')
        # Check database is real
