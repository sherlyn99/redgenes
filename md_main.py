import argparse
import logging
import pandas as pd
import pathlib
import shutil
import sqlite3
from os import path

from scripts.database_operations import *

# todo: turn all print and error statements into logging
# test run: python md_main.py metadata -m ./test/data/gordon_db_ready.tsv --db "test.db"

EXPECTED_COLS_MD = [
    "external_source",
    "external_accession",
    "filename",
    "filepath",
    "bioproject",
    "biosample",
    "wgs_master",
    "refseq_category",
    "taxid",
    "species_taxid",
    "organism_name",
    "infraspecific_name",
    "isolate",
    "version_status",
    "assembly_level",
    "release_type",
    "genome_rep", 
    "seq_rel_date",
    "asm_name",
    "submitter",
    "gbrs_paired_asm",
    "paired_asm_comp",
    "ftp_path",
    "excluded_from_refseq",
    "relation_to_type_material",
    "asm_not_live_date"
    ]

INSERT_COLS_IDENTIFIER = [
    'filename',
    'filepath',
    'external_accession',
    'external_source'
]

INSERT_COLS_MD = EXPECTED_COLS_MD.copy()
INSERT_COLS_MD.remove('filename')
INSERT_COLS_MD.remove('filepath')

def check_input(path_md):
    """Checks if the input md file is valid and returns metadata as a pandas dataframe."""
    if path.isfile(path_md):
        md = pd.read_csv(path_md, sep='\t')
        actual_cols = md.columns.to_list()
        print(actual_cols)
        actual_cols_n, expected_cols_n = len(actual_cols), len(EXPECTED_COLS_MD)
        if actual_cols_n == expected_cols_n:
            for i in range(actual_cols_n):
                if actual_cols[i] != EXPECTED_COLS_MD[i]:
                    raise IOError(f'Metadata does not contain expected headers. Check {actual_headers[i]}')
        else:
            raise IOError(f'Metadata does not have {expected_cols_n} columns.')
        print('Metadata loaded')
        return md
    else: 
        raise FileNotFoundError(path_md)

def insert_into_identifier(conn, md):
    """Insert relevant columns into identifiers and return corresponding entity ids."""
    cursor = conn.cursor()
    _fileinfo = md[INSERT_COLS_IDENTIFIER]

    # get existing entity_id
    cursor.execute("SELECT MAX(entity_id) FROM identifier")
    max_existing_id = cursor.fetchone()[0]
    if max_existing_id is None:
        max_existing_id = 0

    # insert new records into identifier
    _fileinfo.to_sql('identifier', conn, if_exists='append', index=False)

    # extract entity ids of newly added records
    cursor.execute(f"SELECT entity_id FROM identifier WHERE entity_id > {max_existing_id}")
    entity_ids = [row[0] for row in cursor.fetchall()]

    print('Table identifier updated')
    return entity_ids

def insert_into_metadata(conn, md, entity_ids):
    """Insert metadata into the metadata table."""
    _md = md[INSERT_COLS_MD]
    _entity_ids_df = pd.DataFrame({'entity_id': entity_ids})
    _md = pd.concat([_entity_ids_df, _md], axis=1)
    _md.to_sql('metadata', conn, if_exists='append', index=False)
    print('Table metadata loaded')
    return


def cli():
    parser = argparse.ArgumentParser(prog='md_main')
    subparsers = parser.add_subparsers(title = 'commands')

    parser_group1 = subparsers.add_parser('metadata', help='Insert metadata in batches')
    parser_group1.add_argument('-m', '--md', metavar='', type=pathlib.Path, help='Path to the metadata file (.tsv)')
    # --db will be deleted once we finalize the location of the db file
    parser_group1.add_argument('-db', '--db', metavar='', type=pathlib.Path, help='Path to the database')

    parser_group2 = subparsers.add_parser('quast', help='Run quast results in batches')
    parser_group2.add_argument('-i', '--input', metavar='', type=pathlib.Path, help='Path to the genome directory')

    args = parser.parse_args()
    
    # Metadata functions
    create_databases(args.db)
    conn = sqlite3.connect(args.db)
    md = check_input(args.md)
    eids = insert_into_identifier(conn, md)
    insert_into_metadata(conn, md, eids)
    conn.close()

if __name__ == '__main__':
    cli()