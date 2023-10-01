import argparse
import shutil
import sqlite3
import pathlib
import pandas as pd
from os import path

from scripts.database_operations import *

# todo: turn all print and error statements into logging
# test run: ython md_main.py metadata -m ./test/data/gordon_db_ready.tsv --db "test.db"


def check_input(path_md):
    '''
    Check if the input is valid.

    Args:
        path_md (pathlib.Path): Path to the metadata.tsv

    Returns:
        metadata pandas dataframe
    '''
    if path.isfile(path_md):
        md = pd.read_csv(path_md, sep='\t')
        
        cols = md.columns.to_list()
        ncols = len(cols)
        expected_cols = ["external_source","external_accession","filename","filepath","bioproject", "biosample", "wgs_master",
                        "refseq_category", "taxid", "species_taxid", "organism_name", "infraspecific_name",
                        "isolate", "version_status", "assembly_level", "release_type", "genome_rep", 
                        "seq_rel_date", "asm_name", "submitter", "gbrs_paired_asm", "paired_asm_comp", 
                        "ftp_path", "excluded_from_refseq", "relation_to_type_material", "asm_not_live_date"]
        expected_ncols = len(expected_cols)
        if ncols == expected_ncols:
            for i in range(ncols):
                if cols[i] != expected_cols[i]:
                    raise IOError(f'Metadata does not contain expected headers. Check {actual_headers[i]}')
        else:
            raise IOError(f'Metadata does not have {expected_ncols} columns.')
        print('md loaded')
        return md
    else: 
        raise FileNotFoundError(path_md)

def insert_into_identifier(conn, md):
    '''
    Insert into identifiers

    Args:
        conn (SQLite3 db connector): connection to gg2.db 
        md (pd.DataFrame): a pandas df consists of fileinfo and metadata

    Returns:
        metadata pandas dataframe
    '''
    expected_identifier_cols = ['filename', 'filepath', 'external_accession', 'external_source']
    _fileinfo = md[expected_identifier_cols]
    _fileinfo = _fileinfo.iloc[1:]
    _fileinfo.to_sql('identifier', conn, if_exists='append', index=False)
    print('identifier loaded')
    return

def insert_into_metadata(conn, md):
    '''
    Insert into metadata

    Args:
        conn (SQLite3 db connector): connection to gg2.db 
        md (pd.DataFrame): a pandas df consists of fileinfo and metadata

    Returns:
        metadata pandas dataframe
    '''
    expected_md_cols = ["external_accession", "bioproject", "biosample", "wgs_master",
                        "refseq_category", "taxid", "species_taxid", "organism_name", "infraspecific_name",
                        "isolate", "version_status", "assembly_level", "release_type", "genome_rep", 
                        "seq_rel_date", "asm_name", "submitter", "gbrs_paired_asm", "paired_asm_comp", 
                        "ftp_path", "excluded_from_refseq", "relation_to_type_material", "asm_not_live_date"]
    _md = md[expected_md_cols]
    _md = _md.iloc[1:]
    _md.to_sql('metadata', conn, if_exists='append', index=False)
    print('metadata loaded')
    return


def cli():
    ## define parsers
    parser = argparse.ArgumentParser(prog='md_main')
    subparsers = parser.add_subparsers(title = 'commands')

    parser_group1 = subparsers.add_parser('metadata', help='Insert metadata in batches')
    parser_group1.add_argument('-m', '--md', metavar='', type=pathlib.Path, help='Path to the metadata tsv')
    parser_group1.add_argument('-db', '--db', metavar='', type=pathlib.Path, help='Path to the database')

    parser_group2 = subparsers.add_parser('quast', help='Run quast results in batches')
    parser_group2.add_argument('-i', '--input', metavar='', type=pathlib.Path, help='Path to the genomes')

    args = parser.parse_args()
    
    ## metadata functions
    create_databases(args.db)
    conn = sqlite3.connect(args.db)
    md = check_input(args.md)
    insert_into_identifier(conn, md)
    insert_into_metadata(conn, md)
    conn.close()

if __name__ == '__main__':
    cli()