import click
import logging
import pandas as pd
import sqlite3
from os import path

from scripts.database_operations import create_databases

logging.basicConfig(
    filename='md_main.log',
    level=logging.DEBUG, 
    format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')

# test run: python md_main.py metadata --md ./test/data/gordon_db_ready.tsv --db "test.db"

EXPECTED_COLS_MD = frozenset([
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
    "asm_not_live_date"])

INSERT_COLS_IDENTIFIER = frozenset([
    'filename',
    'filepath',
    'external_accession',
    'external_source'])

INSERT_COLS_MD = EXPECTED_COLS_MD - {'filename', 'filepath'}

def check_input(path_md):
    """Checks if the input md file is valid and returns metadata as a pandas dataframe."""
    if path.isfile(path_md):
        md = pd.read_csv(path_md, sep='\t')
        actual_cols = set(md.columns.to_list())
        if actual_cols != EXPECTED_COLS_MD:
            raise IOError(f'Metadata does not contain expected headers, missing {EXPECTED_COLS_MD - actual_cols}')
        logging.info('metadata valid and loaded')
        return md
    else: 
        raise FileNotFoundError(path_md)


@click.group()
def cli():
    pass

@cli.command()
@click.option(
    '--md',
    type=click.Path(exists=True, dir_okay=False),
    help='Path to the metadata file (.tsv)')
@click.option(
    '--db',
    type=click.Path(dir_okay=False),
    help='Path to the database')
def metadata(md, db):
    """Insert metadata into the metadata table."""
    # --db and the following line to be removed once we finalize the db path
    create_databases(db)
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    # check metadata
    metadata = check_input(md)

    try:
        # insert into identifier and grab entity ids
        _fileinfo = metadata[INSERT_COLS_IDENTIFIER]
        entity_ids = []
        for _, row in _fileinfo.iterrows():
            cursor.execute(
                '''
                insert into identifier(filename, filepath, external_accession, external_source)
                values(?, ?, ?, ?)
                returning entity_id
                ''',
                (row['filename'], row['filepath'], row['external_accession'], row['external_source'])
            )
            entity_id = cursor.fetchone()[0]
            entity_ids.append(entity_id)
        logging.info('table identifier updated')
    
        # insert into metadata
        _md = metadata[INSERT_COLS_MD]
        _entity_ids_df = pd.DataFrame({'entity_id': entity_ids})
        _md = pd.concat([_entity_ids_df, _md], axis=1)
        _md.to_sql('metadata', conn, if_exists='append', index=False)
        logging.info('table metadata updated')

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    
    conn.close()

@cli.command()
@click.option('--input')
def quast(input):
    return

if __name__ == '__main__':
    cli()
