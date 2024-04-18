import pandas as pd
from sql_connection import TRN


def extract_md_info(md_path):
    """Load data from input and checks data integrity."""
    df = pd.read_csv(md_path, sep="\t", dtype=str)

    return df


def insert_metadata(row):
    """Insert relevant columns in identifier and metadata"""
    local_path = row["local_path"].strip()
    filename = row["assembly_accession"].strip()
    source = row["source"].strip()
    external_accession = row["assembly_accession"].strip()

    with TRN:
        sql_identifier = """
                INSERT INTO identifier (filename_full, filepath)
                VALUES (?, ?)
                RETURNING entity_id"""
        args_identifer = [filename, local_path]
        TRN.add(sql_identifier, args_identifer)
        entity_id = TRN.execute_fetchflatten()

        sql_md_info = """
            INSERT INTO md_info (entity_id, source, external_accession)
            VALUES (?, ?, ?)"""
        args_md_info = [source, external_accession]
        args_md_info = entity_id + args_md_info
        TRN.add(sql_md_info, args_md_info)

    return entity_id
