import os
from pathlib import Path
from collections import defaultdict
from skbio.io import read
from sql_initialize_db import initialize_db
from sql_connection import TRN

# Define constants for table names
TABLE_NAMES = ["RefSeq", "SO", "UniParc", "UniRef", "KEGG", "PFAM"]

# Define SQL templates for inserting data into different tables
INSERT_SQL_TEMPLATE = """INSERT INTO {} (bakta_accession, {})
                        VALUES (?, ?)"""

def read_gff_file(gff_file: str) -> list:
    """
    Read GFF file using skbio.

    Args:
        gff_file (str): Path to the GFF file.

    Returns:
        list: List of records from the GFF file.
    """
    if not Path(gff_file).exists():
        raise FileNotFoundError(f"Error: GFF file not found at {gff_file}")
    return read(gff_file, format="gff3")

def process_dbxref(dbxref_entry: str) -> defaultdict:
    """
    Process dbxref entry and return a dictionary.

    Args:
        dbxref_entry (str): Dbxref entry string.

    Returns:
        defaultdict: Processed dbxref entry as a dictionary.
    """
    entry_dict = defaultdict(list)
    if dbxref_entry:
        entries = [entry.split(':') for entry in dbxref_entry.split(',')]
        for entry_type, entry_value in entries:
            entry_dict[entry_type.strip()].append(entry_value.strip())
    return entry_dict

def insert_into_table(table_name: str, bakta_accession: int, values: list):
    """
    Insert values into a specified table.

    Args:
        table_name (str): Name of the table.
        bakta_accession (int): Bakta accession identifier.
        values (list): List of values to insert into the table.
    """
    # I made a different table for each TABLE_NAME
    sql = INSERT_SQL_TEMPLATE.format(table_name, table_name.lower())

    with TRN:
        for value in values:
            args = [bakta_accession, value]
            TRN.add(sql, args)
            TRN.execute()

def insert_bakta(entity_id: str, run_accession: int, contig_id: int, position: int, interval) -> int:
    """
    Insert data into bakta table and return bakta_accession.

    Args:
        entity_id (str): Entity identifier.
        run_accession (int): Run accession identifier.
        contig_id (int): Contig identifier.
        position (int): Position of the interval.
        interval: Interval object.

    Returns:
        int: Bakta accession identifier.
    """
    sql = """INSERT INTO bakta (entity_id, contig_id, position, gene_id, source, type, start, end, 
                               strand, phase, name, product, run_accession) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            returning bakta_accession"""
    args = [entity_id, contig_id, position, interval.metadata.get("ID"),
            interval.metadata.get("source"), interval.metadata.get("type"), 
            interval.bounds[0][0], interval.bounds[0][1], interval.metadata.get("strand"),
            interval.metadata.get("phase"), interval.metadata.get("Name"),
            interval.metadata.get("product"), run_accession]

    with TRN:
        TRN.add(sql, args)
        return TRN.execute_fetchflatten()[0]

def main_bakta(entity_id: str, temp_dir: str, run_accession: int) -> int:
    """
    Process bakta data and insert into the database.

    Args:
        entity_id (str): Entity identifier.
        temp_dir (str): Path to the temporary directory.
        run_accession (int): Run accession identifier.

    Returns:
        int: Return value (usually 0).
    """

    # Wasn't sure if this function needed to be divided further into smaller chunks
    gff_file = f"{temp_dir}/{entity_id}.gff3"
    contig_id = 0

    for rec in read_gff_file(gff_file):
        contig_id, interval_metadata = rec[0], rec[1]
        # I couldn't figure out a different way to convert the interval_metadata object to a list 
        interval_metadata = list(interval_metadata.query(metadata={'score':'.'}))

        for position, interval in enumerate(interval_metadata[1:], start=1):
            bakta_accession = insert_bakta(entity_id, run_accession, contig_id, position, interval)

            if 'db_xref' in interval.metadata:
                db_xref_dict = process_dbxref(interval.metadata['db_xref'])

                for table_name in TABLE_NAMES:
                    values = db_xref_dict.get(table_name)
                    if values:
                        insert_into_table(table_name, bakta_accession, values)

    return 0

# Example usage:
if __name__ == "__main__":
    initialize_db()
    temp_dir = "/Users/reneeoles/Desktop/bakta_plan/redgenes/redgenes/tests/data"    
    entity_id = "GCA_001699855.1"
    bakta_run_accession = 1
    main_bakta(str(entity_id), temp_dir, bakta_run_accession)
    entity_id = "GCA_008569345.1"
    bakta_run_accession = 2
    main_bakta(str(entity_id), temp_dir, bakta_run_accession)
