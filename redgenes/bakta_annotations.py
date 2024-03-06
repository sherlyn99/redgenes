from sql_connection import TRN
import pandas as pd
from pathlib import Path
from collections import defaultdict
from sql_initialize_db import initialize_db
from utils import (
    copy_and_unzip,
    run_command_and_check_outputs,
    read_gff_file,
    extract_gff_info,
    process_gff_info,
)
from exceptions import (
    InvalidInputTsv,
    BaktaError,
)
from sql_connection import TRN

bakta_version = 1
bakta_commands = 1

# Ensure all necessary imports are included
def process_dbxref(dbxref):
    """
    Process dbxref entry and return a structured dictionary.
    """
    entry_dict = defaultdict(list)
    if pd.notna(dbxref):
        entries = dbxref.split(';')
        for entry in entries:
            if ':' in entry:
                entry_type, entry_value = entry.split(':', 1)
                entry_dict[entry_type.strip()].append(entry_value.strip())
    return entry_dict

def insert_dbxref_info(bakta_accesion, dbxref_data):
    """
    Insert dbxref information into corresponding tables.
    """
    valid_tables = {'kegg', 'refseq', 'uniparc', 'uniref', 'so', 'pfam'} 
    for dbxref_type, accession_list in dbxref_data.items():
        table_name = dbxref_type.lower()  # Assuming table names are lowercase versions of dbxref types
        if table_name in valid_tables:
            sql = f"INSERT INTO {table_name} (bakta_accession, {table_name.upper()}) VALUES (?, ?)"
            for accession in accession_list:
                TRN.add(sql, [bakta_accesion, accession])
    TRN.execute()

def run_bakta(input_fasta: Path, outdir: Path, bakta_db):
    """
    Run Bakta and return the path to the output GFF and the command string.
    """
    stem = input_fasta.stem
    bakta_output_tsv = outdir / f"{stem}.tsv"
    commands = [
        "bakta",
        "--skip-plot",
        "--keep-contig-headers",
        "--db", str(bakta_db),
        "--output", str(outdir),
        "--prefix", str(stem),
        "--locus-tag", str(stem), 
        "--force",
        str(input_fasta)
    ]
    _, commands_str = run_command_and_check_outputs(
        commands, BaktaError, [bakta_output_tsv]
    )
    return bakta_output_tsv, commands_str

def extract_md_info(tsv_file: Path):
    """
    Load data from TSV and check data integrity.
    """
    if not tsv_file.exists():
        raise FileNotFoundError(f"TSV file not found: {tsv_file}")
    df = pd.read_csv(tsv_file, sep="\t", dtype=str)
    if df.shape[1] != 40:
        raise InvalidInputTsv(f"Invalid input TSV: {tsv_file}")
    df["annotation_date"] = pd.to_datetime(df["annotation_date"], errors="coerce")
    dtype_map = {
        "taxid": "Int64",
        "species_taxid": "Int64",
        "gc_percent": float,
        "replicon_count": "Int64",
        "scaffold_count": "Int64",
        "contig_count": "Int64",
        "total_gene_count": "Int64",
        "protein_coding_gene_count": "Int64",
        "non_coding_gene_count": "Int64",
    }
    df = df.astype(dtype_map)
    return df

def extract_bakta_results(gff_path: Path):
    """
    Extract information from a Bakta GFF output file.
    """
    # Use the determined position to read the file, skipping rows up to the header
    column_names = ["contig_ID", "type", "start", "stop", "strand", "locus_tag", "gene", "product", "dbxrefs"]
    df = pd.read_csv(gff_path, header=0, names=column_names, comment='#', sep='\t')
    return df

def annotation_pipeline(df: pd.DataFrame, tmpdir: Path, bakta_db: Path):
    """
    Annotate genomes based on Bakta and insert information into the database.
    """
    for _, row in df.iterrows():
        local_path, filename = Path((row["local_path"])), row["assembly_accession"]
        source, external_accession = row["source"], row["assembly_accession"]
        with copy_and_unzip(str(local_path), tmpdir) as input_fasta:
            bakta_output_tsv, bakta_commands = run_bakta(input_fasta, tmpdir, bakta_db)
        bakta_df = extract_bakta_results(str(bakta_output_tsv))

        with TRN:
            sql_identifier = """
                INSERT INTO identifier (filename_full, filepath)
                VALUES (?, ?)
                RETURNING entity_id"""
            args_identifer = [filename, str(local_path)]
            TRN.add(sql_identifier, args_identifer)
            entity_id = TRN.execute_fetchflatten()

            sql_md_info = """
                INSERT INTO md_info (entity_id, source, external_accession)
                VALUES (?, ?, ?)"""
            args_md_info = [source, external_accession]
            args_md_info = entity_id + args_md_info
            TRN.add(sql_md_info, args_md_info)

            sql_extract_bakta_run_info = """
                SELECT run_id FROM run_info
                WHERE software = 'bakta' and version = ? and commands = ?"""
            TRN.add(
                sql_extract_bakta_run_info, [bakta_version, bakta_commands]
            )
            bakta_run_id = TRN.execute_fetchflatten()

            if not bakta_run_id:
                sql_run_info_bakta = """
                    INSERT INTO run_info (software, version, commands)
                    VALUES (?, ?, ?) RETURNING run_id"""
                args_run_info_bakta = [
                    "bakta",
                    bakta_version,
                    bakta_commands,
                ]
                TRN.add(sql_run_info_bakta, args_run_info_bakta)
                bakta_run_id = TRN.execute_fetchflatten()

            bakta_df.insert(0, "entity_id", entity_id[0])
            bakta_df.insert(9, "run_accession", bakta_run_id[0])
            args_bakta_info = bakta_df.iloc[:, 0:10].values.tolist()
            sql_bakta_info = """
                INSERT INTO bakta (
                    entity_id,
                    contig_id,
                    type,
                    start,
                    stop,
                    strand,
                    locus_tag,
                    gene,
                    product,
                    run_accession)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING bakta_accession;"""
            TRN.add(sql_bakta_info, args_bakta_info, many=True)
            bakta_accession = TRN.execute()
            nested_list = [item for sublist in bakta_accession if sublist for item in sublist]
            bakta_df['bakta_accession'] = [item[0] for item in nested_list[2:] if item]

            for _, row in bakta_df.iterrows():
                dbxref_data = process_dbxref(row['dbxrefs'])
                insert_dbxref_info(row['bakta_accession'], dbxref_data)
    return

def main():
    """
    Main function to initialize DB and start annotation pipeline.
    """
    initialize_db()
    md_tsv = Path("tests/data/md_gordon_2.tsv")
    md_df = extract_md_info(md_tsv)
    tmpdir = Path("test_out")
    bakta_db = Path("/panfs/roles/redgenes/redgenes/bakta/db")  # Define your Bakta database path here
    annotation_pipeline(md_df, tmpdir, bakta_db)

if __name__ == "__main__":
    main()
