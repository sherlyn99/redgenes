import subprocess
import pandas as pd
from pathlib import Path
from redgenes.sql_initialize_db import initialize_db
from redgenes.utils import (
    copy_and_unzip,
    run_command_and_check_outputs,
    read_gff_file,
    extract_gff_info,
    process_gff_info,
)
from redgenes.exceptions import (
    ProdigalError,
    KofamscanError,
    BarrnapError,
    InvalidInputTsv,
)
from redgenes.sql_connection import TRN

# insert results pandas df + entity_id into the sqlite3 tables
# save runinfo and runid
# write tests for gene_annotations
# test the workflow on NCBI genomes
# DONE: zip and unzip - make a copy into a genome folder under temp dir and remove when finished (use context managers)
# DONE: move prodigal, kofamscan, barrnap parameters to annotation_pipeline
# DONE: get rid of repetitive parts of the function, pipe stdout


def extract_md_info(tsv_file):
    """Load data from tsv and checks data integrity."""
    df = pd.read_csv(tsv_file, sep="\t", dtype=str)

    # Check the structure of input tsv
    try:
        assert df.shape[1] == 40
    except:
        raise InvalidInputTsv(f"Invalid input TSV: {tsv_file}")

    # Modify data types
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


def run_prodigal(input_fasta, outdir):
    stem = Path(input_fasta).stem  # extract genome name from filestem. e.g. hA10
    prodigal_output_gff = Path(outdir) / f"{stem}_cds.gff"
    prodigal_output_faa = Path(outdir) / f"{stem}.faa"

    # fmt: off
    commands = [
        "prodigal",
        "-i", input_fasta,
        "-o", prodigal_output_gff,
        "-a", prodigal_output_faa,
        "-f", "gff",
        "-q"
    ]
    # fmt: on
    run_command_and_check_outputs(
        commands, ProdigalError, [prodigal_output_gff, prodigal_output_faa]
    )
    return str(prodigal_output_gff), str(prodigal_output_faa)


def run_kofamscan(input_faa: Path, profile, kolist, outdir, tmpdir, cpu=1):
    stem = Path(input_faa).stem
    kofamscan_output_tsv = Path(outdir) / f"{stem}_kofamscan.tsv"

    # fmt: off
    commands = [
        "exec_annotation", input_faa,
        "-o", kofamscan_output_tsv,
        "-p", profile,
        "-k", kolist,
        "-f", "detail-tsv",
        "--no-report-unannotated",
        "--cpu", str(cpu),
        "--tmp-dir", Path(tmpdir) / f"kofamscan_tmp_{stem}",     
    ]
    # fmt: On
    run_command_and_check_outputs(commands, KofamscanError, [kofamscan_output_tsv])
    return str(kofamscan_output_tsv)


def run_barrnap(input_fasta, outdir, cpu=1):
    stem = Path(input_fasta).stem
    barrnap_output_gff = Path(outdir) / f"{stem}_rrna.gff"

    commands = ["barrnap", input_fasta, "--threads", str(cpu), "--quiet"]

    res = run_command_and_check_outputs(commands, BarrnapError)

    try:
        with open(barrnap_output_gff, "w") as output_file:
            # res.stdout is bytes-like object; use decode() to convert it to string
            output_file.write(res.stdout.decode())
    except Exception as e:
        raise BarrnapError(f"There is an error writing barrnap output: {e}")

    return str(barrnap_output_gff)


def extract_prodigal_results(gff_path):
    """Take a gff file path from prodigal output and return a formatted DataFrame
    containing all GFF3 information."""
    attributes_df = extract_gff_info(read_gff_file(gff_path))
    dtype_map = {
        "start": int,
        "end": int,
        "score": float,
        "phase": int,
        "gc_cont": float,
        "conf": float,
        "cscore": float,
        "rscore": float,
        "uscore": float,
        "tscore": float,
        "start_fuzzy": bool,
        "end_fuzzy": bool,
    }
    cols_to_front = ["contig_id", "ID", "type", "start", "end", "score"]
    gff_df = process_gff_info(attributes_df, cols_to_front, dtype_map)
    return gff_df


def extract_kofamscan_results(tsv_file):
    """Take a tsv file from kofamscan output and return a formatted
    DataFrame cotaining all kofamscan output information."""
    colnames = [
        "tab",
        "gene_name",
        "ko",
        "threshold",
        "score",
        "e_value",
        "ko_definition",
    ]
    df = pd.read_csv(
        tsv_file, sep="\t", comment="#", dtype=str, skiprows=2, names=colnames
    )

    if df.shape[1] != 7:
        raise InvalidInputTsv(f"Invalid kofamscan tsv file: {tsv_file}")

    df = df.drop("tab", axis="columns")
    dtype_map = {"threshold": float, "score": float, "e_value": float}
    df = df.astype(dtype_map)

    return df


def extract_barrnap_results(gff_path):
    """Take a gff file path from barrnap output and return a formatted DataFrame
    containing all GFF3 information."""
    attributes_df = extract_gff_info(read_gff_file(gff_path))
    # Keep only 16s rRNA
    attributes_df = attributes_df[attributes_df["Name"] == "16S_rRNA"]

    dtype_map = {
        "score": float,
        "start": int,
        "end": int,
        "start_fuzzy": bool,
        "end_fuzzy": bool,
    }
    cols_to_first = [
        "contig_id",
        "type",
        "Name",
        "start",
        "end",
        "score",
        "strand",
        "product",
    ]
    gff_df = process_gff_info(attributes_df, cols_to_first, dtype_map)
    return gff_df


def annotation_pipeline(df, tmpdir, kofamscan_profile, kofamscan_kolist):
    for index, row in df.iterrows():
        # Prepare data for 'identifier' table and 'md_info' table
        local_path = row["local_path"]

        _source_detailed = row["source"]
        # TODO: move this step out of this function, do data cleaning in pandas
        if _source_detailed in ["Gordon"]:  # to add more non-ncbi categories
            _source = "non-ncbi"
        else:
            _source = "ncbi"
        _external_accession = row["assembly_accession"]

        # Run genome annotation pipeline
        ## Step 1: Unzip fna.gz file
        with copy_and_unzip(local_path, tmpdir) as input_fasta:
            tmpdir_curr = input_fasta.parent  # e.g. tmpdir/hA10

            ## Step 2: Run prodigal to get {filename}_cds.gff
            # TODO: also save ' '.join(commands) and inserted into run_info
            prodigal_output_gff, prodigal_output_faa = run_prodigal(
                input_fasta, tmpdir_curr
            )

            ## Step 3: Run kofamscan to get {filename}_kofamscan.tsv
            kofamscan_output_tsv = run_kofamscan(
                prodigal_output_faa,
                kofamscan_profile,
                kofamscan_kolist,
                tmpdir_curr,
                tmpdir_curr,
            )

            ## Step 4: Run barrnap to get {filename}_rrna.gff
            barrnap_output_gff = run_barrnap(input_fasta, tmpdir_curr)

            ## Step 5: Extract results
            cds_df = extract_prodigal_results(prodigal_output_gff)
            kofamscan_df = extract_kofamscan_results(kofamscan_output_tsv)
            rrna_df = extract_barrnap_results(barrnap_output_gff)

        # ## identifier and md_info
        # sql_identifier = """
        #     INSERT INTO identifier (filename_full, filepath)
        #     VALUES (?, ?)
        #     RETURNING entity_id"""
        # args_identifer = [_filename_full, _local_path]
        # sql_md_info = """
        #     INSERT INTO md_info (entity_id, source, source_detailed, external_accession)
        #     VALUES (?, ?, ?, ?)"""
        # args_md_info = [_source, _source_detailed, _external_accession]

        # with TRN:
        #     TRN.add(sql_identifier, args_identifer)
        #     entity_id = TRN.execute_fetchflatten()
        #     args_md_info = entity_id + args_md_info
        #     TRN.add(sql_md_info, args_md_info)

        #     sql_cds_info = """
        #         INSERT INTO cds_info (
        #             entity_id,
        #             contig_id,
        #             gene_id,
        #             gene_type,
        #             start, end,
        #             strand, partial,
        #             start_type,
        #             stop_type,
        #             rbs_motif,
        #             rbs_spacer,
        #             gc_cont,
        #             conf,
        #             score,
        #             cscore,
        #             sscore,
        #             rscore,
        #             uscore,
        #             tscore,
        #             mscore)
        #         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
        #     args_cds_info = [entity_id + res for res in cds_results]
        #     TRN.add(sql_cds_info, args_cds_info, many=True)

        #     sql_ko_info = """
        #         INSERT INTO ko_info (
        #             entity_id,
        #             gene_name,
        #             ko,
        #             threshold,
        #             score,
        #             e_value,
        #             ko_definition)
        #         VALUES (?, ?, ?, ?, ?, ?, ?);"""
        #     args_ko_info = [entity_id + res for res in kofamscan_results]
        #     TRN.add(sql_ko_info, args_ko_info, many=True)

        #     sql_rrna_info = """
        #         INSERT INTO rrna_info (
        #             entity_id,
        #             contig_id,
        #             rrna_type,
        #             start,
        #             end,
        #             strand,
        #             source,
        #             score,
        #             product,
        #             note)
        #         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
        #     args_rrna_info = [entity_id + res for res in rrna_results]
        #     TRN.add(sql_rrna_info, args_rrna_info, many=True)

        #     TRN.execute()

    return


def main():
    # 1. Initialize db and run new patch files
    initialize_db()

    # 2. get genomes and their genome paths
    ## fetch from tsv tables
    md_tsv = "./redgenes/tests/data/md_gordon_2.tsv"
    md_df = extract_md_info(md_tsv)

    # 3. run genome annotation pipelines
    ## kofamscan parameters
    ko_profile = "/projects/greengenes2/20231117_annotations_prelim/kofam_scan/profiles/test_subset.hal"
    ko_kolist = (
        "/projects/greengenes2/20231117_annotations_prelim/kofam_scan/ko_list_subset"
    )
    annotation_pipeline(md_df, "./test_out", ko_profile, ko_kolist)


if __name__ == "__main__":
    main()
