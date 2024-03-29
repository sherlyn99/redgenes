import sys
import time
import numpy as np
import pandas as pd
from pathlib import Path
from redgenes.sql_initialize_db import initialize_db
from redgenes.utils import (
    MD_HEADER,
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
    _, commands_str = run_command_and_check_outputs(
        commands, ProdigalError, [prodigal_output_gff, prodigal_output_faa]
    )
    return str(prodigal_output_gff), str(prodigal_output_faa), commands_str


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
    _, commands_str = run_command_and_check_outputs(commands, KofamscanError, [kofamscan_output_tsv])
    return str(kofamscan_output_tsv), commands_str


def run_barrnap(input_fasta, outdir, cpu=1):
    stem = Path(input_fasta).stem
    barrnap_output_gff = Path(outdir) / f"{stem}_rrna.gff"

    commands = ["barrnap", input_fasta, "--threads", str(cpu), "--quiet"]

    res, commands_str = run_command_and_check_outputs(commands, BarrnapError)

    if res:
        try:
            with open(barrnap_output_gff, "w") as output_file:
                # res.stdout is bytes-like object; use decode() to convert it to string
                output_file.write(res.stdout.decode())
        except Exception as e:
            raise BarrnapError(f"There is an error writing barrnap output: {e}")

    return str(barrnap_output_gff), commands_str


def extract_md_info(md_str):
    """Load data from input and checks data integrity."""
    data = md_str.split("\t")
    data_with_nan = [np.nan if x == "" else x for x in data]
    df = pd.DataFrame([data_with_nan], columns=MD_HEADER)
    # df = pd.read_csv(tsv_file, sep="\t", dtype=str)

    if df.shape[1] != 40:
        raise InvalidInputTsv(f"Invalid input metadata string: {data[0]}")

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
        "start_fuzzy": str,
        "end_fuzzy": str,
    }
    cols_to_front = ["contig_id", "ID", "type", "start", "end", "conf", "score"]
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
    # Rename columns to be consistent with database tables
    attributes_df = attributes_df.rename(columns={"Name": "rrna_name"})

    dtype_map = {
        "score": float,
        "start": int,
        "end": int,
        "start_fuzzy": str,
        "end_fuzzy": str,
    }
    cols_to_first = [
        "contig_id",
        "type",
        "rrna_name",
        "start",
        "end",
        "strand",
        "source",
        "score",
        "start_fuzzy",
        "end_fuzzy",
    ]
    gff_df = process_gff_info(attributes_df, cols_to_first, dtype_map)
    return gff_df


def annotation_pipeline(df, tmpdir, kofamscan_profile, kofamscan_kolist):
    for _, row in df.iterrows():
        # Prepare data for 'identifier' table and 'md_info' table
        local_path = row["local_path"].strip()
        filename = row["assembly_accession"].strip()

        source = row["source"].strip()
        external_accession = row["assembly_accession"].strip()

        # Run genome annotation pipeline
        with copy_and_unzip(local_path, tmpdir) as input_fasta:
            tmpdir_curr = input_fasta.parent  # e.g. tmpdir/hA10

            prodigal_output_gff, prodigal_output_faa, prodigal_commands = run_prodigal(
                input_fasta, tmpdir_curr
            )

            res, _ = run_command_and_check_outputs(
                ["exec_annotation", "-v"], ValueError
            )
            kofamscan_version = res.stdout.decode().strip()

            kofamscan_output_tsv, kofamscan_commands = run_kofamscan(
                prodigal_output_faa,
                kofamscan_profile,
                kofamscan_kolist,
                tmpdir_curr,
                tmpdir_curr,
            )

            barrnap_output_gff, barrnap_commands = run_barrnap(input_fasta, tmpdir_curr)

            cds_df = extract_prodigal_results(prodigal_output_gff)
            kofamscan_df = extract_kofamscan_results(kofamscan_output_tsv)
            rrna_df = extract_barrnap_results(barrnap_output_gff)

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

            prodigal_version = cds_df["source"].tolist()[0]
            sql_extract_prodigal_run_info = """
                SELECT run_id FROM run_info
                WHERE software = 'prodigal' and version = ? and commands = ?"""
            TRN.add(
                sql_extract_prodigal_run_info, [prodigal_version, prodigal_commands]
            )
            prodigal_run_id = TRN.execute_fetchflatten()

            if not prodigal_run_id:
                sql_run_info_prodigal = """
                    INSERT INTO run_info (software, version, commands)
                    VALUES (?, ?, ?) RETURNING run_id"""
                args_run_info_prodigal = [
                    "prodigal",
                    prodigal_version,
                    prodigal_commands,
                ]
                TRN.add(sql_run_info_prodigal, args_run_info_prodigal)
                prodigal_run_id = TRN.execute_fetchflatten()

            cds_df = cds_df.drop("note", axis=1)
            cds_df.insert(0, "entity_id", entity_id[0])
            cds_df["run_id"] = prodigal_run_id[0]
            args_cds_info = cds_df.values.tolist()
            sql_cds_info = """
                INSERT INTO cds_info (
                    entity_id,
                    contig_id,
                    gene_id,
                    gene_type,
                    start,
                    end,
                    conf,
                    score,
                    source,
                    strand,
                    phase,
                    partial,
                    start_type,
                    rbs_motif,
                    rbs_spacer,
                    gc_cont,
                    cscore,
                    sscore,
                    rscore,
                    uscore,
                    tscore,
                    start_fuzzy,
                    end_fuzzy,
                    run_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            TRN.add(sql_cds_info, args_cds_info, many=True)

            sql_extract_kofamscan_run_info = """
                SELECT run_id FROM run_info
                WHERE software = 'kofamscan' and version = ? and commands = ?"""
            TRN.add(
                sql_extract_kofamscan_run_info, [kofamscan_version, kofamscan_commands]
            )
            kofamscan_run_id = TRN.execute_fetchflatten()

            if not kofamscan_run_id:
                sql_run_info_kofamscan = """
                    INSERT INTO run_info (software, version, commands)
                    VALUES (?, ?, ?) RETURNING run_id"""
                args_run_info_kofamscan = [
                    "kofamscan",
                    kofamscan_version,
                    kofamscan_commands,
                ]
                TRN.add(sql_run_info_kofamscan, args_run_info_kofamscan)
                kofamscan_run_id = TRN.execute_fetchflatten()

            kofamscan_df.insert(0, "entity_id", entity_id[0])
            kofamscan_df["run_id"] = kofamscan_run_id[0]
            args_ko_info = kofamscan_df.values.tolist()
            sql_ko_info = """
                INSERT INTO ko_info (
                    entity_id,
                    gene_name,
                    ko,
                    threshold,
                    score,
                    e_value,
                    ko_definition,
                    run_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?);"""
            TRN.add(sql_ko_info, args_ko_info, many=True)

            barrnap_version = rrna_df["source"].tolist()[0]
            sql_extract_barrnap_run_info = """
                SELECT run_id FROM run_info
                WHERE software = 'barrnap' and version = ? and commands = ?"""
            TRN.add(sql_extract_barrnap_run_info, [barrnap_version, barrnap_commands])
            barrnap_run_id = TRN.execute_fetchflatten()

            if not barrnap_run_id:
                sql_run_info_barrnap = """
                    INSERT INTO run_info (software, version, commands)
                    VALUES (?, ?, ?) RETURNING run_id"""
                args_run_info_barrnap = [
                    "barrnap",
                    barrnap_version,
                    barrnap_commands,
                ]
                TRN.add(sql_run_info_barrnap, args_run_info_barrnap)
                barrnap_run_id = TRN.execute_fetchflatten()

            rrna_df.insert(0, "entity_id", entity_id[0])
            rrna_df["run_info"] = barrnap_run_id[0]
            args_rrna_info = rrna_df.values.tolist()
            print("***** ready to insert with arguments")
            print(args_rrna_info)
            sql_rrna_info = """
                INSERT INTO rrna_info (
                    entity_id,
                    contig_id,
                    gene_type,
                    rrna_name,
                    start,
                    end,
                    strand,
                    source,
                    score,
                    start_fuzzy,
                    end_fuzzy,
                    product,
                    note,
                    run_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            TRN.add(sql_rrna_info, args_rrna_info, many=True)

            TRN.execute()

    return


def main():
    initialize_db()

    # md_tsv = "./redgenes/tests/data/md_gordon_2.tsv"
    md_str = sys.argv[1]
    md_df = extract_md_info(md_str)

    # start_time = time.time()

    ko_profile = "/projects/greengenes2/20231117_annotations_prelim/kofam_scan/profiles/test_subset.hal"
    ko_kolist = (
        "/projects/greengenes2/20231117_annotations_prelim/kofam_scan/ko_list_subset"
    )
    annotation_pipeline(md_df, "/panfs/y1weng/test_out2", ko_profile, ko_kolist)

    # end_time = time.time()
    # execution_time = end_time - start_time

    # with open("home/y1weng/execution_time.txt", "w") as file:
    #   file.write(f"Script execution time: {execution_time:.2f} seconds")


if __name__ == "__main__":
    main()
