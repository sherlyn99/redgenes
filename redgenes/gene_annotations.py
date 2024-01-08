import pandas as pd
import subprocess
from pathlib import Path
from redgenes.sql_initialize_db import initialize_db
from redgenes.utils import run_zip_fna, run_unzip_fna
from redgenes.exceptions import (
    ProdigalError,
    KofamscanError,
    BarrnapError,
    InvalidInputTsv,
)
from redgenes.sql_connection import TRN


def load_tsv(tsv_file):
    """Load data from tsv and checks data integrity."""
    df = pd.read_csv(tsv_file, sep="\t", dtype=str)

    try:
        assert df.shape[1] == 40
    except:
        raise InvalidInputTsv(f"Invalid input TSV: {tsv_file}")
    return df


def run_prodigal(unzipped_input, outdir, filename):
    commands = [
        "prodigal",
        "-i",
        f"{unzipped_input}",
        "-o",
        f"{Path(outdir) / f'{filename}.gff'}",
        "-a",
        Path(outdir) / f"{filename}_proteins.faa",
        "-f",
        "gff",
    ]
    try:
        res = subprocess.run(commands, capture_output=True, check=True)
        assert res.returncode == 0
        return
    except Exception as e:
        raise ProdigalError(f"There is an error: {e}")


def run_kofamscan(
    profile_dir, kolist_dir, ko_number, outdir, filename, temp_dir, cpu=1
):
    commands = [
        "exec_annotation",
        "-f",
        "detail-tsv",
        "-p",
        Path(profile_dir) / f"test_{ko_number}.hal",
        "-k",
        Path(kolist_dir) / f"ko_list_{ko_number}",
        "-o",
        Path(outdir) / f"{filename}_kofamscan.tsv",
        Path(outdir) / f"{filename}_proteins.faa",
        "--no-report-unannotated",
        "--tmp-dir",
        Path(temp_dir) / f"tmp{ko_number}",
        "--cpu",
        f"{cpu}",
    ]
    try:
        res = subprocess.run(commands, capture_output=True, check=True)
        assert res.returncode == 0
        # sanity check for the results
    except Exception as e:
        raise KofamscanError(f"There is an error: {e}")


def run_barrnap(input_genome, outdir, filename, cpu=1):
    commands = f"barrnap --threads {cpu} {input_genome} > {Path(outdir) / f'{filename}_rrna.gff'}"
    try:
        res = subprocess.run(commands, shell=True, capture_output=True, check=True)
        assert res.returncode == 0
    except Exception as e:
        raise BarrnapError(f"There is an error: {e}")


def extract_gff_contents(gff_path):
    data = []
    with open(gff_path, "r") as file:
        for line in file:
            if line.startswith("#"):
                continue
            parts = line.strip().split("\t")
            if len(parts) != 9:
                raise ValueError("Unexpected format in GFF3 line")
            seqid, runinfo, type, start, end, score, strand, phase, attributes = parts
            contig_id, run_info, type, start, end, score, strand, phase, attributes = (
                seqid,
                runinfo,
                type,
                int(start),
                int(end),
                float(score),
                strand,
                int(phase),
                attributes,
            )
            data.append(
                [
                    contig_id,
                    type,
                    start,
                    end,
                    score,
                    strand,
                    phase,
                    attributes,
                    run_info,
                ]
            )
    return data


def annotation_pipeline(df):
    for index, row in df.iterrows():
        # Prepare data for 'identifier' table and 'md_info' table
        _filename_full = row["ftp_path"].split("/")[-1]
        _local_path = row["local_path"]
        _source_detailed = row["source"]

        if _source_detailed in ["Gordon"]:
            _source = "non-ncbi"
        else:
            _source = "ncbi"
        _external_accession = row["assembly_accession"]

        # Prepare data for running the genome annotation pipeline
        ## prodigal
        outdir = "./test_out"
        Path(outdir).mkdir(parents=True, exist_ok=True)
        filename_stem = _filename_full.split(".")[0]  # no suffix
        unzipped_input = _local_path.replace(".gz", "")

        ## kofamscan
        profile_dir = (
            "/projects/greengenes2/20231117_annotations_prelim/kofam_scan/profiles/"
        )
        ko_number = "subset"
        kolist_dir = "/projects/greengenes2/20231117_annotations_prelim/kofam_scan/"
        kofamscan_temp_dir = "/panfs/y1weng/"

        # Running genome annotation pipeline
        ## Step 1: Unzip fna.gz file
        run_unzip_fna(_local_path)

        ## Step 2: Run prodigal to get {filename}_cds.gff
        run_prodigal(unzipped_input, outdir, filename_stem)

        ## Step 3: Run kofamscan to get {filename}_kofamscan.tsv
        run_kofamscan(
            profile_dir,
            kolist_dir,
            ko_number,
            outdir,
            filename_stem,
            kofamscan_temp_dir,
        )

        ## Step 4: Run barrnap to get {filename}_rrna.gff
        run_barrnap(unzipped_input, outdir, filename_stem)

        ## Step 5: Zip fna file
        run_zip_fna(unzipped_input)

        ## Step 6: Insert results into tables
        ## prodigal
        cds_results = extract_gff_contents(f"{Path(outdir) / f'{filename_stem}.gff'}")

        ## identifier and md_info
        sql_identifier = "insert into identifier (filename_full, filepath) values (?, ?) returning genome_id"
        args_identifer = [_filename_full, _local_path]
        sql_md_info = "insert into md_info (genome_id, source, source_detailed, external_accession) values (?, ?, ?, ?)"
        args_md_info = [_source, _source_detailed, _external_accession]

        with TRN:
            TRN.add(sql_identifier, args_identifer)
            genome_id = TRN.execute_fetchflatten()
            args_md_info = genome_id + args_md_info
            TRN.add(sql_md_info, args_md_info)
            TRN.execute()

        ## Step 7: Remove generated files

    return


def main():
    # 1. Initialize db and run new patch files
    initialize_db()

    # 2. get genomes and their genome paths
    ## fetch from tables
    ## fetch from local
    tsv_file = "./redgenes/tests/data/md_gordon_2.tsv"
    df = load_tsv(tsv_file)

    # 3. run genome annotation pipelines
    annotation_pipeline(df)

    # 4. save results


if __name__ == "__main__":
    main()
