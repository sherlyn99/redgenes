import subprocess
import pandas as pd
from pathlib import Path
from redgenes.sql_initialize_db import initialize_db
from redgenes.utils import (
    run_zip_fna,
    run_unzip_fna,
    read_gff_file,
    process_qualifers,
    extract_qualifier,
    generate_insert_stmt,
    copy_and_unzip,
)
from redgenes.exceptions import (
    ProdigalError,
    KofamscanError,
    BarrnapError,
    InvalidInputTsv,
)
from redgenes.sql_connection import TRN

# zip and unzip - make a copy into a genome folder under temp dir and remove when finished (use context managers) => DONE
# check for file existence in unzip_fna => DONE
# get rid of repetitive parts of the function => DONE
# switch GFF3 parser: add phase information in extract_prodigal_results
# save runinfo and runid
# write tests for gene_annotations
# test the workflow on NCBI genomes


def load_tsv(tsv_file):
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


def run_command_and_check_outputs(commands, error, files, shell_bool=False):
    try:
        res = subprocess.run(
            commands, capture_output=True, check=True, shell=shell_bool
        )
        assert res.returncode == 0
    except AssertionError as e:
        raise error(f"Commands did not finsih with exit code 0: {commands}")
    except Exception as e:
        raise error(f"There is an error: {e}")

    # Check if outputs exist
    if files:
        for file in files:
            if not Path(file).exists:
                raise error(f"Output file {file} not generated.")

    return


def run_prodigal(unzipped_input, outdir, filename):
    prodigal_output_gff = f"{Path(outdir) / f'{filename}_cds.gff'}"
    prodigal_output_faa = f"{Path(outdir) / f'{filename}_proteins.faa'}"

    commands = [
        "prodigal",
        "-i",
        f"{unzipped_input}",
        "-o",
        prodigal_output_gff,
        "-a",
        prodigal_output_faa,
        "-f",
        "gff",
    ]
    run_command_and_check_outputs(
        commands, ProdigalError, [prodigal_output_gff, prodigal_output_faa]
    )


def run_kofamscan(
    profile_dir, kolist_dir, ko_number, outdir, filename, temp_dir, cpu=1
):
    ko_output_tsv = Path(outdir) / f"{filename}_kofamscan.tsv"

    commands = [
        "exec_annotation",
        "-f",
        "detail-tsv",
        "-p",
        Path(profile_dir) / f"test_{ko_number}.hal",
        "-k",
        Path(kolist_dir) / f"ko_list_{ko_number}",
        "-o",
        ko_output_tsv,
        Path(outdir) / f"{filename}_proteins.faa",
        "--no-report-unannotated",
        "--tmp-dir",
        Path(temp_dir) / f"tmp{ko_number}",
        "--cpu",
        f"{cpu}",
    ]
    run_command_and_check_outputs(commands, KofamscanError, [ko_output_tsv])


def run_barrnap(input_genome, outdir, filename, shell_mode, cpu=1):
    barrnap_output_gff = f"{Path(outdir) / f'{filename}_rrna.gff'}"

    commands = f"barrnap --threads {cpu} {input_genome} > {Path(outdir) / f'{filename}_rrna.gff'}"
    run_command_and_check_outputs(
        commands, BarrnapError, [barrnap_output_gff], shell_bool=shell_mode
    )


def extract_prodigal_results(gff_path):
    results = []
    dtype_map = {
        "partial": str,
        "start_type": str,
        "stop_type": str,
        "rbs_motif": str,
        "rbs_spacer": str,
        "gc_cont": float,
        "conf": float,
        "score": float,
        "cscore": float,
        "sscore": float,
        "rscore": float,
        "uscore": float,
        "tscore": float,
        "mscore": float,
    }
    records = read_gff_file(gff_path)
    for rec in records:
        for feature in rec.features:
            tmp = [
                rec.id,
                feature.id,
                feature.type,
                int(feature.location.start),
                int(feature.location.end),
                int(feature.location.strand),
            ]
            tmp_qualifiers = process_qualifers(feature.qualifiers, dtype_map)
            tmp += tmp_qualifiers
            results.append(tmp)
    return results


def extract_kofamscan_results(tsv_file):
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

    try:
        assert df.shape[1] == 7
    except Exception as e:
        raise InvalidInputTsv(f"Invalid kofamscan tsv file: {tsv_file}")

    df = df.drop("tab", axis="columns")
    dtype_map = {"threshold": float, "score": float, "e_value": float}
    df = df.astype(dtype_map)

    return df.values.tolist()


def extract_barrnap_results(gff_path):
    results = []

    records = read_gff_file(gff_path)
    for rec in records:
        for feature in rec.features:
            if (
                rrna_type := extract_qualifier(feature.qualifiers, "Name", str)
            ) == "16S_rRNA":
                # only record 16s rRNA genes
                tmp = [
                    rec.id,
                    # feature.id,
                    rrna_type,
                    int(feature.location.start),
                    int(feature.location.end),
                    int(feature.location.strand),
                ]
                dtype_map = {"source": str, "score": float, "product": str, "note": str}
                tmp_quali = process_qualifers(feature.qualifiers, dtype_map)
                tmp += tmp_quali
                results.append(tmp)
    return results


def annotation_pipeline(df):
    for index, row in df.iterrows():
        # Prepare data for 'identifier' table and 'md_info' table
        _filename_full = row["ftp_path"].split("/")[-1]
        _local_path = row["local_path"]
        _source_detailed = row["source"]

        if _source_detailed in ["Gordon"]:  # to add more non-ncbi categories
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

        # Run genome annotation pipeline
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
        run_barrnap(unzipped_input, outdir, filename_stem, True)

        ## Step 5: Zip fna file
        run_zip_fna(unzipped_input)

        ## Step 6: Insert results into tables
        ## prodigal
        cds_results = extract_prodigal_results(
            f"{Path(outdir) / f'{filename_stem}_cds.gff'}"
        )

        ## kofamscan
        ko_results = extract_kofamscan_results(
            f"{Path(outdir) / f'{filename_stem}_kofamscan.tsv'}"
        )

        ## barrnap
        rrna_results = extract_barrnap_results(
            f"{Path(outdir) / f'{filename_stem}_rrna.gff'}"
        )

        ## identifier and md_info
        sql_identifier = "insert into identifier (filename_full, filepath) values (?, ?) returning entity_id"
        args_identifer = [_filename_full, _local_path]
        sql_md_info = "insert into md_info (entity_id, source, source_detailed, external_accession) values (?, ?, ?, ?)"
        args_md_info = [_source, _source_detailed, _external_accession]

        with TRN:
            TRN.add(sql_identifier, args_identifer)
            entity_id = TRN.execute_fetchflatten()
            args_md_info = entity_id + args_md_info
            TRN.add(sql_md_info, args_md_info)

            sql_cds_info = "insert into cds_info (entity_id, contig_id, gene_id, gene_type, start, end, strand, partial, start_type, stop_type, rbs_motif, rbs_spacer, gc_cont, conf, score, cscore, sscore, rscore, uscore, tscore, mscore) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            args_cds_info = [entity_id + res for res in cds_results]
            TRN.add(sql_cds_info, args_cds_info, many=True)

            colnames_ko_info = [
                "entity_id",
                "gene_name",
                "ko",
                "threshold",
                "score",
                "e_value",
                "ko_definition",
            ]
            sql_ko_info = generate_insert_stmt("ko_info", colnames_ko_info)
            args_ko_info = [entity_id + res for res in ko_results]
            TRN.add(sql_ko_info, args_ko_info, many=True)

            colnames_rrna_info = [
                "entity_id",
                "contig_id",
                "rrna_type",
                "start",
                "end",
                "strand",
                "source",
                "score",
                "product",
                "note",
            ]
            sql_rrna_info = generate_insert_stmt("rrna_info", colnames_rrna_info)
            args_rrna_info = [entity_id + res for res in rrna_results]
            TRN.add(sql_rrna_info, args_rrna_info, many=True)

            TRN.execute()

        ## Step 7: Remove generated files

    return


def main():
    # 1. Initialize db and run new patch files
    initialize_db()

    # 2. get genomes and their genome paths
    ## fetch from tsv tables
    tsv_file = "./redgenes/tests/data/md_gordon_2.tsv"
    df = load_tsv(tsv_file)

    # 3. run genome annotation pipelines
    annotation_pipeline(df)


if __name__ == "__main__":
    main()
