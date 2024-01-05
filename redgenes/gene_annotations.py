import pandas as pd
import subprocess
from pathlib import Path
from redgenes.utils import (
    PatchDirectoryNotFound,
    InvalidPatchFile,
    PatchFileExecutionError,
    ProdigalError,
    KofamscanError,
    BarrnapError,
)
from redgenes.sql_connection import TRN


def get_patch_list(patch_dir):
    """Returns the list of patch files in order."""
    # Check if patch_dir exists and is a directory because
    # Path(file_path).glob('*.sql') does report error if file_path does not exist
    path_patch_dir = Path(patch_dir)

    if path_patch_dir.exists() and path_patch_dir.is_dir():
        patch_list = sorted(path_patch_dir.glob("*.sql"), key=lambda x: int(x.stem))
        return patch_list
    else:
        raise PatchDirectoryNotFound(f"Directory not found: {patch_dir}")


def get_sqls_from_patch(patch):
    """Get sql contents from one patch file"""
    try:
        with open(patch) as f:
            sqls = f.read()
        return sqls
    except Exception as e:
        raise InvalidPatchFile(f"Cannot open {patch}")


def execute_patch_file(patch: Path):
    """Execute one patch file."""
    try:
        with open(patch) as f:
            sql_script = f.read()
    except Exception as e:
        raise RuntimeError(f"Cannot open {patch}")

    with TRN:
        TRN.executescript(sql_script)


def initialize_db():
    """Update patch files in settings table and execute new patches."""
    # Get patch file lists
    patch_list = get_patch_list("./redgenes/support_files")
    patch_ids = [[int(patch.stem)] for patch in patch_list]  # for TRN.add(many=True)
    patch_dict = {int(patch.stem): patch for patch in patch_list}
    patch_init = patch_list[0]

    # Creat the settings table by running the first patch
    execute_patch_file(patch_init)

    with TRN:
        # Update the settings table for new patches
        sql = "insert or ignore into settings (patch_id) values (?)"
        TRN.add(sql, patch_ids, many=True)

        # Set the execution status of the first patch to be 1 if applicable
        sql = "update settings set executed = 1, modified_at = current_timestamp where patch_id = ? and executed = 0"
        TRN.add(sql, [0])

        # Get the list of unexecuted sql scripts
        sql = "select patch_id from settings where executed = 0"
        TRN.add(sql)
        patch_ids_to_execute = TRN.execute_fetchflatten()

    # Execute unexecuted patches if applicable
    if patch_ids_to_execute:
        for patch_id in patch_ids_to_execute:
            patch = patch_dict[patch_id]
            try:
                execute_patch_file(patch)
            except Exception as e:
                raise PatchFileExecutionError(
                    f"There is an error in running patch file {patch_id}"
                )
            else:
                with TRN:
                    sql = "update settings set executed = 1, modified_at = current_timestamp where patch_id = ?"
                    TRN.add(sql, [patch_id])
                    TRN.execute()


def load_tsv(tsv_file):
    df = pd.read_csv(tsv_file, sep="\t")
    return df


def run_unzip_fna(filepath):
    commands = [
        "gzip",
        "-d",
        f"{filepath}",
    ]
    try:
        res = subprocess.run(commands)
        assert res.returncode == 0
        return
    except Exception as e:
        raise (f"There is an error: {e}")


def run_zip_fna(filepath):
    commands = [
        "gzip",
        "-c",
        f"{filepath}",
    ]
    try:
        res = subprocess.run(commands)
        assert res.returncode == 0
        return
    except Exception as e:
        raise (f"There is an error: {e}")


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
