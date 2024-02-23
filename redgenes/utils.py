import gzip
import shutil
import subprocess
import pandas as pd
from pathlib import Path
from skbio.io import read
from contextlib import contextmanager


################################
# Constant values
################################
MD_HEADER = [
    "assembly_accession",
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
    "asm_submitter",
    "gbrs_paired_asm",
    "paired_asm_comp",
    "ftp_path",
    "excluded_from_refseq",
    "relation_to_type_material",
    "asm_not_live_date",
    "assembly_type",
    "group",
    "genome_size",
    "genome_size_ungapped",
    "gc_percent",
    "replicon_count",
    "scaffold_count",
    "contig_count",
    "annotation_provider",
    "annotation_name",
    "annotation_date",
    "total_gene_count",
    "protein_coding_gene_count",
    "non_coding_gene_count",
    "pubmed_id",
    "source",
    "local_path",
]


################################
# Extract data from GFF3 files
################################
def read_gff_file(gff_path: str):
    """Read GFF3 file into a generator."""
    gen = read(gff_path, format="gff3")
    return gen


def extract_gff_info(gen):
    """Extract GFF3 information into a pandas DataFrame."""
    attributes_list = []
    for contig in gen:
        contig_id = contig[0]
        for record in contig[1]._intervals:
            attributes = record.metadata
            attributes["contig_id"] = contig_id  # Assumes contig_id always not null

            if "note" not in attributes:
                attributes["note"] = None

            try:
                # Sanity check: start, end values should be convertable to integers
                attributes["start"], attributes["end"] = map(int, record.bounds[0])
            except Exception as e:
                raise ValueError(f"Invalid start and end values from GFF3 file.")

            attributes["start_fuzzy"], attributes["end_fuzzy"] = record.fuzzy[
                0
            ]  # Skbio says this should be [False, False] by default, not sure why sometimes these values are True

            attributes_list.append(attributes)

    attributes_df = pd.DataFrame(attributes_list, dtype=str)  # May generate NaN values
    return attributes_df


def process_gff_info(attributes_df, cols_to_front, dtype_map):
    """Format and process the DataFrame containing GFF3 information."""
    for col in cols_to_front:
        if col not in attributes_df:
            raise ValueError(
                f"Invalid cols_to_front: {col} does not exist in the dataframe."
            )

    # Adjust column order
    new_order = cols_to_front + [
        col for col in attributes_df.columns if col not in cols_to_front
    ]
    attributes_df = attributes_df[new_order]

    # Adjust data type
    try:
        gff_df = attributes_df.astype(dtype_map)
    except Exception as e:
        raise ValueError(f"Error: {e}; This could be due to an invalid dtype map.")

    return gff_df


################################
# Run bash commands
################################
def run_command_and_check_outputs(commands, error, files=None, shell_bool=False):
    res = None
    try:
        res = subprocess.run(
            commands, capture_output=True, check=True, shell=shell_bool
        )
        assert res.returncode == 0
    except AssertionError as e:
        error_message = (
            f"Commands did not finsih with exit code 0: \nThe commands were {commands}."
        )
        if res:
            error_message += f"\n Additional error details: {res.stderr}"
        raise error(error_message)
    except Exception as e:
        error_message = f"There is an error {e}"
        if res:
            eror_message += f": \n{res.stderr}"
        raise error(error_message)

    # Check if outputs exist
    if files:
        for file in files:
            if not Path(file).exists:
                raise error(f"Output file {file} not generated.")

    return res, " ".join(list(map(str, commands)))


################################
# Zip and unzip fasta files
################################
@contextmanager
def copy_and_unzip(zip_path, tmp_dir):
    """Given a zipped fna file and a temp directory, creates a subdirectory and
    unzip the fna file in the subdirectory. Remove the subdirectory when done."""

    if "*" in zip_path:
        source_paths = list(Path(zip_path).parent.glob("*.fna.gz"))
        if not source_paths:
            raise FileNotFoundError(f"No matching files found for pattern: {zip_path}")
        else:
            source_path = source_paths[0]

    else:
        source_path = Path(zip_path)
        if not source_path.exists():
            raise FileNotFoundError(
                f"Error in context manager: the zipfile {zip_path} does not exist."
            )

    source_filename = source_path.name  # genome1.fna.gz
    source_filename_unzipped = Path(source_filename).stem  # genome1.fna
    source_stem = Path(source_filename_unzipped).stem  # genome1

    target_path = Path(tmp_dir) / source_stem
    target_path.mkdir(parents=True)

    target_file = target_path / source_filename_unzipped

    open_func = gzip.open if str(source_path).endswith(".gz") else open

    try:
        # Unzip a fasta file into tmpdir/genomename
        with open_func(source_path, "rt") as compressed_file, open(
            target_file, "w"
        ) as decompressed_file:
            shutil.copyfileobj(compressed_file, decompressed_file)

        yield target_file

    finally:
        shutil.rmtree(target_path)


# if __name__ == "__main__":
#     import sys
#     import numpy as np

#     md_str = sys.argv[1]
#     data = md_str.split("\t")
#     data_with_nan = [np.nan if x == "" else x for x in data]
#     df = pd.DataFrame([data_with_nan], columns=MD_HEADER)

#     if df.shape[1] != 40:
#         raise ValueError(f"Invalid input metadata string: {data[0]}")

#     df["annotation_date"] = pd.to_datetime(df["annotation_date"], errors="coerce")
#     dtype_map = {
#         "taxid": "Int64",
#         "species_taxid": "Int64",
#         "gc_percent": float,
#         "replicon_count": "Int64",
#         "scaffold_count": "Int64",
#         "contig_count": "Int64",
#         "total_gene_count": "Int64",
#         "protein_coding_gene_count": "Int64",
#         "non_coding_gene_count": "Int64",
#     }
#     df = df.astype(dtype_map)

#     for _, row in df.iterrows():
#         # Prepare data for 'identifier' table and 'md_info' table
#         local_path = row["local_path"]
#         print(repr(local_path.strip()))
