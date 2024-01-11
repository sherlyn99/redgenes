import gzip
import shutil
import subprocess
from BCBio import GFF
from pathlib import Path
from contextlib import contextmanager
from redgenes.exceptions import InvalidFna


@contextmanager
def copy_and_unzip(zip_path, tmp_dir):
    """Given a zipped fna file and a temp directory, creates a subdirectory and
    unzip the fna file in the subdirectory. Remove the subdirectory when done."""
    try:
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

        # Unzip a fasta file into tmpdir/genomename
        with gzip.open(source_path, "rt") as compressed_file, open(
            target_file, "w"
        ) as decompressed_file:
            shutil.copyfileobj(compressed_file, decompressed_file)

        yield target_file

    finally:
        shutil.rmtree(target_path)


################################
# Generate sql insert statements
################################
def generate_insert_stmt(table_name, column_names):
    stmt = f"insert into {table_name} ({', '.join(column_names)}) values ({', '.join(['?']*len(column_names))});"
    return stmt


################################
# Extract data from GFF3 files
################################
def read_gff_file(gff_file):
    """Parse a gff file and return all of its records"""
    if not Path(gff_file).exists():
        raise FileNotFoundError(f"Error: GFF file not found at {gff_file}")
    records = []
    with open(gff_file, "r") as in_handle:
        records = list(GFF.parse(in_handle))
    return records


def extract_qualifier(qualiers_dict, qualifier_key, qualifier_dtype):
    """Extract a qualifier field from SeqRecord.feature.qualifiers and returns its value in the specified dtype.

    If the field does not exist, returns 'NA' or 0 depending on the specified
    dtype; If the field has a list of values, returns the first item of the
    list in the specified dtype. See https://github.com/hyattpd/prodigal/wiki/understanding-the-prodigal-output for an explaination of qualifier key values
    """
    if isinstance(tmp := qualiers_dict.get(qualifier_key), list):
        # If qualifier is a list, take the first value
        qualifier_value = tmp[0]
    else:
        qualifier_value = qualiers_dict.get(qualifier_key, None)

    if qualifier_value:
        # Returns the qualifier value in the correct dtype
        if qualifier_value == "None":
            return None
        return qualifier_dtype(qualifier_value)
    else:
        return qualifier_value


def process_qualifers(qualifiers_dict, dtype_map):
    """Extract selected qualifier fields from SeqRecord.feature.qualifiers in specified dtypes."""
    processed_qualifers = []
    for field, dtype in dtype_map.items():
        value = extract_qualifier(qualifiers_dict, field, dtype)
        processed_qualifers.append(value)
    return processed_qualifers


################################
# Run bash commands
################################
def run_command(commands, errortype, errortext):
    """Run bash commands using subprocess."""
    try:
        res = subprocess.run(commands, capture_output=True, check=True)
        assert res.returncode == 0
    except Exception as e:
        raise errortype(f"There is an {errortext}: {e}.")


################################
# Zip and unzip fasta files
################################
def run_unzip_fna(filepath):
    """Checks if a zipped file exists and unzip it."""
    if Path(filepath).exists():
        commands = [
            "gzip",
            "-d",
            f"{filepath}",
        ]
        run_command(commands, InvalidFna, "error")
    elif (unzipped_path := Path(filepath.replace(".gz", ""))).exists():
        raise ValueError(f"Unzipped file already exists: {str(unzipped_path)}.")
    else:
        raise FileNotFoundError(f"File not found: {filepath}.")


def run_zip_fna(filepath):
    """Checks if an unzipped file exists and zip it."""
    if not Path(filepath).exists():
        raise FileNotFoundError(f"Unzipped file does not exist: {filepath}.")

    zipped_path = filepath + ".gz"
    if Path(zipped_path).exists():
        raise ValueError(f"Zipped file already exists: {zipped_path}.")

    commands = [
        "gzip",
        f"{filepath}",
    ]
    run_command(commands, InvalidFna, "error")
