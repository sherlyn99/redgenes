import time
import click
import atexit
import logging
import tempfile
from pathlib import Path
from sql_initialize_db import initialize_db
from utils import _unlink_directory, create_logfile
from sql_connection import TRN
from metadata import extract_md_info
from quality_control import qc_bash_and_db_insertion
from bakta_annotations import annotation_pipeline


timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
my_logger = logging.getLogger("redgenes")

@click.group()
def redgenes():
    pass


@redgenes.command()
@click.option("--metadata", type=click.Path(exists=True), required=True)
@click.option("--working-dir", type=click.Path(exists=True), required=False)

# metadata should contain the columns - local_path, assembly_accession, bakta_path, checkm_path

def db_insertion(metadata, working_dir):
    logger = create_logfile(my_logger, f"./redgenes_insertion_{timestamp}.log")

    if not working_dir:
        working_dir = tempfile.TemporaryDirectory()
        atexit.register(_unlink_directory(working_dir))

    initialize_db()

    md_df = extract_md_info(metadata)
    for _, row in md_df.iterrows():
        qc_bash_and_db_insertion(row, working_dir, logger)
        annotation_pipeline(row, working_dir, logger)


if __name__ == "__main__":
    redgenes()
