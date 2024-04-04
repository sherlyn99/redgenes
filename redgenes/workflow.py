import time
import click
import atexit
import logging
import tempfile
from pathlib import Path
from redgenes.sql_initialize_db import initialize_db
from redgenes.utils import _unlink_directory, create_logfile
from redgenes.sql_connection import TRN
from redgenes.metadata import extract_md_info
from redgenes.quality_control import qc_bash_and_db_insertion


timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
my_logger = logging.getLogger("redgenes")


@click.group()
def redgenes():
    pass


@redgenes.command()
@click.option("--metadata", type=click.Path(exists=True), required=True)
@click.option("--working-dir", type=click.Path(exists=True), required=False)
@click.option("--threads", type=int, default=1, required=False)
def qc(metadata, working_dir, threads):
    logger = create_logfile(my_logger, f"./redgenes_qc_{timestamp}.log")

    if not working_dir:
        working_dir = tempfile.TemporaryDirectory()
        atexit.register(_unlink_directory(working_dir))

    initialize_db()

    md_df = extract_md_info(metadata)
    for _, row in md_df.iterrows():
        qc_bash_and_db_insertion(row, working_dir, threads, logger)


if __name__ == "__main__":
    redgenes()
