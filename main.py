import argparse
import tempfile
import shutil
import os
from test import Test
from scripts.database_operations import *
from scripts.process_bakta_output import *
from scripts.submit_bakta import submit_bakta_job, monitor_job_status

# Define default quast values
quast_software = "Default Software"
quast_version = "1.0"
quast_arguments = "--default-args"
quast_description = "Default Description"

# Define default bakta values
bakta_software = "Default Software"
bakta_version = "1.0"
bakta_arguments = "--default-args"
bakta_description = "Default Description"
bakta_db = "bakta.db"

def copy_fasta_to_temporary_folder(fasta, entity_id):
    try:
        # Create a temporary directory
        temp_dir = tempfile.mkdtemp()

        # Construct the destination path by joining the temporary directory with the entity name
        destination_path = os.path.join(temp_dir, entity_id)

        # Copy the fasta file to the temporary folder and rename it
        shutil.copy(fasta, destination_path)

        # Return the path to the copied and renamed file
        return temp_dir

    except Exception as e:
        # Handle any exceptions that may occur during the copy and rename process
        print(f"Error: {e}")
        return None

def run_quast(db, entity_id):
    software_accession = database_operations.insert_software_info(db, quast_software, quast_version, quast_arguments, quast_description)
    run_accession = database_operations.insert_run_info(db, entity_id, software_accession)

    # Step 4.1: Run quast
    # Implement quast execution here if needed

    return run_accession

def run_bakta(db, entity_id, temp_dir):
    slurm_job_id = submit_bakta_job(entity_id, temp_dir, bakta_db)
    monitor_job_status(slurm_job_id)

    software_accession = database_operations.insert_software_info(db, bakta_software, bakta_version, bakta_arguments, bakta_description)
    run_accession = database_operations.insert_run_info(db, entity_id, software_accession)

    return run_accession

def cli():
    ap = argparse.ArgumentParser(description='Package version ', add_help=False)
    apr = ap.add_argument_group('main arguments')
    apr.add_argument('-f', '--fasta', help='Filename of fasta file',
                     required=True, type=str)
    apr.add_argument('-p', '--path', help='File path of fasta file',
                     required=True, type=str)
    apr.add_argument('-d', '--db', help='Filename of database',
                     required=True, type=str)

    master = Test(ap)

    # Step 1: Create or initialize the databases
    create_databases(master.db)

    # Step 2: Insert into main database
    entity_id = database_operations.insert_identifier(master.db, master.fasta, master.path)

    # Step 3: Load fasta metadata and insert

    # Step 4: Run quast and insert into db with run information
    quast_run_accession = run_quast(master.db, entity_id)

    # Step 5: Submit a Bakta job for the provided FASTA file
    temp_dir = copy_fasta_to_temporary_folder(master.fasta, entity_id)
    bakta_run_accession = run_bakta(master.db, entity_id, temp_dir)

    # Step 6: Add bakta output to the bakta database
    parse_bakta(master.db, entity_id, temp_dir, bakta_run_accession)
    delete_bakta_output_files(entity_id, temp_dir)

if __name__ == '__main__':
    cli()
