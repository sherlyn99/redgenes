import argparse
import tempfile
import shutil
import os
from test import Test
from scripts.database_operations import *
from scripts.process_bakta_output import *
from scripts.submit_bakta import submit_bakta_job, monitor_job_status
from print.print import print_identifier, print_run_info, print_software_info, print_bakta

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
temp_path = "/panfs/roles/temp"
bakta_db = "/panfs/roles/Panpiper/panpiper/databases/bakta/db"

def copy_fasta_to_temporary_folder(fasta, fasta_path, temp_path, entity_id):
    try:
        # Construct the destination path by joining the temporary directory with the entity name
        destination_path = os.path.join(temp_path, entity_id)
        fasta_path = os.path.join(fasta_path, fasta)

        # Copy the fasta file to the temporary folder and rename it
        shutil.copy(fasta_path, destination_path)

        # Return the path to the copied and renamed file
        return temp_path

    except Exception as e:
        # Raise an exception and stop the code execution
        raise Exception(f"Error: {e}")
        return None

def run_bakta(db, entity_id, temp_dir):
    slurm_job_id = submit_bakta_job(entity_id, temp_dir, bakta_db)
    job_info = monitor_job_status(slurm_job_id)

    if job_info["status"] == "Failed":
        raise Exception("Bakta job failed. Check the Slurm job logs for details.")

    software_accession = insert_software_info(db, bakta_software, bakta_version, bakta_arguments, bakta_description)
    run_accession = insert_run_info(db, slurm_job_id, software_accession)

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

    # Initialize the databases
    create_databases(master.db)
    # Retrieve entity id
    entity_id = insert_identifier(master.db, master.fasta, master.path)

    temp_dir = copy_fasta_to_temporary_folder(master.fasta, master.path, temp_path, str(entity_id))
    bakta_run_accession = run_bakta(master.db, str(entity_id), temp_dir)
    
    # Step 6: Add bakta output to the bakta database
    parse_bakta(master.db, str(entity_id), temp_dir, bakta_run_accession)
    delete_bakta_output_files(str(entity_id), temp_dir)

if __name__ == '__main__':
    cli()
