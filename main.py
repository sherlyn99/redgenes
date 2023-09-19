import argparse
from test import Test
from scripts.database_operations import *
from scripts.process_bakta_output import *


# make sure you can take fasta.gz as input as well 
# Should we initialize the main database file somewhere else? 

def cli():
    ap = argparse.ArgumentParser(description='Package version ', add_help=False)
    # Required
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
    # TODO: add external accession and external source 
    entity_id = database_operations.insert_identifier(master.db, master.fasta, master.path)

    # Step 3: Load fasta metadata and insert 

    # Step 4.1: Run quast 

    # Step 4.2 and insert into db with run information
    software_accession = database_operations.insert_software_info(master.db, software, version, arguments, description)
    run_accession = database_operations.insert_run_info(master.db, slurm_job_id, software_accession)


    # Step 5.1: Submit a Bakta job for the provided FASTA file
    # Temporarily change the file name to be the entity id and make sure that is the bakta output
    # Returns filename, slurm job id, and the path to the bakta output 

    #job_info, bakta_out = submit_jobs.submit_bakta_job(filename)

    # Returns end run time 
    #updated_job_info = submit_jobs.monitor_job_status(job_info)

    # Step 5.2: Add bakta output to the bakta database 
    parse_bakta(master.db, entity_id, run_accession)
    delete_bakta_output_files(entity_id)

    # TODO: need to parse software run information
    # TODO: there is a mix of lists and dictionaries here 
    # software_id = database_operations.insert_software(db, software)
    # job_info.append(software_id)
    # database_operations.insert_run(db, job_info)

if __name__ == '__main__':
    cli()