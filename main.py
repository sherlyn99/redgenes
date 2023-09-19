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
    apr.add_argument('-d', '--db', help='Filename of database',
                     required=True, type=str)

    master = Test(ap)

    # Step 1: Create or initialize the databases
    create_databases(master.db)

    # Step 2: Insert into main database 

    # Step 3: Load fasta metadata and insert 

    # Step 4.1: Run quast 

    # Step 4.2 and insert into db with run information
#    database_operations.insert_software(software_info)
#   database_operations.insert_run(run_info)

    # Step 5.1: Submit a Bakta job for the provided FASTA file
    # Returns filename, slurm job id, and the path to the bakta output 
    #job_info, bakta_out = submit_jobs.submit_bakta_job(filename)
    # Returns end run time 
    #updated_job_info = submit_jobs.monitor_job_status(job_info)

    bakta_out = "/Users/reneeoles/Desktop/bakta_plan/SRR20635833"

    # Step 5.2: Add bakta output to the bakta database 
    bakta_output_file = f"{bakta_out}.gff3"
    parse_bakta(master.db, bakta_output_file)
    delete_bakta_output_files(bakta_out)

    # TODO: need to parse software run information
    # TODO: there is a mix of lists and dictionaries here 
    # software_id = database_operations.insert_software(db, software)
    # job_info.append(software_id)
    # database_operations.insert_run(db, job_info)

if __name__ == '__main__':
    cli()