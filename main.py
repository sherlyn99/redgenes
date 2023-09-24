import argparse
from test import Test
from scripts.database_operations import *
from scripts.process_bakta_output import *


# 1. make sure you can take fasta.gz as input as well 
# 2. Should we initialize the main database file somewhere else? Yes, eventually we 
# can initialize it on /projects/greengenes2/gg2.db 
# 3. In the db generation, the argument should be name of database, and we append
# ".db" automatically. Then get rid of the corresponding suffix check in 
# check_db() in test.py
# 4. note that if we run python main.py -f <fasta_file> -d db.db twice the first 
# db will be re-initialized, losing all contents. We should probably find a way
# to avoid this. 

def cli():
    ap = argparse.ArgumentParser(description='Package version ', add_help=True)
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