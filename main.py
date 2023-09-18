# main.py

# make sure you can take fasta.gz as input as well 

import argparse
from db_creation import database_operations
from db_insertion import submit_bakta, process_bakta_output

def main(filename):
    # Step 1: Create or initialize the databases
    database_operations.create_databases()

    # Step 2: Insert into main database 

    # Step 3: Load fasta metadata and insert 

    # Step 4.1: Run quast 

    # Step 4.2 and insert into db with run information
    database_operations.insert_software(software_info)
    database_operations.insert_run(run_info)

    # Step 5.1: Submit a Bakta job for the provided FASTA file
    # Returns filename, slurm job id
    job_info = submit_jobs.submit_bakta_job(filename)
    # Returns end run time 
    updated_job_info = submit_jobs.monitor_job_status(job_info)

    # Step 5.2: Add bakta output to the bakta database 
    bakta_output_file = f"{bakta_output_dir}/{updated_job_info['sample']}.gff"
    process_bakta_output.process_bakta_output(bakta_output_file)
    process_bakta_output.delete_bakta_output_files(bakta_output_file)

    # TODO: need to parse software run information
    database_operations.insert_software(software)
    database_operations.insert_run(updated_job_info)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Workflow")
    parser.add_argument("fasta_file", help="Path to the FASTA file")    
    
    args = parser.parse_args()
    main(args.filename)
