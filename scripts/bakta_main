# main.py

import argparse
from bakta_package import database_operations, submit_jobs, process_bakta_output

def main(fasta_file, bakta_output_dir, software_info, run_info):
    # Step 1: Create or initialize the databases
    database_operations.create_databases()

    # Step 2: Submit a Bakta job for the provided FASTA file
    job_info = submit_jobs.submit_bakta_job(fasta_file)
    updated_job_info = submit_jobs.monitor_job_status(job_info)

    # Step 3: Process Bakta output and update the Bakta output database
    for job in updated_job_info:
        bakta_output_file = f"{bakta_output_dir}/{job['sample']}.gff"
        process_bakta_output.process_bakta_output(bakta_output_file)
        process_bakta_output.delete_bakta_output_files(bakta_output_file)

    # Step 4: Insert software and run information into the databases
    database_operations.insert_software(software_info)
    database_operations.insert_run(run_info)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bakta Workflow")
    parser.add_argument("fasta_file", help="Path to the FASTA file")
    parser.add_argument("bakta_output_dir", help="Path to the Bakta output directory")
    parser.add_argument("software_name", help="Name of the software")
    parser.add_argument("software_version", help="Version of the software")
    parser.add_argument("software_arguments", help="Arguments used with the software")
    parser.add_argument("software_description", help="Description of the software")
    parser.add_argument("run_accession", help="Run accession")
    parser.add_argument("slurm_job_id", help="SLURM job ID")
    parser.add_argument("run_timestamp", help="Run timestamp")
    
    args = parser.parse_args()

    software_info = (args.software_name, args.software_version, args.software_arguments, args.software_description)
    run_info = (args.run_accession, args.slurm_job_id, args.run_timestamp)

    main(args.fasta_file, args.bakta_output_dir, software_info, run_info)
