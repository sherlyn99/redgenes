# database_operations.py
# Implement functions for database operations (create, insert, check)

# submit_jobs.py
# Submit Bakta jobs and collect job information

# process_bakta_output.py
# Process Bakta output, insert data into the Bakta output database

# main.py
from database_operations import create_databases, insert_software, insert_run
from submit_jobs import submit_bakta_jobs, monitor_job_status
from process_bakta_output import process_bakta_output

# Step 1: Create databases and tables
create_databases()

# Step 2: Submit Bakta jobs and collect job information
job_info = submit_bakta_jobs()

# Step 3: Monitor job status and wait for jobs to finish
monitor_job_status(job_info)

# Step 4: Process Bakta output and update the database
for job in job_info:
    bakta_output = retrieve_bakta_output(job)
    process_bakta_output(bakta_output)

    # Optionally, delete Bakta output files
    delete_bakta_output_files(bakta_output)

# End of the workflow
