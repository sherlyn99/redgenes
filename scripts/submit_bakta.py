import subprocess
import os
import time  # Import the time module for time-related operations

def submit_bakta_jobs(filename, bakta_db_dir, temp_dir):
    """
    Submit a Bakta job to a cluster.

    Args:
        filename (str): The input file for Bakta.
        bakta_db_dir (str): Directory where Bakta's database is located.
        temp_dir (str): Temporary directory for job output.

    Returns:
        tuple: A tuple containing job information (filename and slurm_job_id) and the path to Bakta output (bakta_out).
    """
    # Replace with your Bakta submission command
    cmd = ["sbatch", "bash/bakta.sh", filename, bakta_db_dir, temp_dir]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    # Where should we store the Bakta output?
    bakta_out = os.path.join(temp_dir, filename)

    if process.returncode == 0:
        slurm_job_id = stdout.decode().strip()
        job_info = (filename, slurm_job_id)

    return job_info, bakta_out

def monitor_job_status(job_info):
    """
    Monitor the status of a submitted job using Slurm.

    Args:
        job_info (tuple): A tuple containing job information (filename and slurm_job_id).

    Returns:
        dict: A dictionary with job status and finish time if completed or failed.
    """
    filename, job_id = job_info

    while True:
        # Replace with the appropriate Slurm command to check job status (e.g., sacct)
        status_command = f"sacct -j {job_id} --format=State --noheader --parsable2"
        try:
            status_output = subprocess.check_output(status_command, shell=True, stderr=subprocess.STDOUT, text=True)
            job_status = status_output.strip().split('\n')[-1]  # Get the latest status
        except subprocess.CalledProcessError as e:
            job_info["status"] = "Failed"
            return job_info

        if job_status == "COMPLETED":
            # Job has finished, update the job_info with the finish time
            job_info["status"] = "Completed"
            job_info["finish_time"] = time.strftime("%Y-%m-%d %H:%M:%S")
            return job_info
        elif job_status == "FAILED":
            job_info["status"] = "Failed"
            return job_info
        else:
            # Job is still running, check again after a delay
            time.sleep(60)  # Adjust the polling interval as needed
