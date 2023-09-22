import subprocess
import os
import re
import time

def submit_bakta_job(entity_id, temp_dir, bakta_db):
    """
    Submit a Bakta job to a cluster.

    Args:
        entity_id (int): The entity ID.
        temp_dir (str): The path to the temporary directory.
        bakta_db (str): The path to the Bakta database.

    Returns:
        str: The Slurm job ID for the submitted Bakta job.
    """
    # Replace with your Bakta submission command
    cmd = ["sbatch", "bash_scripts/bakta.sh", str(entity_id), temp_dir, bakta_db]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        raise Exception(f"Bakta job submission failed with error code {process.returncode}: {stderr.decode()}")

    slurm_job_id = stdout.decode().strip()
    slurm_job_id = re.search(r'\b(\d+)\b', slurm_job_id).group(1)
    return slurm_job_id

def monitor_job_status(job_id):
    """
    Monitor the status of a submitted job using Slurm.

    Args:
        job_id (str): A string containing the slurm_job_id.

    Returns:
        dict: A dictionary with job status and finish time if completed or failed.
    """
    while True:
        # Replace with the appropriate Slurm command to check job status (e.g., sacct)
        status_command = f"sacct -j {job_id} --format=State --noheader --parsable2"
        try:
            status_output = subprocess.check_output(status_command, shell=True, stderr=subprocess.STDOUT, text=True)
            job_status = status_output.strip().split('\n')[-1]  # Get the latest status
            print(job_status)
        except subprocess.CalledProcessError as e:
            # Handle the error gracefully
            print(f"Error checking job status: {e}")
            job_info = {
                "status": "Failed"
            }
            return job_info

        if job_status == "COMPLETED":
            # Job has finished, update the job_info with the finish time
            job_info = {
                "status": "Completed",
                "finish_time": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            return job_info
        elif job_status == "PENDING":
            # Job is still pending, wait and check again after a delay
            time.sleep(60)  # Adjust the polling interval as needed
        else:
            # Job is running or in another state, continue monitoring
            time.sleep(60)  # Adjust the polling interval as needed
