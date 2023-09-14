import subprocess

def submit_bakta_jobs(sample_list):
    job_info = []

    for sample in sample_list:
        # Replace with your Bakta submission command
        cmd = ["sbatch", "bakta.sh", sample]
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        if process.returncode == 0:
            slurm_job_id = stdout.decode().strip()
            job_info.append({"sample": sample, "slurm_job_id": slurm_job_id})

    return job_info

def monitor_job_status(job_info):
    for job in job_info:
        # Replace with code to monitor Slurm job status using slurm commands (e.g., sacct)
        # Check if the job has finished and update job_info with the status
        job["status"] = "completed"  # Replace with actual status

    return job_info
