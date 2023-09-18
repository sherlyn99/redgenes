import subprocess

def submit_bakta_jobs(filename):
    job_info = []
    
    # Replace with your Bakta submission command
    cmd = ["sbatch", "bash/bakta.sh", filename]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    if process.returncode == 0:
        slurm_job_id = stdout.decode().strip()
        job_info.append({"filename": filename, "slurm_job_id": slurm_job_id})

    return job_info

def monitor_job_status(job_info):
    job_id = job_info["job_id"]

    while True:
        # Replace with the appropriate slurm command to check job status (e.g., sacct)
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
