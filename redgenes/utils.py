import subprocess


def run_command(commands, errortext):
    """Basic function to run bash commands using subprocess."""
    try:
        res = subprocess.run(commands)
        assert res.returncode == 0
        return
    except Exception as e:
        raise (f"There is an {errortext}: {e}")


def run_unzip_fna(filepath):
    commands = [
        "gzip",
        "-d",
        f"{filepath}",
    ]
    run_command(commands, "error")


def run_zip_fna(filepath):
    commands = [
        "gzip",
        "-c",
        f"{filepath}",
    ]
    run_command(commands, "error")
