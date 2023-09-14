import sqlite3
import csv
from datetime import datetime

def create_and_populate_databases(input_file, run_db, software_db):
    # Create and connect to the 'run' database
    run_db = sqlite3.connect(run_db)
    run_cursor = run_db.cursor()

    # Create the 'run' table
    run_cursor.execute('''
        CREATE TABLE IF NOT EXISTS run (
            run_accession varchar PRIMARY KEY,
            slurm_job_id varchar,
            run_at datetime,
            software varchar,
        )
    ''')

    # Create and connect to the 'software' database
    software_db = sqlite3.connect(software_db)
    software_cursor = software_db.cursor()

    # Create the 'software' table
    software_cursor.execute('''
        CREATE TABLE IF NOT EXISTS software (
            id varchar PRIMARY KEY,
            software varchar,
            version varchar,
            arguments varchar,
            description varchar
        )
    ''')

    # Read and insert data from the input file
    with open(input_file, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Insert data into the 'run' table
            run_cursor.execute('''
                INSERT INTO run (run_accession, slurm_job_id, run_at)
                VALUES (?, ?, ?)
            ''', (row['run_accession'], row['slurm_job_id'], datetime.strptime(row['run_at'], '%Y-%m-%d %H:%M:%S')))

            # Insert data into the 'software' table
            software_cursor.execute('''
                INSERT INTO software (id, software, version, arguments, description)
                VALUES (?, ?, ?, ?, ?)
            ''', (row['id'], row['software'], row['version'], row['arguments'], row['description']))

    # Commit changes and close database connections
    run_db.commit()
    run_db.close()
    software_db.commit()
    software_db.close()

# Usage example
input_file = "input_data.csv"  # Replace with your input file
create_and_populate_databases(input_file, "run.db", "software.db")
