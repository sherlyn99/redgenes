import sqlite3

def create_databases():
    # Create software.db
    software_conn = sqlite3.connect("software.db")
    software_cursor = software_conn.cursor()
    software_cursor.execute('''
        CREATE TABLE IF NOT EXISTS software (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            software VARCHAR,
            version VARCHAR,
            arguments VARCHAR,
            description VARCHAR
        )
    ''')
    software_conn.commit()
    software_conn.close()

    # Create run.db
    run_conn = sqlite3.connect("run.db")
    run_cursor = run_conn.cursor()
    run_cursor.execute('''
        CREATE TABLE IF NOT EXISTS run (
            run_accession VARCHAR PRIMARY KEY,
            slurm_job_id VARCHAR,
            run_at DATETIME,
            software_id INTEGER,
            FOREIGN KEY (software_id) REFERENCES software (id)
        )
    ''')
    run_conn.commit()
    run_conn.close()

def insert_software(software_info):
    conn = sqlite3.connect("software.db")
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO software (software, version, arguments, description)
        VALUES (?, ?, ?, ?)
    ''', software_info)
    conn.commit()
    conn.close()

def insert_run(run_info):
    conn = sqlite3.connect("run.db")
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO run (run_accession, slurm_job_id, run_at, software_id)
        VALUES (?, ?, ?, ?)
    ''', run_info)
    conn.commit()
    conn.close()
