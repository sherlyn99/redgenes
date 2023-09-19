import sqlite3

def create_databases(db_file):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Create software.db
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS software (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            software VARCHAR,
            version VARCHAR,
            arguments VARCHAR,
            description VARCHAR
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS run (
            run_accession VARCHAR PRIMARY KEY,
            slurm_job_id VARCHAR,
            run_at DATETIME,
            software_id INTEGER,
            FOREIGN KEY (software_id) REFERENCES software (id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS bakta (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT,
            contig_id TEXT,
            gene_id TEXT,
            source TEXT,
            type TEXT,
            start INTEGER,
            end INTEGER,
            strand TEXT,
            phase TEXT,
            gene_name TEXT,
            locus_tag TEXT,
            product TEXT,
            dbxref TEXT
        )
    ''')
    conn.commit()
    conn.close()

def insert_bakta(db, bakta_info):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO bakta (filename, contig_id, gene_id, source, type, start, end, strand, phase, gene_name, locus_tag, product, dbxref)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', bakta_info)
    conn.commit()
    conn.close()

def insert_software(db, software_info):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO software (software, version, arguments, description)
        VALUES (?, ?, ?, ?)
    ''', software_info)
    conn.commit()
    conn.close()

def insert_run(db, run_info):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO run (filename, slurm_job_id, run_at, software_id)
        VALUES (?, ?, ?, ?)
    ''', run_info)
    conn.commit()
    conn.close()
