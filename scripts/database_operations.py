import sqlite3

# db_file: '/projects/greengenes2/gg2.db'

def create_databases(db_file):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # create table: identifier
    # active: 1 = active, 0 = inactive
    # create_at: currently displays UTC timezone, can use pytz to convert to San Diego time
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS identifier(
            entity_id integer primary key autoincrement,
            filename varchar not null,
            filepath varchar not null,
            external_accession varchar,
            external_source varchar,
            active int default 1,
            created_at timestamp default current_timestamp,
            unique(filename, filepath),
            unique(external_accession, external_source)
        )
    ''')

    # create table: run_info
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS run_info(
            run_accession integer primary key autoincrement,
            slurm_job_id varchar,
            software_accession integer,
            run_at timestamp default current_timestamp,
            foreign key (software_accession) references software_info (software_accession)
        )
    ''')

    # create table: software_info
    # constraint - unique(software_name, version, arguments)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS software_info (
            software_accession integer primary key autoincrement,
            software_name varchar,
            version varchar,
            arguments varchar,
            description varchar,
            created_at timestamp default current_timestamp,
            unique(software_name, version, arguments)
        )
    ''')
    
    # create table: bakta
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS bakta (
            bakta_accession integer primary key autoincrement,
            entity_id varchar,
            contig_id varchar,
            gene_id varchar,
            source varchar,
            type varchar,
            start integer,
            end integer,
            strand varchar,
            phase varchar,
            gene_name varchar,
            locus_tag varchar,
            product varchar,
            dbxref varchar,
            run_accession integer,
            created_at timestamp default current_timestamp,
            foreign key (run_accession) references run_info (run_accession),
            foreign key (entity_id) references identifier (entity_id)
        )
    ''')

    # create table: quast
    # may reduce the number of fields in the future
    # is it convenient to switch the order of columns in the parser?
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS quast (
            quast_accession integer primary key autoincrement,
            entity_id varchar,
            total_length integer,
            largest_contig integer,
            ncontigs integer,
            gc integer,
            n50 integer,
            n75 integer,
            aun integer,
            l50 integer,
            l75 integer,
            ncontigs_0bp integer,
            ncontigs_1000bp integer,
            ncontigs_5000bp integer,
            ncontigs_10000bp integer,
            ncontigs_25000bp integer,
            ncontigs_50000bp integer,
            total_length_0bp integer,
            total_length_1000bp integer,
            total_length_5000bp integer,
            total_length_10000bp integer,
            total_length_25000bp integer,
            total_length_50000bp integer,
            nN_per_100kbp integer,
            run_accession integer,
            created_at timestamp default current_timestamp,
            foreign key (run_accession) references run_info (run_accession),
            foreign key (entity_id) references identifier (entity_id)
        )
    ''')

    # create table: metadata
    # does this table need
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS metadata (
            md_id integer primary key autoincrement,
            entity_id integer,
            assembly_accession varchar,
            bioproject varchar,
            biosample varchar,
            wgs_master varchar,
            refseq_category varchar,
            taxid integer,
            species_taxid varchar,
            organism_name varchar,
            infraspecific_name varchar,
            isolate varchar,
            version_status varchar,
            assembly_level varchar,
            release_type varchar,
            genome_rep varchar,
            seq_rel_date varchar,
            asm_name varchar,
            submitter varchar,
            gbrs_paired_asm varchar,
            paired_asm_comp varchar,
            ftp_path varchar,
            excluded_from_refseq varchar,
            relation_to_type_material varchar,
            asm_not_live_date varchar,
            created_at timestamp default current_timestamp,
            foreign key (entity_id) references identifier (entity_id)
        )
    ''')
    conn.commit()
    conn.close()


# need to take into account that some fields are optional, therefore the length of identifier_info can vary
import sqlite3

def insert_identifier(db, filename, filepath, external_accession=None, external_source=None):
    """
    Insert a new record into the 'identifier' table.

    Args:
        db (str): Path to the SQLite database file.
        filename (str): Name of the file.
        filepath (str): Path to the file.
        external_accession (str, optional): External accession identifier. Default is None.
        external_source (str, optional): Source of the external accession. Default is None.

    Returns:
        int: The auto-generated 'entity_id' value for the inserted record.
    """
    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    try:
        # Insert into the table without specifying the auto-incremented 'entity_id'
        cursor.execute('''
            INSERT INTO identifier (filename, filepath, external_accession, external_source)
            VALUES (?, ?, ?, ?)
        ''', (filename, filepath, external_accession, external_source))

        # Get the auto-generated 'entity_id' value
        entity_id = cursor.lastrowid

        # Commit the transaction and close the connection
        conn.commit()
        conn.close()

        return entity_id

    except sqlite3.Error as e:
        print("SQLite error:", e)
        conn.rollback()
        conn.close()
        return None

def insert_run_info(db, slurm_job_id, software_accession):
    """
    Insert a new record into the 'run_info' table.

    Args:
        db (str): Path to the SQLite database file.
        slurm_job_id (str): Slurm job ID.
        software_accession (int): The corresponding software accession.

    Returns:
        int: The auto-generated 'run_accession' value for the inserted record.
    """
    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    try:
        # Insert into the table without specifying the auto-incremented 'run_accession'
        cursor.execute('''
            INSERT INTO run_info (slurm_job_id, software_accession) VALUES (?, ?)
        ''', (slurm_job_id, software_accession))

        # Get the auto-generated 'run_accession' value
        run_accession = cursor.lastrowid

        # Commit the transaction and close the connection
        conn.commit()
        conn.close()

        return run_accession

    except sqlite3.Error as e:
        print("SQLite error:", e)
        conn.rollback()
        conn.close()
        return None


def insert_software_info(db, software_name, version, arguments, description):
    """
    Insert a new record into the 'software_info' table.

    Args:
        db (str): Path to the SQLite database file.
        software_name (str): Name of the software.
        version (str): Version of the software.
        arguments (str): Arguments used for running the software.
        description (str): Description of the software.

    Returns:
        int: The auto-generated 'software_accession' value for the inserted record.
    """
    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    try:
        # Insert into the table without specifying the auto-incremented 'software_accession'
        cursor.execute('''
            INSERT INTO software_info (software_name, version, arguments, description)
            VALUES (?, ?, ?, ?)
        ''', (software_name, version, arguments, description))

        # Get the auto-generated 'software_accession' value
        software_accession = cursor.lastrowid

        # Commit the transaction and close the connection
        conn.commit()
        conn.close()

        return software_accession

    except sqlite3.Error as e:
        print("SQLite error:", e)
        conn.rollback()
        conn.close()
        return None

def insert_bakta(db, bakta_info):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO bakta (
            entity_id, contig_id, gene_id, source, type, start, end, strand, phase, gene_name, locus_tag, product, dbxref, run_accession)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''',bakta_info)
    conn.commit()
    conn.close()

def insert_quast(db, quast_info):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO quast (
            entity_id, total_length, largest_contig, ncontigs, gc, n50, n75, aun, l50, l75, ncontigs_0bp, ncontigs_1000bp, ncontigs_5000bp, ncontigs_10000bp, ncontigs_25000bp, ncontigs_50000bp, total_length_0bp, total_length_1000bp, total_length_5000bp, total_length_10000bp, total_length_25000bp, total_length_50000bp, nN_per_100kbp, run_accession
            )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', quast_info)
    conn.commit()
    conn.close()

def insert_md(db, md_info):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO metadata (
            entity_id, assembly_accession, bioproject, biosample, wgs_master, refseq_category, taxid, species_taxid, organism_name, infraspecific_name, isolate, version_status, assembly_level, release_type, genome_rep, seq_rel_date, asm_name, submitter, gbrs_paired_asm, paired_asm_comp, ftp_path, excluded_from_refseq, relation_to_type_material, asm_not_live_date
            )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', md_info)
    conn.commit()
    conn.close()
