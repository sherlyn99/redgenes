import pandas as pd
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
            name varchar,
            product varchar,
            RefSeq varchar,
            SO varchar,
            UniParc varchar,
            Uniref varchar,
            KEGG varchar,
            PFAM varchar,
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

def insert_identifier(db, filename, filepath, external_accession=None, external_source=None):
    """
    Insert a new record into the 'identifier' table or retrieve the existing entity_id.

    Args:
        db (str): Path to the SQLite database file.
        filename (str): Name of the file.
        filepath (str): Path to the file.
        external_accession (str, optional): External accession identifier. Default is None.
        external_source (str, optional): Source of the external accession. Default is None.

    Returns:
        int: The 'entity_id' value for the inserted or existing record.
    """
    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    try:
        # Check if a record with the same filename and filepath already exists
        cursor.execute('''
            SELECT entity_id FROM identifier
            WHERE filename = ? AND filepath = ?
        ''', (filename, filepath))

        existing_entity_id = cursor.fetchone()

        if existing_entity_id:
            # If a record already exists, return the existing entity_id
            conn.close()
            return existing_entity_id[0]
        else:
            # Insert a new record and return the auto-generated entity_id
            cursor.execute('''
                INSERT INTO identifier (filename, filepath, external_accession, external_source)
                VALUES (?, ?, ?, ?)
            ''', (filename, filepath, external_accession, external_source))

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
    Insert a new record into the 'software_info' table or retrieve the existing software_accession.

    Args:
        db (str): Path to the SQLite database file.
        software_name (str): Name of the software.
        version (str): Version of the software.
        arguments (str): Arguments used for running the software.
        description (str): Description of the software.

    Returns:
        int: The 'software_accession' value for the inserted or existing record.
    """
    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    try:
        # Check if a record with the same software_name, version, and arguments already exists
        cursor.execute('''
            SELECT software_accession FROM software_info
            WHERE software_name = ? AND version = ? AND arguments = ?
        ''', (software_name, version, arguments))

        existing_software_accession = cursor.fetchone()

        if existing_software_accession:
            # If a record already exists, return the existing software_accession
            conn.close()
            return existing_software_accession[0]
        else:
            # Insert a new record and return the auto-generated software_accession
            cursor.execute('''
                INSERT INTO software_info (software_name, version, arguments, description)
                VALUES (?, ?, ?, ?)
            ''', (software_name, version, arguments, description))

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


def insert_bakta(db, entity_id, contig_id, gene_id, source, type, start, end, strand, phase, name, product, refseq, so, uniparc, uniref, kegg, pfam, run_accession):
    """
    Insert data into the 'bakta' table.

    Args:
        cursor (sqlite3.Cursor): The SQLite cursor.
        entity_id (str): The identifier for the entity associated with the Bakta output.
        contig_id (str): Contig identifier.
        gene_id (str): Gene identifier.
        source (str): Source information.
        type (str): Feature type.
        start (int): Start position.
        end (int): End position.
        strand (str): Strand information.
        phase (str): Phase information.
        name (str): Gene description.
        product (str): Product information.
        dbxref (dict): Dictionary of database cross-reference information.
        run_accession (int): The accession identifier for the run.

    Returns:
        None
    """
    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    try:
        cursor.execute('''
            INSERT INTO bakta (entity_id, contig_id, gene_id, source, type, start, end, strand, phase, name, product, RefSeq, SO, UniParc, Uniref, KEGG, PFAM, run_accession)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (entity_id, contig_id, gene_id, source, type, start, end, strand, phase, name, product, refseq, so, uniparc, uniref, kegg, pfam, run_accession))
        conn.commit()
        conn.close()

        return 0

    except sqlite3.Error as e:
        print("SQLite error:", e)
        conn.rollback()
        conn.close()
        return None

def insert_md(db, entity_id, md):
    """
    Insert data into the 'metadata' table.

    Args:
        cursor (sqlite3.Cursor): The SQLite cursor.
        entity_id (str): The identifier for the entity associated with the metadata.

        assembly_accession (str): Contig identifier.
        bioproject (str): bioproject ID. 
        biosample (str): biosample ID.
        wgs_master (str): wgs project master accession.
        refseq_category (str): RefSeq category.
        taxid (int): taxonomy ID.
        species_taxid (str): taxonomy ID specific to the species.
        organism_name (str): organism name.
        infraspecific_name (str): subspecies or strain name.
        isolate (str): information on isolate.
        version_status (str): version of genomic data.
        assembly_level (str): level of completeness.
        release_type (str): type of data release or publication.
        genome_rep (str): repretation of the genome.
        seq_rel_date (str): release date.
        asm_name (str): asm name.
        submitter (str): submitter.
        gbrs_paired_asm (str): gbrs_paired_asm.
        paired_asm_comp (str): paired_asm_comp.
        ftp_path (str): filepath.
        excluded_from_refseq (str): whethere genome is exluded from refseq.
        relation_to_type_material (str): relation to type material.
        asm_not_live_date (str): date of asm not live.

    Returns:
        None
    """
    # check if md is empty
    try: 
        df = pd.read_excel(md)
    except pd.errors.EmptyDataError: 
        raise ValueError("Metadata is empty")

    # check if md has corrected headers
    expected_headers = ["assembly_accession", "bioproject", "biosample", "wgs_master", 
                        "refseq_category", "taxid", "species_taxid", "organism_name", "infraspecific_name",
                        "isolate", "version_status", "assembly_level", "release_type", "genome_rep", 
                        "seq_rel_date", "asm_name", "submitter", "gbrs_paired_asm", "paired_asm_comp", 
                        "ftp_path", "excluded_from_refseq", "relation_to_type_material", "asm_not_live_date"]
    actual_headers = df.columns.to_list()
    if len(except_headers) != len(actual_headers):
        raise ValueError(f"Metadata does not contain the correct number of columns")
    for i in rannge(len(actual_headers)):
        if actual_headers[i] != expected_headers[i]:
            raise ValueError(f"Metadata does not contain expected headers. Check {actual_headers[i]}")

    # insert info into table
    # metadata is not for individual fasta files?
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


