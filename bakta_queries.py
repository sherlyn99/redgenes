import sqlite3
from Bio import SeqIO
import os
from skbio import read, write, DNA

def connect_to_database(db_connection):
    """
    Connect to the SQLite database and return the connection and cursor objects.
    """
    if isinstance(db_connection, str):
        conn = sqlite3.connect(db_connection)
    elif isinstance(db_connection, sqlite3.Connection):
        conn = db_connection
    else:
        raise TypeError("db_connection must be a file path (str) or a sqlite3.Connection object.")
    
    cursor = conn.cursor()
    return conn, cursor

def close_database_connection(conn):
    """
    Close the SQLite database connection.
    """
    conn.close()

def get_gene_info_by_name(cursor, gene_name, column):
    """
    Retrieve gene information from the database based on the gene name.
    """
    # Execute a query to fetch all entries from the specified column in the table
    cursor.execute(f"SELECT {column} FROM bakta")
    # Fetch the results
    entries = cursor.fetchall()
    # Print all entries from the specified column
    for entry in entries:
        print(entry[0])  # Assuming you want to print the first column of each row

    query = f"""
        SELECT b.contig_id, b.start, b.end, b.strand, b.entity_id, b.gene_id
        FROM bakta AS b
        WHERE {column} = ?
    """
    cursor.execute(query, (gene_name,))
    return cursor.fetchall()

# In this there could be an issue with contig_id from bakta annotation versus original fasta file
def extract_gene_sequence_from_fasta(fasta_file_path, contig_id, start, stop, strand):
    """
    Extract the gene sequence from a FASTA file based on coordinates and strand.
    """
    gene_sequence = None

    for record in read(fasta_file_path, format='fasta'):
        if record.metadata['id'] == contig_id:
            if strand == "1":
                gene_sequence = str(record[start:stop])
            elif strand == "-1":
                gene_sequence = str(DNA(record[start:stop]).reverse_complement())
            break
    if gene_sequence is not None:
        return gene_sequence
    else:
        raise KeyError(f"{contig_id} not found in {fasta_file_path}")

def write_gene_sequences_to_fasta(output_file_path, sequences):
    write(sequences, format='fasta', into=output_file_path)

def extract_gene_sequence_to_fasta(db_file, gene_name, column, fasta_path, output_file_path):
    """
    Extract gene sequences based on gene name and write them to an output FASTA file.
    """
    conn, cursor = connect_to_database(db_file)

    sequences = DNA()

    gene_info = get_gene_info_by_name(cursor, gene_name, column)
    print(gene_info)

    for result in gene_info:
        contig_id, start, stop, strand, entity_id, gene_id, = result
        fasta_file_path = fasta_path + entity_id + ".fna"
        gene_sequence = extract_gene_sequence_from_fasta(fasta_file_path, contig_id, start, stop, strand)
        sequences.append(DNA(gene_sequence, metadata={'id': f'{gene_id}'}))

    write_gene_sequences_to_fasta(output_file_path, sequences)

    close_database_connection(conn)

# Example usage:
if __name__ == "__main__":
    db_file = "db.db"
    gene_name = "WP_032577796.1"
    output_folder = "temp.fna"
    column = "RefSeq"
    fasta_path = "/Users/reneeoles/Desktop/bakta_plan/test/"
    extract_gene_sequence_to_fasta(db_file, gene_name, column, fasta_path, output_folder)
