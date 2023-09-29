import sqlite3
from Bio import SeqIO
import os

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
    query = f"""
        SELECT b.contig_id, b.start, b.end, b.strand, b.entity_id, b.gene_id, i.filename, i.filepath
        FROM bakta AS b
        INNER JOIN identifier AS i ON b.entity_id = i.entity_id
        WHERE b.{column} = ?
    """
    cursor.execute(query, (gene_name,))
    return cursor.fetchall()

def extract_gene_sequence_from_fasta(fasta_file_path, contig_id, start, end, strand):
    """
    Extract the gene sequence from a FASTA file based on coordinates and strand.
    """
    gene_sequence = ""
    for record in SeqIO.parse(fasta_file_path, "fasta"):
        if record.id == contig_id:
            if strand == "1":
                gene_sequence = record.seq[start:end]  # Adjust for 0-based indexing
            elif strand == "-1":
                gene_sequence = record.seq[start:end].reverse_complement()
            break
    return gene_sequence

def write_gene_sequences_to_fasta(output_file_path, gene_sequences):
    """
    Write gene sequences to an output FASTA file.
    """
    with open(output_file_path, "w") as output_file:
        output_file.write(gene_sequences)

def extract_gene_sequence_to_fasta(db_file, gene_name, column, output_file_path):
    """
    Extract gene sequences based on gene name and write them to an output FASTA file.
    """
    conn, cursor = connect_to_database(db_file)

    all_gene_sequences = ""

    gene_info = get_gene_info_by_name(cursor, gene_name, column)

    for result in gene_info:
        contig_id, start, end, strand, entity_id, gene_id, fasta_filename, fasta_filepath = result
        fasta_file_path = os.path.join(fasta_filepath, fasta_filename)
        gene_sequence = ""
        for record in SeqIO.parse(fasta_file_path, "fasta"):
            if record.id == contig_id:
                if strand == "1":
                    gene_sequence = record.seq[start:end]
                elif strand == "-1":
                    print("negative", gene_id)
                    gene_sequence = record.seq[start:end]
                    gene_sequence = gene_sequence.reverse_complement()
                    print(gene_sequence)
                break
        all_gene_sequences += f">{fasta_filename}_{contig_id}_{gene_id}_{strand}\n{str(gene_sequence)}\n"

    write_gene_sequences_to_fasta(output_file_path, all_gene_sequences)

    close_database_connection(conn)

# Example usage:
if __name__ == "__main__":
    db_file = "db.db"
    gene_name = "0001217"
    output_folder = "temp.fna"
    column = "SO"
    extract_gene_sequence_to_fasta(db_file, gene_name, column, output_folder)
