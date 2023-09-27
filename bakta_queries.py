import sqlite3
from Bio import SeqIO
import os

def extract_gene_sequence_to_fasta(db_file, gene_name, column, output_file_path):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Initialize a variable to accumulate gene sequences
    all_gene_sequences = ""

    # Step 1: Search for the gene by name and retrieve contig_id, start, end, strand, entity_id, gene_id
    query = f"""
        SELECT b.contig_id, b.start, b.end, b.strand, b.entity_id, b.gene_id, i.filename, i.filepath
        FROM bakta AS b
        INNER JOIN identifier AS i ON b.entity_id = i.entity_id
        WHERE b.{column} = ?
    """
    cursor.execute(query, (gene_name,))
    results = cursor.fetchall()

    for result in results:
        contig_id, start, end, strand, entity_id, gene_id, fasta_filename, fasta_filepath = result
        fasta_filepath = ""

        # Step 2: Query the "identifier" table to get the fasta file name and path
        fasta_file_path = os.path.join(fasta_filepath, fasta_filename)

        # Step 3: Read the FASTA file and extract the gene sequence based on coordinates and reading frame
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

        # Append the gene sequence to the accumulator
        all_gene_sequences += f">{fasta_filename}_{contig_id}_{gene_id}_{strand}\n{str(gene_sequence)}\n"

    # Step 4: Write all accumulated gene sequences to a single output FASTA file
    with open(output_file_path, "w") as output_file:
        output_file.write(all_gene_sequences)

    conn.close()

# Example usage:
db_file = "db.db"
gene_name = "0001217"
output_folder = "temp.fna"
column = "SO"
extract_gene_sequence_to_fasta(db_file, gene_name, column, output_folder)
