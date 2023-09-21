import sqlite3
from tabulate import tabulate

def find_matching_rows(db_file, value_to_find):
    # Connect to the database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Define your query
    query = "SELECT * FROM bakta WHERE gene_name = ?"

    # Execute the query with the specified value
    cursor.execute(query, (value_to_find,))

    # Fetch all the matching rows
    matching_rows = cursor.fetchall()

    # Close the database connection
    conn.close()

    # Check if there are matching rows
    if matching_rows:
        # Define the table headers based on your table columns
        headers = ["bakta_accession", "entity_id", "contig_id", "gene_id", "source", "type", "start", "end", "strand", "phase", "gene_name", "locus_tag", "product", "dbxref", "run_accession", "created_at"]

        # Convert the matching rows to a list of lists for tabulate
        data = [list(row) for row in matching_rows]

        # Return the table as a string
        table = tabulate(data, headers, tablefmt="pretty")
        return table
    else:
        return "No matching rows found."

if __name__ == "__main__":
    # Example usage
    db_file = 'your_database.db'
    value_to_find = "defg"
    result = find_matching_rows(db_file, value_to_find)
    print(result)
