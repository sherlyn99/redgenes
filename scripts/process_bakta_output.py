import sqlite3
from BCBio import GFF
from .database_operations import insert_bakta

def parse_bakta(db, entity_id, temp_dir, run_accession):
    """
    Parse a Bakta GFF3 output file and insert the data into an SQLite database.

    Args:
        db (str): Path to the SQLite database.
        entity_id (int): The identifier for the entity associated with the Bakta output.
        temp_dir (str): Path to the temporary directory containing the GFF3 file.
        run_accession (int): The accession identifier for the run.

    Returns:
        int: Return code (0 for success).
    """
    # List of qualifier keys to extract from GFF3
    qualifier_keys = ["ID", "source", "Name", "locus_tag", "product", "Dbxref"]
    
    gff_file = f"{temp_dir}/{entity_id}.gff3"
    
    # Parse the GFF3 file and insert data into the SQLite table
    with open(gff_file, 'r') as in_handle:
        for rec in GFF.parse(in_handle):
            for feature in rec.features:
                # Extract and process qualifiers
                qualifiers = feature.qualifiers
                processed_qualifiers = {}  # Dictionary to store processed qualifiers

                # Process each qualifier
                for key in qualifier_keys:
                    value = qualifiers.get(key, None)
                    if isinstance(value, list):
                        processed_value = ", ".join(value)
                    else:
                        processed_value = value
                    processed_qualifiers[key] = processed_value

                bakta_info = [
                    entity_id, rec.id, processed_qualifiers.get("ID", None),
                    processed_qualifiers.get("source", None), feature.type,
                    feature.location.start, feature.location.end,
                    feature.location.strand, processed_qualifiers.get("phase", None),
                    processed_qualifiers.get("Name", None),
                    processed_qualifiers.get("locus_tag", None),
                    processed_qualifiers.get("product", None),
                    processed_qualifiers.get("Dbxref", None), run_accession
                ]

                # Insert each line into the bakta database 
                insert_bakta(db, bakta_info)
    
    return 0

def delete_bakta_output_files(bakta_output):
    """
    Delete Bakta output files after processing.

    Args:
        bakta_output (str): Path to Bakta output files.

    Returns:
        int: Return code (0 for success).
    """
    # Implement code to delete Bakta output files after processing
    # Use os.remove() or shutil.rmtree() to delete files or directories
    return 0
