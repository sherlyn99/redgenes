import os
import shutil
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

                # Extract and process Dbxref entries
                dbxref_entry = processed_qualifiers.get("Dbxref", "")
                entry_dict = {}

                if dbxref_entry:
                    entries = dbxref_entry.split(',')
                    for entry in entries:
                        entry_type, entry_value = entry.split(':')
                        if entry_type not in entry_dict:
                            entry_dict[entry_type] = [entry_value]
                        else:
                            entry_dict[entry_type].append(entry_value)
                else:
                    # If no Dbxref entries are present, initialize an empty dictionary
                    entry_dict = {}

                dbxref_edit = {}
                for key, value in entry_dict.items():
                    # Check if the value is a list
                    if isinstance(value, list):
                        # Convert the list to a comma-separated string
                        value_str = ', '.join(value)
                        dbxref_edit[key] = value_str
                    else:
                        # If it's not a list, keep the original value
                        dbxref_edit[key] = value
                        
                # Insert each line into the bakta database 
                insert_bakta(db, entity_id, rec.id, processed_qualifiers.get("ID", None),
                    processed_qualifiers.get("source", None), feature.type,
                    feature.location.start, feature.location.end,
                    feature.location.strand, processed_qualifiers.get("phase", None),
                    processed_qualifiers.get("Name", None),
                    processed_qualifiers.get("product", None),
                    dbxref_edit.get("RefSeq", None), dbxref_edit.get("SO", None), dbxref_edit.get("UniParc", None),
                    dbxref_edit.get("UniRef", None), dbxref_edit.get("KEGG", None), dbxref_edit.get("PFAM", None), run_accession)
    return 0

def delete_bakta_output_files(bakta_output, temp_dir):
    """
    Delete Bakta output files after processing.

    Args:
        bakta_output (str): Path to Bakta output files or directories.
        temp_dir (str): Path to the temporary directory.

    Returns:
        int: Return code (0 for success).
    """
    # Construct the full path to the Bakta output directory
    bakta_output_path = os.path.join(temp_dir, bakta_output)

    if os.path.exists(bakta_output_path):
        if os.path.isfile(bakta_output_path):
            # If it's a file, remove the file
            os.remove(bakta_output_path)
        elif os.path.isdir(bakta_output_path):
            # If it's a directory, remove the directory and its contents
            shutil.rmtree(bakta_output_path)
        else:
            # Handle other cases (e.g., symbolic links, etc.) if needed
            print(f"Warning: Unrecognized file/directory type at {bakta_output_path}")

        return 0  # Return 0 for success
    else:
        print(f"Error: Bakta output path does not exist: {bakta_output_path}")
        return 1  # Return a non-zero code for failure

