import sqlite3
from BCBio import GFF

# Define the GFF3 file and SQLite database filename
gff_file = "SRR15667803.gff3"
db_file = "your_database.db"

# Create an SQLite database connection
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# Define SQLite table schema
cursor.execute('''
    CREATE TABLE IF NOT EXISTS features (
        filename TEXT,
        contig TEXT,
        seqid TEXT,
        source TEXT,
        type TEXT,
        start INTEGER,
        end INTEGER,
        strand TEXT,
        phase TEXT,
        genename TEXT,
        locustag TEXT,
        product TEXT,
        dbxref TEXT
    )
''')

# Parse the GFF3 file and insert data into the SQLite table
# List of variable X values
qualifier_keys = ["ID", "source", "Name", "locus_tag", "product", "Dbxref"]

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

            # Insert data into the SQLite table using processed qualifiers
            cursor.execute('''
                INSERT INTO features (filename, contig, seqid, source, type, start, end, strand, phase, genename, locustag, product, dbxref)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (gff_file, rec.id, processed_qualifiers.get("ID", None), processed_qualifiers.get("source", None), 
                feature.type, feature.location.start, feature.location.end, feature.location.strand, 
                processed_qualifiers.get("phase", None), processed_qualifiers.get("Name", None),
                processed_qualifiers.get("locus_tag", None), processed_qualifiers.get("product", None),
                processed_qualifiers.get("Dbxref", None)))

# Commit the changes and close the database connection
conn.commit()
conn.close()

