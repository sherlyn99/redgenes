import os
import shutil
from BCBio import GFF
from pathlib import Path
from sql_initialize_db import initialize_db
from sql_connection import TRN

def read_gff_file(gff_file):
    if not Path(gff_file).exists():
        raise FileNotFoundError(f"Error: GFF file not found at {gff_file}")
    records = []
    with open(gff_file, "r") as in_handle:
        records = list(GFF.parse(in_handle))
    return records

def process_qualifiers(qualifiers, qualifier_keys):
    processed_qualifiers = {}
    for key in qualifier_keys:
        value = qualifiers.get(key, None)
        if isinstance(value, list):
            processed_value = ", ".join(value)
        else:
            processed_value = value
        processed_qualifiers[key] = processed_value
    return processed_qualifiers

def process_dbxref(dbxref_entry):
    entry_dict = {}
    if dbxref_entry:
        entries = dbxref_entry.split(',')
        for entry in entries:
            entry_type, entry_value = entry.split(':')
            entry_value = entry_value.strip()
            entry_type = entry_type.strip()
            if entry_type not in entry_dict:
                entry_dict[entry_type] = [entry_value]
            else:
                entry_dict[entry_type].append(entry_value)
    else:
        entry_dict = {}
    dbxref_edit = {}
    for key, value in entry_dict.items():
        if isinstance(value, list):
            value_str = ', '.join(value)
            dbxref_edit[key] = value_str
        else:
            dbxref_edit[key] = value
    return dbxref_edit

def insert_bakta(entity_id, contig_id, position, gene_id, source, type, start, end, strand, phase, 
                name, product, refseq, so, uniparc, uniref, kegg, pfam, run_accession):
    sql = "INSERT INTO bakta (entity_id, contig_id, position, gene_id, source, type, start, end, strand, phase, name, product, RefSeq, SO, UniParc, Uniref, KEGG, PFAM, run_accession) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    args = [entity_id, contig_id, position, gene_id, source, type, start, end, strand, phase, 
                name, product, refseq, so, uniparc, uniref, kegg, pfam, run_accession]
    with TRN: 
        TRN.add(sql, args)
        TRN.execute()

def parse_bakta(entity_id, temp_dir, run_accession):
    qualifier_keys = ["ID", "source", "Name", "locus_tag", "product", "Dbxref"]
    gff_file = f"{temp_dir}/{entity_id}.gff3"
    contig_id = 0

    for rec in read_gff_file(gff_file):
        for feature in rec.features:
            qualifiers = feature.qualifiers
            processed_qualifiers = process_qualifiers(qualifiers, qualifier_keys)
            dbxref_entry = processed_qualifiers.get("Dbxref", "")
            dbxref_edit = process_dbxref(dbxref_entry)

            # Increment position for each gene on a contig
            if contig_id:
                if contig_id == rec.id:
                    position += 1
                else:
                    position = 0
            else:
                contig_id = rec.id
                position = 1

            insert_bakta(entity_id, rec.id, position, 
                         processed_qualifiers.get("ID", None),
                         processed_qualifiers.get("source", None), feature.type,
                         int(feature.location.start), int(feature.location.end),
                         feature.location.strand, processed_qualifiers.get("phase", None),
                         processed_qualifiers.get("Name", None),
                         processed_qualifiers.get("product", None),
                         dbxref_edit.get("RefSeq", None), dbxref_edit.get("SO", None), dbxref_edit.get("UniParc", None),
                         dbxref_edit.get("UniRef", None), dbxref_edit.get("KEGG", None), dbxref_edit.get("PFAM", None), run_accession)
    return 0

def delete_bakta_output_files(bakta_output, temp_dir):
    bakta_output_path = os.path.join(temp_dir, bakta_output)
    if os.path.exists(bakta_output_path):
        try:
            if os.path.isfile(bakta_output_path):
                os.remove(bakta_output_path)
            elif os.path.isdir(bakta_output_path):
                shutil.rmtree(bakta_output_path)
            else:
                print(f"Warning: Unrecognized file/directory type at {bakta_output_path}")
        except Exception as e:
            print(f"Error deleting Bakta output: {e}")
            return 1  # Return a non-zero code for failure
        return 0  # Return 0 for success
    else:
        print(f"Error: Bakta output path does not exist: {bakta_output_path}")
        return 1  # Return a non-zero code for failure

# Example usage:
if __name__ == "__main__":
    initialize_db()
    temp_dir = "/Users/reneeoles/Desktop/bakta_plan/redgenes/redgenes/tests/data/"
    entity_id = "GCA_001699855.1"
    bakta_run_accession = 1
    parse_bakta(str(entity_id), temp_dir, bakta_run_accession)
    entity_id = "GCA_008569345.1"
    bakta_run_accession = 2
    parse_bakta(str(entity_id), temp_dir, bakta_run_accession)

