import pandas as pd
from pathlib import Path
from collections import defaultdict
from sql_connection import TRN
#from add_accession import add_gene_accession
# from add_embedding import add_embedding, load_kmer_vectors

#kmer2vec_file = "/panfs/roles/redgenes/redgenes/kmernode2vec/emp500_kmer-node2vec-embedding.txt"

def process_dbxref(dbxref):
    """
    Process dbxref entry and return a structured dictionary.
    """
    entry_dict = defaultdict(list)
    if pd.notna(dbxref):
        entries = dbxref.split(', ')
        for entry in entries:
            if ':' in entry:
                entry_type, entry_value = entry.split(':', 1)
                entry_dict[entry_type.strip()].append(entry_value.strip())
    return entry_dict

def insert_dbxref_info(bakta_accession, dbxref_data):
    """
    Insert dbxref information into corresponding tables.
    """
    valid_tables = {'kegg', 'refseq', 'uniparc', 'uniref', 'so', 'pfam'} 
    for dbxref_type, accession_list in dbxref_data.items():
        table_name = dbxref_type.lower()
        if table_name in valid_tables:
            sql = f"INSERT INTO {table_name} (bakta_accession, {table_name.upper()}) VALUES (?, ?)"
            for accession in accession_list:
                TRN.add(sql, [bakta_accession, accession])
    TRN.execute()

def extract_bakta_results(tsv_path):
    """
    Extract information from a Bakta tsv output file.
    """
    column_names = ["contig_ID", "type", "start", "stop", "strand", "locus_tag", "gene", "product", "dbxrefs"]
    df = pd.read_csv(tsv_path, header=0, names=column_names, comment='#', sep='\t')
    df = df[df["type"] != "gap"]
    return df

def fetch_entity_id(row):
    """
    Fetch and return the entity ID based on filename and filepath.
    """
    try:
        local_path, filename = Path(row["local_path"]), row["assembly_accession"]
        with TRN:
            sql_fetch = """
                SELECT entity_id
                FROM identifier
                WHERE filename_full = ? AND filepath = ?"""
            args_fetch = [filename, str(local_path)]
            TRN.add(sql_fetch, args_fetch)
            entity_id = TRN.execute_fetchflatten()
            if entity_id:
                return entity_id[0]  # Return the first (and should be the only) ID fetched
            else:
                print("No entity found with the given filename and filepath.")
                return None
    except Exception as e:
        print(f"Error fetching entity ID: {e}")
        return None

def insert_bakta_results(entity_id, args_bakta):
    """
    Insert Bakta results into the database.
    """
    sql_bakta_info = """
        INSERT INTO bakta (
            entity_id,
            contig_id,
            type,
            start,
            stop,
            strand,
            locus_tag,
            gene,
            product)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        RETURNING bakta_accession;"""
    TRN.add(sql_bakta_info, args_bakta, many=True)
    bakta_accession = TRN.execute()
    return bakta_accession

def annotation_pipeline(row, tmpdir, logger):
    """
    Annotate genomes based on Bakta and insert information into the database.
    """
    logger.info("Bakta insertion started")
    bakta_path, filename = Path(row["bakta_path"]), row["assembly_accession"]
    bakta_df = extract_bakta_results(str(bakta_path))

    entity_id = fetch_entity_id(row)
    bakta_df.insert(loc=0, column='entity_id', value=entity_id)
    bakta_res = bakta_df.iloc[:, 0:9].values.tolist()

    with TRN:
        bakta_accession = insert_bakta_results(entity_id, bakta_res)
        bakta_df['bakta_accession'] = [sublist[0][0] if sublist else None for sublist in bakta_accession]

        if entity_id:
            for _, row in bakta_df.iterrows():
                dbxref_data = process_dbxref(row['dbxrefs'])
                insert_dbxref_info(row['bakta_accession'], dbxref_data)

            # Uncomment or implement the following as needed:
            # kmer_vectors = load_kmer_vectors(kmer2vec_file)
            # for _, row in bakta_res.iterrows():
            #     add_embedding(kmer_vectors, row['bakta_accession'], local_path, row['contig_ID'], row['start'], row['stop'])
            #     add_gene_accession(row['bakta_accession'])

    return
