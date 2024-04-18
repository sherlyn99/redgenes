import ast
from pathlib import Path
from sql_connection import TRN
from metadata import insert_metadata


def extract_checkm_results(inpath):
    with open(inpath, "r") as file:
        lines = file.readlines()
    checkm_dict = ast.literal_eval(lines[0].split("\t")[1])

    cols = [
        "marker lineage",
        "Completeness",
        "Contamination",
        "# scaffolds",
        "# contigs",
        "Longest scaffold",
        "Longest contig",
        "N50 (scaffolds)",
        "N50 (contigs)",
        "Mean scaffold length",
        "Mean contig length",
        "Coding density",
        "Translation table",
        "# predicted genes",
    ]
    checkm_res = [checkm_dict.get(col, None) for col in cols]
    return checkm_res


def insert_checkm_results(entity_id, checkm_res):
    with TRN:
        sql_checkm = """
            INSERT INTO qc_info (
                entity_id,
                marker_lineage,
                completeness,
                contamination,
                num_scaffolds,
                num_contigs,
                longest_scaffold,
                longest_contig,
                N50_scaffolds,
                N50_contigs,
                mean_scaffold_length,
                mean_contig_length,
                coding_density,
                translation_table,
                num_predicted_genes)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        args_checkm = entity_id + checkm_res
        TRN.add(sql_checkm, args_checkm)


def extract_and_insert_checkm_results(inpath, entity_id):
    checkm_res = extract_checkm_results(inpath)
    insert_checkm_results(entity_id, checkm_res)


def qc_db_insertion(row, logger):
    checkm_path = row["checkm_path"].strip()

    try:
        with TRN:
            entity_id = insert_metadata(row)
            extract_and_insert_checkm_results(checkm_path, entity_id)
    except Exception as e:
        logger.error(f"Error at database insertion: {e}")
    else:
        logger.info("Checkm insertion finished")


def qc_bash_and_db_insertion(row, working_dir, logger):
    logger.info("CheckM insertion started")

    qc_db_insertion(row, logger)
