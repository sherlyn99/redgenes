import ast
import pandas as pd
from pathlib import Path
from redgenes.sql_connection import TRN
from redgenes.quality_control_utils import run_bash


def run_checkm(indir, outdir, threads=1):
    commands = ["checkm", "lineage_wf", indir, outdir, "-t", str(threads)]
    run_bash(commands)
    return str(Path(outdir) / "storage" / "bin_stats_ext.tsv")


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
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        args_checkm = [entity_id] + checkm_res
        TRN.add(sql_checkm, args_checkm)
