#!/bin/bash

FASTA="path/to/fastadir"
BAKTA="path/to/bakta"
OUT="path/to/outputdir"
FILE="your_file_name_without_extension"

input_gen="${FASTA}/${FILE}/${FILE}.fna"
params_db="${BAKTA}/db"
params_name="${FILE}"
params_outdir="${OUT}/Pangenome/Bakta/${FILE}"
log="${OUT}/report/bakta_${FILE}.log"

conda activate envs/bakta

bakta --skip-plot --db "${params_db}" --output "${params_outdir}" --prefix "${params_name}" "${input_gen}" &> "${log}"

conda deactivate
