#!/bin/bash
#SBATCH -J 500_ncbi_redgenes
#SBATCH -p short
#SBATCH -t 20:00:00
#SBATCH -N 1
#SBATCH -c 4
#SBATCH --mem 64g
#SBATCH -o /panfs/roles/redgenes/slurm-%x-%A-%a-%j-%N.out
#SBATCH -e /panfs/roles/redgenes/slurm-%x-%A-%a-%j-%N.err
#SBATCH --export ALL
#SBATCH --mail-type=ALL,TIME_LIMIT_50,TIME_LIMIT_90
#SBATCH --mail-user=roles@health.ucsd.edu

set -x                                                                          
set -e                                                                          
set -o pipefail

METADATA=""
WORKING=""

source /home/roles/anaconda/bin/actviate
conda activate redgenes

python workflow.py db_insertion --metadata $METADATA --working-dir $WORKING

conda deactivate