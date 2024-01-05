#!/bin/bash
#SBATCH -J annotation_test_subset
#SBATCH -p short
#SBATCH -t 5:00:00
#SBATCH -N 1
#SBATCH -c 8
#SBATCH --mem 64g
#SBATCH -o /projects/greengenes2/20231117_annotations_prelim/logs/slurm-%x-%A-%a-%j-%N.out
#SBATCH -e /projects/greengenes2/20231117_annotations_prelim/logs/slurm-%x-%A-%a-%j-%N.err
#SBATCH --export ALL
#SBATCH --mail-type=ALL,TIME_LIMIT_50,TIME_LIMIT_90
#SBATCH --mail-user=y1weng@ucsd.edu

set -x
set -e

ko_number="subset"

export TMPDIR=/panfs/y1weng/tmp${ko_number}
mkdir -p $TMPDIR
export TMPDIR=$(mktemp -d)
function cleanup {
  echo "Removing $TMPDIR"
  rm -r $TMPDIR
  unset TMPDIR
}
trap cleanup EXIT

# activate checkm2 environment
source /home/y1weng/miniconda3/etc/profile.d/conda.sh
conda activate /home/y1weng/mambaforge/envs/gg2_bakta_db

# ncbi test genome
# test_genome="/panfs/y1weng/05_ncbi_genomes/GCF/002/287/175/GCF_002287175.1_ASM228717v1/*.fna.gz"
# # non-ncbi test genome

test_genome=/projects/greengenes2/20231117_annotations_prelim/hA10.fna
filename=$(basename "$test_genome")
outdir="/projects/greengenes2/20231117_annotations_prelim/test_${ko_number}"

#####################################
# prodigal for cds gene detection
#####################################

mkdir -p ${outdir}

/usr/bin/time -v prodigal -i ${test_genome} \
  -a ${outdir}/${filename}_proteins.faa \
  -f gff

#####################################
# kofam_scan for selected gene marker annotation
conda deactivate
conda activate /home/y1weng/mambaforge/envs/kofamscan
#####################################

/usr/bin/time -v exec_annotation \
  -f detail-tsv \
  -p /projects/greengenes2/20231117_annotations_prelim/kofam_scan/profiles/test_${ko_number}.hal \
  -k /projects/greengenes2/20231117_annotations_prelim/kofam_scan/ko_list_${ko_number} \
  -o ${outdir}/${filename}_kofamscan_output.tsv \
  ${outdir}/${filename}_proteins.faa \
  --no-report-unannotated \
  --tmp-dir /panfs/y1weng/tmp${ko_number} \
  --cpu 8

#####################################
# barrnap for 16S gene detection
#####################################

barrnap --thread 8 ${test_genome} > ${outdir}/${filename}_rrna.gff

conda deactivate