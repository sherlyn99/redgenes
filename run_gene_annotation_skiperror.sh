#!/bin/bash
#SBATCH -J 500_ncbi_redgenes
#SBATCH -p short
#SBATCH -t 20:00:00
#SBATCH -N 1
#SBATCH -c 4
#SBATCH --mem 64g
#SBATCH -o /home/y1weng/14_gg2_sqlitedb/redgenes/slurm-%x-%A-%a-%j-%N.out
#SBATCH -e /home/y1weng/14_gg2_sqlitedb/redgenes/slurm-%x-%A-%a-%j-%N.err
#SBATCH --export ALL
#SBATCH --mail-type=ALL,TIME_LIMIT_50,TIME_LIMIT_90
#SBATCH --mail-user=y1weng@ucsd.edu

set -x                                                                          
set -e                                                                          
set -o pipefail

export TMPDIR=/panfs/y1weng/tmp2
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

cd /home/y1weng/14_gg2_sqlitedb/redgenes

while IFS= read -r fp; do
    echo "************" 1>&2
    echo "${fp}" 1>&2

    set +e
    python redgenes/gene_annotations.py "${fp}"
    status=$?
    set -e

    if [ $status -ne 0 ]; then
        echo "${fp} failed!" 1>&2
    fi
# done < <(tail -n +2 ./redgenes/tests/data/md_gordon_2.tsv)
# done < <(tail -n +2 ./redgenes/tests/data/md_gordon_10.tsv)
# done < <(tail -n +2 ./redgenes/tests/data/md_noncbinoMY_100.tsv)
# done < <(tail -n +2 ./redgenes/tests/data/md_noncbinoMY_1000.tsv)
# done < <(tail -n +2 ./redgenes/tests/data/md_gcf_1000.tsv)
done < <(tail -n +2 ./redgenes/tests/data/md_gcf_500.tsv)

conda deactivate

                                                            
                                                                                

