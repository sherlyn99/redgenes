#!/bin/bash -l

#SBATCH --nodes=1
#SBATCH --time=6:00:00
#SBATCH --ntasks=1
#SBATCH --mem=10G
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=roles@ucsd.edu

python main.py -f SRR13269209.fna -p /panfs/roles/BF/Assembly/SRR13269209 -d db.db
python main.py -f SRR9205877.fna -p /panfs/roles/BF/Assembly/SRR9205877 -d db.db
python main.py -f SRR15283788.fna -p /panfs/roles/BF/Assembly/SRR15283788 -d db.db
python main.py -f MCT420_76_S84_L002.fna -p /panfs/roles/BF/Assembly/MCT420_76_S84_L002 -d db.db
