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

source /home/roles/anaconda/bin/activate
conda activate checkm_bakta

# Check input parameters
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <input_dir_with_fasta_files> <output_dir> <number_of_parallel_jobs>"
    exit 1
fi

input_dir=$1
output_dir=$2
parallel_jobs=$3

# Create output directory if it doesn't exist
mkdir -p "$output_dir"

# Function to process each FASTA file
process_fasta() {
    fasta_file=$1
    base_name=$(basename "$fasta_file" .fasta)

    # Create directory for each fasta file in the specified output directory
    sample_dir="$output_dir/$base_name"
    mkdir -p "$sample_dir/checkm"
    mkdir -p "$sample_dir/bakta"

    # Run CheckM
    checkm lineage_wf --tab_table -x fasta "$fasta_file" "$sample_dir/checkm" > "$sample_dir/checkm/lineage.log"
    if [ $? -ne 0 ]; then
        echo "CheckM failed for $fasta_file"
        return 1
    fi

    # Extract completeness and contamination from lineage.log
    completeness=$(awk '/Completeness/ {print $12}' "$sample_dir/checkm/lineage.log")
    contamination=$(awk '/Contamination/ {print $13}' "$sample_dir/checkm/lineage.log")
    if (( $(echo "$completeness > 95 && $contamination < 5" | bc -l) )); then
        echo "PASS" > "$sample_dir/checkm_results.txt"
    else
        echo "FAIL" > "$sample_dir/checkm_results.txt"
        return 1
    fi

    # If passed, run Bakta
    if [ "$(cat "$sample_dir/checkm_results.txt")" == "PASS" ]; then
        bakta --cpus 4 --outdir "$sample_dir/bakta" --prefix "$base_name" "$fasta_file"
        if [ $? -ne 0 ]; then
            echo "Bakta annotation failed for $fasta_file"
            echo "FAIL" > "$sample_dir/bakta_results.txt"
            return 1
        else
            echo "PASS" > "$sample_dir/bakta_results.txt"
        fi
    fi
}

export -f process_fasta

# Find all fasta files and process them in parallel, but serially per file for CheckM and Bakta
find "$input_dir" -name "*.fasta" -print0 | parallel -0 -j $parallel_jobs -I{} bash -c 'process_fasta "{}"'

# Create metadata file
metadata_file="$output_dir/${output_dir##*/}_metadata.txt"
touch "$metadata_file"

for fasta_file in "$input_dir"/*.fasta; do
    base_name=$(basename "$fasta_file" .fasta)
    sample_dir="$output_dir/$base_name"
    checkm_output_file="$sample_dir/checkm/lineage.log"
    bakta_output_file="$sample_dir/bakta/$base_name.tsv"
    echo -e "$fasta_file\t$checkm_output_file\t$bakta_output_file" >> "$metadata_file"
done

echo "Metadata compilation complete."
conda deactivate
