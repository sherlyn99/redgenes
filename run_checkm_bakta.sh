#!/bin/bash
#SBATCH --nodes=1
#SBATCH --time=60:00:00
#SBATCH --mem=50G
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=roles@ucsd.edu


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
    local_output_dir=$2
    echo "Processing Fasta file: $fasta_file"
    echo "Using Output directory: $local_output_dir"

    base_name=$(basename "$fasta_file" .fa)

    # Create directory for each fasta file in the specified output directory
    sample_dir="$local_output_dir/$base_name"
    echo "Creating Sample Directory: $sample_dir"

    mkdir -p "$sample_dir/checkm"
    mkdir -p "$sample_dir/bakta"

    cp $fasta_file $sample_dir

    # Run CheckM
    checkm lineage_wf --tab_table -x fa "$sample_dir" "$sample_dir/checkm" > "$sample_dir/checkm/lineage.log"
    if [ $? -ne 0 ]; then
        echo "CheckM failed for $fasta_file"
        return 1
    fi

    # Extract completeness and contamination from lineage.log
    completeness=$(awk -v id="$base_name" '$0 ~ id {print $13}' "$sample_dir/checkm/lineage.log")
    contamination=$(awk -v id="$base_name" '$0 ~ id {print $14}' "$sample_dir/checkm/lineage.log")

    echo "$completeness $contamination"
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
export input_dir
export output_dir

# Use xargs to process fasta files
find "$input_dir" -name "*.fa" -print0 | xargs -0 -n 1 -P $parallel_jobs -I {} bash -c 'process_fasta "{}" "$output_dir"'

# Create metadata file
metadata_file="$output_dir/${output_dir##*/}_metadata.txt"
touch "$metadata_file"

# Iterate over fasta files to build the metadata file
for fasta_file in "$input_dir"/*.fa; do
    base_name=$(basename "$fasta_file" .fa)
    sample_dir="$output_dir/$base_name"
    checkm_output_file="$sample_dir/checkm/lineage.log"
    bakta_output_file="$sample_dir/bakta/$base_name.tsv"
    echo -e "$fasta_file\t$checkm_output_file\t$bakta_output_file" >> "$metadata_file"
done

echo "Metadata compilation complete."
