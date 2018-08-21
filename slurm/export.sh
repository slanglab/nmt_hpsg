#!/bin/bash
#
#SBATCH --job-name=hpsg-export
#SBATCH --nodes=1 --ntasks=1
#SBATCH --time=30:00
#SBATCH --mem=200MB
#SBATCH --output=log/slurm/export_%a.out
#SBATCH --error=log/slurm/export_%a.err

INPUT_DIR=./logon/lingo/lkb/src/tsdb/home/erg/1214
TASK_ID=$(printf '%0'$digits'd' $SLURM_ARRAY_TASK_ID)

. /home/jwei/miniconda3/etc/profile.d/conda.sh
conda activate base
echo $TASK_ID

python src/wikiwoods.py \
    --directory $INPUT_DIR/$TASK_ID \
    --output $OUTPUT_DIR/$TASK_ID \
    --derivation \
    --preprocess \
    --includena \
    --parser-error
