#!/bin/bash
#
#SBATCH --job-name=hpsg-export
#SBATCH --nodes=1 --ntasks=1
#SBATCH --time=30:00
#SBATCH --mem=200MB
#SBATCH --output=log/slurm/pre_%A_%a.out
#SBATCH --error=log/slurm/pre_%A_%a.err

INPUT_DIR=./logon/lingo/lkb/src/tsdb/home/erg/1214
OUTPUT_DIR=./data/export
TASK_ID=$(printf '%0'$digits'd' $SLURM_ARRAY_TASK_ID)

source activate nmt_hpsg
echo $TASK_ID

python src/wikiwoods.py \
    -directory $INPUT_DIR/$TASK_ID \
    -output $OUTPUT_DIR/$TASK_ID \
    -derivation \
    -preprocess \
    -na
