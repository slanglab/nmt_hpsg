#!/bin/bash
#
#SBATCH --job-name=hpsg-parse
#SBATCH --nodes=1
#SBATCH --time=20:00:00
#SBATCH --output=log/slurm/parse_%A_%a.out
#SBATCH --error=log/slurm/parse_%A_%a.err

LOGON_ROOT=./logon
INPUT_DIR=./data/parse/splits
TASK_ID=$(printf '%0'$digits'd' $SLURM_ARRAY_TASK_ID)

kill $(ps -aux | grep pvmd | head -1 | sed 's/\s\+/\t/g' | cut -f2)

$LOGON_ROOT/parse \
    --binary \
    --erg+tnt \
    --best 1 \
    --count $ntasks \
    --text $INPUT_DIR/$TASK_ID
