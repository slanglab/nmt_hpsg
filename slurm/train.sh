#!/bin/bash
#
#SBATCH --job-name=train-nematus
#SBATCH --nodes=1
#SBATCH --partition=gpu
#SBATCH --gres=gpu:1
#SBATCH --exclude=gpu-0-0
#SBATCH --time=72:00:00
#SBATCH --mem=20G
#SBATCH --output=log/slurm/train_nematus.out
#SBATCH --error=log/slurm/train_nematus.err

echo $CUDA_VISIBLE_DEVICES

source activate nematus
module load cuda/9.0.176
module load cudnn/7.0-cuda_9.0

./nematus/nematus/nmt.py \
  --model data/translate/models/model.npz \
  --datasets data/translate/splits/train.source data/translate/splits/train.target \
  --dictionaries data/translate/preprocess/source.tok.json data/translate/preprocess/target.tok.json \
  --valid_source_dataset data/translate/splits/dev.source \
  --valid_target_dataset data/translate/splits/dev.target \
  --valid_batch_size 80 \
  --validFreq 5000 \
  --dim_word 512 \
  --dim 1024 \
  --maxlen 50 \
  --source_vocab_sizes 40000 \
  --optimizer adam \
  --learning_rate 0.0001 \
  --batch_size 80 \
  --dispFreq 100 \
  --finish_after 100000 \
  --dropout_embedding 0.4 \
  --dropout_hidden 0.3 \
  --dropout_source 0.3 \
  --dropout_target 0.3 \
  --use_layer_norm \
  --enc_depth 2 \
  --dec_depth 2 \
  --tie_decoder_embeddings
