#!/bin/bash
#SBATCH --job-name=dl_luna
#SBATCH --ntasks=1
#SBATCH --mem=16000
#SBATCH --output=/work/bdgr/LUNA25/outputs/download/dl_%j.out
#SBATCH --time=2-0

LD_LIBRARY_PATH="/home/bdgr/.conda/envs/luna25/lib:$LD_LIBRARY_PATH"
export PATH="/work/bdgr/LUNA25/scripts:$PATH"
# export PATH="/work/bdgr/LUNA25/data:$PATH"

# python /work/bdgr/LUNA25/gpu_test.py
python /work/bdgr/LUNA25/scripts/download.py