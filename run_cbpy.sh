#!/bin/bash

QUEUE="$(squeue --format="%.18i %.9P %.100j %.8u %.8T %.10M %.9l %.6D %R" -u lab)"

# Start CBPy for denoising
if [[ $QUEUE != *"CBPy"*  ]]; then
sbatch /h20/home/lab/scripts/run_cbpy.sh
fi
