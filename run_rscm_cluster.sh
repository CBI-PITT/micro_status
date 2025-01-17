#!/bin/bash

QUEUE="$(squeue --format="%.18i %.9P %.100j %.8u %.8T %.10M %.9l %.6D %R" -u lab)"

cd /h20/home/lab/scripts

if [[ $QUEUE != *"DASK_SCHED"*  ]]; then
./run_rscm_dask_scheduler.sh
fi

if [[ $QUEUE != *"DASK_WORKER"*  ]]; then
./run_rscm_dask_workers.sh
fi

if [[ $QUEUE != *"RSCM_Listen"*  ]]; then
sbatch ./run_RSCM_stitch_listen.sh
sleep 5
fi
