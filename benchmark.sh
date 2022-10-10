#!/bin/bash -x
#SBATCH --account=deepacf
#SBATCH --nodes=1
#SBATCH --gres=gpu:0
#SBATCH --output=log_out.%j
#SBATCH --error=log_err.%j
#SBATCH --time=10:00:00
#SBATCH --partition=batch

module --force purge
module use $OTHERSTAGES
module load Stages/2020

module load GCC/10.3.0
module load ParaStationMPI/5.4.10-1
module load mpi4py/3.0.3-Python-3.8.5

for runs in {1..3}
do
    for ntasks in {2..25}
    do
        srun --ntasks=${ntasks} python3 -m mpi4py.futures test_pystager.py ${ntasks} 1
    done
done
