#!/bin/bash
#
#SBATCH -p serial_requeue # partition (queue)
#SBATCH -c 4 # number of cores per job in the array
#SBATCH --mem 48GB # memory per job in the array
#SBATCH -t 1-00:00 # time (D-HH:MM)

snakemake --cores 1
