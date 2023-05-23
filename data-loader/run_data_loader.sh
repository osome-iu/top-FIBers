#!/bin/bash

# Purpose:
#    This script makes a connection to LISA and then runs the data-loader/server.py
#    script to send new data to the database. Will be run each month by the
#    scripts/monthly_master_script.sh script
#
# Inputs:
#    None
#
# Configs:
#    python     : This will get the conda environment path
#    loader_home: Add the data-loader path
#
# Output:
#    None 
#
# How to call:
#    ```
#    bash /home/data/apps/topfibers/repo/data-loader/run_data_loader.sh
#    ```
#
# Author: Pasan Kamburugamuwa

python="/home/data/apps/topfibers/repo/environments/env_code/bin/python"
loader_home="/home/data/apps/topfibers/repo/data-loader"

# Create SSH tunnel
ssh -i /u/truthy/.ssh/id_rsa -4 -N -L 5580:localhost:5432 truthy@lisa.luddy.indiana.edu &

# Store the PID of the SSH tunnel process
tunnel_pid=$!

# Run Python script
${python} ${loader_home}/server.py

# Kill SSH tunnel process
kill $tunnel_pid

touch /home/data/apps/topfibers/repo/success.log