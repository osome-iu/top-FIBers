#!/bin/bash

# Download Facebook data for specific months listed below.
#
# Notes:
#    - The script will overwrite existing files!!!
#    - Must be run as the `truthy` user (to use the CrowdTangle API key, set as an environment variable)
#
# Author: Matthew DeVerna
# -------------------------

PYTHON_ENV="/home/data/apps/topfibers/repo/environments/env_code/bin/python"
IFFY_FILES_DIR="/home/data/apps/topfibers/repo/data/iffy_files/"
FACEBOOK_DATA_DIR="/home/data/apps/topfibers/repo/data/raw/facebook"

# Define the months
LAST_MONTHS=("2021_10" "2021_11" "2021_12")

# Loop over the months and run the command
for month in "${LAST_MONTHS[@]}"
do
    $PYTHON_ENV scripts/data_collection/crowdtangle_dl_fb_links.py -d $IFFY_FILES_DIR -o $FACEBOOK_DATA_DIR -l $month -n 1
done
