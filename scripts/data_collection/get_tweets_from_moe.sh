#!/bin/bash

# Purpose:
#   This is a script that downloads the latest iffy list, then retrieve and save relevant raw twitter data from the past month.
#   The process goes between different servers(Lenny, Lisa, Moe) for convinience and load balancing.
#
# Inputs:
#   None
#
# Configs:
#   fiber_home    :  topfibers project folder
#   tavern_job    :  moe's tavern is used for raw twitter data retrival, files first export from Moe to a shared path on Lisa, then transfered to Lenny.
#   KEY           :  the ssh key for Moe as appuser
#   iffy_filename :  filename for downloaded iffy list
#
# Output:
#   A folder named YYYY-MM containing data retrival results for the past month using the latest iffy list
#
# How to call:
#   ```
#   cd /home/data/apps/topfibers/repo
#   nohup ./scripts/data_collection/iffy_get_data.sh > ./logs/iffy_get_data.out 2>&1 &
#   ```
#
#   The above will output results to ${fiber_home}moe_twitter_data/YYYY-MM and save logs within /var/log/iffy_get_data.out
#
# Author: Nick Liu & Matthew DeVerna


# Set date-related variables
year_month=$(date --date='last month' '+%Y-%m')
end_of_last_month=$(date -d "$(date +%Y-%m-01) -1 day" +%Y-%m-%d)
today="$(date +%Y-%m-%d)__"

# check if year_month is empty
if [[ -z "$year_month" ]]; then
  echo "year_month variable is empty. Exiting script."
  exit 1
fi

# Set paths and filenames
fiber_home="/home/data/apps/topfibers/"
tavern_job="osome_swap/moe/top_fibers_data/" # relative path for the shared disk bewteen Lisa and Moe; prefix "/mnt/" -> Moe, prefix "/home/data/" -> Lisa
KEY=${HOME}/.ssh/id_rsa_moe
iffy_filename="iffy_list.txt"

# Move to project repository root
cd ${fiber_home}repo

# Copy to Lisa-Moe shared drive
echo "$(date -Is) : Copying the iffy list to the Lisa-Moe shared drive."
rsync -at data/iffy_files/${today}${iffy_filename} truthy@lisa.luddy.indiana.edu:/home/data/${tavern_job}${today}${iffy_filename} &> logs/copy_iffy_log.txt
if grep -q "failed" logs/copy_iffy_log.txt; then
  echo "$(date -Is) : Copy Faild, Will Exit Now"
  exit 1
else
  echo "$(date -Is) : SUCCESS."
fi
rm logs/copy_iffy_log.txt

# Clean up tavern directory if exists
echo "$(date -Is) : Cleaning up the tarvern directory if it exists. '/mnt/${tavern_job}${year_month}'"
ssh -i ${KEY} appuser@moe-ln01.soic.indiana.edu "if [ -d "/mnt/${tavern_job}${year_month}" ]; then rm -Rf /mnt/${tavern_job}${year_month}; fi"


# Create a tavern job
echo "$(date -Is) : Running tavern query..."
ssh -i ${KEY} -o ServerAliveInterval=60 -o ServerAliveCountMax=10 appuser@moe-ln01.soic.indiana.edu "/home/appuser/truthy-cmd.sh get-tweets-with-meme -memes "/mnt/${tavern_job}${today}${iffy_filename}" -tstart "${year_month}-01" -tend "${end_of_last_month}" -tid false -ntweets 1000000 -outdir /mnt/${tavern_job}${year_month}/ -torf false > /dev/null 2>&1"

# check if the directory is empty
echo "$(date -Is) : Check if tavern query finished successfully..."
ssh -i ${KEY} appuser@moe-ln01.soic.indiana.edu "ls -A1 /mnt/'${tavern_job}${year_month}'/*" &> logs/tavern_job_check.txt
if grep -q "No such file or directory" logs/tavern_job_check.txt; then
  echo "$(date -Is) : Job folder empty. Will exit now"
  exit 1
else
  echo "$(date -Is) : SUCCESS."
fi
rm logs/tavern_job_check.txt

# Clean up raw tweet directory if exists
echo "$(date -Is) : Clean up raw tweet directory if exists...'${fiber_home}moe_twitter_data/${year_month}'"
if [ -d "${fiber_home}moe_twitter_data/${year_month}" ]; then rm -Rf ${fiber_home}moe_twitter_data/${year_month}; fi

# Copy results to Lenny:topfibers
echo "$(date -Is) : Copy results to Lenny:topfibers...'${fiber_home}moe_twitter_data/'"
rsync -rt truthy@lisa.luddy.indiana.edu:/home/data/${tavern_job}${year_month} ${fiber_home}moe_twitter_data/ &> logs/copy_twitter_raw_log.txt
if grep -q "failed" logs/copy_twitter_raw_log.txt; then
  echo "$(date -Is) : Copy Faild, Will Exit Now"
  exit 1
else
  echo "$(date -Is) : SUCCESS."
fi
rm logs/copy_twitter_raw_log.txt

touch ${fiber_home}repo/success.log
