#! bin/bash

# Purpose:
#   This is the master script that will be run each month for this project. It will activate all other
#   scripts in order, to ensure that each portion of the process happens properly.
#
# Inputs:
#   None
#
# Output:
#   Please see each of the scripts called below for their outputs/effect on the system.
#
# How to call:
#   ```
#   cd /home/data/apps/topfibers/repo
#   nohup bash monthly_master_script.sh > logs/YYYY-MM-DD__month_master_script.out 2>&1 &
#   ````
#
# Author: Matthew DeVerna

### Set variables for script
# -------------------------------------
PYTHON_ENV="/home/data/apps/topfibers/repo/environments/env_code/bin/python"
PROJECT_ROOT="/home/data/apps/topfibers/repo"
IFFY_FILES_DIR="/home/data/apps/topfibers/repo/data/iffy_files/"

# Data directories
RAW_DATA_DIR="/home/data/apps/topfibers/repo/data/raw"
FACEBOOK_DATA_DIR="/home/data/apps/topfibers/repo/data/raw/facebook"
TWITTER_DATA_DIR="/home/data/apps/topfibers/repo/data/raw/twitter"
FACEBOOK_SYM_DIR="/home/data/apps/topfibers/repo/data/symbolic_links/facebook"
TWITTER_SYM_DIR="/home/data/apps/topfibers/repo/data/symbolic_links/twitter"
FIB_OUT_DIR_TWITTER="/home/data/apps/topfibers/repo/data/derived/fib_results/twitter"
FIB_OUT_DIR_FACBOOK="/home/data/apps/topfibers/repo/data/derived/fib_results/facebook"
POST_COUNTS_DIR="/home/data/apps/topfibers/repo/data/derived/post_counts"

# Logs, dates, and files
LOG_DIR="/home/data/apps/topfibers/repo/logs"
CURR_DATE="$(date +%Y-%m-%d)"
CURR_YYYY_MM="$(date +%Y_%m)"
LAST_MONTH=$(date --date='last month' '+%Y_%m')
MASTER_LOG=$LOG_DIR/${CURR_DATE}_master_script.log
SCRIPT_NAME=${BASH_SOURCE[0]}


# Change directory to project repo root
# -------------------------------------
echo "$(date -Is) : Changing directory to the project root." >> $MASTER_LOG
cd $PROJECT_ROOT
if [ $? -eq 0 ]; then
   echo "$(date -Is) : SUCCESS." >> $MASTER_LOG
else
   echo "$(date -Is) : FAILED. Exiting <${SCRIPT_NAME}>." >> $MASTER_LOG
   exit 1
fi

### Download the latest Iffy news domains file
# Log file saved here: ./logs/iffy_update.log
# -------------------------------------
echo "$(date -Is) : Downloading a new iffy domains file." >> $MASTER_LOG
$PYTHON_ENV scripts/data_collection/iffy_update.py -d $IFFY_FILES_DIR
if [ -e success.log ]; then
   echo "$(date -Is) : SUCCESS." >> $MASTER_LOG
else
   echo "$(date -Is) : FAILED. Exiting <${SCRIPT_NAME}>." >> $MASTER_LOG
   exit 1
fi
# Remove after checking for successful completion
rm success.log

### Download the Facebook data
# Log file saved here:
# -------------------------------------
echo "$(date -Is) : Downloading Facebook data..." >> $MASTER_LOG
$PYTHON_ENV scripts/data_collection/crowdtangle_dl_fb_links.py -d $IFFY_FILES_DIR -o $FACEBOOK_DATA_DIR -l $LAST_MONTH -n 1
if [ -e success.log ]; then
   echo "$(date -Is) : SUCCESS." >> $MASTER_LOG
else
   echo "$(date -Is) : FAILED. Exiting <${SCRIPT_NAME}>." >> $MASTER_LOG
   exit 1
fi
# Remove after checking for successful completion
rm success.log

### Running the below script completes the following things:
#    1. Copies the latest Iffy news domains file to the Lisa-Moe shared drive
#    2. Creates a tavern job to pull the Twitter data for the past month
#    3. Copies those results to Lisa:/home/data/apps/topfibers/moe_twitter_data
# -------------------------------------
monthly_moe_outfile=${LOG_DIR}/${CURR_DATE}_monthly_moe.log
echo "$(date -Is) : Running moe job to get Twitter data..." >> $MASTER_LOG
echo "$(date -Is) : See progress here: ${monthly_moe_outfile}" >> $MASTER_LOG
bash scripts/data_collection/get_tweets_from_moe.sh > $monthly_moe_outfile 2>&1
if [ -e success.log ]; then
   echo "$(date -Is) : SUCCESS." >> $MASTER_LOG
else
   echo "$(date -Is) : FAILED. Exiting <${SCRIPT_NAME}>." >> $MASTER_LOG
   exit 1
fi
# Remove after checking for successful completion
rm success.log

### Move raw data to the proper place and format it correctly
# Log file saved here: ./logs/move_twitter_raw.log
# -------------------------------------
echo "$(date -Is) : Moving Twitter data to the proper place..." >> $MASTER_LOG
$PYTHON_ENV scripts/data_prep/move_twitter_raw.py
if [ -e success.log ]; then
   echo "$(date -Is) : SUCCESS." >> $MASTER_LOG
else
   echo "$(date -Is) : FAILED. Exiting <${SCRIPT_NAME}>." >> $MASTER_LOG
   exit 1
fi
# Remove after checking for successful completion
rm success.log

### Create the symbolic links
# -------------------------------------
# TWITTER
# Log file saved here: UPDATE ME
echo "$(date -Is) : Creating symbolic links for Twitter..." >> $MASTER_LOG
$PYTHON_ENV scripts/data_prep/create_data_file_symlinks.py -d $TWITTER_DATA_DIR -o $TWITTER_SYM_DIR -m $CURR_YYYY_MM -n 3
if [ -e success.log ]; then
   echo "$(date -Is) : SUCCESS." >> $MASTER_LOG
else
   echo "$(date -Is) : FAILED. Exiting <${SCRIPT_NAME}>." >> $MASTER_LOG
   exit 1
fi
# Remove after checking for successful completion
rm success.log

# FACEBOOK
# Log file saved here: UPDATE ME
echo "$(date -Is) : Creating symbolic links for Facebook..." >> $MASTER_LOG
$PYTHON_ENV scripts/data_prep/create_data_file_symlinks.py -d $FACEBOOK_DATA_DIR -o $FACEBOOK_SYM_DIR -m $CURR_YYYY_MM -n 3
if [ -e success.log ]; then
   echo "$(date -Is) : SUCCESS." >> $MASTER_LOG
else
   echo "$(date -Is) : FAILED. Exiting <${SCRIPT_NAME}>." >> $MASTER_LOG
   exit 1
fi
# Remove after checking for successful completion
rm success.log

### Calculate the FIB index stuff
# -------------------------------------
# TWITTER
# Log file saved here: UPDATE ME
echo "$(date -Is) : Calculating FIB indices for Twitter..." >> $MASTER_LOG
$PYTHON_ENV scripts/data_processing/calc_twitter_fib_indices.py -d $TWITTER_SYM_DIR/${CURR_YYYY_MM} -o $FIB_OUT_DIR_TWITTER -m $CURR_YYYY_MM -n 3
if [ -e success.log ]; then
   echo "$(date -Is) : SUCCESS." >> $MASTER_LOG
else
   echo "$(date -Is) : FAILED. Exiting <${SCRIPT_NAME}>." >> $MASTER_LOG
   exit 1
fi
# Remove after checking for successful completion
rm success.log

# FACEBOOK
# Log file saved here: UPDATE ME
echo "$(date -Is) : Calculating FIB indices for Facebook..." >> $MASTER_LOG
$PYTHON_ENV scripts/data_processing/calc_crowdtangle_fib_indices.py -d $FACEBOOK_SYM_DIR/${CURR_YYYY_MM} -o $FIB_OUT_DIR_FACBOOK -m $CURR_YYYY_MM -n 3
if [ -e success.log ]; then
   echo "$(date -Is) : SUCCESS." >> $MASTER_LOG
else
   echo "$(date -Is) : FAILED. Exiting <${SCRIPT_NAME}>." >> $MASTER_LOG
   exit 1
fi
# Remove after checking for successful completion
rm success.log

### Update the Twitter profile image links
# Log file saved here: ./logs/get_latest_profile_image_links.log
#
# NOTE: The script updates images for FIBers found for the new month.
#    To update links for ALL FIBers found since the inception of this project,
#    include either "-a" or "--all-users" when executing the script below.
# -------------------------------------
echo "$(date -Is) : Updating new top FIBer Twitter profile image links..." >> $MASTER_LOG
$PYTHON_ENV scripts/data_processing/get_latest_profile_image_links.py
if [ -e success.log ]; then
   echo "$(date -Is) : SUCCESS." >> $MASTER_LOG
else
   echo "$(date -Is) : FAILED. Exiting <${SCRIPT_NAME}>." >> $MASTER_LOG
   exit 1
fi
# Remove after checking for successful completion
rm success.log

### Send the data to the database on Lisa
# -------------------------------------
echo "$(date -Is) : Feeding data into the database..." >> $MASTER_LOG
bash data-loader/run_data_loader.sh
if [ -e success.log ]; then
   echo "$(date -Is) : SUCCESS." >> $MASTER_LOG
else
   echo "$(date -Is) : FAILED. Exiting <${SCRIPT_NAME}>." >> $MASTER_LOG
   exit 1
fi
# Remove after checking for successful completion
rm success.log

### Update the post counts data frame
# Log file saved here: ./logs/post_count.log
# -------------------------------------

# TWITTER
echo "$(date -Is) : Calculating post counts for Twitter..." >> $MASTER_LOG
$PYTHON_ENV scripts/data_processing/count_num_posts.py -o $POST_COUNTS_DIR -d $RAW_DATA_DIR -p twitter
if [ -e success.log ]; then
   echo "$(date -Is) : SUCCESS." >> $MASTER_LOG
else
   echo "$(date -Is) : FAILED. Exiting <${SCRIPT_NAME}>." >> $MASTER_LOG
   exit 1
fi
# Remove after checking for successful completion
rm success.log

# FACEBOOK
echo "$(date -Is) : Calculating post counts for Facebook..." >> $MASTER_LOG
$PYTHON_ENV scripts/data_processing/count_num_posts.py -o $POST_COUNTS_DIR -d $RAW_DATA_DIR -p facebook
if [ -e success.log ]; then
   echo "$(date -Is) : SUCCESS." >> $MASTER_LOG
else
   echo "$(date -Is) : FAILED. Exiting <${SCRIPT_NAME}>." >> $MASTER_LOG
   exit 1
fi
# Remove after checking for successful completion
rm success.log

echo "$(date -Is) : Script complete." >> $MASTER_LOG
