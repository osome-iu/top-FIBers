"""
Purpose:
    Move raw (moe generated) Twitter data into the proper repo subdirectory, renaming
    the file to include the proper date and format.

    NOTE: The script works by checking all YYYY-MM combinations between 2021-10
    (the first ever month) and the current month. If the output file does not exist,
    it will be created. If the output file for that month already exists, the
    script will report that fact and skip it. If the output file does not exist AND
    the data file does not exist, it will report that as well.

Inputs:
    - None

Outputs:
    Generates copies of raw data files with the form: YYYY-MM-DD__tweets_w_links.jsonl.gzip
    Note that DD will always be 01.
"""
import datetime
import os
import shutil
import sys

import pandas as pd

from top_fibers_pkg.utils import get_logger
from top_fibers_pkg.dates import retrieve_paths_from_dir


FIRST_MONTH = "2021-10-01"
LOG_DIR = "./logs"
LOG_FNAME = "move_twitter_raw.log"
OUTPUT_DIR = "/home/data/apps/topfibers/repo/data/raw/twitter"
OUTPUT_SUFFIX = "__tweets_w_links.jsonl.gzip"
RAW_DATA_DIR = "/home/data/apps/topfibers/moe_twitter_data"
REPO_ROOT = "/home/data/apps/topfibers/repo"
SUCCESS_FNAME = "success.log"


if __name__ == "__main__":
    if not (os.getcwd() == REPO_ROOT):
        sys.exit(
            "ALL SCRIPTS MUST BE RUN FROM THE REPO ROOT!!\n"
            f"\tCurrent directory: {os.getcwd()}\n"
            f"\tRepo root        : {REPO_ROOT}\n"
        )

    script_name = os.path.basename(__file__)
    logger = get_logger(LOG_DIR, LOG_FNAME, script_name=script_name, also_print=True)
    logger.info("-" * 50)
    logger.info(f"Begin script: {__file__}")

    todays_date = datetime.datetime.now()

    # Create date range like: 2021-10-01, 2021-11-01, 2021-12-01, ...
    dates = pd.date_range(FIRST_MONTH, todays_date.strftime("%Y-%m-%d"), freq="MS")

    for date in dates:
        # This is what the new output file should be
        new_out_file = os.path.join(
            OUTPUT_DIR, f"{date.strftime('%Y-%m-%d')}{OUTPUT_SUFFIX}"
        )
        if os.path.exists(new_out_file):
            logger.info(f"Skipping file that already exists: {new_out_file}")
            continue

        raw_month_dir = os.path.join(RAW_DATA_DIR, date.strftime("%Y-%m"))
        raw_file_list = retrieve_paths_from_dir(raw_month_dir, "*.gz")

        # If there are no files, we don't have this month's data
        if len(raw_file_list) == 0:
            logger.info(f"Skipping month that does not exist: {raw_month_dir}")
            continue

        # If there are more than one file, we almost certainly have an issue
        if len(raw_file_list) > 1:
            logger.error(f"More than one file found!! Please investigate!!")
            logger.error(f"Month: {raw_month_dir}")
            logger.error(f"# Files: {len(raw_file_list)}")
            continue

        # Otherwise, there will be one file which we grab here
        raw_file = raw_file_list[0]

        # Otherwise, we move it to the new name
        logger.info(f"Adding file to repo dir...")
        logger.info(f"\t Moving   : {raw_file}")
        logger.info(f"\t To become: {new_out_file}")
        shutil.move(raw_file, new_out_file)

        # Remove the directory now that we've gotten the file
        shutil.rmtree(raw_month_dir)

    with open(os.path.join(REPO_ROOT, SUCCESS_FNAME), "w+") as outfile:
        pass
    logger.info("~~~ Script complete! ~~~")
