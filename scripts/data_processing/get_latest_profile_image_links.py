#!/usr/bin/env python3
"""
Purpose:
    Get links to top TWITTER FIBers profile image.

    NOTE: This script will update old links to profile images if we have a new one.
        It does this by iterating through files in reverse chronological order and then
        removing a user until links for all users have been found. By default, only 
        FIBers that have been found in the latest month are updated, however, to look for
        and update ALL top FIBers, make sure to pass the --all-users flag to the script.

Inputs:
    - Those loaded by top_fibers_pkg.utils.parse_cl_args_fib

    NOTE:
    - Call the calc_twitter_fib_indices.py -h flag to get input/flag details.
    - Input files contain Twitter posts.

Output:
    Two .parquet files containing:
    1. {YYYY_mm_dd}__fib_indices_twitter.parquet: a pandas dataframe with the following columns:
        - user_id (str) : a unique Twitter user ID
        - fib_index (int) : a specific user's FIB index
        - total_reshares (int) : total number of reshares earned by user_id
    2. {YYYY_mm_dd}__top_spreader_posts_twitter.parquet: a pandas dataframe with the following columns:
        - user_id (str) : a unique Twitter user ID
        - post_id (str) : a unique Twitter post ID
        - num_reshares (int) : the number of times post_id was reshared
        - timestamp (str) : timestamp when post was sent

    NOTE: YYYY_mm_dd will be representative of the machine's current date

What is the FIB-index?
    Please see our working paper for details.
    - https://arxiv.org/abs/2207.09524

Author: Matthew DeVerna
"""
import argparse
import datetime
import glob
import gzip
import json
import os
import sys

import pandas as pd

from dateutil.relativedelta import relativedelta
from top_fibers_pkg.utils import get_logger
from top_fibers_pkg.data_model import Tweet_v1


SCRIPT_PURPOSE = "Update the profile image links for Top FIBers."
REPO_ROOT = "/home/data/apps/topfibers/repo"
LOG_DIR = "./logs"
LOG_FNAME = "get_latest_profile_image_links.log"
DATA_DIR = "/home/data/apps/topfibers/repo/data/raw/twitter"
DATA_FILE_SUFFIX = "__tweets_w_links.jsonl.gzip"
FIBER_DATA_DIR = "/home/data/apps/topfibers/repo/data/derived/fib_results/twitter/"
FIBER_FILE_SUFFIX = "__fib_indices_twitter.parquet"
OUTPUT_FILE = "/home/data/apps/topfibers/repo/data/derived/twitter_profile_links/top_fiber_profile_image_links.parquet"
SUCCESS_FNAME = "success.log"
NUM_FIBERS = 50


def parse_cl_args(script_purpose="", logger=None):
    """
    Read command line arguments for the symlink creation script.
        - top-fibers/scripts/create_data_file_symlinks.py

    Parameters:
    --------------
    - script_purpose (str) : Purpose of the script being utilized. When printing
        script help message via `python script.py -h`, this will represent the
        script's description. Default = "" (an empty string)
    - logger : logging object

    Returns
    --------------
    None

    Exceptions
    --------------
    None
    """
    logger.info("Parsing command line arguments...")

    # Initiate the parser
    parser = argparse.ArgumentParser(description=script_purpose)

    # Add long and short argument
    msg = "If included, try and update the profile image links for all top FIBers ever found"
    parser.add_argument(
        "-a",
        "--all-users",
        help=msg,
        action="store_true",
    )

    # Read parsed arguments from the command line into "args"
    args = parser.parse_args()

    return args


def get_raw_files(update_all):
    """
    Return a list of full paths to raw twitter data files.

    Parameters
    -----------
    - update_all (boolean) : from the commandline flag. If True, return all data
        files for all months. Otherwise, return the data file for only the current month.

    Returns
    -----------
    - files (list): list containing full path strings to raw twitter data files
    """
    if update_all:
        # Sorts all files in the data directory in reverse chronological order
        files = sorted(
            glob.glob(os.path.join(DATA_DIR, f"*{DATA_FILE_SUFFIX}")), reverse=True
        )
    else:
        # Creates a list containing only the file for the latest month
        now = datetime.datetime.now()
        files = []

        # For one month, we want the last three months of data
        for x in range(1, 4):
            first_day_month = f"{(now - relativedelta(months=x)).strftime('%Y-%m')}-01"
            files.append(os.path.join(DATA_DIR, f"{first_day_month}{DATA_FILE_SUFFIX}"))
    return files


def get_FIBer_files(update_all):
    """
    Return the full path to the twitter top FIBers files.

    Parameters
    -----------
    - update_all (boolean) : from the commandline flag. If True, return all data
        files for all months. Otherwise, return the data file for only the current month.

    Returns
    -----------
    - files (list): list containing full path strings to raw twitter data files
    """
    files = []
    if update_all:
        for subdir, _, temp_files in os.walk(FIBER_DATA_DIR):
            for file in temp_files:
                if file.endswith("__fib_indices_twitter.parquet"):
                    files.append(os.path.join(subdir, file))
    else:
        # Creates a list containing only the file for the latest month
        year_month_str = datetime.datetime.now().strftime("%Y_%m")
        correct_data_dir = os.path.join(FIBER_DATA_DIR, year_month_str)
        fiber_file = glob.glob(os.path.join(correct_data_dir, f"*{FIBER_FILE_SUFFIX}"))
        files = fiber_file
    return files


def load_top_fiber_uids(files):
    """
    Load all of the top FIBers users IDs from the specified files.

    Parameters
    -----------
    - files (list) : list of full paths to top FIBers files

    Returns
    -----------
    - fiber_uids (set): set of user ids (strs) for all users in the provided files
    """
    fiber_uids = set()
    for file in files:
        df = pd.read_parquet(file)
        top_users = df.head(NUM_FIBERS).copy()
        for uid in top_users.user_id:
            fiber_uids.add(uid)
    return fiber_uids


def get_profile_image_links(fiber_uids, files):
    """
    Collect profile image links for all provided user_ids.

    Parameters
    -----------
    - fiber_uids (set) : set of user IDs for the top FIBers
    - files (list) : the files to iterate through to find profile image links

    Returns
    -----------
    - image_link_df (pandas.DataFrame): a dataframe containing the following
        columns:
            - user_id (str) : the top FIBer user ID,
            - profile_image_url : the url to `user_id`'s profile image
    """
    uid_imageurl_records = []
    num_urls_to_collect = len(fiber_uids)
    urls_collected = 0
    remaining_uids = True
    for file in files:
        logger.info(f"Loading tweets from file: {file} ...")
        with gzip.open(file, "rb") as f:
            for line in f:
                tweet = Tweet_v1(json.loads(line.decode()))

                uid = tweet.get_user_ID()
                if uid in fiber_uids:
                    profile_image_url = tweet.get_user_profile_image_url()
                    uid_imageurl_records.append(
                        {"user_id": uid, "profile_image_url": profile_image_url}
                    )
                    fiber_uids.remove(uid)
                    urls_collected += 1
                    logger.info(
                        f"\t- Collected: {urls_collected}/{num_urls_to_collect}"
                    )

                    if len(fiber_uids) == 0:
                        remaining_uids = False
                        break

        if not remaining_uids:
            logger.info("All profile image links have been collected!")
            break

    image_link_df = pd.DataFrame.from_records(uid_imageurl_records)
    return image_link_df


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

    args = parse_cl_args(SCRIPT_PURPOSE, logger)
    update_all = args.all_users

    logger.info("Building a list of raw files to load...")
    raw_files = get_raw_files(update_all)
    logger.info("\t- Success.")

    logger.info("Building a list of top FIBer files to load...")
    fiber_files = get_FIBer_files(update_all)
    logger.info("\t- Success.")

    logger.info("Loading the FIBer uids...")
    fiber_uid_set = load_top_fiber_uids(fiber_files)
    logger.info("\t- Success.")

    logger.info(f"Retrieving profile image links for {len(fiber_uid_set)} FIBers...")
    image_link_df = get_profile_image_links(fiber_uid_set, raw_files)
    logger.info("\t- Success.")

    logger.info(f"Saving profile image link file here:")
    logger.info(f"\t- {OUTPUT_FILE}")
    image_link_df.to_parquet(OUTPUT_FILE, engine="pyarrow")
    with open(os.path.join(REPO_ROOT, SUCCESS_FNAME), "w+") as outfile:
        pass
    logger.info("~~~ Script complete! ~~~")
