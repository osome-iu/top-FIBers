#!/usr/bin/env python3
"""
Purpose:
    Calculate the FIB index for all users present in CrowdTangle files downloaded
    with INSERT DOWNLOAD SCRIPT NAME HERE.

Inputs:
    - Those loaded by top_fibers_pkg.utils.parse_cl_args_fib

    NOTE:
    - Call the calc_crowdtangle_fib_indices.py -h flag to get input/flag details.
    - Input files contain Facebook posts.


Output:
    Two .parquet files containing:
    1. {YYYY_mm_dd}__fib_indices_crowdtangle.parquet: a pandas dataframe with the following columns:
        - user_id (str) : a unique Facebook user ID
        - fib_index (int) : a specific user's FIB index
        - total_reshares (int) : total number of reshares earned by user_id
    2. {YYYY_mm_dd}__top_spreader_posts_crowdtangle.parquet: a pandas dataframe with the following columns:
        - user_id (str) : a unique Facebook user ID
        - post_id (str) : a unique Facebook post ID
        - num_reshares (int) : the number of times post_id was reshared
        - timestamp (str) : timestamp when post was sent

    NOTE: YYYY_mm_dd will be representative of the machine's current date

What is the FIB-index?
    Please see our working paper for details.
    - https://arxiv.org/abs/2207.09524

Author: Matthew DeVerna
"""
### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Load Packages ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import datetime
import glob
import gzip
import json
import os
import sys

from collections import defaultdict
from top_fibers_pkg.data_model import FbIgPost
from top_fibers_pkg.dates import get_earliest_date
from top_fibers_pkg.utils import parse_cl_args_fib, get_logger
from top_fibers_pkg.fib_helpers import (
    create_userid_total_reshares,
    create_userid_reshare_lists,
    create_fib_frame,
    get_top_spreaders,
    create_top_spreader_df,
)

REPO_ROOT = "/home/data/apps/topfibers/repo"
LOG_DIR = "./logs"
LOG_FNAME = "calc_facebook_fib_indices.log"
SCRIPT_PURPOSE = (
    "Return the FIB indices for all users present in the provided data "
    "as well as the posts sent by the worst misinformation spreaders."
)
MATCHING_STR = "*.jsonl.gzip"
SUCCESS_FNAME = "success.log"

# NOTE: Set the number of top ranked spreaders to select and which type
NUM_SPREADERS = 50
SPREADER_TYPE = "fib_index"  # Options: ["total_reshares", "fib_index"]

# Set the number of months to calculate the FIB index from
NUM_MONTHS = 3

### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Set Functions ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def extract_data_from_files(data_files, earliest_date_tstamp):
    """
    Extract necessary data from the list of input files.

    Parameters:
    -----------
    - data_files (list) : list of full paths to data files to parse
    - earliest_date_tstamp (timestamp) : the earliest date from which to consider
        data for calculating FIB indices

    Returns:
    -----------
    - userid_username (dict) : maps user IDs to usernames
    - userid_postids (dict) : maps user IDs to a set of (str) post IDs
    - postid_timestamp (dict) : maps post IDs to (str) timestamps
    - postid_num_reshares (dict) : maps post IDs to number of reshares (int)
    - postid_url (dict) : maps post IDs to (str) post URLs

    Exceptions:
    -----------
    TypeError
    """
    if not isinstance(data_files, list):
        raise TypeError("`data_files` must be a list!")

    # Initialize data objects to populate
    userid_username = dict()
    userid_postids = defaultdict(set)

    postid_timestamp = dict()
    postid_url = dict()
    postid_num_reshares = defaultdict(int)

    logger.info("Begin extracting data.")
    try:
        for file in data_files:
            logger.info(f"\t- Processing: {os.path.basename(file)} ...")
            with gzip.open(file, "rb") as f:
                for line in f:
                    post_obj = FbIgPost(json.loads(line.decode()))
                    if not post_obj.is_valid():
                        continue

                    post_id = post_obj.get_post_ID()
                    post_url = post_obj.get_link_to_post()
                    timestamp_str = post_obj.get_post_time(timestamp=True)
                    timestamp = datetime.datetime.fromtimestamp(
                        int(timestamp_str)
                    ).timestamp()
                    # Skip anything posted before the earliest date
                    if timestamp < earliest_date_tstamp:
                        continue
                    user_id = post_obj.get_user_ID()
                    username = post_obj.get_user_handle()

                    # This handles certain types of accounts like groups and pages that
                    # do not have "handles" (or don't provide one) but instead have "names"
                    if username in [None, ""]:
                        username = post_obj.get_account_name()
                    reshare_count = post_obj.get_reshare_count()
                    if reshare_count is None:
                        reshare_count = 0

                    postid_num_reshares[post_id] = reshare_count
                    postid_timestamp[post_id] = timestamp_str
                    postid_url[post_id] = post_url
                    userid_username[user_id] = username
                    userid_postids[user_id].add(post_id)

        num_posts = len(postid_num_reshares.keys())
        num_users = len(userid_username.keys())
        logger.info(f"Total Posts Ingested = {num_posts:,}")
        logger.info(f"Total Number of Users = {num_users:,}")

        return (
            userid_username,
            dict(userid_postids),
            postid_timestamp,
            dict(postid_num_reshares),
            postid_url,
        )

    except Exception as e:
        logger.exception(f"Problem parsing data file: {file}")
        raise Exception(e)


# Execute the program
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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

    # Parse input flags
    args = parse_cl_args_fib(SCRIPT_PURPOSE, logger)
    data_dir = args.data_dir
    output_dir = args.out_dir
    month_calculated = args.month_calculated
    num_months = int(args.num_months)

    # Retrieve all paths to data files
    logger.info("Data will be extracted from here:")
    logger.info(f"\t- {data_dir}")
    data_files = sorted(glob.glob(os.path.join(data_dir, MATCHING_STR)))

    num_files = len(data_files)
    logger.info(f"Num. files to process: {num_files}")

    # Get the first date of
    earliest_date_tstamp = get_earliest_date(
        months_earlier=num_months, as_timestamp=True, month_calculated=month_calculated
    )

    # Wrangle data and calculate FIB indices
    (
        userid_username,
        userid_postids,
        postid_timestamp,
        postid_num_reshares,
        postid_url,
    ) = extract_data_from_files(data_files, earliest_date_tstamp)

    logger.info("Creating output dataframes...")
    try:
        userid_total_reshares = create_userid_total_reshares(
            postid_num_reshares, userid_postids
        )
        userid_reshare_lists = create_userid_reshare_lists(
            postid_num_reshares, userid_postids
        )
    except Exception as e:
        logger.exception(f"Problem creating secondary lookup maps!")
        raise Exception(e)

    try:
        fib_frame = create_fib_frame(
            userid_reshare_lists, userid_username, userid_total_reshares
        )
    except Exception as e:
        logger.exception(f"Problem creating FIB frame!")
        raise Exception(e)

    logger.info("Top spreader information:")
    logger.info(f"\t- Num. spreaders to select   : {NUM_SPREADERS}")
    logger.info(f"\t- Type of spreaders to select: {SPREADER_TYPE}")
    try:
        top_spreaders = get_top_spreaders(fib_frame, NUM_SPREADERS, SPREADER_TYPE)
        top_spreader_df = create_top_spreader_df(
            top_spreaders,
            userid_postids,
            postid_num_reshares,
            postid_timestamp,
            postid_url,
        )
    except Exception as e:
        logger.exception(f"Problem creating top spreaders df")
        raise Exception(e)

    fib_frame = fib_frame.sort_values("fib_index", ascending=False).reset_index(
        drop=True
    )
    top_spreader_df = top_spreader_df.sort_values(
        "num_reshares", ascending=False
    ).reset_index(drop=True)

    # Save files
    outdir_with_month = os.path.join(output_dir, month_calculated)
    logger.info("Saving data here:")
    logger.info(f"\t- {outdir_with_month}")
    if not os.path.exists(outdir_with_month):
        os.makedirs(outdir_with_month)
    today = datetime.datetime.now().strftime("%Y_%m_%d")
    output_fib_fname = os.path.join(
        outdir_with_month, f"{today}__fib_indices_crowdtangle.parquet"
    )
    output_rt_fname = os.path.join(
        outdir_with_month, f"{today}__top_spreader_posts_crowdtangle.parquet"
    )
    fib_frame.to_parquet(output_fib_fname, index=False, engine="pyarrow")
    top_spreader_df.to_parquet(output_rt_fname, index=False, engine="pyarrow")

    with open(os.path.join(REPO_ROOT, SUCCESS_FNAME), "w+") as outfile:
        pass
    logger.info("~~~ Script complete! ~~~")
