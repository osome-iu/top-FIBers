#!/usr/bin/env python3
"""
Purpose:
    Calculate the FIB index for all users present in tweet files output by Moe's
    Tavern.

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
### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Load Packages ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import datetime
import glob
import gzip
import json
import os
import sys

from collections import defaultdict
from top_fibers_pkg.data_model import Tweet_v1
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
LOG_FNAME = "calc_twitter_fib_indices.log"
SCRIPT_PURPOSE = (
    "Return the FIB indices for all users present in the provided data "
    "as well as the posts sent by the worst misinformation spreaders."
)
MATCHING_STR = "*.jsonl.gzip"
SUCCESS_FNAME = "success.log"

# NOTE: Set the number of top ranked spreaders to select and which type
NUM_SPREADERS = 50
SPREADER_TYPE = "fib_index"  # Options: ["total_reshares", "fib_index"]


### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Set Functions ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def extract_data_from_files(data_files, earliest_date_tstamp):
    """
    Load tweet data into three dictionaries that include only the
    needed information: user IDs/screennames and retweet counts

    Parameters:
    -----------
    - data_files(list) : a list of paths to files
    - earliest_date_tstamp (timestamp) : the earliest date from which to consider
        data for calculating FIB indices

    Returns:
    -----------
    - tweetid_max_rts (dict) : {tweet_id_str : max number of retweets in data}
    - userid_tweetids (dict) : {userid_x : set([tweetids sent by userid_x])}
    - userid_username (dict) : {userid : username}
        NOTE: the username will be the last one encountered, which will also be
        the most recent.

    Exceptions:
    -----------
    - Exception, TypeError
    """
    if not isinstance(data_files, list):
        raise TypeError("`data_files` must be a list!")
    if not all(isinstance(path, str) for path in data_files):
        raise TypeError("All `data_files` must be a string!")

    tweetid_timestamp = dict()
    tweetid_url = dict()
    tweetid_max_rts = defaultdict(int)

    userid_tweetids = defaultdict(set)
    userid_username = dict()

    try:
        for file in data_files:
            logger.info(f"Loading tweets from file: {file} ...")
            with gzip.open(file, "rb") as f:
                for line in f:
                    tweet = Tweet_v1(json.loads(line.decode()))

                    if not tweet.is_valid():
                        logger.info("Skipping invalid tweet!!")
                        logger.info("-" * 50)
                        logger.info(tweet.post_object)
                        logger.info("-" * 50)
                        continue

                    timestamp_str = tweet.get_post_time(timestamp=True)
                    timestamp = datetime.datetime.fromtimestamp(
                        int(timestamp_str)
                    ).timestamp()
                    # Skip anything posted before the earliest date
                    if timestamp < earliest_date_tstamp:
                        continue

                    # Parse the base-level tweet
                    tweet_id = tweet.get_post_ID()
                    tweet_url = tweet.get_link_to_post()
                    user_id = tweet.get_user_ID()
                    username = tweet.get_user_handle()

                    rt_count = tweet.get_reshare_count()
                    prev_rt_val = tweetid_max_rts[tweet_id]
                    if prev_rt_val > rt_count:
                        rt_count = prev_rt_val

                    # Store the data
                    tweetid_timestamp[tweet_id] = timestamp_str
                    tweetid_max_rts[tweet_id] = rt_count
                    tweetid_url[tweet_id] = tweet_url
                    userid_tweetids[user_id].add(tweet_id)
                    userid_username[user_id] = username

                    # Handle retweets
                    if tweet.is_retweet:

                        # Only keep base retweet objs that occurred on or after the earliest date
                        timestamp_str = tweet.retweet_object.get_post_time(
                            timestamp=True
                        )
                        timestamp = datetime.datetime.fromtimestamp(
                            int(timestamp_str)
                        ).timestamp()
                        if timestamp >= earliest_date_tstamp:
                            tweet_id = tweet.retweet_object.get_post_ID()
                            tweet_url = tweet.retweet_object.get_link_to_post()
                            user_id = tweet.retweet_object.get_user_ID()
                            username = tweet.retweet_object.get_user_handle()

                            rt_count = tweet.retweet_object.get_reshare_count()
                            prev_rt_val = tweetid_max_rts[tweet_id]
                            if prev_rt_val > rt_count:
                                rt_count = prev_rt_val

                            # Store the data
                            tweetid_timestamp[tweet_id] = timestamp_str
                            tweetid_max_rts[tweet_id] = rt_count
                            tweetid_url[tweet_id] = tweet_url
                            userid_tweetids[user_id].add(tweet_id)
                            userid_username[user_id] = username

                    # Handle quotes
                    if tweet.is_quote:

                        # Only keep base quote objs that occurred on or after the earliest date
                        timestamp_str = tweet.quote_object.get_post_time(timestamp=True)
                        timestamp = datetime.datetime.fromtimestamp(
                            int(timestamp_str)
                        ).timestamp()
                        if timestamp >= earliest_date_tstamp:
                            tweet_id = tweet.quote_object.get_post_ID()
                            tweet_url = tweet.quote_object.get_link_to_post()
                            user_id = tweet.quote_object.get_user_ID()
                            username = tweet.quote_object.get_user_handle()

                            rt_count = tweet.quote_object.get_reshare_count()
                            prev_rt_val = tweetid_max_rts[tweet_id]
                            if prev_rt_val > rt_count:
                                rt_count = prev_rt_val

                            # Store the data
                            tweetid_timestamp[tweet_id] = timestamp_str
                            tweetid_max_rts[tweet_id] = rt_count
                            tweetid_url[tweet_id] = tweet_url
                            userid_tweetids[user_id].add(tweet_id)
                            userid_username[user_id] = username

        num_tweets = len(tweetid_max_rts.keys())
        num_users = len(userid_tweetids.keys())
        logger.info(f"Total Tweets Ingested = {num_tweets:,}")
        logger.info(f"Total Number of Users = {num_users:,}")

        return (
            dict(tweetid_max_rts),
            dict(userid_tweetids),
            userid_username,
            tweetid_timestamp,
            tweetid_url,
        )

    # Raise this error if something weird happens loading the data
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
    if output_dir is None:
        output_dir = "."

    # Retrieve all paths to data files
    logger.info("Data will be extracted from here:")
    logger.info(f"\t--> {data_dir}")
    data_files = sorted(glob.glob(os.path.join(data_dir, MATCHING_STR)))

    num_files = len(data_files)
    logger.info(f"Num. files to process: {num_files}")

    # Get the first date of
    earliest_date_tstamp = get_earliest_date(
        months_earlier=num_months, as_timestamp=True, month_calculated=month_calculated
    )

    # Wrangle data and calculate FIB indices
    (
        postid_num_reshares,
        userid_postids,
        userid_username,
        postid_timestamp,
        tweetid_url,
    ) = extract_data_from_files(data_files, earliest_date_tstamp)

    logger.info("Creating output dataframes...")
    userid_total_reshares = create_userid_total_reshares(
        postid_num_reshares, userid_postids
    )
    userid_reshare_lists = create_userid_reshare_lists(
        postid_num_reshares, userid_postids
    )
    fib_frame = create_fib_frame(
        userid_reshare_lists, userid_username, userid_total_reshares
    )

    logger.info("Top spreader information:")
    logger.info(f"\t- Num. spreaders to select   : {NUM_SPREADERS}")
    logger.info(f"\t- Type of spreaders to select: {SPREADER_TYPE}")
    top_spreaders = get_top_spreaders(fib_frame, NUM_SPREADERS, SPREADER_TYPE)
    top_spreader_df = create_top_spreader_df(
        top_spreaders,
        userid_postids,
        postid_num_reshares,
        postid_timestamp,
        tweetid_url,
    )

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
        outdir_with_month, f"{today}__fib_indices_twitter.parquet"
    )
    output_rt_fname = os.path.join(
        outdir_with_month, f"{today}__top_spreader_posts_twitter.parquet"
    )
    fib_frame.to_parquet(output_fib_fname, index=False, engine="pyarrow")
    top_spreader_df.to_parquet(output_rt_fname, index=False, engine="pyarrow")

    with open(os.path.join(REPO_ROOT, SUCCESS_FNAME), "w+") as outfile:
        pass
    logger.info("~~~ Script complete! ~~~")
