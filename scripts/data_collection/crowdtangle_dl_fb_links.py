"""
Purpose:
    Download Facebook posts from CrowdTangle based on a list of links.
    NOTE:
        - Requires a CrowdTangle API token
        - The data pulled is dictated by the constant variables at the top of the script.
        Particularly important are:
            - NUMBER_OF_MONTHS_TO_PULL
            - OFFSET

Inputs:
    Those loaded by top_fibers_pkg.utils.parse_cl_args_ct_dl

Outputs:
    One new-line delimited json.gz file with all posts from FB for the specified period.
    Output file name form:
        - start_date-end_date__candidate_fb_posts.json.gz
            - `start_date` and `end_date` take the following format: YYYY-MM-DD
            - Dates represent the date range of the downloaded data
            - NOTE: end_date is NOT inclusive. This means that a date range like
                2022-11-05-2022-11-06 indicates that the data was pulled for
                only 2022-11-05.

Author:
    Matthew R. DeVerna
"""
import datetime
import glob
import gzip
import json
import os
import time

from top_fibers_pkg.dates import get_start_and_end_dates
from top_fibers_pkg.crowdtangle_helpers import ct_get_search_posts
from top_fibers_pkg.utils import parse_cl_args_ct_dl, load_lines, get_logger

SCRIPT_PURPOSE = "Download Facebook posts from CrowdTangle based on a list of links."
REPO_ROOT = "/home/data/apps/topfibers/repo"
LOG_DIR = "./logs"
LOG_FNAME = "top_fibers_fb_link_dl.log"
SUCCESS_FNAME = "success.log"

NUMBER_OF_POSTS_PER_CALL = 10_000

# Number of seconds to wait before every query, regardless of success or error
WAIT_BTWN_POSTS = 8

# Base number of seconds to wait after encountering an error, raised to the number of try counts
WAIT_BTWN_ERROR_BASE = 2

# Maximum number of times to retry (*consecutive* failures) for a single domain
MAX_ATTEMPTS = 5

# Maximum number of times to retry (*consecutive* failures) for domains that return no posts
MAX_EMPTY_ATTEMPTS = 2

if __name__ == "__main__":
    script_name = os.path.basename(__file__)
    logger = get_logger(LOG_DIR, LOG_FNAME, script_name=script_name)
    logger.info("-" * 50)
    logger.info(f"Begin script: {__file__}")

    args = parse_cl_args_ct_dl(SCRIPT_PURPOSE, logger)
    domains_dir = args.domains_dir  # Includes one domain on each line
    output_dir = args.out_dir
    last_month = args.last_month
    num_months = int(args.num_months)

    logger.info(f"Domains dir: {domains_dir}")
    all_domains_files = sorted(glob.glob(os.path.join(domains_dir, "*iffy_list.txt")))
    latest_domains_filepath = all_domains_files[-1]
    logger.info(f"Domains file: {latest_domains_filepath}")

    # Load domains to match in below query and clean up
    domains = load_lines(latest_domains_filepath)
    domains = [domain.replace("https://", "") for domain in domains]
    domains = [domain.replace("http://", "") for domain in domains]
    domains = [domain.replace("www.", "") for domain in domains]
    domains = [domain.rstrip("/*") for domain in domains]

    # Load CrowdTangle token
    ct_token = os.environ.get("TOP_FIBERS_TOKEN")
    if ct_token is None:
        msg = (
            "Crowdtangle API token not set as an environment variable. "
            "Run: <export TOP_FIBERS_TOKEN='INSERT_TOKEN_HERE'> and try again."
        )
        logger.error(msg)
        raise ValueError(msg)

    # Set start and end dates
    start_date, end_date = get_start_and_end_dates(num_months, last_month)
    current_time = datetime.datetime.now()
    if end_date > current_time:
        msg = (
            "`end_date` cannot be after `current_time`!\n"
            f"\t end_date     : {end_date}\n"
            f"\t current_time : {current_time}\n"
        )
        logger.error(msg)
        raise ValueError(msg)

    logger.info(f"Start date  : {start_date}")
    logger.info(f"End date    : {end_date}")

    # Create output filename with time period
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    output_file_path = os.path.join(
        output_dir, f"{start_str}--{end_str}__fb_posts_w_links.jsonl.gzip"
    )

    logger.info(f"Output file : {output_file_path}")

    # Open file here so we don't have to hold data in memory
    with gzip.open(output_file_path, "wb") as f:
        # Iterate through each site
        for idx, domain in enumerate(domains, start=1):
            logger.info(
                f"Collect posts matching domain {idx} of {len(domains)}: {domain}"
            )

            total_posts = 0
            try_count = 0
            query_count = 0
            max_attempts = MAX_ATTEMPTS
            zero_post_count = 0
            max_empty_attempts = MAX_EMPTY_ATTEMPTS

            start = start_date
            end = end_date
            while True:
                try:
                    # count = 10000 only if you request it, otherwise it's 100
                    # NOTE: This is more than the function says is allowed because we requested
                    # increased API limits from CrowdTangle folks.
                    time.sleep(WAIT_BTWN_POSTS)
                    response = ct_get_search_posts(
                        count=NUMBER_OF_POSTS_PER_CALL,
                        start_time=start,
                        end_time=end,
                        include_history=None,
                        sort_by="date",
                        types=None,
                        search_term=domain,
                        account_types=None,
                        min_interactions=0,
                        offset=0,
                        api_token=ct_token,
                        platforms="facebook",
                        lang=None,
                    )
                    response_json = response.json()

                    # Returns a list of dictionaries where each dict represents one post.
                    # We sort by `date` so the MOST RECENT post will be at the first index.
                    posts = response_json["result"]["posts"]

                    # If we get no results, we try a few more times and then break the loop
                    num_posts = len(posts)

                except Exception as e:  # 6 calls/minute limit if you request them
                    logger.exception(e)
                    try:
                        logger.info(f"FB message: {response_json['message']}")
                    except:
                        pass

                    # Handle the retries...
                    try_count += 1
                    logger.info(f"There are {max_attempts-try_count} tries left.")
                    if (max_attempts - try_count) <= 0:
                        logger.info("Breaking out of loop!")
                        break
                    else:
                        wait_time = WAIT_BTWN_ERROR_BASE**try_count
                        if wait_time > 60:
                            wait_time = wait_time / 60
                            logger.info(f"Waiting {wait_time} minutes...")
                        else:
                            logger.info(f"Waiting {wait_time} seconds...")
                        time.sleep(wait_time)
                        logger.info(f"Retrying...")
                        continue

                else:
                    # Returned CT results successfully, with zero posts
                    if num_posts == 0:
                        logger.info("Zero posts were returned.")
                        try_count += 1
                        zero_post_count += 1
                        logger.info(
                            f"Empty retries remaining: {max_empty_attempts-try_count}"
                        )
                        logger.info(
                            f"Total retries remaining: {max_attempts-try_count}"
                        )
                        if zero_post_count >= MAX_EMPTY_ATTEMPTS:
                            logger.info(
                                "Two consecutive queries with no posts. "
                                "Breaking out of loop!"
                            )
                            break

                        elif (max_attempts - try_count) <= 0:
                            logger.info("Breaking out of loop!")
                            break
                        else:
                            wait_time = WAIT_BTWN_ERROR_BASE**try_count
                            if wait_time > 60:
                                wait_time = wait_time / 60
                                logger.info(f"Waiting {wait_time} minutes...")
                            else:
                                logger.info(f"Waiting {wait_time} seconds...")
                            time.sleep(wait_time)
                            logger.info(f"Retrying...")
                            continue

                    # Returned CT results successfully, with new posts
                    else:
                        # Reset the retry count to zero
                        try_count = 0
                        zero_post_count = 0

                        most_recent_date_str = posts[0]["date"]
                        oldest_date_str = posts[-1]["date"]
                        logger.info(
                            f"\t|--> {oldest_date_str} - {most_recent_date_str}"
                            f": {num_posts:,} posts."
                        )

                        # Convert each post into bytes with a new-line (`\n`)
                        for post in posts:
                            post_in_bytes = f"{json.dumps(post)}\n".encode(
                                encoding="utf-8"
                            )
                            f.write(post_in_bytes)

                        total_posts += num_posts
                        logger.info(f"Total posts collected: {total_posts:,}")

                        # Update the time period we're searching.
                        # ---------------------------------------
                        # Facebook returns data in backwards order, meaning more recent posts are
                        # provided first. If we do not have all data it means that we are missing
                        # OLDER data. So we update the `end` time period (which is the most recent
                        # time parameter) with the oldest/earliest post we find and ensure we do
                        # not pull the same data twice by subtracting by one second to make sure
                        # there is no overlap.
                        # ---------------------------------------
                        oldest_date_dt = datetime.datetime.strptime(
                            oldest_date_str, "%Y-%m-%d %H:%M:%S"
                        )
                        oldest_date_dt = oldest_date_dt - datetime.timedelta(seconds=1)

                    # If this is true, we have a bad query. (start_date is a date object)
                    empty_time = datetime.time(0, 0, 0)
                    if oldest_date_dt <= datetime.datetime.combine(
                        start_date, empty_time
                    ):
                        logger.info(f"\t|--> end <= start so we have all data.")
                        break

                    # More than 500 queries (~5M posts), we break the script.
                    query_count += 1
                    if query_count > 500:
                        break

                    # If all conditionals are passed, we update the date string for query
                    end = oldest_date_dt.strftime("%Y-%m-%dT%H:%M:%S")
                    logger.info(f"\t|--> New end date: {end}")
                    logger.info(f"\t|--> {'-'*50}")
    with open(os.path.join(REPO_ROOT, SUCCESS_FNAME), "w+") as outfile:
        pass
    logger.info("~~~ Script complete! ~~~")
