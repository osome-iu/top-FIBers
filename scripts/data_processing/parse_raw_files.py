"""
Purpose:
    On 2023-4-21 we decided to only use the Iffy News list of low-credibility domains
    that were marked as "low" or "very-low" in the MBFC "factual" category.
    This script is for parsing out tweets that had matched the "mixed" category domains.

    IMPORTANT NOTE:
    - THIS SCRIPT WAS NOT UTILIZED IN THE PIPELINE FOR THE PURPOSE DESCRIBED ABOVE.
      WE KEEP IT HERE ONLY IN CASE IT MIGHT BE USEFUL IN THE FUTURE. INSTEAD OF USING
      THE (potentially error prone) APPROACH OF PARSING THE POSTS, DATA WAS RE-DOWNLOADED
      FOR CT AND REPULLED FOR TWITTER. AS A RESULT, THIS SCRIPTS IS **NOT COMPLETE** AND
      STILL NEEDS TO BE UPDATED TO PARSE POSTS FOR CT. SHOULD YOU BE USING IT, PLEASE MAKE
      SURE TO TEST IT PROPERLY BEFORE USE.

Inputs:
    None. See below for hardcoded paths/files.

Outputs:
    New parsed files will be saved in the specified directory
"""
import argparse
import glob
import gzip
import json
import os

import pandas as pd

from urllib.parse import urlparse

from top_fibers_pkg.utils import load_lines
from top_fibers_pkg.data_model import Tweet_v1, FbIgPost


DOMAINS_DIR = "/home/data/apps/topfibers/repo/data/iffy_files"
RAW_DIR_OLD = "/home/data/apps/topfibers/repo/data/raw_old"
RAW_DIR_NEW = "/home/data/apps/topfibers/repo/data/raw"


def get_platform():
    """
    Load social media platform data to parse from command line argument.
    """
    parser = argparse.ArgumentParser(
        description="Parse the tweets/FB posts to match less Iffy News domains."
    )
    parser.add_argument(
        "-p",
        "--platform",
        choices=["twitter", "facebook"],
        help="Which social media platform to load data from. Options: ['twitter', 'facebook']",
    )

    args = parser.parse_args()
    return args.platform


def load_domains(domains_dir):
    """
    Loads the **latest** iffy list of domains given the proper directory path.
    Prepare them for matching by stripping extra text (e.g., "https://", "http://", "*")

    Parameters
    ------------
    - domains_dir (str) : path to the iffy_list directory

    Returns
    ------------
    - domains (list) : list of domains
    """
    all_domains_files = sorted(glob.glob(os.path.join(domains_dir, "*iffy_list.txt")))
    latest_domains_filepath = all_domains_files[-1]

    # Load domains to match in below query and clean up
    domains = load_lines(latest_domains_filepath)
    domains = [domain.replace("https://", "") for domain in domains]
    domains = [domain.replace("http://", "") for domain in domains]
    domains = [domain.replace("www.", "") for domain in domains]
    domains = [domain.rstrip("/*") for domain in domains]
    return domains


def get_base_domain(url):
    """
    Extract the base domain from a full URL

    Parameters
    ------------
    - url (str) : full URL

    Returns
    ------------
    - base_domain (str) : base domain
    """
    parsed_url = urlparse(url)
    domain_parts = parsed_url.netloc.split(".")
    base_domain = ".".join(domain_parts[-2:])
    return base_domain


def get_tweets(file_path, domains_set):
    """
    Extract tweets from `file_path` that contain the domains we want.

    Parameters
    ------------
    - file_path (str) : path to the raw tweet file
    - domains_set (set) : set of domains

    Yields
    ------------
    - tweet_dict (dict) : tweet dictionary
    """
    with gzip.open(file_path, "rb") as f:
        for line in f:
            tweet_dict = json.loads(line.decode())
            tweet = Tweet_v1(tweet_dict)

            if not tweet.is_valid():
                print("Skipping invalid tweet!!")
                print("-" * 50)
                print(tweet.post_object)
                print("-" * 50)
                continue

            # We want to check the retweeted status object if it has domains bc they are
            # the same for the original tweet and the retweet. We don't want to check
            # the quoted status object because they can contain different domains.
            if tweet.is_retweet:
                tweet = tweet.retweet_object

            # Get list of all tweet URL objects
            urls = tweet.get_value(["entities", "urls"])

            # Collect a set of domains
            base_domains = set()
            for u in urls:
                url = u.get("expanded_url", u.get("url"))
                base_domain = get_base_domain(url)
                base_domains.add(base_domain)

            if base_domains.issubset(domains_set):
                yield tweet_dict


if __name__ == "__main__":
    # Get platform from command-line flag
    platform = get_platform()

    # Load domains list
    domains_set = set(load_domains(DOMAINS_DIR))

    # Get a list of the full paths to each file that we want to parse through
    files_to_clean = glob.glob(
        os.path.join(os.path.join(RAW_DIR_OLD, platform), "*.gzip")
    )

    for file in sorted(files_to_clean):
        # Create the new output file path
        basename = os.path.basename(file)
        output_path = os.path.join(RAW_DIR_NEW, platform, basename.replace("_OLD", ""))

        with gzip.open(output_path, "wb") as f:
            if platform == "twitter":
                for tweet_dict in get_tweets(file, domains_set):
                    json_str = json.dumps(tweet_dict)
                    f.write(json_str.encode("utf-8"))
                    f.write(b"\n")
                print(f"\t- Completed: {basename}")

            elif platform == "facebook":
                if "fb_posts" not in file:
                    raise TypeError("")
                print("Facebook not developed yet. Passing and will do nothing.")
                pass

            # This shouldn't happen
            else:
                raise TypeError(
                    f"Platform must be 'twitter' or 'facebook'. Currently: {platform}"
                )

    print("-- Script complete. ---")
