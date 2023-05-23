"""
Purpose:
    A script to count the number of posts that we have in all raw files contained in
    the data directory provided.

    Note: Previously counted files are skipped.

Inputs:
    -o / --output-dir: Full path to the output directory where you'd like to save post counts
    -d / --data-dir: Full path to the raw posts directory
    -p / --platform: The platform posts you want to count

Outputs:
    File saved in the `output_dir`. Filenames created based on `platform`:
        - {platform}_post_counts_by_file.parquet

Author: Matthew DeVerna
"""
import argparse
import datetime
import glob
import gzip
import os
import sys

from top_fibers_pkg.utils import get_logger

import pandas as pd

SCRIPT_PURPOSE = (
    "Count the number of posts that we have in all "
    "raw files contained in the data dir provided. "
    "Previously counted files are skipped."
)
REPO_ROOT = "/home/data/apps/topfibers/repo"
LOG_DIR = "./logs"
LOG_FNAME = "post_count.log"
SUCCESS_FNAME = "success.log"


def parse_cl_args(script_purpose="", logger=None):
    """
    Read command line arguments.

    Parameters:
    --------------
    - script_purpose (str) : Purpose of the script being utilized. When printing
        script help message via `python script.py -h`, this will represent the
        script's description. Default = "" (an empty string)
    - logger : a logging object

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

    help_msg = (
        "Full path to the output directory where you'd like to save post counts. "
        "Ex: /home/data/apps/topfibers/repo/data/derived/post_counts"
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        metavar="Output directory",
        help=help_msg,
        required=True,
    )
    help_msg = (
        "Full path to the raw posts directory (subdirs should be 'twitter' and 'facebook'). "
        "Ex: /home/data/apps/topfibers/repo/data/raw"
    )
    parser.add_argument(
        "-d",
        "--data-dir",
        metavar="Data dir",
        help=help_msg,
        required=True,
    )
    parser.add_argument(
        "-p",
        "--platform",
        metavar="Platform",
        help="The platform posts you want to count. Options: [twitter, facebook]",
        choices=["twitter", "facebook"],
        required=True,
    )

    # Read parsed arguments from the command line into "args"
    args = parser.parse_args()

    return args


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
    output_dir = args.output_dir
    data_dir = args.data_dir
    platform = args.platform

    logger.info(f"Counting posts for: {platform}")

    # Load output file if it exists
    output_filepath = os.path.join(
        output_dir, f"{platform}_post_counts_by_file.parquet"
    )
    previously_counted_files = None
    if os.path.exists(output_filepath):
        existing_counts_df = pd.read_parquet(output_filepath)
        previously_counted_files = set(existing_counts_df["file_name"])

    # Get all raw files full paths
    raw_files_dir = os.path.join(data_dir, platform)
    logger.info(f"Counting files found here: {raw_files_dir}")
    files = sorted(glob.glob(os.path.join(raw_files_dir, "*.jsonl.gzip")))
    num_files = len(files)
    logger.info(f"Number of files: {num_files}")

    data = []
    for fnum, file in enumerate(files, start=1):
        logger.info(f"Working on file ({fnum}/{num_files}): {file}")
        if previously_counted_files is not None and file in previously_counted_files:
            logger.info("Skipping file because it has already been counted.")
            continue

        with gzip.open(file, "rb") as f:
            data.append({"file_name": file, "num_posts": sum(1 for post in f)})

    logger.info("Creating counts dataframe...")
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    counts_df = pd.DataFrame.from_records(data)
    counts_df["date_counted"] = today

    if previously_counted_files is not None:
        logger.info("Merging existing counts with new counts...")
        counts_df = pd.concat([existing_counts_df, counts_df])

    logger.info(f"Saving counts dataframe here: {output_filepath} ...")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    counts_df.to_parquet(output_filepath, index=False, engine="pyarrow")
    with open(os.path.join(REPO_ROOT, SUCCESS_FNAME), "w+") as outfile:
        pass
    logger.info("~~~ Script complete! ~~~")
