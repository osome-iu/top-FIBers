"""
Purpose:
    Extract the top 50 FIBers and save them to another directory for Zenodo uploads.

Inputs:
    None

Outputs:
    Files for Zenodo uploads that include the top 50 superspreaders for each platform.

Author: Matthew DeVerna
"""
import glob
import os
import sys

import pandas as pd

from top_fibers_pkg.utils import get_logger

REPO_ROOT = "/home/data/apps/topfibers/repo"
LOG_DIR = "./logs"
LOG_FNAME = "prep_zenodo_files.log"

# FIB DIR and GLOB_STRING are combined to find all FIB files under FIB_DIR via glob.glob()
FIB_DIR = "./data/derived/fib_results"
GLOB_STRING = "*/*/*fib_indices*.parquet"
OUTPUT_DIR = "./data/derived/zenodo_uploads"


def create_new_output_fname(file_path):
    """
    Take a file path and return a new file name formatted in the following way:
        - YYYY_mm__fib_indices_platform.csv

    Parameters:
    -----------
    - file_path (str): the original file path

    Returns:
    ----------
    - new_output_fname (str): the new file name
    """
    # paths look like:
    #  './twitter/2022_01/2023_05_06__fib_indices_twitter.parquet'
    #  './facebook/2022_01/2023_05_06__fib_indices_crowdtangle.parquet'
    split_file = file_path.split("/")
    platform, year_month = split_file[-3], split_file[-2]
    new_output_fname = os.path.join(
        OUTPUT_DIR,
        f"{year_month}__fib_indices_{platform}.csv",
    )
    return new_output_fname


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

    files = glob.glob(os.path.join(FIB_DIR, GLOB_STRING))
    logger.info(f"Found {len(files)} files.")

    logger.info(f"Saving files here: {OUTPUT_DIR}")
    for file in files:
        logger.info(f"Processing: {file}")
        df = pd.read_parquet(file)
        new_output_fname = create_new_output_fname(file)
        if os.path.exists(new_output_fname):
            logger.info("Skipping, file already exists:\n\t-", new_output_fname)
            continue
        logger.info(f"Creating: {new_output_fname}")
        df.head(50).to_csv(new_output_fname, index=False)

    logger.info("Script complete.")
