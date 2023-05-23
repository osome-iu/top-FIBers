"""
Purpose:
    The main python script that sends (i.e., "serves") data to the database.
    By default, the script will send data for the current month. If you would like
    to send older data, you must manually update the MONTHS variable with
    the dates you would like to update. See comments below for examples.

Inputs:
    None

Output:
    None

Author: Pasan Kamburugamuwa & Matthew DeVerna
"""
import datetime
import os

# Only used if updating all months
import pandas as pd

from app import controller
from top_fibers_pkg.utils import get_logger

facebook_data_path = "/home/data/apps/topfibers/repo/data/derived/fib_results/facebook"
twitter_data_path = "/home/data/apps/topfibers/repo/data/derived/fib_results/twitter"
twitter_profile_pic_file_path = "/home/data/apps/topfibers/repo/data/derived/twitter_profile_links/top_fiber_profile_image_links.parquet"
PLATFORMS = ["Facebook", "Twitter"]
LOG_DIR = "/home/data/apps/topfibers/repo/logs"
LOG_FNAME = "database_server.log"

### Use commented out only one of the MONTHS rows to dicate the months for which
### data is sent to the database. Default = current month
# --- All months ---
# MONTHS = [dt.strftime("%Y_%m") for dt in pd.date_range("2022-01", "2023-01", freq="MS")]
# --- Specific months ---
#MONTHS = ["2023_01","2023_02","2023_03","2023_04","2023_05"]
# --- Current month (should be the default) ---
MONTHS = [datetime.datetime.now().strftime("%Y_%m")]

def update_database():
    """
    Update the Top FIBers database with the controller.add_data() function.
    """
    logger.info("Begin load past month data")
    for selected_month in MONTHS:
        for platform in PLATFORMS:
            read_dir = (
                facebook_data_path if platform == "Facebook" else twitter_data_path
            )
            try:
                # Read data for the specified month
                logger.info(f"Adding data to database for platform: {platform}, month: {selected_month}...")
                controller.add_data(read_dir, platform, selected_month)

                # Add data to profile link table
                if platform == 'Twitter':
                    logger.info(f"Adding data to profile_link table...")
                    controller.add_profile_pic_links(twitter_profile_pic_file_path, platform)
                logger.info("Success.")
                logger.info("-" * 50)

            except Exception as e:
                logger.exception(f"Problem sending data!")
                raise Exception(e)


if __name__ == "__main__":
    script_name = os.path.basename(__file__)
    logger = get_logger(LOG_DIR, LOG_FNAME, script_name=script_name, also_print=True)
    logger.info("-" * 50)
    logger.info(f"Begin script: {__file__}")

    update_database()

    logger.info(f"Successfully updated database.")
    logger.info("-" * 50)
