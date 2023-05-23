"""
Some simple utility functions used throughout the project.
"""
import argparse
import logging
import os
import sys


def parse_cl_args_symlinks(script_purpose="", logger=None):
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
    msg = (
        "Data directory that contains the raw data files for a specific platform. "
        "E.g.: /home/data/apps/topfibers/repo/data/raw/twitter"
    )
    parser.add_argument(
        "-d",
        "--data",
        metavar="Data Directory",
        help=msg,
        required=True,
    )
    msg = (
        "Full path to the output directory. "
        "This should be the symbolics link directory followed by the platform. "
        "E.g.: dir/to/symbolic_links/facebook. "
        "The input for --month-calculated will be created as a subdirectory of "
        "this directory and data will be saved there."
    )
    parser.add_argument(
        "-o",
        "--out-dir",
        metavar="Output dir",
        help=msg,
        required=True,
    )
    parser.add_argument(
        "-m",
        "--month-calculated",
        metavar="Month calculated",
        help="The month for which you'd like to calculate FIB indices (YYYY_MM)",
        required=True,
    )
    parser.add_argument(
        "-n",
        "--num-months",
        metavar="Number of months considered",
        help="The number of months to consider (e.g., input 3 to create symbolic links for three months)",
        required=True,
    )

    # Read parsed arguments from the command line into "args"
    args = parser.parse_args()

    return args


def parse_cl_args_fib(script_purpose="", logger=None):
    """
    Read command line arguments for the following scripts.
        - top-fibers/scripts/calc_crowdtangle_fib_indices.py
        - top-fibers/scripts/calc_twitter_fib_indices.py

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
    msg = (
        "Full path to the directory containing symbolic links to data files "
        "used in FIB calculations. E.g.: /home/data/apps/topfibers/repo/data/symbolic_links/twitter/2022_02"
    )
    parser.add_argument(
        "-d",
        "--data-dir",
        metavar="Data Directory",
        help=msg,
        required=True,
    )
    msg = (
        "Full path to the output directory. "
        "The input for --month-calculated will be created as a subdirectory of "
        "this directory and data will be saved there. "
        "E.g.: /home/data/apps/topfibers/repo/data/derived/fib_results/twitter"
    )
    parser.add_argument(
        "-o",
        "--out-dir",
        metavar="Output Directory",
        help=msg,
        required=True,
    )
    parser.add_argument(
        "-m",
        "--month-calculated",
        metavar="Month calculated",
        help="The month for which you'd like to calculate FIB indices (YYYY_MM)",
        required=True,
    )
    parser.add_argument(
        "-n",
        "--num-months",
        metavar="Number of months considered",
        help="The number of months to consider (e.g., input 3 to consider three months)",
        required=True,
    )

    # Read parsed arguments from the command line into "args"
    args = parser.parse_args()

    return args


def parse_cl_args_ct_dl(script_purpose="", logger=None):
    """
    Read command line arguments for the script that downloads Facebook posts from
    CrowdTangle.
        - top-fibers/data_collection/crowdtangle_dl_fb_links.py

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
    msg = (
        "Full path to a DIRECTORY of iffy domains files. One domain per line. "
        "Posts will be downloaded that include at least one of these domains."
    )
    parser.add_argument(
        "-d",
        "--domains-dir",
        metavar="Domains dir",
        help=msg,
        required=True,
    )
    msg = (
        "Directory where you'd like to save the output data. E.g.: "
        "/home/data/apps/topfibers/repo/data/raw/facebook"
    )
    parser.add_argument(
        "-o",
        "--out-dir",
        metavar="Output dir",
        help=msg,
        required=True,
    )
    parser.add_argument(
        "-l",
        "--last-month",
        metavar="Last month",
        help="The last month from which you'd like to download facebook posts. (YYYY_MM)",
        required=True,
    )
    parser.add_argument(
        "-n",
        "--num-months",
        metavar="Number of months",
        help="The number of months that you'd like to download (works backwards from --last-month)",
        required=True,
    )

    # Read parsed arguments from the command line into "args"
    args = parser.parse_args()

    return args


def load_lines(file_path):
    """
    Load the lines of a file into a list, stripping unwanted content from the
    rightside of each line (e.g., \n, tabs, etc.)

    Parameters:
    ------------
    - file_path (str): path to the file you want to load

    Returns:
    ------------
    - lines (list[str]): list where each item represents one line from `file_path`
        with newline characters, tabs, and other unwanted content removed.

    Exception:
    ------------
    TypeError
    """
    if not isinstance(file_path, str):
        raise TypeError("file_path must be a string")

    with open(file_path, "r") as f:
        return [line.rstrip() for line in f]


def get_logger(log_dir, log_fname, script_name=None, also_print=False):
    """Create logger."""

    # Create log_dir if it doesn't exist already
    try:
        os.makedirs(f"{log_dir}")
    except:
        pass

    # Create logger and set level
    logger = logging.getLogger(script_name)
    logger.setLevel(level=logging.INFO)

    # Configure file handler
    formatter = logging.Formatter(
        fmt="%(asctime)s-%(name)s-%(levelname)s - %(message)s",
        datefmt="%Y-%m-%d_%H:%M:%S",
    )
    full_log_path = os.path.join(log_dir, log_fname)
    fh = logging.FileHandler(f"{full_log_path}")
    fh.setFormatter(formatter)
    fh.setLevel(level=logging.INFO)
    # Add handlers to logger
    logger.addHandler(fh)

    # If also_print is true, the logger will also print the output to the
    # console in addition to sending it to the log file
    if also_print:
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)
        ch.setLevel(level=logging.INFO)
        logger.addHandler(ch)

    return logger
