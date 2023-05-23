# data_processing

Scripts that process the raw data in any way should be saved here.

### Pipeline Scripts
These scripts are utilized in the monthly pipeline that updates the website each month
- `calc_crowdtangle_fib_indices.py` : creates two output files based on the FACEBOOK posts data for a given time period
    - A file containing the top 50 FIBers
    - A file containing all of their posts
- `calc_twitter_fib_indices.py` : creates two output files based on the TWITTER posts data for a given time period
    - A file containing the top 50 FIBers
    - A file containing all of their posts
- `count_num_posts.py` : count the number of posts that we have in all raw files contained in the data directory provided

### Pipeline Scripts
These scripts are for data processing outside of the scheduled pipeline
- `calc_fib_all.sh` : runs the above `calc_{platform}_fib_indices.py` scripts for all time periods (platform indicated as command-line input)
- `parse_raw_files.py` : an unfinished script (not used in the pipeline or for anything else, currently) for parsing out tweets that had matched the "mixed" category domains. MAKE SURE TO READ THIS SCRIPTS DOCSTRING PRIOR TO USING.