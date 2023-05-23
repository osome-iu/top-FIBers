# data_collection

Scripts that collect data should be saved here

### Pipeline Scripts
These scripts are utilized in the monthly pipeline that updates the website each month
- `crowdtangle_dl_fb_links.py` : Download low-credibiliy Facebook posts for a specific time period using Crowdtangle
- `iffy_update.py`: Download the latest iffy list
- `iffy_get_data.sh`: Retrieve past month's twitter contents related to the iffy list

### Other Scripts
These scripts are for downloading CT data for multiple months outside of the scheduled pipeline
- `collect_ct_data_for_all_months.sh` : download all Facebook data for each month between 2022-01-01 and the previous month, relative to when the script is executed
- `collect_ct_data_specific_months.sh` : download Facebook data for specific months listed in the script