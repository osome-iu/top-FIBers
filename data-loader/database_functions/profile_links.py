"""
Purpose:
    Contains functions used to add data to profile_links for each reading.
    This will read the table in every month and if there is change detect, then the new data will be uploaded to table..
Inputs:
    None

Outputs:
    None

Authors: Pasan Kamburugamuwa & Matthew DeVerna
"""
from library import backend_util

def add_profile_links(user_id, platform, profile_image_url):
    """
    Add data to the profile links database table.

    Parameters
    -----------
    - user_id (str): the id of the user. This will be facebook or twitter.
    - platform (str): which platform -> facebook, twitter
    - profile_image_url (str): url of the profile picture.

    Returns
    -----------
    result (dict): {'user_id': profile_profile_pic_fetch_id}
    """
    with backend_util.get_db_cursor() as cur:
        try:
            add_profile_links = (
                "INSERT INTO profile_links "
                "(user_id, platform, profile_image_url) "
                "values (%s,%s,%s) "
                "RETURNING user_id"
            )
            cur.execute(add_profile_links, (user_id, platform, profile_image_url))
            if cur.rowcount > 0:
                profile_profile_pic_fetch_id = cur.fetchone()[0]
                result = {"user_id": profile_profile_pic_fetch_id}
            return result
        except Exception as ex:
            raise Exception(ex)


def get_all_profile_links():
    """
    Add function gets all the available profile pictures in database data table profile_links

    Parameters
    -----------
     -None

    Returns
    -----------
    result (array): user_ids
    """
    with backend_util.get_db_cursor() as cur:
        select_query = ("SELECT user_id FROM profile_links")
        cur.execute(select_query)
        if cur.rowcount > 0:
            # Fetch all rows and extract user IDs into a list
            user_ids = [row[0] for row in cur.fetchall()]
            # Return the list of user IDs
            return user_ids
