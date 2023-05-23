"""
Purpose:
    Contains functions used to add data to POSTS table.

Inputs:
    None

Outputs:
    None

Authors: Pasan Kamburugamuwa & Matthew DeVerna
"""
from flask import Flask
from library import backend_util

app = Flask(__name__)


def add_posts(post_id, user_id, platform, timestamp, url):
    """
    Add data to the posts database table.

    Parameters
    -----------
    - post_id (str): the id of a social media post
    - user_id (str): the id for the user who sent `post_id`
    - platform (str): the name of the platform (should be "twitter" or "facebook")
    - timestamp (str): second level epoch timestamp indicating when `post_id` was sent
    - url (str): the full url for `post_id`

    Returns
    -----------
    result (dict): {'post_id': post_fetch_id}
    """
    with backend_util.get_db_cursor() as cur:
        try:
            add_post = (
                "INSERT INTO posts "
                "(post_id, user_id, platform, timestamp, url) "
                "values (%s,%s, %s, %s, %s) "
                "RETURNING post_id"
            )
            cur.execute(add_post, (post_id, user_id, platform, timestamp, url))
            if cur.rowcount > 0:
                post_fetch_id = cur.fetchone()[0]
                result = {"post_id": post_fetch_id}
            return result
        except Exception as ex:
            raise Exception(ex)

