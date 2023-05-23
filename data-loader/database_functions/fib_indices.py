"""
Purpose:
    Contains functions for adding data to the FIB INDICES table.

Inputs:
    None

Outputs:
    None

Authors: Pasan Kamburugamuwa & Matthew DeVerna
"""
from flask import Flask
from library import backend_util

app = Flask(__name__)


def add_fib_indices(user_id, report_id, fib_index, total_reshares, username, platform):
    """
    Add data to the FIB indices database table.

    Parameters
    -----------
    - user_id (str): a social media users unique identifying user id
    - report_id (str): the id for a specific report
    - fib_index (int/float): the FIB index for `user_id`
    - total_reshares (int): total reshares earned by `user_id` during this report period
    - username (str): the username of `user_id`
    - platform (str): the name of the platform (should be "twitter" or "facebook")

    Returns
    -----------
    result (dict): {'id': fib_index_id}
    """
    with backend_util.get_db_cursor() as cur:
        try:
            add_fib_index = (
                "INSERT INTO fib_indices "
                "(user_id, report_id, fib_index, total_reshares, username, platform) "
                "values (%s, %s, %s, %s, %s, %s) "
                "RETURNING user_id"
            )
            cur.execute(
                add_fib_index,
                (user_id, report_id, fib_index, total_reshares, username, platform),
            )
            if cur.rowcount > 0:
                fib_index_id = cur.fetchone()[0]
                result = {"id": fib_index_id}
            return result
        except Exception as ex:
            raise Exception(ex)
