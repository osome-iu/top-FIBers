"""
A collection of functions that are utilized in the calc_fib_indices.py script.
"""
import pandas as pd

from collections import defaultdict, Counter


def calc_fib_index(rt_counts):
    """
    Calculate a user's FIB-index based a list of the retweet counts they earned.

    Parameters:
    -----------
    - rt_counts (list) : list of retweet count values for retweets sent by a user

    Return:
    -----------
    - fib_position (int) : a user's FIB index

    Errors:
    -----------
    - TypeError
    """
    if not isinstance(rt_counts, list):
        raise TypeError("`rt_counts` must be a list!")

    rt_counts.sort()
    for fib_position in range(1, len(rt_counts) + 1)[::-1]:
        if rt_counts[-fib_position] >= fib_position:
            return fib_position

    # If the above criteria is never met, we return the fib_position as zero
    fib_position = 0
    return fib_position


def create_userid_total_reshares(postid_num_reshares, userid_postids):
    """
    Create a dictionary mapping userIDs to the total number of reshares
    that they earned across all posts.

    Parameters:
    -----------
    - postid_num_reshares (dict) : {post_id_str : number of reshares in data}
    - userid_postids (dict) : {userid_x : set([postids sent by userid_x])}

    Returns:
    -----------
    - userid_total_reshares (dict) : {userid_x : total rts earned by userid_x}

    Exceptions:
    -----------
    - Exception, TypeError
    """
    if not isinstance(postid_num_reshares, dict):
        raise TypeError("`postid_num_reshares` must be a dict!")
    if not isinstance(userid_postids, dict):
        raise TypeError("`userid_postids` must be a dict!")

    try:
        userid_total_reshares = Counter()
        for userid, postids in userid_postids.items():
            for post_id in postids:
                num_reshares = postid_num_reshares[post_id]
                userid_total_reshares[userid] += num_reshares
        return dict(userid_total_reshares)

    except Exception as e:
        raise Exception(e)


def create_userid_reshare_lists(postid_num_reshares, userid_postids):
    """
    Create dictionaries mapping user IDs to a list of reshare counts they earned
    for all of their posts.

    Parameters:
    -----------
    - postid_num_reshares (dict) : {post_id_str : number of reshares in data}
    - userid_postids (dict) : {userid_x : set([postids sent by userid_x])}

    Returns:
    -----------
    - userid_total_reshares (dict) : {userid_x : total rts earned by userid_x}

    Exceptions:
    -----------
    - Exception, TypeError
    """
    if not isinstance(postid_num_reshares, dict):
        raise TypeError("`postid_num_reshares` must be a dict!")
    if not isinstance(userid_postids, dict):
        raise TypeError("`userid_postids` must be a dict!")

    try:
        userid_reshare_lists = defaultdict(list)
        for userid, post_ids in userid_postids.items():
            for post_id in post_ids:
                num_reshares = postid_num_reshares[post_id]
                userid_reshare_lists[userid].append(num_reshares)
        return dict(userid_reshare_lists)

    except Exception as e:
        raise Exception(e)


def create_fib_frame(userid_reshare_lists, userid_username, userid_total_reshares):
    """
    Create a dataframe where each row contains a single users identification
    information and FIB-index.

    Parameters:
    -----------
    - userid_reshare_lists (dict) : {userid_x : list([reshare count (int) for each post sent by user_x])}
    - userid_username (dict) : {userid : username}

    Returns:
    -----------
    - fib_frame (pandas.DataFrame) : a dataframe containing the following columns:
        - user_id (str) : the user's Twitter user ID
        - username (str) : the user's username/handle
        - fib_index (int) : the fib index for that user (sorted in descending order)

    Exceptions:
    -----------
    - Exception, TypeError
    """
    if not isinstance(userid_reshare_lists, dict):
        raise TypeError("`userid_reshare_lists` must be a dict!")

    user_records = []
    try:
        for userid, rt_cnt_list in userid_reshare_lists.items():
            user_records.append(
                {
                    "user_id": userid,
                    "username": userid_username[userid],
                    "fib_index": calc_fib_index(rt_cnt_list),
                }
            )

        fib_frame = pd.DataFrame.from_records(user_records)

        userid_total_reshares_frame = pd.DataFrame.from_records(
            list(userid_total_reshares.items()), columns=["user_id", "total_reshares"]
        )
        return fib_frame.merge(userid_total_reshares_frame, on="user_id")

    except Exception as e:
        raise Exception(e)


def get_top_spreaders(fib_frame, num, rank_type=None):
    """
    Return the top `num` spreaders of misinformation from the fib_frame.

    Parameters:
    -----------
    - fib_frame (pandas.DataFrame) : a dataframe containing the following columns:
        - user_id (str) : the user's Twitter user ID
        - username (str) : the user's username/handle
        - fib_index (int) : the fib index for that user (sorted in descending order)
    - num (int) : number of top spreaders to return
    - rank_type (str) : the column to utilize for ranking (descending order)
        - Options: ["total_reshares", "fib_index"]

    Return:
    -----------
    - top_spreaders (set) : set of user IDs for the top spreaders

    Exceptions:
    -----------
    TypeError, ValueError
    """
    if not isinstance(fib_frame, pd.DataFrame):
        raise TypeError("`fib_frame` must be a pd.DataFrame!")
    if rank_type not in ["total_reshares", "fib_index"]:
        raise ValueError("`rank_type` must be either 'total_reshares' or 'fib_index'!")
    if not isinstance(num, int):
        raise TypeError("`num` must be an integer!")

    # Get top 50 users with most total retweets
    fib_frame.sort_values(by=rank_type, ascending=False, inplace=True)
    return set(list(fib_frame["user_id"].head(num)))


def create_top_spreader_df(
    top_spreaders, userid_postids, postid_num_reshares, postid_timestamp, postid_url
):
    """
    Create a dataframe containing all posts sent by the top spreaders.

    Parameters:
    ------------
    - top_spreaders (set) : top spreader user IDs
    - userid_postids (dict) : maps user IDs to a set of (str) post IDs
    - postid_num_reshares (dict) : maps post IDs to number of reshares (int)
    - postid_timestamp (dict) : maps post IDs to (str) timestamps
    - postid_url (dict) : maps post IDs to (str) post URLs

    Returns:
    -----------
    - top_spreaders_df (pandas.DataFrame) : dataframe containing the below columns:
        - user_id (str) : unique user ID of the poster
        - post_id (str) : unique ID of the post
        - num_reshares (int) : the number of reshares of `post_id`
        - timestamp (str) : timestamp string
        - post_url (str) : full URL to the post

    Exceptions:
    -----------
    TypeError
    """
    if not isinstance(userid_postids, dict):
        raise TypeError("`userid_postids` must be a dict!")
    if not isinstance(postid_num_reshares, dict):
        raise TypeError("`postid_num_reshares` must be a dict!")
    if not isinstance(postid_timestamp, dict):
        raise TypeError("`postid_timestamp` must be a dict!")
    if not isinstance(top_spreaders, set):
        raise TypeError("`top_spreaders` must be a set!")

    try:
        top_spreader_records = []
        for user_id in top_spreaders:
            user_postids = userid_postids[user_id]
            for post_id in user_postids:
                top_spreader_records.append(
                    {
                        "user_id": user_id,
                        "post_id": post_id,
                        "num_reshares": postid_num_reshares[post_id],
                        "timestamp": postid_timestamp[post_id],
                        "post_url": postid_url[post_id],
                    }
                )
        top_spreaders_df = pd.DataFrame.from_records(top_spreader_records)
        return top_spreaders_df

    except Exception as e:
        raise Exception(e)
