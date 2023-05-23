"""
Functions used to communicate with CrowdTangle API
"""
import requests


def ct_get_search_posts(
    count=100,
    start_time=None,
    end_time=None,
    include_history=None,
    sort_by="date",
    types=None,
    search_term=None,
    account_types=None,
    min_interactions=0,
    offset=0,
    api_token=None,
    platforms="facebook,instagram",
    lang=None,
):
    """
    Retrieve posts from Facebook/Instagram based on the passed parameters.
    REF: https://github.com/CrowdTangle/API/wiki/Search
    Parameters:
        - count (int, optional): The number of posts to return.
            Options: [1-100]
            Default: 10
        - start_time (str, datetime_obj, optional): The earliest time at which a post could be posted.
            Time zone is UTC.
            String format: “yyyy-mm-ddThh:mm:ss” or “yyyy-mm-dd”
                - If date with no time is passed, default time granularity = 00:00:00
        - end_time (str, datetime_obj, optional): The latest time at which a post could be posted.
            Time zone is UTC.
            String format: “yyyy-mm-ddThh:mm:ss” or “yyyy-mm-dd”
                - If date with no time is passed, default time granularity = 00:00:00
            Default time: "now"
        - include_history (str, optional): Includes time step data for the growth of each post returned.
            Options: 'true'
            Default: null (not included)
        - sort_by (str, optional): The method by which to filter and order posts.
            Options:
                - 'date'
                - 'interaction_rate'
                - 'overperforming'
                - 'total_interactions'
                - 'underperforming'
            Default: 'overperforming'
        - types (str, optional): The types of post to include. These can be separated by commas to
            include multiple types. If you want all live videos (whether currently or formerly live),
            be sure to include both live_video and live_video_complete. The "video" type does not
            mean all videos, it refers to videos that aren't native_video, youtube or vine (e.g. a
            video on Vimeo).
            Options:
                - "episode"
                - "extra_clip"
                - "link"
                - "live_video"
                - "live_video_complete"
                - "live_video_scheduled"
                - "native_video"
                - "photo"
                - "status"
                - "trailer"
                - "video"
                - "vine"
                -  "youtube"
            Default: all
        - search_term (str, optional): Returns only posts that match this search term.
            Terms AND automatically. Separate with commas for OR, use quotes for phrases.
            E.g. CrowdTangle API -> AND. CrowdTangle, API -> OR. "CrowdTangle API" -> AND in that
            exact order. You can also use traditional Boolean search with this parameter.
            Default: null
        - account_types: Limits search to a specific Facebook account type. You can use more than
            one type. Requires "platforms=facebook" to be set also. If "platforms=facebook" is not
            set, all post types including IG will be returned. Only applies to Facebook.
            Options:
                - facebook_page
                - facebook_group
                - facebook_profile
            Default: None (no restrictions, all platforms)
        - min_interactions (int, optional): If set, will exclude posts with total interactions
            below this threshold.
            Options: int >= 0
            Default: 0
        - offset (int, optional): The number of posts to offset (generally used for pagination).
            Pagination links will also be provided in the response.
        - api_token (str, optional): The API token needed to pull data. You can locate your API
            token via your CrowdTangle dashboard under Settings > API Access.
        - platforms: the platform to collect data from
            Options: "facebook", "instagram", or "facebook,instagram" (both)
            Default: "facebook,instagram"
        - lang: language of the posts to collect (str)
            Default: None (no restrictions)
            Options: 2-letter code found in reference below. See ref above for some exceptions.
            REF:https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes
    Returns:
        [dict]: The Response contains both a status code and a result. The status will always
            be 200 if there is no error. The result contains an array of post objects and a
            pagination object with URLs for both the next and previous page, if they exist
    Example:
        ct_get_posts(include_history = 'true', api_token="AKJHXDFYTGEBKRJ6535")
    """

    # API-endpoint
    URL_BASE = "https://api.crowdtangle.com/posts/search"
    # Defining a params dict for the parameters to be sent to the API
    PARAMS = {
        "count": count,
        "sortBy": sort_by,
        "token": api_token,
        "minInteractions": min_interactions,
        "offset": offset,
    }

    # add params parameters
    if start_time:
        PARAMS["startDate"] = start_time
    if end_time:
        PARAMS["endDate"] = end_time
    if include_history == "true":
        PARAMS["includeHistory"] = include_history
    if types:
        PARAMS["types"] = types
    if account_types:
        PARAMS["accountTypes"] = account_types
    if search_term:
        PARAMS["searchTerm"] = search_term
    if platforms:
        PARAMS["platforms"] = platforms
    if lang:
        PARAMS["language"] = lang

    # sending get request and saving the response as response object
    r = requests.get(url=URL_BASE, params=PARAMS)
    if r.status_code != 200:
        print(f"status: {r.status_code}")
        print(f"reason: {r.reason}")
        print(f"details: {r.raise_for_status()}")
    return r
