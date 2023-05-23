"""
A data class that is used to extract the information needed in the
fib calculation scripts.
"""
import datetime

from .data import get_dict_val

TWITTER_V1_DT_CONVERSION_STR = "%a %b %d %H:%M:%S %z %Y"
TWITTER_V2_DT_CONVERSION_STR = None  # TODO: Update when V2 added
CROWDTANGLE_DT_CONVERSION_STR = "%Y-%m-%d %H:%M:%S"


class PostBase:
    """
    Base class for social media post.
    Classes for specific platforms can inherit it.
    It defines the common functions that the children classes should have.
    """

    def __init__(self, post_object):
        """
        This function initializes the instance by binding the post_object
        Parameters:
            - post_object (dict): the JSON object of the social media post
        """
        if post_object is None:
            raise ValueError("The post object cannot be None")
        self.post_object = post_object

    def get_value(self, key_list: list = []):
        """
        This is the same as the midterm.get_dict_val() function
        Return `dictionary` value at the end of the key path provided
        in `key_list`.
        Indicate what value to return based on the key_list provided.
        For example, from left to right, each string in the key_list
        indicates another nested level further down in the dictionary.
        If no value is present, a `None` is returned.
        Parameters:
        ----------
        - dictionary (dict) : the dictionary object to traverse
        - key_list (list) : list of strings indicating what dict_obj
            item to retrieve
        Returns:
        ----------
        - key value (if present) or None (if not present)
        """
        return get_dict_val(self.post_object, key_list)

    def is_valid(self):
        """
        Check if the data is valid
        """
        raise NotImplementedError

    def get_post_time(self):
        """
        Get the time a post was shared.
        """
        raise NotImplementedError

    def get_reshare_count(self):
        """
        Return the number of times that the post was reshared
        """
        return NotImplementedError

    def get_post_ID(self):
        """
        Return the ID of the post as a string
        """
        raise NotImplementedError

    def get_link_to_post(self):
        """
        Return the link to the post so that one can click it and check
        the post in a web browser
        """
        raise NotImplementedError

    def get_user_ID(self):
        """
        Return the ID of the user as a string
        """
        raise NotImplementedError

    def get_user_handle(self):
        """
        Return the screen_name of the user (str)
        """
        return NotImplementedError

    def __repr__(self):
        """
        Define the representation of the object.
        """
        return f"<{self.__class__.__name__}() object>"


### Post base for Twitter V1 tweet object
# Reference: https://developer.twitter.com/en/docs/twitter-api/v1/data-dictionary/object-model/tweet
############################################
class Tweet_v1(PostBase):
    """
    Class to handle tweet object (V1 API)
    Ref: https://developer.twitter.com/en/docs/twitter-api/v1/data-dictionary/object-model/tweet
    """

    def __init__(self, tweet_object):
        """
        This function initializes the instance by binding the tweet_object
        Parameters:
            - tweet_object (dict): the JSON object of a tweet
        """
        super().__init__(tweet_object)

        self.is_retweet = "retweeted_status" in self.post_object
        if self.is_retweet:
            self.retweet_object = Tweet_v1(self.post_object["retweeted_status"])

        self.is_quote = "quoted_status" in self.post_object
        if self.is_quote:
            self.quote_object = Tweet_v1(self.post_object["quoted_status"])

    def is_valid(self):
        """
        Check if the tweet object is valid.
        A valid tweet should at least have the following attributes:
            [id_str, user, text, created_at]
        """
        attributes_to_check = ["id_str", "user", "text", "created_at"]
        for attribute in attributes_to_check:
            if attribute not in self.post_object:
                return False
        return True

    def get_post_time(self, timestamp=False):
        """
        Return the "created_at" post time of a post.

        Parameters:
        -----------
        timestamp (bool): whether or not to return the created_at time as a timestamp

        Returns:
        -----------
        - post_time (str): if timestamp=False, return "created_at" time as is. If
            timestamp=True, first convert "created_at" time to a timestamp
        """
        created_at = self.get_value(["created_at"])
        if not timestamp:
            return created_at
        try:
            dt_obj = datetime.datetime.strptime(
                created_at, TWITTER_V1_DT_CONVERSION_STR
            )
            return str(int(dt_obj.timestamp()))
        except:
            return None

    def get_reshare_count(self):
        """
        Return the number of of times this post was reshared (i.e., the retweet count)
        """
        return self.get_value(["retweet_count"])

    def get_post_ID(self):
        """
        Return the ID of the tweet (str)
        This is different from the id of the retweeted tweet or
        quoted tweet
        """
        return self.get_value(["id_str"])

    def get_link_to_post(self):
        """
        Return the link to the tweet (str)
        so that one can click it and check the tweet in a web browser
        """
        return (
            f"https://twitter.com/{self.get_user_handle()}/status/{self.get_post_ID()}"
        )

    def get_user_ID(self):
        """
        Return the ID of the base-level user (str)
        """
        return self.get_value(["user", "id_str"])

    def get_user_handle(self):
        """
        Return the screen_name of the user (str)
        """
        return self.get_value(["user", "screen_name"])
    
    def get_user_profile_image_url(self):
        """
        Return the profile image URL for the poster of this tweet object
        """
        return self.get_value(["user", "profile_image_url"])

    def get_link_to_author(self):
        """
        Return the link to the tweet author
        """
        return f"https://twitter.com/i/user/{self.get_user_handle()}"

    def __repr__(self):
        """
        Define the representation of the object.
        """
        return "".join(
            [
                f"{self.__class__.__name__} object from @{self.get_user_handle()}\n",
                f"Link: {self.get_link_to_post()}",
            ]
        )


### Post base for Twitter V2 tweet object
# Reference: https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet
# TODO: Update when Decahose begins returning V2
############################################
class Tweet_v2(PostBase):
    """
    Class to handle tweet object (V2 API)
    Ref: https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet
    """

    def __init__(self, tweet_object):
        """
        This function initializes the instance by binding the tweet_object
        Parameters:
            - tweet_object (dict): the JSON object of a tweet
        """
        super().__init__(tweet_object)


class FbIgPost(PostBase):
    """
    Class to handle CrowdTangle Facebook and Instagram post objects returned by
    the /posts/search endpoint.
    Endpoint Ref: https://github.com/CrowdTangle/API/wiki/Search
    Response Ref: https://github.com/CrowdTangle/API/wiki/Search#response
    """

    def __init__(self, post_object):
        """
        This function initializes the instance by binding the post_object
        Parameters:
            - post_object (dict): the JSON object of the social media post
        """
        super().__init__(post_object)

        if post_object is None:
            raise ValueError("The post object cannot be None")
        self.post_object = post_object

        self.platform = self.get_value(["platform"])
        self.is_fb_post = True if self.platform == "Facebook" else False
        self.is_ig_post = True if self.platform == "Instagram" else False

    def is_valid(self):
        """
        Check if the post object is valid.
        At minimum, it should have a post "id"
        """
        if "id" not in self.post_object:
            return False
        return True

    def get_post_time(self, timestamp=False):
        """
        Return the "date" field, indicating when a post was sent.

        Parameters:
        -----------
        timestamp (bool): whether or not to return the "date" time as a timestamp

        Returns:
        -----------
        - post_time (str): if timestamp=False, return "date" time as is. If
            timestamp=True, first convert "date" time to a timestamp
        """
        created_at = self.get_value(["date"])
        if not timestamp:
            return created_at
        try:
            dt_obj = datetime.datetime.strptime(
                created_at, CROWDTANGLE_DT_CONVERSION_STR
            )
            return str(int(dt_obj.timestamp()))
        except:
            return None

    def get_reshare_count(self):
        """
        Return the number of times that the post was reshared
        """
        return self.get_value(["statistics", "actual", "shareCount"])

    def get_post_ID(self):
        """
        Return the ID of the post as a string
        """
        return str(self.get_value(["platformId"]))

    def get_link_to_post(self):
        """
        Return the link to the post so that one can click it and check
        the post in a web browser
        """
        return self.get_value(["postUrl"])

    def get_user_ID(self):
        """
        Return the ID of the user as a string
        """
        return str(self.get_value(["account", "platformId"]))

    def get_user_handle(self):
        """
        Return the account handle of the user (str)
        """
        return self.get_value(["account", "handle"])
    
    def get_account_name(self):
        """
        Some accounts do not have "handles" and instead have "names." For example,
        if "accountType" is facebook_page or facebook_group.
        """
        return self.get_value(["account", "name"])

    def get_link_to_author(self):
        """
        Return the link to the authors page
        """
        return self.get_value(["account", "url"])

    def __repr__(self):
        """
        Define the representation of the object.
        """
        return "".join(
            [
                f"{self.__class__.__name__} object from @{self.get_user_handle()}\n",
                f"Link: {self.get_link_to_post()}",
            ]
        )
