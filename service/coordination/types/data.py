from typing import List, TypedDict

from .base import PostID, UserID, TimeStamp


class Post(TypedDict):
    """Represents a single post with its metadata."""

    user_id: UserID
    post_id: PostID
    timestamp: TimeStamp
    text: str
    hashtags: List[str]


class HashtagData(TypedDict):
    """Represents hashtag data extracted from a post."""

    UserID: UserID
    Hashtag: str
    Time: TimeStamp
    PostID: PostID


class RawPostData(TypedDict):
    """Represents raw post data for storing in results."""

    User_ID: UserID
    Screen_Name: str
    Post_ID: PostID


class PostData(TypedDict):
    """Represents processed post data with time and links."""

    time: TimeStamp
    links: List[str]
