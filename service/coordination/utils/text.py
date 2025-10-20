import re
from typing import List
from ..types.base import HashtagID


def extract_hashtags(text: str) -> List[HashtagID]:
    """
    Extracts hashtags from text and returns a list.
    """
    pattern = r"(#\w+)"  # r"#[^\s!@#$%^&*()=+./,\[{\]};:'\"?><]+"
    return re.findall(pattern, text)


def extract_urls(text: str) -> List[str]:
    """
    Extracts urls from text and returns a list. Note that the URLs are
    returned exactly as found in the text. If you want them to be resolved
    fully (to avoid short URLs etc) then do this prior to indexing
    and then pull those resolved URLs from the index and not from the text
    """
    pattern = r"https?://(?:www\.)?[a-zA-Z0-9@:%._+~#=]{1,256}\.[a-z]{2,6}\b(?:[-a-zA-Z0-9%_+.~#?&@/=]*[a-zA-Z0-9%_+~#@/=])?"
    return re.findall(pattern, text)
