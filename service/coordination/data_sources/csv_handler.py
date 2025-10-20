import pandas as pd
import fsspec
import io
import csv

from ..types.base import HashtagID
from ..utils.text import extract_hashtags
from typing import Any, Dict, List, Set, Tuple


def read_post(
    file_name: str, path_useless_hashtags: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Read and process posts from a CSV file.
    """

    data = pd.read_csv(
        file_name,
        delimiter=",",
        skipinitialspace=True,
        converters={"Post_ID": str, "User_ID": str},
    )

    # rather than using pd.read_csv we just read each line as this also works
    # in the case where the user has sent an empty file of excludes
    with fsspec.open(path_useless_hashtags, "r", encoding="utf-8") as f:
        useless_hashtags = set(line.strip().lower() for line in f)

    # these hold the column numbers so a) the columns can move around and
    # b) we only have to look them up once
    text_column = data.columns.get_loc("Post_text")
    user_column = data.columns.get_loc("User_ID")
    time_column = data.columns.get_loc("Post_time")
    post_column = data.columns.get_loc("Post_ID")
    link_column = None
    if "Post_links" in data.columns:
        link_column = data.columns.get_loc("Post_links")

    hashtag_data: List[Dict[str, Any]] = []
    for row in data.itertuples(index=False):
        if link_column is None:
            text = row[text_column]  # 'Post_text'
            hashtags: List[HashtagID] = extract_hashtags(text.lower())
            # make sure to remove any duplicates
            hashtags = set(hashtags)
        else:
            links = row[link_column]
            hashtags: Set[HashtagID] = set()
            # parse the content of the links column itself as a CSV.
            # This is slightly cumbersome but I wanted a well-defined way
            # of giving multiple values in one column without having to
            # invent my own rules for escaping
            for links_row in csv.reader(io.StringIO(links), skipinitialspace=True):
                hashtags.update(links_row)

        for hashtag in hashtags:
            if hashtag not in useless_hashtags:
                hashtag_data.append(
                    {
                        "UserID": row[user_column],  # 'User_ID'
                        "Hashtag": hashtag,
                        "Time": pd.Timestamp(
                            row[time_column].split("+")[0]
                        ).timestamp(),  # 'Post_time'
                        "PostID": row[post_column],  # 'Post_ID'
                    }
                )

    hashtag_df = pd.DataFrame(hashtag_data)

    hashtag_df.columns = ["UserID", "Link", "PostDate", "PostID"]
    Posts = hashtag_df.iloc[1:][["UserID", "Link", "PostDate", "PostID"]]

    return Posts[["UserID", "Link", "PostDate", "PostID"]], data
