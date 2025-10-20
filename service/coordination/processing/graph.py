import pandas as pd
import numpy as np
import math

from ..types.base import (
    MEBGraphDataFrame,
    PostsDataFrame,
    SEBGraphDataFrame,
    TimeStamp,
)


def multi_edge_graph(posts: PostsDataFrame) -> MEBGraphDataFrame:
    """
    Generate a multi-edge bipartite graph from the posts dataframe.
    """

    # posts['PostDate'] = pd.to_datetime(posts['PostDate'])

    grouped = posts.groupby("Link")

    max_time: TimeStamp = 0
    total_time: TimeStamp = 0
    counter = 0

    for name, group in grouped:
        group = group.sort_values(by="PostDate")

        t0 = group.iloc[0]["PostDate"]
        tn = group.iloc[-1]["PostDate"]
        time_decay = tn - t0

        if time_decay > max_time:
            max_time = time_decay

        total_time += time_decay
        counter += 1

    alpha = math.log(10000) / (total_time / counter)

    user_id = []
    exp_t = []
    link = []
    user_nid = []
    link_nid = []
    post_id = []

    for name, group in grouped:
        group = group.sort_values(by="PostDate")
        t0 = group.iloc[0]["PostDate"]
        group["TimeDecay"] = (
            group["PostDate"].sub(t0).apply(lambda x: math.exp(-alpha * x))
        )
        mask = group["TimeDecay"] > 0.00001
        group = group[mask]

        user_id.extend(group["UserID"])
        exp_t.extend(group["TimeDecay"])
        link.extend(group["Link"])
        user_nid.extend(group["Numeric_UID"])
        link_nid.extend(group["Numeric_Link_ID"])
        post_id.extend(group["PostID"])

    data = {
        "UserID": user_id,
        "TimeDecay": exp_t,
        "Link": link,
        "Numeric_UID": user_nid,
        "Numeric_Link_ID": link_nid,
        "PostID": post_id,
    }

    df = pd.DataFrame(data)
    return df


def single_edge_graph_summation(posts: MEBGraphDataFrame) -> SEBGraphDataFrame:
    """
    Convert Muti-edge Bipartite graph into Single_edge_Bipartite graph.
    """

    grouped = posts.groupby(["UserID", "Link"])
    user_id = []
    user_nid = []
    usage = []
    link = []
    link_nid = []
    number_link_used = []
    post_ids = []

    for _, group in grouped:
        group_size = len(group)
        usage.append(np.sum(group["TimeDecay"]))
        number_link_used.append(group_size)

        first_row = group.iloc[0]
        user_id.append(first_row["UserID"])
        link.append(first_row["Link"])
        user_nid.append(first_row["Numeric_UID"])
        link_nid.append(first_row["Numeric_Link_ID"])
        post_ids.append(group["PostID"].tolist())

    data = {
        "UserID": user_id,
        "Usage": usage,
        "Link": link,
        "Numeric_UID": user_nid,
        "Numeric_Link_ID": link_nid,
        "Number_link_used": number_link_used,
        "PostIDs": post_ids,
    }

    df = pd.DataFrame(data)
    return df
