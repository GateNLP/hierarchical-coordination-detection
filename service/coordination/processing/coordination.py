import os
from typing import Tuple
import numpy as np
import pandas as pd
from ..types.base import PostsDataFrame, CoordinationDataFrame, SpeedOption
from .matrix import (
    create_edges,
    link_usage_behaviour_matrix,
    divergence_assessment,
    calculate_edge_weight,
    user_link_matrix,
    calculate_edges_with_chunk,
)
from .graph import multi_edge_graph, single_edge_graph_summation


def assign_numerical_ID(
    posts: PostsDataFrame, field: str, id_field: str
) -> Tuple[PostsDataFrame, int]:
    """
    Assign a numerical ID to each unique entity in the specified field of the DataFrame.

    Example dataframe:
    | UserID | Link  | PostID | Timestamp | Text |
    |--------|-------|--------|-----------|------|
    | 1      | A     | 101    | 1620000000| ...  |
    | 2      | B     | 102    | 1620000001| ...  |
    | 1      | A     | 103    | 1620000002| ...  |
    | 3      | C     | 104    | 1620000003| ...  |
    | 2      | A     | 105    | 1620000004| ...  |
    | 3      | B     | 106    | 1620000005| ...  |

    USe the function will create a new column 'Numeric_UID' or 'Numeric_Link_ID' in the DataFrame
    with numerical IDs for each unique UserID or Link, respectively.
    Output:
    | UserID | Link  | PostID | Timestamp | Text | Numeric_UID | Numeric_Link_ID |
    |--------|-------|--------|-----------|------|-------------|-----------------|
    | 1      | A     | 101    | 1620000000| ...  | 0           | 0               |
    | 2      | B     | 102    | 1620000001| ...  | 1           | 1               |
    | 1      | A     | 103    | 1620000002| ...  | 0           | 0               |
    | 3      | C     | 104    | 1620000003| ...  | 2           | 2               |
    | 2      | A     | 105    | 1620000004| ...  | 1           | 0               |
    | 3      | B     | 106    | 1620000005| ...  | 2           | 1               |
    """

    # unique entities in the field
    unique_set = posts[field].unique()

    # Create a mapping from entity to numerical ID
    entity_to_id = {entity: id for id, entity in enumerate(unique_set)}

    # Map the original field to the numerical ID
    posts[id_field] = posts[field].map(entity_to_id)

    return posts, len(unique_set)


def recursive_remove(posts: PostsDataFrame) -> PostsDataFrame:
    """
    Recursively remove once repeated entities and users from the DataFrame.
    """

    if len(posts) == 0:
        return posts

    while True:
        init_len = len(posts)

        # Step 1: Identify and remove *links* that are shared once or less
        link_user_counts = posts.groupby("Link")["UserID"].nunique()
        links_to_remove = set(link_user_counts[link_user_counts <= 1].index)

        if len(links_to_remove) > 0:
            posts = posts[~posts["Link"].isin(links_to_remove)]

        # If we've removed all rows
        if len(posts) == 0:
            return posts

        # Step 2: Identify and remove *users* that are shared once or less
        user_link_counts = posts.groupby("UserID")["Link"].nunique()
        users_to_remove = set(user_link_counts[user_link_counts <= 1].index)

        if len(users_to_remove) > 0:
            posts = posts[~posts["UserID"].isin(users_to_remove)]

        # Exit if no changes were made
        if len(posts) == init_len:
            break

    return posts

    # Flag = True
    # while Flag:
    #     len1 = len(posts)
    #     grouped = posts.groupby("Link")
    #     once_shared = set()

    #     for _, group_df in grouped:
    #         user_ids = group_df["UserID"].unique()
    #         if len(user_ids) <= 1:
    #             once_shared.add(group_df["Link"].iloc[0])

    #     posts = posts[~posts["Link"].isin(once_shared)]

    #     grouped = posts.groupby("UserID")
    #     shared_one_link = set()
    #     for _, group_df in grouped:
    #         links = group_df["Link"].unique()
    #         if len(links) <= 1:
    #             shared_one_link.add(group_df["UserID"].iloc[0])

    #     posts = posts[~posts["UserID"].isin(shared_one_link)]

    #     len3 = len(posts)
    #     if len3 == len1:
    #         Flag = False

    # return posts


def filter_top_users(posts: PostsDataFrame, percentage: int) -> PostsDataFrame:
    """
    Filter the top percentage of users based on their sharing counts.
    """
    print("filtering users")

    user_sharing_counts = posts["UserID"].value_counts().reset_index()
    user_sharing_counts.columns = ["UserID", "User_sharing_count"]
    user_sharing_counts.sort_values(
        by="User_sharing_count", ascending=False, inplace=True
    )
    k = int(len(user_sharing_counts) * (percentage / 100))
    top_users = user_sharing_counts.head(k)
    posts = posts[np.isin(posts["UserID"], top_users["UserID"])]

    return posts


def generate_result(
    result_df: CoordinationDataFrame, posts: PostsDataFrame
) -> CoordinationDataFrame:
    """
    Convert numerical IDs to original IDs and write the final results in output file.
    """
    distinct_pairs = posts[["Link", "Numeric_Link_ID"]].drop_duplicates()
    merged = pd.merge(result_df, distinct_pairs, on="Numeric_Link_ID", how="left")
    merged = merged.drop(columns=["Numeric_Link_ID"])

    distinct_pairs = posts[["UserID", "Numeric_UID"]].drop_duplicates()
    merged = pd.merge(
        merged, distinct_pairs, left_on="From", right_on="Numeric_UID", how="left"
    )
    merged = merged.drop(columns=["From", "Numeric_UID"])
    merged = merged.rename(columns={"UserID": "From"})

    distinct_pairs = posts[["UserID", "Numeric_UID"]].drop_duplicates()
    merged = pd.merge(
        merged, distinct_pairs, left_on="To", right_on="Numeric_UID", how="left"
    )
    merged = merged.drop(columns=["To", "Numeric_UID"])
    merged = merged.rename(columns={"UserID": "To"})

    new_order = [
        "From",
        "To",
        "Link",
        "Weight",
        "PostIDs_from",
        "PostIDs_to",
    ]  # Adjust the column names accordingly
    merged = merged[new_order]

    return merged.sort_values(by=["Weight"], ascending=False)


def calcCoordination(
    posts: PostsDataFrame, speed_option: SpeedOption
) -> CoordinationDataFrame:
    """
    Calculate the coordination of users based on their posts and links.

    TODO: Break this function into smaller functions so that it is easier to test.
    """
    # What percentage of users with the highest number of shared entities should be included?
    # (a number between 1 to 100)
    top_percent = 100
    # How many threads should be used for multi-threaded execution?
    num_process = int(os.environ.get("COORDINATION_WORKER_THREADS", "8"))
    # How many chunks should each thread consider?
    chunk_size = int(os.environ.get("COORDINATION_WORKER_CHUNK_SIZE", "1000"))

    # don't filter if we actually want everyone anyway
    if top_percent != 100:
        posts = filter_top_users(posts, top_percent)

    posts = recursive_remove(posts)
    posts, user_count = assign_numerical_ID(posts, "UserID", "Numeric_UID")
    posts, link_count = assign_numerical_ID(posts, "Link", "Numeric_Link_ID")

    if len(posts) == 0:
        return pd.DataFrame(
            columns=["From", "To", "Link", "Weight", "PostIDs_from", "PostIDs_to"]
        )

    # Create a multi-edge bipartite graph
    MEBgraph = multi_edge_graph(posts)

    # Convert the multi-edge graph to a single-edge bipartite graph
    SEBgraph = single_edge_graph_summation(MEBgraph)

    # For each hashtag: identifying users sharing suspiciously and edeg between them  (individual level)
    users_behaviour, edges_users, coordinated_link, PostIDs = (
        link_usage_behaviour_matrix(SEBgraph)
    )
    df_behaviour = create_edges(users_behaviour, edges_users, coordinated_link, PostIDs)

    if speed_option == SpeedOption.NO_FILTERING:
        Final_coordination_records = df_behaviour.copy()

        pair_counts = Final_coordination_records.groupby(["From", "To"]).size()
        more_once__pairs = pair_counts[pair_counts > 1].index
        Final_coordination_records = Final_coordination_records[
            Final_coordination_records.set_index(["From", "To"]).index.isin(
                more_once__pairs
            )
        ]

        Final_coordination_records["Weight"] = Final_coordination_records[
            ["Beha_1", "Beha_2"]
        ].min(axis=1)

        # can we rename instead of copying the columns
        Final_coordination_records["PostIDs_from"] = Final_coordination_records[
            "PostIDs_1"
        ]
        Final_coordination_records["PostIDs_to"] = Final_coordination_records[
            "PostIDs_2"
        ]

        return generate_result(Final_coordination_records, posts)

    # Identifying users sharing suspiciously (pairwise level)
    edges = calculate_edges_with_chunk(df_behaviour, MEBgraph, num_process, chunk_size)
    column_names = [
        "From",
        "To",
        "Numeric_Link_ID",
        "Weight",
        "PostIDs_from",
        "PostIDs_to",
    ]
    pairwis_coordination = pd.DataFrame(
        edges
    )  # {col: edges[:, idx] for idx, col in enumerate(columns)})
    pairwis_coordination.columns = column_names

    if speed_option == SpeedOption.PAIRWISE_FILTERING:
        Final_coordination_records = pairwis_coordination.copy()

        return generate_result(Final_coordination_records, posts)

    # Identifying users sharing suspiciously (group level)
    user_link_prob = user_link_matrix(posts, user_count, link_count)
    group_coordination = calculate_edge_weight(pairwis_coordination)
    Final_coordination_records = divergence_assessment(
        group_coordination, link_count, user_count, user_link_prob, pairwis_coordination
    )

    result = generate_result(Final_coordination_records, posts)

    return result
