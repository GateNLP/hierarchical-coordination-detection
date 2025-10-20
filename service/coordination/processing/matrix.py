import pandas as pd
import numpy as np
import itertools
from typing import List
from numpy.typing import NDArray
from collections.abc import Callable
from tqdm import tqdm
from multiprocessing import Pool
from warnings import filterwarnings

from ..types.base import (
    MEBGraphDataFrame,
    PostsDataFrame,
    SEBGraphDataFrame,
)
from .entropy import (
    find_max_entropy_partition,
    calculate_entropy,
    cosine_similarity,
    JSD_divergence,
)
from ..types.graph import UserBehaviour, UserPair


def link_usage_behaviour_matrix(
    SEBgraph: SEBGraphDataFrame,
) -> tuple[
    List[List[float]],
    List[List[int]],
    List[int],
    List[List[List[str]]],
]:
    """
    Identify users suspected for coordination based on individual sharing behaviour.

    This function processes a DataFrame containing user behaviour data and identifies
    users who exhibit similar sharing patterns for the same entity (link). It groups
    users by their numeric link IDs and sorts their behaviour values. The function
    then applies a maximum entropy partitioning algorithm to identify coordinated
    groups of users.

    The output includes lists of user behaviours, user IDs, and
    the corresponding link IDs. The function also handles cases where the behaviour
    values are not strictly decreasing, ensuring that the identified groups are
    meaningful and relevant for further analysis.
    """
    user_behaviour = []
    user_ID_behaviour = []
    user_behaviour_on_link = []
    PostIDs = []

    grouped = SEBgraph.groupby("Numeric_Link_ID")

    for group in grouped:
        group_df = group[1]
        numeric_link_id = group_df["Numeric_Link_ID"].values[0]
        beha = group_df["Usage"].tolist()
        user_ids = group_df["Numeric_UID"].tolist()
        post_ids = group_df["PostIDs"].tolist()

        if len(beha) > 1:
            sorted_indices = np.argsort(beha)[::-1]
            beha = np.array(beha)[sorted_indices].tolist()
            user_ids = np.array(user_ids)[sorted_indices].tolist()
            post_ids = np.array(post_ids, dtype=object)[sorted_indices].tolist()
            #            post_ids=np.array(post_ids)[sorted_indices].tolist()
            vector = find_max_entropy_partition(beha)

            if len(vector) == len(beha):
                if abs(calculate_entropy(beha) - calculate_entropy(beha[:-1])) < abs(
                    np.std(beha) - np.std(beha[:-1])
                ):
                    vector = vector[:-1]

            coordination_size = len(vector)

            if coordination_size > 1:
                user_ID_behaviour.append(user_ids[:coordination_size])
                user_behaviour.append(beha[:coordination_size])
                user_behaviour_on_link.append(numeric_link_id)
                PostIDs.append(post_ids[:coordination_size])

    return user_behaviour, user_ID_behaviour, user_behaviour_on_link, PostIDs


def create_edges(
    users_behaviour: List[List[float]],
    edges_users: List[List[float]],
    coordinated_link: List[int],
    PostIDs: List[List[List[str]]],
) -> pd.DataFrame:
    """
    Create a dataframe with pairs of users, their behaviour in sharing and the entity.
    """
    edges_from, edges_to, beha_1, beha_2, posts_1, posts_2, coor_link = (
        [],
        [],
        [],
        [],
        [],
        [],
        [],
    )
    for i in range(len(edges_users)):
        for j, k in itertools.combinations(range(len(edges_users[i])), 2):
            user1, user2 = edges_users[i][j], edges_users[i][k]
            b1, b2 = users_behaviour[i][j], users_behaviour[i][k]
            p1, p2 = PostIDs[i][j], PostIDs[i][k]
            link_i = coordinated_link[i]

            if user1 < user2:
                edges_from.append(user1)
                edges_to.append(user2)
                beha_1.append(b1)
                beha_2.append(b2)
                posts_1.append(p1)
                posts_2.append(p2)
            else:
                edges_from.append(user2)
                edges_to.append(user1)
                beha_1.append(b2)
                beha_2.append(b1)
                posts_1.append(p2)
                posts_2.append(p1)
            coor_link.append(link_i)

    behaviour: UserBehaviour = dict(
        From=edges_from,
        To=edges_to,
        Beha_1=beha_1,
        Beha_2=beha_2,
        Numeric_Link_ID=coor_link,
        PostIDs_1=posts_1,
        PostIDs_2=posts_2,
    )

    df_behaviour = pd.DataFrame(behaviour)
    df_behaviour = df_behaviour.sort_values(["From", "To"])
    return df_behaviour


def dtw(dist_mat: NDArray) -> NDArray:
    """
    Dynamic Time Warping (DTW) algorithm to compute the cost matrix.
    """
    N, M = dist_mat.shape

    # Initialise cost matrix
    cost_mat = np.zeros((N + 1, M + 1))
    for i in range(1, N + 1):
        cost_mat[i, 0] = np.inf
    for i in range(1, M + 1):
        cost_mat[0, i] = np.inf

    traceback_mat = np.zeros((N, M))

    for i in range(N):
        for j in range(M):
            penalty = [
                cost_mat[i, j],  # Match
                cost_mat[i, j + 1],  # Insert
                cost_mat[i + 1, j],  # Delete
            ]
            i_penalty = np.argmin(penalty)
            cost_mat[i + 1, j + 1] = dist_mat[i, j] + penalty[i_penalty]
            traceback_mat[i, j] = i_penalty

    # Return matrix without padding
    cost_mat = cost_mat[1:, 1:]
    return cost_mat


# Functions for the parallel process to assess pairwise sharing behaviour


# Chunk function
def unequal_chunks(list: List, chunk_size: int) -> List[List]:
    """
    Split a list into chunks of unequal size based on the `chunk_size` parameter.

    This function is a part of the parallel processing to assess pairwise sharing behaviour.
    """
    chunks = []
    b = 0
    for i in range(0, len(list), chunk_size):
        s = b + chunk_size
        tb = b + chunk_size + 1

        # Ensure items with the same behaviour are in the same chunk
        while (
            s < len(list)
            and list[s][0] == list[s - 1][0]
            and list[s][1] == list[s - 1][1]
        ):
            s += 1
            tb += 1

        if len(list[b:s]) > 0:
            chunks.append(list[b:s])
        b = s
    return chunks


def calculated_edges(
    chunks: NDArray, df_behaviour: pd.DataFrame, MEBgraph: MEBGraphDataFrame
) -> NDArray:
    """
    Calculate edges for each chunk of user behaviour data.

    This function is a part of the parallel processing to assess pairwise sharing behaviour.

    TODO: Break this function into smaller functions so that it is easier to test.
    """
    second_Edges_form = []
    second_Edges_to = []
    second_Edges_weight = []
    second_beha_1 = []
    second_beha_2 = []
    second_post_1 = []
    second_post_2 = []
    second_coor_link = []

    TmpFrom = chunks[:, 0]
    TmpTo = chunks[:, 1]

    chunk_userids_From = set(TmpFrom)
    chunk_userids_To = set(TmpTo)
    df_behaviour_temp = df_behaviour.loc[
        (df_behaviour["From"].isin(chunk_userids_From))
        & (df_behaviour["To"].isin(chunk_userids_To))
    ].copy()

    mask = np.zeros(len(df_behaviour_temp), dtype=bool)
    preFrom = -1
    preTo = -1
    MEBgraph_temp = MEBgraph.loc[
        (MEBgraph.Numeric_UID.isin(chunk_userids_From))
        | (MEBgraph.Numeric_UID.isin(chunk_userids_To))
        & (MEBgraph.Numeric_Link_ID.isin(set(chunks[:, 4])))
    ].copy()

    for i in range(len(TmpFrom)):
        v1 = TmpFrom[i]
        v2 = TmpTo[i]
        if v1 != preFrom or v2 != preTo:
            preFrom = v1
            preTo = v2

            mask = (df_behaviour_temp["From"] == v1) & (df_behaviour_temp["To"] == v2)
            matrix = df_behaviour_temp[mask][
                [
                    "From",
                    "To",
                    "Beha_1",
                    "Beha_2",
                    "Numeric_Link_ID",
                    "Numeric_Link_ID",
                    "PostIDs_1",
                    "PostIDs_2",
                ]
            ].to_numpy()
            if len(matrix) > 1:
                ZXZX = []
                for row in matrix:
                    X = MEBgraph_temp.loc[
                        (MEBgraph_temp.Numeric_UID == row[0])
                        & (MEBgraph_temp.Numeric_Link_ID == row[4])
                    ]["TimeDecay"]

                    Y = MEBgraph_temp.loc[
                        (MEBgraph_temp.Numeric_UID == row[1])
                        & (MEBgraph_temp.Numeric_Link_ID == row[4])
                    ]["TimeDecay"]

                    x = np.array(sorted(X))
                    y = np.array(sorted(Y))
                    N = x.shape[0]
                    M = y.shape[0]

                    # with open("/home/ahmad/Downloads/Datasets/Coordination_groundtruth/array_shape.txt", "a") as file:
                    #    file.write(f"Shape of the array: {len(matrix)}  {row[0]}  {row[4]} {row[1]} {X} {Y} {N}    {M}  \n")
                    if N > 1000 and M > 1000:
                        ZXZX.append(1)
                    else:
                        dist_mat = np.zeros((N, M))
                        for i in range(N):
                            for j in range(M):
                                dist_mat[i, j] = abs(x[i] - y[j])
                        cost_mat = dtw(dist_mat)
                        ZXZX.append(float(1 / (1 + (cost_mat[N - 1, M - 1]))))
                added = np.concatenate(
                    [np.abs(matrix[:, 2:3] - matrix[:, 3:4])], axis=1
                )
                matrix = np.concatenate([matrix, added], axis=1)

                added2 = np.array(ZXZX)
                matrix = np.hstack((matrix, added2[:, np.newaxis]))

                sorted_indices = np.argsort(matrix[:, 8])
                matrix = matrix[sorted_indices]
                flag = True
                pivot = 2

                while flag:
                    s1 = matrix[:pivot, 2]
                    s2 = matrix[:pivot, 3]
                    d1 = matrix[:pivot, 8]

                    cosine = cosine_similarity(s1, s2)
                    diver = np.sum(np.multiply(d1, d1))
                    if diver == 0:
                        diver = 0.00000001
                    cosine = cosine / diver
                    if cosine < 1 or pivot == len(matrix):
                        flag = False
                        if cosine < 1:
                            pivot -= 1
                    else:
                        pivot += 1

                if pivot > 1:
                    u1 = np.min([matrix[0, 0], matrix[0, 1]])
                    u2 = np.max([matrix[0, 0], matrix[0, 1]])

                    for ctr in range(pivot):
                        second_Edges_form.append(u1)
                        second_Edges_to.append(u2)
                        second_beha_1.append(matrix[ctr][2])
                        second_beha_2.append(matrix[ctr][3])
                        second_coor_link.append(matrix[ctr][4])
                        second_post_1.append(matrix[ctr][6])
                        second_post_2.append(matrix[ctr][7])
                        second_Edges_weight.append(matrix[ctr][9])

    # Create a structured array to hold the lists
    result = np.empty(
        shape=(len(second_Edges_form),),
        dtype=[
            ("int_col1", int),
            ("int_col2", int),
            ("int_col3", int),
            ("float_col", float),
            ("list_col1", list),
            ("list_col2", list),
        ],
    )

    # Assign values to the structured array
    result["int_col1"] = second_Edges_form
    result["int_col2"] = second_Edges_to
    result["int_col3"] = second_coor_link
    result["float_col"] = second_Edges_weight
    result["list_col1"] = second_post_1
    result["list_col2"] = second_post_2

    # result = np.empty(shape=(len(second_Edges_form), 6), dtype=[('',int),('',int),('',int),('',float),('',object),('',object)])
    # print(second_post_1,type(second_post_1))
    # result = np.empty((len(second_Edges_form), 6))
    # result[:,0]=second_Edges_form
    # result[:,1]=second_Edges_to
    # result[:,2]=second_coor_link
    # result[:,3]=second_Edges_weight
    # for i in range(len(second_Edges_form)):
    #    result[i,4]=second_post_1[i]
    #    result[i,5]=second_post_2[i]
    del MEBgraph_temp

    return result


def multiprocess_edge_calculation(
    function_reference: Callable,
    file_chunks: int,
    arg1: pd.DataFrame,
    arg2: pd.DataFrame,
    num_process: int,
) -> NDArray:
    """
    Multiprocess the edge calculation function.
    """
    pool = Pool(num_process)
    pbar = tqdm(total=len(file_chunks))

    def update(arg):
        pbar.update()

    results = []
    for i in range(pbar.total):
        result = pool.apply_async(
            function_reference,
            args=(file_chunks[i],)
            + (
                arg1,
                arg2,
            ),
            callback=update,
        )
        results.append(result)
    pool.close()
    pool.join()
    final_result = np.concatenate([r.get() for r in results], axis=0)
    return final_result


def calculate_edges_with_chunk(
    dataframe: pd.DataFrame,
    MEBgraph: MEBGraphDataFrame,
    num_process: int,
    chunk_size: int,
) -> NDArray:
    """
    Decompress the list and call function to run the edge calculation.
    """
    edge_list = dataframe.values
    chunks = unequal_chunks(edge_list, chunk_size)
    return multiprocess_edge_calculation(
        calculated_edges, chunks, dataframe, MEBgraph, num_process
    )


def user_link_matrix(
    Posts: PostsDataFrame, user_count: int, link_count: int
) -> NDArray:
    """
    Create a user-link probability matrix, where each row represents a user and
    each column represents a link. The value at (i, j) is 1 if user i has
    interacted with link j, and 0 otherwise.
    """

    user_links = np.zeros((user_count, link_count))
    user_links[Posts["Numeric_UID"], Posts["Numeric_Link_ID"]] = 1
    return user_links


def calculate_edge_weight(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the weight of edges between pairs of users (sum of the coordination
    weight for all entities).
    """
    grouped = df.groupby(["From", "To"])

    From_ID = []
    To_ID = []
    Weight = []

    for group, group_df in grouped:
        temp = np.sum(group_df["Weight"])
        From_ID.append(group[0])
        To_ID.append(group[1])
        Weight.append(temp)

    out: UserPair = dict(
        Source=From_ID,
        Target=To_ID,
        Weight=Weight,
    )

    df_out = pd.DataFrame(out)
    return df_out


def divergence_assessment(
    Thirs_df_behaviour: pd.DataFrame,
    link_count: int,
    user_count: int,
    user_link_prob: NDArray,
    Result_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Assess the similarity of non-coordinated users and suspicious users.
    TODO: Break this function into smaller functions so that it is easier to test.
    """

    filterwarnings("ignore")
    From_ID = []
    To_ID = []
    Numeric_Link_ID = []
    Weight = []
    PostIDs_1 = []
    PostIDs_2 = []

    grouped = Result_df.groupby(["From", "To"])

    for group in grouped:
        new_weight = 1
        matrix = (list(group)[1]).values.tolist()
        node_id1 = int(matrix[0][0])
        node_id2 = int(matrix[0][1])
        link_IDs = list(np.array(matrix, dtype=object)[:, 2].astype(int))

        max_weight1 = np.sum(np.array(matrix, dtype=object)[:, 3])

        Z = np.zeros(link_count)
        temp_nodes_ids1 = np.where(np.all(user_link_prob[:, link_IDs], axis=1))[0]

        temp_nodes_ids1 = np.delete(
            temp_nodes_ids1,
            np.where((temp_nodes_ids1 == node_id1) | (temp_nodes_ids1 == node_id2)),
        )

        len_temp_nodes_ids1 = len(temp_nodes_ids1)

        # Filter nodes based on the maximum weight
        a0 = Thirs_df_behaviour.loc[
            (Thirs_df_behaviour.Weight.values >= max_weight1)
            & (
                (
                    (Thirs_df_behaviour.Source.values == node_id1)
                    & (np.isin(Thirs_df_behaviour.Target, temp_nodes_ids1))
                )
                | (
                    (np.isin(Thirs_df_behaviour.Source, temp_nodes_ids1))
                    & (Thirs_df_behaviour.Target.values == node_id1)
                )
            )
        ]
        b0 = Thirs_df_behaviour.loc[
            (Thirs_df_behaviour.Weight.values >= max_weight1)
            & (
                (
                    (Thirs_df_behaviour.Source.values == node_id2)
                    & (np.isin(Thirs_df_behaviour.Target, temp_nodes_ids1))
                )
                | (
                    (np.isin(Thirs_df_behaviour.Source, temp_nodes_ids1))
                    & (Thirs_df_behaviour.Target.values == node_id2)
                )
            )
        ]
        c0 = pd.concat([a0, b0], ignore_index=True)
        Removed_node_IDs = list(set(c0.Source.unique()).union(set(c0.Target.unique())))
        temp_nodes_ids1 = np.delete(
            temp_nodes_ids1, np.where(np.isin(temp_nodes_ids1, Removed_node_IDs))
        )

        # Calculate JSD divergence
        a = Thirs_df_behaviour.loc[
            (
                (Thirs_df_behaviour.Source.values == node_id1)
                & (np.isin(Thirs_df_behaviour.Target, temp_nodes_ids1))
            )
            | (
                (np.isin(Thirs_df_behaviour.Source, temp_nodes_ids1))
                & (Thirs_df_behaviour.Target.values == node_id1)
            )
        ]
        b = Thirs_df_behaviour.loc[
            (
                (Thirs_df_behaviour.Source.values == node_id2)
                & (np.isin(Thirs_df_behaviour.Target, temp_nodes_ids1))
            )
            | (
                (np.isin(Thirs_df_behaviour.Source, temp_nodes_ids1))
                & (Thirs_df_behaviour.Target.values == node_id2)
            )
        ]
        c = pd.concat([a, b], ignore_index=True)

        f1 = set(c.Source.unique())
        f2 = set(c.Target.unique())

        temp_nodes_weights_ids1 = list(f1.union(f2))

        Weight_vector = np.zeros(user_count)

        sum_weight = 0
        if len(temp_nodes_ids1) > 0:
            # Calculate the weight for each node
            for ctr2 in temp_nodes_ids1:
                X = 0
                if ctr2 in temp_nodes_weights_ids1:
                    c1 = c.loc[
                        (
                            (np.isin(c.Source, [node_id1, node_id2]))
                            & (c.Target.values == ctr2)
                        )
                        | (
                            (c.Source.values == ctr2)
                            & (np.isin(c.Target, [node_id1, node_id2]))
                        )
                    ]
                    X = c1["Weight"].max()

                X = (max_weight1 - X) / max_weight1
                Weight_vector[ctr2] = X
                sum_weight += X

            # Calculate the JSD divergence
            User_IDs = set(temp_nodes_ids1)
            R_temp = (
                np.multiply(user_link_prob[i, :], Weight_vector[i]) for i in User_IDs
            )
            Z = np.sum(R_temp, axis=0)
            Z = np.divide(Z, sum_weight)
            JSD1 = JSD_divergence(user_link_prob[node_id1], Z)
            JSD2 = JSD_divergence(user_link_prob[node_id2], Z)
            JSD3 = JSD_divergence(user_link_prob[node_id1], user_link_prob[node_id2])
            new_weight = np.subtract(np.min([JSD1, JSD2]), JSD3)

        for i in range(len(matrix)):
            From_ID.append(node_id1)
            To_ID.append(node_id2)
            Numeric_Link_ID.append(matrix[i][2])
            x = matrix[i][3]
            if len_temp_nodes_ids1 > 0:
                divided = sum_weight / len_temp_nodes_ids1
                x = x * (1 - (divided)) + x * new_weight * (divided)
            Weight.append(x)
            PostIDs_1.append(matrix[i][4])
            PostIDs_2.append(matrix[i][5])

    Result_df = pd.DataFrame(
        {
            "From": From_ID,
            "To": To_ID,
            "Numeric_Link_ID": Numeric_Link_ID,
            "Weight": Weight,
            "PostIDs_from": PostIDs_1,
            "PostIDs_to": PostIDs_2,
        }
    )
    Result_df = Result_df.loc[Result_df.Weight.values > 0]
    return Result_df
