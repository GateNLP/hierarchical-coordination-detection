import pandas as pd
import numpy as np
from typing import List, Optional, Dict, Any, Set, Callable, Type
from collections import defaultdict

import networkx as nx
from community import community_louvain

from ..config import IndexMapping, ElasticsearchConfig
from ..model import ElasticsearchJob

from ..types.graph import CoordinationEdge, UserNode
from ..types.results import CoordinationResult
from ..types.base import CoordinationDataFrame, PostID

from ..data_sources.es_handler import build_es_query

from logging import getLogger

logger = getLogger(__name__)


def generate_json_result(
    df: CoordinationDataFrame,
    raw_data: pd.DataFrame,
    index_config: Optional[IndexMapping] = None,
    post_data: Optional[Dict[str, Any]] = None,
    es_config: Optional[ElasticsearchConfig] = None,
    job_config: Optional[ElasticsearchJob] = None
) -> CoordinationResult:
    """
    Generate JSON result for the coordination data.
    """
    nodes: List[UserNode] = []
    edges: List[CoordinationEdge] = []

    # we don't want to pass back the data for all the posts, just those that contribiute
    # to a node or edge so we have a set here where we can collect them. Note that we only
    # need to collect when processing nodes as they contain all the relevant posts for each
    # user, so there won't be anything extra on the edges.
    ids: Set[PostID] = set()

    combined = pd.pivot_table(
        df,
        index=["From", "To"],
        values=["Weight", "Link", "PostIDs_from", "PostIDs_to"],
        aggfunc={
            "Weight": ["sum", list],
            "Link": list,
            "PostIDs_from": list,
            "PostIDs_to": list,
        },
    )

    if combined.empty:
        return dict(nodes=nodes, edges=edges)

    node_names = (pd.concat([df["From"], df["To"]])).unique()

    aggregations = {"screen_name":("Screen_Name", "first"), "posts":("Post_ID", list)}

    if index_config is not None and index_config.extra_fields:
        aggregation_map: Dict[str, Callable] = {
            "first": "first",
            "list": list,
            "max": "max",
            "min": "min",
            "sum": "sum",
            "mean": "mean"
        }
        for extra in index_config.extra_fields:
            aggregations[extra] = (extra, aggregation_map[index_config.extra_fields[extra].operation])

    post_text = (
        raw_data.groupby(["User_ID"])
        .agg(**aggregations)
        .reset_index()
    )
    post_text = post_text[np.isin(post_text["User_ID"], node_names)]

    combined = combined.sort_values(by=("Weight", "sum"), ascending=False).reset_index()
    combined.columns = ["from", "to", "hashtags", "source", "target", "weights", "size"]

    # we don't really need the NetworkX graph as a graph (we generate our own graphology
    # format dict later) but this is the easiest way of getting data into the format that
    # the louvain lib uses to do the community detection so.....
    G = nx.from_pandas_edgelist(combined, "from", "to", "size")

    # I've fixed the random_state used by the algorithm so that we get more consistent
    # results. Essentially louvain is none deterministic but I'd like thae same dataset
    # to produce the same set of clusters. Using a fixed random state make this more
    # likely and from testing I'm now seeing the same results.
    partition = community_louvain.best_partition(G, weight="size", random_state=42, randomize=False)

    partitionById = defaultdict(set)

    for _, row in post_text.iterrows():

        row_dict = row.to_dict()

        userId = str(row_dict["User_ID"])

        partitionId = partition[userId]

        partitionById[partitionId].add(userId)

        attributes = dict(
            label=str(row_dict["screen_name"]),
            posts=row_dict["posts"],
            community=partitionId,
        )

        if index_config is not None and index_config.extra_fields:
            for i, extra in enumerate(index_config.extra_fields):
                attributes[extra] = row_dict[extra]

        nodes.append(
            dict(key=str(str(row_dict["User_ID"])), attributes=attributes)
        )

        ids.update(row_dict["posts"])

    for row in combined.itertuples():
        edges.append(
            dict(
                source=str(row[1]),
                target=str(row[2]),
                attributes=dict(
                    size=row[7] * 1,
                    hashtags=row[3],
                    weights=row[6],
                    source=row[4],
                    target=row[5],
                ),
            )
        )

    result = dict(nodes=nodes, edges=edges)

    if post_data is not None:
        result.update({
            "posts": {key: post_data[key] for key in ids},
            "range": [raw_data["Timestamp"].min(), raw_data["Timestamp"].max()]
        })

    if index_config is not None and es_config is not None:
        # at this point we want to collect further info about each cluster
        # need to build the main query and add a filter for the cluster
        # users.
        # We will then add aggregations to get the other info we want

        # a list to store the partition data in. partitions are numbered
        # from zero upwards so we don't need a map just a list with the
        # results in the right order
        partition_data = {
            "text": []
        }

        if index_config.user_bio is not None:
            partition_data["bios"] = []

        final_query = build_es_query(job_config, index_config)

        with es_config.connect() as client:

            # then  for each partition
            for pId, user_ids in sorted(partitionById.items()):

                partition_query = {
                    "bool": {
                        "filter": [
                            final_query,
                            {
                                "terms": {
                                    index_config.user_id: list(user_ids)
                                }
                            }
                        ]
                    }
                }

                search_params = {
                    "query": partition_query,
                    "size": 0,
                    "aggregations": {}
                }

                search_params["aggregations"]["terms"] = {
                    "significant_text": {
                        "field": index_config.text,
                        "filter_duplicate_text": "true",
                        "size": 50
                    }
                }

                if index_config.user_bio is not None:
                    search_params["aggregations"]["bios"] = {
                        "significant_text": {
                            "field": index_config.user_bio,
                            "filter_duplicate_text": "true",
                            "size": 50
                        }
                    }

                if index_config.runtime_mappings:
                    search_params["runtime_mappings"] = index_config.runtime_mappings

                resp = client.search(index=index_config.index_name, body=search_params)

                if index_config.user_bio is not None:
                    bio_terms = []
                    for term in resp["aggregations"]["bios"]["buckets"]:
                        bio_terms.append(dict(text=term["key"], value=term["score"]))

                    partition_data["bios"].append(bio_terms)

                text_terms = []
                for term in resp["aggregations"]["terms"]["buckets"]:
                    text_terms.append(dict(text=term["key"], value=term["score"]))

                partition_data["text"].append(text_terms)

        result.update({"communities": partition_data})

    return result