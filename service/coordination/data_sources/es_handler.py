import logging
import re
from typing import Any, Dict, Tuple, List, Literal

import pandas as pd

from ..types.data import HashtagData, PostData, RawPostData
from ..types.base import PostsDataFrame, RawDataFrame

from ..model import ElasticsearchJob
from ..config import IndexMapping, ElasticsearchConfig, DEFAULT_LINK_TYPE

from ..utils.text import extract_hashtags, extract_urls

logger = logging.getLogger(__name__)


def build_es_query(
    job_config: ElasticsearchJob, index_config: IndexMapping
) -> Dict[str, Any]:
    """
    Build the Elasticsearch query based on the job configuration.
    """
    if (
        job_config.link_type
        and job_config.link_type in index_config.link_types
        and any(
            f.field is not None for f in index_config.link_types[job_config.link_type]
        )
    ):
        # we want to add some extra clauses to the query to filter for
        # only documents that contain a value for at least one of the
        # linking fields.  We do this with a "bool" query with a "should"
        # list requiring each of the link fields to exist, and
        # minimum_should_match set to 1 so at least one of them must exist.
        # As to where we put that clause - if the top-level query is
        # a bool query and doesn't already have a should section then
        # we can simply insert our "should" list into that query, but if
        # the top-level query is another type or is a bool that already has
        # a "should" section, then we wrap it in another layer of "bool" with
        # the original query as a "filter"
        if "bool" in job_config.query and "should" not in job_config.query["bool"]:
            final_query = job_config.query
        else:
            final_query = {
                "bool": {
                    "filter": [job_config.query],
                }
            }

        # Create one "exists" query for each field name in the link type
        final_query["bool"]["should"] = [
            {"exists": {"field": f.field}}
            for f in index_config.link_types[job_config.link_type]
            if f.field is not None
        ]
        final_query["bool"]["minimum_should_match"] = 1

    else:
        # No link_type, or the link_type value is not one we support
        final_query = job_config.query

    return final_query


def _flatten_into(
    target: dict[str, list],
    dictionary: dict[str, list],
    parent_key: str = "",
    separator=".",
) -> None:
    """
    Recursively flatten a fields dictionary
    :param target: the target dictionary into which we are flattening
    :param dictionary: the current dictionary we want to traverse.  If any of
            its values are nested fields then we will recurse into those.
    :param parent_key: the ``separator``-separated key path to the current
            ``dictionary`` from the original root
    :param separator: separator to separate this level of keys from the next
    """
    for key, vals in dictionary.items():
        full_key = (parent_key + separator + key) if parent_key else key
        for value in vals:
            if isinstance(value, dict):
                _flatten_into(target, value, full_key, separator=separator)
            else:
                # This would be easier with a defaultdict but that would mess
                # up the REQUIRED logic in the fld function
                if (lst := target.get(full_key)) is None:
                    lst = []
                    target[full_key] = lst
                lst.append(value)


def flatten(dictionary: dict[str, list], separator=".") -> None:
    """
    Takes a nested dictionary as returned as the ``fields`` property of an elasticsearch
    hit and flattens it *in place* to a one level dict where the keys
    use standard dot notation to refer to the leaves. All values are lists even when
    only one value is present. This may seem an odd thing to do, but it allows us to
    take a ``fields`` object from an Elasticsearch hit and flatten out any nested fields
    allowing them to be referenced using the simple dot notation.

    This will only work for well-formed elasticsearch "fields" dictionaries, in
    particular it must not be the case that the initial dict already contains the
    both a "flat" key ``foo.bar`` *and* a nested key that would flatten to the same
    path.
    """
    # Freeze the initial list of keys before the dictionary changes
    orig_keys = list(dictionary.keys())
    for k in orig_keys:
        values = dictionary[k]
        if values and isinstance(values[0], dict):
            # remove the "branch" node from the original dict
            del dictionary[k]
            # and recursively flatten the leaves of each of its values
            for value in values:
                _flatten_into(dictionary, value, k, separator=separator)


# Sentinel default value for fld "default" parameter to distinguish
# between "no default, field is required" vs "field is optional, default None"
REQUIRED = object()


def fld(
    hit,
    name: str,
    default: Any = REQUIRED,
    *,
    operation: Literal["first", "max"] = "first",
) -> str:
    """
    Extract a field value from a search hit.
    :param hit: the search hit, as yielded by helpers.scan, with its "fields" flattened
    :param name: the field name
    :param default: the default value to return if the field isn't found. If omitted,
            the field is required.
    :param operation: the operation to perform on the field - "first" to take
            the first value in the list, "max" to take the highest value.
    :return: if the specified ``name`` is ``_id``, the document ID,
             otherwise the first value in ``hit["fields"][name]``
    """
    if name == "_id":
        return hit["_id"]
    if default is REQUIRED:
        vals = hit["fields"][name]
    else:
        vals = hit["fields"].get(name, [default])

    match operation:
        case "max":
            return max(vals)
        case _:
            return vals[0]


def process_es_data(
    es_config: ElasticsearchConfig,
    index_config: IndexMapping,
    job_config: ElasticsearchJob,
    search_params: Dict[str, Any],
) -> Tuple[PostsDataFrame, RawDataFrame, Dict[str, PostData]]:
    """
    Process the data returned from Elasticsearch.
    """
    from elasticsearch import helpers

    with es_config.connect() as client:
        data = helpers.scan(
            client,
            index=index_config.index_name,
            _source=False,
            fields=index_config.field_names(job_config.link_type),
            size=1000,
            track_total_hits=True,
            query=search_params,
        )

        hashtag_data: List[HashtagData] = []

        raw_data: List[RawPostData] = []

        post_data: Dict[str, PostData] = {}

        skipped: int = 0

        for d in data:
            try:
                # flatten the fields object so we can handle any nested fields
                # using the simple dot notation
                flatten(d["fields"])

                # Entries from "fields" are _always_ arrays
                user_id = fld(d, index_config.user_id)
                # If there is no screen name, use the user ID as the name
                screen_name = fld(d, index_config.screen_name, user_id)
                post_id = fld(d, index_config.post_id)
                timestamp = fld(d, index_config.timestamp)
                timestamp = pd.Timestamp(timestamp.split("+")[0]).timestamp()
                # If there is no text, assume an empty string
                text = fld(d, index_config.text, "")

                raw_row = {
                    "User_ID": user_id,
                    "Screen_Name": screen_name,
                    "Post_ID": post_id,
                    "Timestamp": timestamp,
                }

                if index_config.extra_fields:
                    for extra, spec in index_config.extra_fields.items():
                        field = fld(d, spec.field, operation=spec.operation)

                        if spec.timestamp:
                            field = pd.Timestamp(field).timestamp()

                        raw_row[extra] = field

                raw_data.append(raw_row)

                links = [DEFAULT_LINK_TYPE]
                if (
                    job_config.link_type
                    and job_config.link_type in index_config.link_types
                ):
                    links = index_config.link_types[job_config.link_type]

                hashtags = set()
                for link in links:
                    field_values = set()
                    # Extract the values for this link type
                    if link.field is not None:
                        field_values = set(d["fields"].get(link.field, []))
                    elif link.standard_pattern == "hashtag":
                        field_values = set(extract_hashtags(text))
                    elif link.standard_pattern == "url":
                        field_values = set(extract_urls(text))
                    elif link.custom_pattern is not None:
                        field_values = set(re.findall(link.custom_pattern, text))

                    if link.lower_case:
                        hashtags.update(f.lower() for f in field_values)
                    else:
                        hashtags.update(field_values)

                hashtags = hashtags.difference(job_config.ignore)

                post_data[post_id] = {"time": timestamp, "links": list(hashtags)}

                for hashtag in hashtags:
                    hashtag_data.append(
                        {
                            "UserID": user_id,
                            "Hashtag": hashtag,
                            "Time": timestamp,
                            "PostID": post_id,
                        }
                    )
            except KeyError:
                # Missing a required field
                skipped += 1

        logger.info("Processed %d hits.", len(raw_data))
        if skipped > 0:
            logger.warning("%d skipped due to missing fields", skipped)

        raw_data = pd.DataFrame(raw_data)

        hashtag_df = pd.DataFrame(hashtag_data)
        hashtag_df.columns = ["UserID", "Link", "PostDate", "PostID"]
        posts_df = hashtag_df.iloc[1:][["UserID", "Link", "PostDate", "PostID"]]

        return posts_df, raw_data, post_data
