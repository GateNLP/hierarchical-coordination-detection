from dataclasses import dataclass, field
from typing import Any

from dataclass_wizard import JSONWizard, JSONFileWizard

from .types.base import SpeedOption


@dataclass
class ElasticsearchJob(JSONWizard, JSONFileWizard):
    """
    Class representing a job that takes its input from Elasticsearch
    """

    elasticsearch: str
    """
    The name of the elasticsearch connection configuration - must be a
    valid key in the ``AppConfig.elasticsearch`` dict.
    """

    index: str
    """
    The name of the index configuration with the configured elasticsearch
    config block - must be a valid key in the ``indexes`` dict of the selected
    config.
    """

    query: dict[str, Any]
    """
    Elasticsearch query DSL query pulling the documents for this job.
    """

    expected_hits: int = 0
    """
    Number of hits that you expect the query to return.  This does not
    necessarily have to be an accurate number, the purpose is to determine
    whether a cached result from a previous job based on the same query
    is still valid or whether the network needs to be re-computed following
    changes to the index.
    """

    ignore: list[str] = field(default_factory=list)
    """
    Hashtags to ignore when building the coordination graph.
    """

    link_type: str = None
    """
    Type of links to consider when building the coordination graph.
    If omitted, the service will use a regular expression to detect
    hashtags in the document text.
    """

    speed: SpeedOption = 3
    """
    The speed option to use when generating the network. Value can be 1, 2 or 3.
    1: seems to just generate the edges based on the entities (i.e. no filtering).
    2: does the pairwise level filtering.
    3: does both pairwise and group level filtering.
    """

    def to_canonical_json(self) -> bytes:
        """
        Write this job configuration to JSON bytes in a "canonical" form (UTF-8
        character encoding, avoiding ``\\uNNNN`` escapes where possible,
        dict keys sorted alphabetically, compact representation with no
        spaces around the ``,`` and ``:`` separators).  The same settings
        should always serialize byte-for-byte as the same JSON, so the MD5
        hash is consistent.
        """
        return self.to_json(
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
