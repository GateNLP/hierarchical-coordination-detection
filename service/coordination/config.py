import os
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Literal, Any

from dataclass_wizard import JSONWizard, YAMLWizard
from elasticsearch import Elasticsearch

from .datastore import Datastore


class ExpandEnvFiles(defaultdict):
    """
    Utility to expand format strings including ``{VAR_NAME}`` environment variable
    references, with two differences from a plain ``format_str.format(os.environ)``:

      1. Undefined variables expand to an empty string rather than raising an exception
      2. If the variable ``VAR_NAME`` is not set but the variable ``VAR_NAME_FILE``
         *is* set, then ``{VAR_NAME}`` expands to the contents of the file pointed
         to by ``VAR_NAME_FILE``.  In this case an exception *is* raised if the named
         file does not exist.
    """

    def __init__(self):
        super().__init__(str, os.environ)

    def __missing__(self, key):
        if f"{key}_FILE" in self:
            with open(self[f"{key}_FILE"], "r") as f:
                return f.read()
        return ""

    def expand(self, format_str: str | None):
        if format_str is None:
            return None
        return format_str.format_map(self)


@dataclass
class ExtraFieldMapping(JSONWizard):
    field: str
    operation: str
    timestamp: bool = False


@dataclass
class LinkType(JSONWizard):
    field: str | None = None
    """
    The default behaviour - take the value from a field in the index.
    """
    standard_pattern: str | None = None
    """
    Name of a standard pattern such as "url" or "hashtag" to look up
    in the text.
    """
    custom_pattern: str | None = None
    """
    Custom regular expression to run against the text.
    """
    lower_case: bool = False
    """
    Should we convert the link values (whether from a field or from a
    pattern) to lower case?
    """


DEFAULT_LINK_TYPE = LinkType(standard_pattern="hashtag", lower_case=True)


@dataclass
class IndexMapping(JSONWizard):
    user_id: str
    screen_name: str
    post_id: str
    timestamp: str
    text: str
    index_name: str = None
    embeddings_index_name: str = None
    """
    If specified, this is the separate index (pattern) that stores the
    embeddings used by the clustering module.  Normally the embeddings
    are a field in the main index, but it is possible to have a separate
    index or set of indexes containing just the document IDs and their
    embeddings.  If this value is set, then rather than taking the embeddings
    from the posts retrieved from the main index, the clustering module
    will make a secondary search on the embeddings index for the document
    IDs taken from the main index.
    """
    user_bio: str = None
    extra_fields: dict[str, ExtraFieldMapping] = None
    link_types: dict[str, list[LinkType | str]] = field(default_factory=dict)
    """
    Valid "link types" that this index supports.  Keys are the "link-type"
    value passed in with job configuration, the corresponding value is the
    list of one or more sources for linking values, where each source can
    be a document field from the index, a "standard" predefined pattern to
    search for in the text, or a custom regular expression.  For backwards
    compatibility with existing configurations, if a link type value is
    a plain string then it is treated as a field name in the index.
    
    If a job does not specify a link type, fall back to the standard
    behaviour of detecting hashtags in the text with a regular expression.
    """

    runtime_mappings: dict[str, Any] = None
    """
    Any runtime field mappings to add to the Elasticsearch query.  If you
    need to compute the screen name or user ID fields, you would define
    runtime mappings and then use the runtime field name as the appropriate
    configuration option.
    """

    examples: dict[str, Any] = None

    def __post_init__(self):
        """
        Post process the link_types map to convert old configs where the
        link type is a plain str into the new style LinkType(field=name)
        """
        if self.link_types:
            for links in self.link_types.values():
                for i in range(len(links)):
                    if isinstance(links[i], str):
                        links[i] = LinkType(field=links[i])

    def field_names(self, link_type=None):
        """
        Returns a list of the field names that we want the search to return,
        which is (a) the user_id, screen_name, post_id, timestamp and text
        fields, except if any of them is "_id" (as this refers to the document
        identifier, not to a real field), (b) any configured extra_fields, and
        (c) the fields corresponding to the requested link_type, if any.
        """
        fields = [
            f
            for f in (
                self.user_id,
                self.screen_name,
                self.post_id,
                self.timestamp,
                self.text,
            )
            if f != "_id"
        ]

        if self.extra_fields:
            for extra in self.extra_fields:
                fields.append(self.extra_fields[extra].field)

        if link_type and (links := self.link_types.get(link_type)):
            for link in links:
                if link.field is not None:
                    fields.append(link.field)

        return fields


@dataclass
class ElasticsearchConfig:
    hosts: list[str]
    username: str = None
    password: str = None
    api_key: str = None
    ca_certs: str = None
    timeout: int = 45
    max_retries: int = 3
    indexes: dict[str, IndexMapping] = field(default_factory=dict)

    def connect(self) -> Elasticsearch:
        env = ExpandEnvFiles()
        elastic_kwargs = {
            "hosts": [env.expand(val) for val in self.hosts],
            "timeout": self.timeout,
            "max_retries": self.max_retries,
        }
        if api_key := env.expand(self.api_key):
            elastic_kwargs["api_key"] = api_key
        else:
            if (username := env.expand(self.username)) and (
                password := env.expand(self.password)
            ):
                elastic_kwargs["http_auth"] = (username, password)
        if ca_certs := env.expand(self.ca_certs):
            elastic_kwargs["ca_certs"] = ca_certs

        return Elasticsearch(**elastic_kwargs)


@dataclass
class DatastoreConfig:
    type: Literal["shared_folder", "s3"] = "shared_folder"

    # Parameters for the "shared_folder" datastore type
    path: str | None = "/coordination/data"

    # Parameters for the "s3" datastore type
    bucket: str | None = None
    prefix: str | None = None

    def create_datastore(self) -> Datastore:
        env = ExpandEnvFiles()
        if self.type == "shared_folder":
            from .datastore.shared_folder import SharedFolderDatastore

            return SharedFolderDatastore(env.expand(self.path))

        if self.type == "s3":
            from .datastore.s3_datastore import S3Datastore

            return S3Datastore(env.expand(self.bucket), env.expand(self.prefix))

        raise ValueError(f"Unknown datastore type {self.type}")


@dataclass
class AppConfig(JSONWizard, YAMLWizard):
    # We have to opt in to "v1" behaviour here because without that it doesn't support
    # the "list[LinkType | str]" union type in IndexMapping
    class _(JSONWizard.Meta):
        v1 = True
        v1_unsafe_parse_dataclass_in_union = True
        v1_key_case = "A"

    elasticsearch: dict[str, ElasticsearchConfig] = field(default_factory=dict)
    datastore: DatastoreConfig = field(default_factory=DatastoreConfig)


if "COORDINATION_APP_CONFIG" in os.environ:
    APP_CONFIG = AppConfig.from_yaml_file(os.environ["COORDINATION_APP_CONFIG"])
else:
    print(
        "Warning: no COORDINATION_APP_CONFIG set in environment, using empty configuration."
    )
    APP_CONFIG = AppConfig()
