"""
This module re-exports types from various submodules for easier importing.
"""

# Base types
from .base import (
    PostID,
    UserID,
    HashtagID,
    LinkID,
    TimeStamp,
    Weight,
    FloatArray,
    PostsDataFrame,
    RawDataFrame,
    MEBGraphDataFrame,
    SEBGraphDataFrame,
    CoordinationDataFrame,
)

# Data types
from .data import Post, HashtagData, RawPostData, PostData

# Graph types
from .graph import UserNode, EdgeAttributes, CoordinationEdge, UserPair, UserBehaviour

# Result types
from .results import CoordinationResult, CoordinationEdgeResult

# For backward compatibility, make all types available at module level
__all__ = [
    # Base types
    "PostID",
    "UserID",
    "HashtagID",
    "LinkID",
    "TimeStamp",
    "Weight",
    "FloatArray",
    "PostsDataFrame",
    "RawDataFrame",
    "MEBGraphDataFrame",
    "SEBGraphDataFrame",
    "CoordinationDataFrame",
    # Data types
    "Post",
    "HashtagData",
    "RawPostData",
    "PostData",
    # Graph types
    "UserNode",
    "EdgeAttributes",
    "CoordinationEdge",
    "UserPair",
    "UserBehaviour",
    # Result types
    "CoordinationResult",
    "CoordinationEdgeResult",
]
