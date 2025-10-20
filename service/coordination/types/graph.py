from typing import Dict, List, Any, TypedDict

from .base import UserID, Weight


class UserNode(TypedDict):
    """Represents a user node in the coordination graph."""

    key: str
    attributes: Dict[str, Any]


class EdgeAttributes(TypedDict):
    """Attributes for edges in the coordination graph."""

    size: float
    hashtags: List[str]
    weights: List[float]
    source: List[str]
    target: List[str]


class CoordinationEdge(TypedDict):
    """Represents an edge between users in the coordination graph."""

    source: str
    target: str
    attributes: EdgeAttributes


class UserPair(TypedDict):
    """Represents a pair of users with their coordination metrics."""

    Source: UserID
    Target: UserID
    Weight: Weight


class UserBehaviour(TypedDict):
    """Represents user behaviour in sharing entities."""

    From: int  # Numeric user ID
    To: int  # Numeric user ID
    Beha_1: float  # Behaviour measure for first user
    Beha_2: float  # Behaviour measure for second user
    Numeric_Link_ID: int  # Numeric link ID
    PostIDs_1: List[str]  # Post IDs for first user
    PostIDs_2: List[str]  # Post IDs for second user
