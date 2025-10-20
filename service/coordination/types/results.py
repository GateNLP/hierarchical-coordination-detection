from typing import Dict, List, Any, Optional, TypedDict

from .graph import UserNode, CoordinationEdge


class CoordinationResult(TypedDict):
    """Final result of coordination analysis."""

    nodes: List[UserNode]
    edges: List[CoordinationEdge]
    posts: Optional[Dict[str, Any]]


class CoordinationEdgeResult(TypedDict):
    """Represents a coordination edge in the final result."""

    From: str  # Source user ID
    To: str  # Target user ID
    Link: str  # Shared entity (hashtag)
    Weight: float  # Coordination weight
    PostIDs_from: List[str]  # Post IDs from source user
    PostIDs_to: List[str]  # Post IDs from target user
