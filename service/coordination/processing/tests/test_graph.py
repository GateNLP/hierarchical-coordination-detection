import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import patch

from ..graph import multi_edge_graph, single_edge_graph_summation
from ...types.base import (
    MEBGraphDataFrame,
    PostsDataFrame,
)


# Fixtures for test data
@pytest.fixture
def sample_posts_df() -> PostsDataFrame:
    """Create a sample posts dataframe for testing."""
    # Create timestamps with controlled time decay
    base_date = datetime(2023, 1, 1)

    data = {
        "PostID": ["post1", "post2", "post3", "post4", "post5", "post6"],
        "UserID": ["user1", "user1", "user2", "user2", "user3", "user3"],
        "Link": ["link1", "link1", "link1", "link2", "link2", "link3"],
        "PostDate": [
            base_date,
            base_date + timedelta(days=7),
            base_date + timedelta(days=14),
            base_date + timedelta(days=21),
            base_date + timedelta(days=28),
            base_date + timedelta(days=35),
        ],
        "Numeric_UID": [1, 1, 2, 2, 3, 3],
        "Numeric_Link_ID": [101, 101, 101, 102, 102, 103],
    }

    df = pd.DataFrame(data)
    # Convert datetime to timestamp for consistency with the function
    df["PostDate"] = (df["PostDate"] - datetime(1970, 1, 1)).dt.total_seconds()
    return df


@pytest.fixture
def sample_meb_graph_df() -> MEBGraphDataFrame:
    """Create a sample multi-edge bipartite graph dataframe for testing."""
    data = {
        "UserID": ["user1", "user1", "user2", "user2", "user3"],
        "TimeDecay": [1.0, 0.5, 0.3, 0.7, 0.9],
        "Link": ["link1", "link1", "link1", "link2", "link3"],
        "Numeric_UID": [1, 1, 2, 2, 3],
        "Numeric_Link_ID": [101, 101, 101, 102, 103],
        "PostID": ["post1", "post2", "post3", "post4", "post6"],
    }
    return pd.DataFrame(data)


def test_multi_edge_graph_structure(sample_posts_df):
    """Test that multi_edge_graph returns a dataframe with the expected structure."""
    result = multi_edge_graph(sample_posts_df)

    # Check the resulting dataframe has the expected columns
    expected_columns = [
        "UserID",
        "TimeDecay",
        "Link",
        "Numeric_UID",
        "Numeric_Link_ID",
        "PostID",
    ]
    assert all(col in result.columns for col in expected_columns)

    # Check that we have the right number of rows (rows with TimeDecay > 0.00001)
    # In our sample data, all rows should be included
    assert len(result) > 0

    # Ensure TimeDecay values are between 0 and 1
    assert all(0 <= val <= 1 for val in result["TimeDecay"])


def test_multi_edge_graph_time_decay_calculation(sample_posts_df):
    """Test that the time decay calculation in multi_edge_graph works correctly."""
    result = multi_edge_graph(sample_posts_df)

    # For links with multiple posts, earlier posts should have higher decay values
    link1_posts = result[result["Link"] == "link1"].sort_values(by="PostID")
    if len(link1_posts) > 1:
        # Check that time decay is decreasing for later posts
        decay_values = link1_posts["TimeDecay"].tolist()
        for i in range(1, len(decay_values)):
            assert decay_values[i - 1] >= decay_values[i]


def test_multi_edge_graph_filtering(sample_posts_df):
    """Test that posts with TimeDecay <= 0.00001 are filtered out."""
    # Create a mock of math.exp to control the TimeDecay values
    with patch("math.exp", side_effect=[1.0, 0.5, 0.0000009, 0.7, 0.00001, 0.9]):
        result = multi_edge_graph(sample_posts_df)

        # Check that the resulting dataframe doesn't include the filtered out rows
        assert len(result) < len(sample_posts_df)
        assert all(val > 0.00001 for val in result["TimeDecay"])


# Tests for single_edge_graph_summation function
def test_single_edge_graph_structure(sample_meb_graph_df):
    """Test that single_edge_graph_summation returns a dataframe with the expected structure."""
    result = single_edge_graph_summation(sample_meb_graph_df)

    # Check the resulting dataframe has the expected columns
    expected_columns = [
        "UserID",
        "Usage",
        "Link",
        "Numeric_UID",
        "Numeric_Link_ID",
        "Number_link_used",
        "PostIDs",
    ]
    assert all(col in result.columns for col in expected_columns)

    # Check that we have the right number of rows
    # Each unique (UserID, Link) pair should result in one row
    expected_row_count = len(sample_meb_graph_df.groupby(["UserID", "Link"]))
    assert len(result) == expected_row_count


def test_single_edge_graph_usage_calculation(sample_meb_graph_df):
    """Test that the Usage calculation in single_edge_graph_summation works correctly."""
    result = single_edge_graph_summation(sample_meb_graph_df)

    # Check specific usage values for user1-link1 combination
    user1_link1 = result[(result["UserID"] == "user1") & (result["Link"] == "link1")]
    if not user1_link1.empty:
        # The usage should be the sum of TimeDecay values (1.0 + 0.5 = 1.5)
        expected_usage = 1.5
        assert abs(user1_link1["Usage"].iloc[0] - expected_usage) < 1e-6

    # Check that all usage values are positive
    assert all(result["Usage"] > 0)


def test_single_edge_graph_number_link_used(sample_meb_graph_df):
    """Test that Number_link_used is calculated correctly."""
    result = single_edge_graph_summation(sample_meb_graph_df)

    # Check specific Number_link_used values for user1-link1 combination
    user1_link1 = result[(result["UserID"] == "user1") & (result["Link"] == "link1")]
    if not user1_link1.empty:
        # User1 used link1 twice in our sample data
        expected_count = 2
        assert user1_link1["Number_link_used"].iloc[0] == expected_count

    # Check that all Number_link_used values are positive integers
    assert all(isinstance(val, int) and val > 0 for val in result["Number_link_used"])


def test_single_edge_graph_post_ids(sample_meb_graph_df):
    """Test that PostIDs are collected correctly."""
    result = single_edge_graph_summation(sample_meb_graph_df)

    # Check specific PostIDs for user1-link1 combination
    user1_link1 = result[(result["UserID"] == "user1") & (result["Link"] == "link1")]
    if not user1_link1.empty:
        # User1's posts for link1 should be ["post1", "post2"]
        expected_post_ids = ["post1", "post2"]
        assert set(user1_link1["PostIDs"].iloc[0]) == set(expected_post_ids)

    # Check that all PostIDs lists are non-empty
    assert all(len(post_ids) > 0 for post_ids in result["PostIDs"])


def test_end_to_end_processing(sample_posts_df):
    """Test the entire processing pipeline from posts to single-edge graph."""
    meb_graph = multi_edge_graph(sample_posts_df)
    seb_graph = single_edge_graph_summation(meb_graph)

    print("Multi-edge graph:", len(meb_graph))
    print("Single-edge graph:", len(seb_graph))

    expected_row_count = len(pd.DataFrame(meb_graph).groupby(["UserID", "Link"]))
    assert len(seb_graph) == expected_row_count

    # Verify that the PostIDs in the single-edge graph trace back to the original posts
    for _, row in seb_graph.iterrows():
        user_id = row["UserID"]
        link = row["Link"]
        post_ids = row["PostIDs"]

        # Check that each PostID in the list corresponds to a post in the original dataframe
        # with the matching UserID and Link
        for post_id in post_ids:
            matching_posts = sample_posts_df[
                (sample_posts_df["UserID"] == user_id)
                & (sample_posts_df["Link"] == link)
                & (sample_posts_df["PostID"] == post_id)
            ]
            assert len(matching_posts) == 1, (
                f"PostID {post_id} not found for user {user_id} and link {link}"
            )
