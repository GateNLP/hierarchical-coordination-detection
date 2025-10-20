import pytest
import pandas as pd

from ..coordination import (
    assign_numerical_ID,
    recursive_remove,
    filter_top_users,
    generate_result,
)


@pytest.fixture
def sample_posts_df():
    """Create a sample DataFrame for testing."""
    data = {
        "UserID": [1, 2, 1, 3, 2, 3],
        "Link": ["A", "B", "A", "C", "A", "B"],
        "PostID": [101, 102, 103, 104, 105, 106],
        "Timestamp": [
            1620000000,
            1620000001,
            1620000002,
            1620000003,
            1620000004,
            1620000005,
        ],
        "Text": ["text1", "text2", "text3", "text4", "text5", "text6"],
    }
    return pd.DataFrame(data)


@pytest.fixture
def sample_posts_df_2():
    data = {
        "UserID": [1, 2, 4, 3, 3, 2, 1],
        "Link": ["A", "B", "B", "A", "C", "C", "D"],
        "PostID": [101, 102, 102, 104, 105, 106, 107],
    }
    return pd.DataFrame(data)


@pytest.fixture
def sample_posts_df_stable():
    """Create a sample DataFrame that will have remaining data after recursive_remove.
    No rows should be removed.
    """
    data = {
        "UserID": [1, 1, 2, 2, 3, 3, 4, 4, 5, 5],
        "Link": ["A", "B", "A", "C", "B", "D", "C", "D", "A", "B"],
        "PostID": [101, 102, 103, 104, 105, 106, 107, 108, 109, 110],
        "Timestamp": [
            1620000000,
            1620000001,
            1620000002,
            1620000003,
            1620000004,
            1620000005,
            1620000006,
            1620000007,
            1620000008,
            1620000009,
        ],
        "Text": [
            "text1",
            "text2",
            "text3",
            "text4",
            "text5",
            "text6",
            "text7",
            "text8",
            "text9",
            "text10",
        ],
    }
    return pd.DataFrame(data)


@pytest.fixture
def sample_result_df():
    """Create a sample result DataFrame for testing."""
    data = {
        "From": [0, 1, 2],
        "To": [1, 2, 0],
        "Numeric_Link_ID": [0, 1, 2],
        "Weight": [0.8, 0.6, 0.4],
        "PostIDs_from": ["101,103", "102,105", "104,106"],
        "PostIDs_to": ["105", "106", "101,103"],
    }
    return pd.DataFrame(data)


class TestAssignNumericalID:
    def test_assign_numerical_id_user(self, sample_posts_df):
        """Test assigning numerical IDs to UserID field."""
        result_df, unique_count = assign_numerical_ID(
            sample_posts_df, "UserID", "Numeric_UID"
        )

        assert unique_count == 3

        # Check if all original users got mapped to some ID
        assert set(result_df["Numeric_UID"].unique()) == {0, 1, 2}

        # Check if the mapping is consistent
        user_id_map = (
            result_df[["UserID", "Numeric_UID"]]
            .drop_duplicates()
            .set_index("UserID")["Numeric_UID"]
            .to_dict()
        )
        for index, row in result_df.iterrows():
            assert row["Numeric_UID"] == user_id_map[row["UserID"]]

    def test_assign_numerical_id_link(self, sample_posts_df):
        """Test assigning numerical IDs to Link field."""
        result_df, unique_count = assign_numerical_ID(
            sample_posts_df, "Link", "Numeric_Link_ID"
        )

        assert unique_count == 3

        # Check if all original links got mapped to some ID
        assert set(result_df["Numeric_Link_ID"].unique()) == {0, 1, 2}

        # Check if the mapping is consistent
        link_id_map = (
            result_df[["Link", "Numeric_Link_ID"]]
            .drop_duplicates()
            .set_index("Link")["Numeric_Link_ID"]
            .to_dict()
        )
        for index, row in result_df.iterrows():
            assert row["Numeric_Link_ID"] == link_id_map[row["Link"]]

    def test_assign_numerical_id_empty_df(self):
        """Test assigning numerical IDs to an empty DataFrame."""
        empty_df = pd.DataFrame({"UserID": [], "Link": []})
        result_df, unique_count = assign_numerical_ID(empty_df, "UserID", "Numeric_UID")

        assert unique_count == 0
        assert len(result_df) == 0


class TestRecursiveRemove:
    def test_recursive_remove_no_change(self):
        """Test recursive_remove when no rows should be removed."""
        # Create a DataFrame where all links are shared by multiple users and all users share multiple links
        data = {
            "UserID": [1, 2, 1, 2],
            "Link": ["A", "A", "B", "B"],
            "PostID": [101, 102, 103, 104],
        }
        df = pd.DataFrame(data)

        result = recursive_remove(df)
        assert len(result) == 4  # No rows should be removed

    @pytest.mark.parametrize(
        "fixture_name",
        ["sample_posts_df", "sample_posts_df_2", "sample_posts_df_stable"],
    )
    def test_recursive_remove_with_change(self, request, fixture_name):
        """Test recursive_remove with expected removals."""

        sample_posts_df = request.getfixturevalue(fixture_name)

        result = recursive_remove(sample_posts_df)

        assert "UserID" in result.columns
        assert "Link" in result.columns
        assert "PostID" in result.columns

        # Verify that some filtering happened
        assert len(result) <= len(sample_posts_df)

        # Verify the function's general purpose:
        # Links shared by only one user should be removed
        link_user_counts = result.groupby("Link")["UserID"].nunique()

        for link, count in link_user_counts.items():
            assert count > 1, f"Link {link} is only shared by {count} user"

        # Users who share only one link should be removed
        user_link_counts = result.groupby("UserID")["Link"].nunique()

        for user, count in user_link_counts.items():
            assert count > 1, f"User {user} only shares {count} link"


class TestFilterTopUsers:
    def test_filter_top_users(self, sample_posts_df):
        """Test filtering top percentage of users."""
        # Filter top 50% users
        result = filter_top_users(sample_posts_df, 50)

        # Check that the function filtered some users (not returning all users)
        assert set(result["UserID"].unique()) < set(sample_posts_df["UserID"].unique())

        # Check that it kept high-frequency users
        # First get the user counts from the original DataFrame
        user_counts = sample_posts_df["UserID"].value_counts()

        # Get the highest count
        highest_count = user_counts.max()

        # Verify that all users in the filtered result have the highest count
        for user in result["UserID"].unique():
            assert user_counts[user] == highest_count, (
                f"User {user} doesn't have the highest count"
            )

    def test_filter_top_users_all(self, sample_posts_df):
        """Test filtering 100% of users (no filtering)."""
        result = filter_top_users(sample_posts_df, 100)

        # No users should be filtered out
        assert set(result["UserID"].unique()) == set(sample_posts_df["UserID"].unique())
        assert len(result) == len(sample_posts_df)

    def test_filter_top_users_none(self, sample_posts_df):
        """Test filtering 0% of users (all filtered out)."""
        result = filter_top_users(sample_posts_df, 0)

        # All users should be filtered out
        assert len(result) == 0


class TestGenerateResult:
    def test_generate_result(self, sample_posts_df, sample_result_df):
        """Test generating the final result DataFrame."""
        # First assign numerical IDs to the sample posts
        posts_with_ids, _ = assign_numerical_ID(
            sample_posts_df, "UserID", "Numeric_UID"
        )
        posts_with_ids, _ = assign_numerical_ID(
            posts_with_ids, "Link", "Numeric_Link_ID"
        )

        result = generate_result(sample_result_df, posts_with_ids)

        # Check if the columns are in the expected order
        expected_columns = [
            "From",
            "To",
            "Link",
            "Weight",
            "PostIDs_from",
            "PostIDs_to",
        ]
        assert list(result.columns) == expected_columns

        # Check if the result is sorted by weight in descending order
        assert result["Weight"].is_monotonic_decreasing

        # Check if the numerical IDs were correctly mapped back to original IDs
        # The From/To in sample_result_df were [0, 1, 2], which should map to UserIDs [1, 2, 3]
        # The Numeric_Link_ID in sample_result_df were [0, 1, 2], which should map to Links ['A', 'B', 'C']
        expected_from = [1, 2, 3]
        expected_to = [2, 3, 1]
        expected_link = ["A", "B", "C"]

        assert list(result["From"]) == expected_from
        assert list(result["To"]) == expected_to
        assert list(result["Link"]) == expected_link
