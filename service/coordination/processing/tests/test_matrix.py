import pandas as pd
import numpy as np

from ..matrix import (
    dtw,
    unequal_chunks,
    user_link_matrix,
    create_edges,
    calculate_edge_weight,
)


class TestDTW:
    def test_simple_case(self):
        """Test the Dynamic Time Warping function with a simple case"""
        dist_mat = np.array([[0, 1, 2], [1, 0, 1], [2, 1, 0]])
        result = dtw(dist_mat)

        # Expected result for this simple case
        expected = np.array([[0, 1, 3], [1, 0, 1], [3, 1, 0]])

        np.testing.assert_array_equal(result, expected)

    def test_zero_distance(self):
        dist_mat = np.zeros((3, 3))
        result = dtw(dist_mat)

        # Expected result should also be zeros
        expected = np.zeros((3, 3))

        np.testing.assert_array_equal(result, expected)

    def test_asymmetric_matrix(self):
        dist_mat = np.array([[0, 1], [2, 3], [4, 5]])
        result = dtw(dist_mat)

        # The actual expected result based on the DTW implementation
        expected = np.array([[0, 1], [2, 3], [6, 7]])

        np.testing.assert_array_equal(result, expected)

        # Also verify the structure of the result
        assert result.shape == (3, 2)
        assert result[0, 0] == 0 and result[0, 1] == 1
        assert result[1, 0] == 2 and result[1, 1] == 3
        assert result[2, 0] == 6 and result[2, 1] == 7


class TestUnequalChunks:
    def test_basic_chunking(self):
        # Test basic chunking functionality
        test_list = [[1, 1], [2, 2], [3, 3], [4, 4], [5, 5]]
        chunk_size = 2

        result = unequal_chunks(test_list, chunk_size)

        # Should split into chunks of size 2
        expected = [[[1, 1], [2, 2]], [[3, 3], [4, 4]], [[5, 5]]]

        assert result == expected

    def test_same_behaviour_in_same_chunk(self):
        # Test that items with same behaviour stay in the same chunk
        test_list = [
            [1, 1],
            [1, 1],  # Same behaviour
            [2, 2],
            [3, 3],
            [4, 4],
            [4, 4],  # Same behaviour
            [5, 5],
        ]
        chunk_size = 2

        result = unequal_chunks(test_list, chunk_size)

        # First chunk should include both [1,1] entries
        expected = [[[1, 1], [1, 1]], [[2, 2], [3, 3]], [[4, 4], [4, 4]], [[5, 5]]]

        assert result == expected

    def test_empty_list(self):
        # Test with an empty list
        test_list = []
        chunk_size = 2

        result = unequal_chunks(test_list, chunk_size)

        # Should return an empty list
        expected = []

        assert result == expected


class TestUserLinkMatrix:
    def test_basic_matrix_creation(self):
        # Create a test dataframe
        posts_data = {"Numeric_UID": [0, 1, 0, 2], "Numeric_Link_ID": [0, 1, 2, 0]}
        posts_df = pd.DataFrame(posts_data)

        user_count = 3  # Users 0, 1, 2
        link_count = 3  # Links 0, 1, 2

        result = user_link_matrix(posts_df, user_count, link_count)

        # Expected matrix:
        # User 0 has links 0 and 2
        # User 1 has link 1
        # User 2 has link 0
        expected = np.array(
            [
                [1, 0, 1],  # User 0
                [0, 1, 0],  # User 1
                [1, 0, 0],  # User 2
            ]
        )

        np.testing.assert_array_equal(result, expected)

    def test_empty_dataframe(self):
        # Test with an empty dataframe
        posts_df = pd.DataFrame({"Numeric_UID": [], "Numeric_Link_ID": []})

        user_count = 2
        link_count = 2

        result = user_link_matrix(posts_df, user_count, link_count)

        # Expected: all zeros
        expected = np.zeros((user_count, link_count))

        np.testing.assert_array_equal(result, expected)


class TestCreateEdges:
    def test_basic_edge_creation(self):
        # Test data
        users_behaviour = [[0.8, 0.7, 0.5], [0.9, 0.8]]
        edges_users = [[101, 102, 103], [201, 202]]
        coordinated_link = [1, 2]
        post_ids = [[["p1"], ["p2"], ["p3"]], [["p4"], ["p5"]]]

        result = create_edges(users_behaviour, edges_users, coordinated_link, post_ids)

        # Check the shape of the result
        assert isinstance(result, pd.DataFrame)
        assert (
            len(result) == 4
        )  # Should have 4 pairs: (101,102), (101,103), (102,103), (201,202)

        # Check that first group has 3 pairs from the first link
        first_link_pairs = result[result["Numeric_Link_ID"] == 1]
        assert len(first_link_pairs) == 3

        # Check that second group has 1 pair from the second link
        second_link_pairs = result[result["Numeric_Link_ID"] == 2]
        assert len(second_link_pairs) == 1

        # Check columns
        expected_columns = [
            "From",
            "To",
            "Beha_1",
            "Beha_2",
            "Numeric_Link_ID",
            "PostIDs_1",
            "PostIDs_2",
        ]
        assert all(col in result.columns for col in expected_columns)

        # Check that pairs are ordered correctly (From < To)
        assert all(result["From"] < result["To"])

        # Check data consistency
        assert result.loc[0, "From"] == 101 and result.loc[0, "To"] == 102
        assert result.loc[1, "From"] == 101 and result.loc[1, "To"] == 103
        assert result.loc[2, "From"] == 102 and result.loc[2, "To"] == 103
        assert result.loc[3, "From"] == 201 and result.loc[3, "To"] == 202

    def test_empty_input(self):
        # Test with empty inputs
        users_behaviour = []
        edges_users = []
        coordinated_link = []
        post_ids = []

        result = create_edges(users_behaviour, edges_users, coordinated_link, post_ids)

        # Should return empty dataframe with expected columns
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        expected_columns = [
            "From",
            "To",
            "Beha_1",
            "Beha_2",
            "Numeric_Link_ID",
            "PostIDs_1",
            "PostIDs_2",
        ]
        assert all(col in result.columns for col in expected_columns)


class TestCalculateEdgeWeight:
    def test_basic_weight_calculation(self):
        # Create test dataframe
        df_data = {
            "From": [1, 1, 2, 2],
            "To": [2, 2, 3, 3],
            "Weight": [0.5, 0.3, 0.7, 0.2],
        }
        df = pd.DataFrame(df_data)

        result = calculate_edge_weight(df)

        # Check the result
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2  # Should have 2 unique pairs: (1,2) and (2,3)

        # Check columns
        assert all(col in result.columns for col in ["Source", "Target", "Weight"])

        # Check weights are summed correctly
        # Weight for (1,2) should be 0.5 + 0.3 = 0.8
        # Weight for (2,3) should be 0.7 + 0.2 = 0.9
        expected_weights = {(1, 2): 0.8, (2, 3): 0.9}

        for _, row in result.iterrows():
            pair = (row["Source"], row["Target"])
            assert abs(row["Weight"] - expected_weights[pair]) < 1e-6

    def test_empty_dataframe(self):
        # Test with empty dataframe
        df = pd.DataFrame({"From": [], "To": [], "Weight": []})

        result = calculate_edge_weight(df)

        # Should return empty dataframe with expected columns
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert all(col in result.columns for col in ["Source", "Target", "Weight"])
