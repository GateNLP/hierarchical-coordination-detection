import pytest
import numpy as np
from ..entropy import (
    calculate_entropy,
    update_entropy_incremental,
    find_max_entropy_partition,
    Entropy1,
    Entropy2,
    JSD_divergence,
    cosine_similarity,
)


class TestProcessingEntropy:
    def test_calculate_entropy_uniform(self):
        """Test entropy calculation with uniform distribution."""
        probabilities = [0.25, 0.25, 0.25, 0.25]
        expected = -np.sum(np.array(probabilities) * np.log(np.array(probabilities)))
        result = calculate_entropy(probabilities)
        assert np.isclose(result, expected)
        # Uniform distribution with 4 values should be log(4)
        assert np.isclose(result, np.log(4))

    def test_calculate_entropy_delta(self):
        """Test entropy calculation with delta distribution (one value = 1, others = 0)."""
        probabilities = [0, 0, 1, 0]
        result = calculate_entropy(probabilities)
        assert np.isclose(result, 0)

    def test_calculate_entropy_skewed(self):
        """Test entropy calculation with skewed distribution."""
        probabilities = [0.1, 0.2, 0.3, 0.4]
        expected = -np.sum(np.array(probabilities) * np.log(np.array(probabilities)))
        result = calculate_entropy(probabilities)
        assert np.isclose(result, expected)

    def test_calculate_entropy_unnormalized(self):
        """Test entropy calculation with unnormalized values."""
        unnormalized = [1, 2, 3, 4]
        normalized = np.array(unnormalized) / np.sum(unnormalized)
        expected = -np.sum(normalized * np.log(normalized))
        result = calculate_entropy(unnormalized)
        assert np.isclose(result, expected)

    def test_calculate_entropy_zeros(self):
        """Test entropy calculation with zeros."""
        probabilities = [0, 0, 0, 0]
        result = calculate_entropy(probabilities)
        assert np.isclose(result, 0)

    def test_update_entropy_incremental_basic(self):
        """Test incremental entropy update with simple case."""
        Y = [0.25, 0.25, 0.25]
        h1 = calculate_entropy(Y)
        m = 0.25

        # Expected result after adding m
        expected = calculate_entropy(Y + [m])
        result = update_entropy_incremental(h1, Y, m)
        assert np.isclose(result, expected)

    def test_update_entropy_incremental_edge_cases(self):
        """Test incremental entropy update with edge cases."""
        # Empty list
        result = update_entropy_incremental(0, [], 1.0)
        assert np.isclose(result, 0)

        # Zero m
        Y = [0.5, 0.5]
        h1 = calculate_entropy(Y)
        result = update_entropy_incremental(h1, Y, 0)
        assert np.isclose(result, h1)

    def test_find_max_entropy_partition_uniform(self):
        """Test finding max entropy partition with uniform distribution."""
        behaviours = np.array([0.25, 0.25, 0.25, 0.25])
        result = find_max_entropy_partition(behaviours)
        # For uniform distribution, any partition should be equally good
        assert result is not None

    def test_find_max_entropy_partition_skewed(self):
        """Test finding max entropy partition with skewed distribution."""
        behaviours = np.array([0.1, 0.2, 0.3, 0.4])
        result = find_max_entropy_partition(behaviours)
        assert result is not None

    def test_find_max_entropy_partition_zeros(self):
        """Test finding max entropy partition with zeros."""
        behaviours = np.array([0, 0, 0, 0])
        result = find_max_entropy_partition(behaviours)
        assert result is None

    def test_entropy1(self):
        """Test Entropy1 function."""
        X = np.array([0.1, 0.2, 0.3, 0.4])
        expected = -np.sum(X * np.log(X))
        result = Entropy1(X)
        assert np.isclose(result, expected)

    def test_entropy1_with_zeros(self):
        """Test Entropy1 function with zeros."""
        X = np.array([0, 0.5, 0, 0.5])
        expected = -np.sum(X[X > 0] * np.log(X[X > 0]))
        result = Entropy1(X)
        assert np.isclose(result, expected)

    def test_entropy2(self):
        """Test Entropy2 function."""
        X = np.array([0.1, 0.2, 0.3, 0.4])
        s = np.sum(X)
        expected = -np.sum((X / s) * np.log(X / s))
        result = Entropy2(X, s)
        assert np.isclose(result, expected)

    def test_entropy2_with_zeros(self):
        """Test Entropy2 function with zeros."""
        X = np.array([0, 0.5, 0, 0.5])
        s = np.sum(X)
        nonzero = X[X > 0]
        expected = -np.sum((nonzero / s) * np.log(nonzero / s))
        result = Entropy2(X, s)
        assert np.isclose(result, expected)

    def test_jsd_divergence_same_distribution(self):
        """Test JSD divergence with identical distributions."""
        dist = np.array([0.1, 0.2, 0.3, 0.4])
        result = JSD_divergence(dist, dist)
        assert np.isclose(result, 0)

    def test_jsd_divergence_different_distributions(self):
        """Test JSD divergence with different distributions."""
        dist1 = np.array([0.1, 0.2, 0.3, 0.4])
        dist2 = np.array([0.4, 0.3, 0.2, 0.1])
        result = JSD_divergence(dist1, dist2)
        assert result > 0

        # JSD should be symmetric
        reverse_result = JSD_divergence(dist2, dist1)
        assert np.isclose(result, reverse_result)

    def test_cosine_similarity_same_vector(self):
        """Test cosine similarity with identical vectors."""
        vec = np.array([1, 2, 3, 4])
        result = cosine_similarity(vec, vec)
        assert np.isclose(result, 1.0)

    def test_cosine_similarity_orthogonal(self):
        """Test cosine similarity with orthogonal vectors."""
        vec1 = np.array([1, 0, 0, 0])
        vec2 = np.array([0, 1, 0, 0])
        result = cosine_similarity(vec1, vec2)
        assert np.isclose(result, 0.0)

    def test_cosine_similarity_opposite(self):
        """Test cosine similarity with opposite vectors."""
        vec1 = np.array([1, 2, 3, 4])
        vec2 = np.array([-1, -2, -3, -4])
        result = cosine_similarity(vec1, vec2)
        assert np.isclose(result, -1.0)

    def test_cosine_similarity_zero_vector(self):
        """Test cosine similarity with zero vector."""
        vec1 = np.array([1, 2, 3, 4])
        vec2 = np.array([0, 0, 0, 0])
        result = cosine_similarity(vec1, vec2)
        assert np.isclose(result, 0.0)

        # Test both vectors being zero
        result = cosine_similarity(vec2, vec2)
        assert np.isclose(result, 0.0)


# Additional parameterized tests
@pytest.mark.parametrize(
    "distribution,expected",
    [
        ([1 / 3, 1 / 3, 1 / 3], np.log(3)),
        ([1, 0, 0], 0),
        ([0.5, 0.5], np.log(2)),
    ],
)
def test_calculate_entropy_parametrized(distribution, expected):
    """Parameterized test for calculate_entropy with various distributions."""
    result = calculate_entropy(distribution)
    assert np.isclose(result, expected)


@pytest.mark.parametrize(
    "vec1,vec2,expected",
    [
        (np.array([1, 0]), np.array([0, 1]), 0),
        (np.array([1, 1]), np.array([1, 1]), 1),
        (np.array([1, 0]), np.array([1, 0]), 1),
        (np.array([1, 2, 3]), np.array([-1, -2, -3]), -1),
    ],
)
def test_cosine_similarity_parametrized(vec1, vec2, expected):
    """Parameterized test for cosine_similarity with various vector pairs."""
    result = cosine_similarity(vec1, vec2)
    assert np.isclose(result, expected)
