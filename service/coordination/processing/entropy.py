import numpy as np
from typing import List, Union, Optional
from numpy.linalg import norm
from ..types.base import FloatArray


def calculate_entropy(probabilities: Union[List[float], FloatArray]) -> float:
    """
    Calculate the entropy of a list of probabilities.
    """
    probabilities = np.array(probabilities)
    s = np.sum(probabilities)
    if s > 0:
        probabilities = probabilities / s
        # Exclude zero probabilities
        probabilities = probabilities[probabilities > 0]
        return -np.sum(probabilities * np.log(probabilities))
    return 0


def update_entropy_incremental(
    h1: float, Y: Union[List[float], FloatArray], m: float
) -> float:
    """Update entropy after adding value m to list Y."""
    s = np.sum(Y)
    H = 0
    if s + m > 0:
        p1 = s / (s + m)  # New total sum after adding m
        p2 = m / (s + m)
        if s == 0:
            return 0
        if p1 + p2 == 0:
            H = 0
        elif p1 == 0:
            H = -p2 * np.log(p2)
        elif p2 == 0:
            H = p1 * h1 - p1 * np.log(p1)
        else:
            H = p1 * h1 - p1 * np.log(p1) - p2 * np.log(p2)
    return H


def find_max_entropy_partition(
    behaviours: Union[List[float], FloatArray],
) -> Optional[FloatArray]:
    """
    Find the maximum entropy partition of a list of behaviours.
    """
    max_value = -1
    max_partition = None
    h1, h2 = 0, 0
    E1, E2 = [], []

    # Z = _behaviour.copy()
    s = np.sum(behaviours)
    if s == 0:
        return None

    behavs = behaviours / s

    for pivot in range(len(behavs)):
        pivot2 = len(behavs) - pivot
        if pivot == 0 or pivot == len(behavs):
            h1 = 0
        else:
            h1 = update_entropy_incremental(h1, behavs[: pivot - 1], behavs[pivot - 1])

        if pivot2 == len(behavs):
            h2 = 0
        else:
            h2 = update_entropy_incremental(h2, behavs[pivot2:], behavs[pivot2 - 1])

        E1.append(h1)
        E2.append(h2)

    for pivot in range(len(behavs)):
        h1 = E1[pivot]
        h2 = E2[len(behavs) - 1 - pivot]
        if h1 > h2:
            if h1 > max_value:
                max_value = h1
                max_partition = (behavs[:pivot], behavs[pivot:])
        elif h2 > max_value:
            max_value = h2
            max_partition = (behavs[:pivot], behavs[pivot:])

    if max_partition:
        return max_partition[0] if len(max_partition[0]) > 0 else max_partition[1]
    return None


def Entropy1(X: FloatArray) -> float:
    """
    Calculate the entropy of a vector X.
    """
    nonzero_indices = np.nonzero(X)
    nonzero_values = X[nonzero_indices]
    h = np.sum(np.multiply(np.negative(nonzero_values), np.log(nonzero_values)))
    return h


def Entropy2(X: FloatArray, s: float) -> float:
    """
    Calculate the entropy of a vector X with a given sum s.
    """

    nonzero_indices = np.nonzero(X)
    nonzero_values = X[nonzero_indices]
    h = np.sum(
        np.multiply(
            np.divide(np.negative(nonzero_values), s),
            np.log(np.divide(nonzero_values, s)),
        )
    )
    return h


def JSD_divergence(user_link1: FloatArray, user_link2: FloatArray) -> float:
    """
    Calculate the Jensen-Shannon Divergence between two distributions.
    """

    # removed this as it didn't seem to be used
    # XX=np.zeros(len(user_link1))
    s1 = np.sum(user_link1)
    s2 = np.sum(user_link2)

    XX = np.multiply(np.add(np.divide(user_link1, s1), np.divide(user_link2, s2)), 0.5)

    JSD = Entropy1(XX)
    JSD = np.subtract(JSD, np.multiply(Entropy2(user_link1, s1), 0.5))
    JSD = np.subtract(JSD, np.multiply(Entropy2(user_link2, s2), 0.5))

    return JSD


def cosine_similarity(user_link1: FloatArray, user_link2: FloatArray) -> float:
    """
    Calculate the cosine similarity between two vectors.
    """
    if norm(user_link1) * norm(user_link2) == 0:
        return 0
    return np.dot(user_link1, user_link2) / (norm(user_link1) * norm(user_link2))
