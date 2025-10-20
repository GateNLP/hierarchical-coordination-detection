from enum import Enum
import pandas as pd
import numpy as np
from numpy.typing import NDArray

# Define simple type aliases for clarity
PostID = str
UserID = str
HashtagID = str
LinkID = str
TimeStamp = float
Weight = float

# Common numpy array types
FloatArray = NDArray[np.float64]

# DataFrame type aliases for better type hints
PostsDataFrame = pd.DataFrame  # DataFrame containing posts
RawDataFrame = pd.DataFrame  # DataFrame containing raw data
MEBGraphDataFrame = pd.DataFrame  # Multi-edge bipartite graph
SEBGraphDataFrame = pd.DataFrame  # Single-edge bipartite graph
CoordinationDataFrame = pd.DataFrame  # DataFrame with coordination metrics


class SpeedOption(Enum):
    NO_FILTERING = 1
    PAIRWISE_FILTERING = 2
    PAIRWISE_GROUP_FILTERING = 3
