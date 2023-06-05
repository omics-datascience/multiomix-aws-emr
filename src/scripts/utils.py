import os
from typing import Tuple, Literal
import numpy as np
import pandas as pd

# Shared folder between Multiomix and this project where datasets are stored
DATASETS_PATH: str = os.getenv('DATASETS_PATH')

# Name of the column that contains the class
NEW_CLASS_NAME = 'class'

# To prevent some errors with SVM
# EPSILON = 1.E-03
EPSILON = 1

# Available models descriptions to use
ModelName = Literal['svm', 'rf', 'clustering']

# Available SurvivalSVM kernels to use
KernelName = Literal['linear', 'poly', 'rbf', 'cosine']

# Available SurvivalSVM optimizers to use
OptimizerName = Literal["avltree", "rbtree"]


def read_survival_data(molecules_dataset: str, clinical_dataset: str) -> Tuple[pd.DataFrame, np.ndarray]:
    """
    Reads and preprocess survival dataset (in CSV format with sep='\t' and decimal='.').
    NOTE: This method considers that both datasets where correctly preprocessed.
    :param molecules_dataset: Molecules CSV dataset file path.
    :param clinical_dataset: Clinical CSV dataset file path.
    :return: Tuple with the filtered DataFrame, Y data.
    """
    # Concatenates the shared folder path to both datasets file paths
    molecules_dataset = os.path.join(DATASETS_PATH, molecules_dataset)
    clinical_dataset = os.path.join(DATASETS_PATH, clinical_dataset)

    # Gets molecules and clinical DataFrames
    molecules_df = pd.read_csv(molecules_dataset, sep='\t', decimal='.', index_col=0)
    clinical_df = pd.read_csv(clinical_dataset, sep='\t', decimal='.', index_col=0)

    # Formats clinical data to a Numpy structured array
    clinical_data = np.core.records.fromarrays(clinical_df.to_numpy().transpose(), names='event, time',
                                               formats='bool, float')

    return molecules_df, clinical_data


def get_columns_from_df(combination: np.array, molecules_df: pd.DataFrame) -> pd.DataFrame:
    """
    Gets a specific subset of features from a Pandas DataFrame.
    @param molecules_df: Pandas DataFrame with all the features.
    @param combination: Combination of features to extract.
    @return: A Pandas DataFrame with only the combinations of features.
    """
    # Get subset of features
    if isinstance(combination, np.ndarray):
        # In this case it's a Numpy array with int indexes (used in metaheuristics)
        subset: pd.DataFrame = molecules_df.iloc[combination]
    else:
        # In this case it's a list of columns names (used in Blind Search)
        molecules_to_extract = np.intersect1d(molecules_df.index, combination)
        subset: pd.DataFrame = molecules_df.loc[molecules_to_extract]

    # Discards NaN values
    subset = subset[~pd.isnull(subset)]

    # Makes the rows columns
    subset = subset.transpose()
    return subset
