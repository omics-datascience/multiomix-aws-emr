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
    Reads and preprocess survival dataset (in CSV format).
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


def get_columns_from_df(columns_list: np.array, df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a set of columns of a DataFrame. The usefulness of this method is that it works for categorical indexes or
    strings
    """
    if np.issubdtype(columns_list.dtype, np.number):
        # Gets by int indexes
        non_zero_idx = np.nonzero(columns_list)
        return df.iloc[:, non_zero_idx[0]]

    # Gets by column names
    return df[columns_list]
