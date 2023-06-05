import logging
import os
from typing import Tuple, cast, Dict, List, Literal
import numpy as np
import pandas as pd
from sklearn.metrics import confusion_matrix
from sklearn.preprocessing import label_binarize

# Nombre de la columna de la clase en los DataFrame
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


def specificity(y_true, y_pred):
    """
    Computa la metrica de Especificidad ya que Sklearn no lo implementa en la libreria
    Solution taken from https://stackoverflow.com/questions/33275461/specificity-in-scikit-learn
    :param y_true: Y true
    :param y_pred: Y pred
    :return: Especificidad
    """
    conf_res = confusion_matrix(y_true, y_pred).ravel()
    tn, fp = conf_res[0], conf_res[1]
    return tn / (tn + fp)


def clean_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes NaN and Inf values
    :param df: DataFrame to clean
    :return: Cleaned DataFrame
    """
    assert isinstance(df, pd.DataFrame), "df needs to be a pd.DataFrame"
    df = df.dropna(axis='columns')
    indices_to_keep = ~df.isin([np.nan, np.inf, -np.inf]).any('columns')
    return df[indices_to_keep].astype(np.float64)


def read_survival_data(add_epsilon: bool,
                       dataset_folder: DatasetName = 'Breast_Invasive_Carcinoma') -> Tuple[pd.DataFrame, np.ndarray]:
    """
    Reads and preprocess survival dataset
    :param add_epsilon: If True it adds an epsilon to 0s in Y data to prevent errors in SVM training
    :param dataset_folder: Dataset's folder's name
    :return: Tuple with the filtered DataFrame, Y data
    """
    # Gets X
    x_file_path = os.path.join(os.path.dirname(__file__),
                               f'Datasets/{dataset_folder}/data_mrna_seq_v2_rsem_zscores_ref_normal_samples.txt')
    x = pd.read_csv(x_file_path, sep='\t', index_col=0)
    x = x[x.index.notnull()]  # Removes NaN indexes

    # Removes '-1' suffix to make the intersection
    x.columns = x.columns.str.replace("-01$", "", regex=True)
    patients_x = x.columns.values

    # Gets Y
    y_file_path = os.path.join(os.path.dirname(__file__), f'Datasets/{dataset_folder}/data_clinical_patient.txt')
    y = pd.read_csv(y_file_path, sep='\t', skiprows=4, index_col=0)

    # Keep only survival data
    y = y.loc[:, ['OS_STATUS', 'OS_MONTHS']]
    cond_living = y['OS_STATUS'] == '0:LIVING'
    y.loc[cond_living, 'OS_STATUS'] = False
    y.loc[~cond_living, 'OS_STATUS'] = True

    # Removes NaNs samples
    indices_to_keep = ~y.isin([np.nan, np.inf, -np.inf]).any('columns')
    y = y[indices_to_keep]

    # Gets in common patients
    patients_y = y.index.values
    patients_intersect = np.intersect1d(patients_x, patients_y)
    y = y.loc[patients_intersect, :]

    # Removes zeros
    if add_epsilon:
        zeros_cond = y['OS_MONTHS'] == 0
        y.loc[zeros_cond, 'OS_MONTHS'] = y.loc[zeros_cond, 'OS_MONTHS'] + 1
        assert y[y['OS_MONTHS'] == 0].empty

    # Removes unneeded column and transpose to keep samples as columns
    x.drop('Entrez_Gene_Id', axis=1, inplace=True)
    x = x.transpose()
    x = x.loc[patients_intersect, :]

    # Removes NaN and Inf values
    x = clean_dataset(x)

    # TODO: REMOVE! ONLY USEFUL FOR DEBUG
    # x = x.iloc[:, :50]

    # Formats Y to a structured array
    y = np.core.records.fromarrays(y.to_numpy().transpose(), names='event, time', formats='bool, float')

    return x, y


def rename_class_column_name(df: pd.DataFrame, class_name_old: str):
    """
    Renames the DataFrame class column to generalize the algorithms
    :param df: DataFrame
    :param class_name_old: Current class column name
    """
    df.rename(columns={class_name_old: NEW_CLASS_NAME}, inplace=True)


def binarize_y(y: pd.Series) -> Tuple[np.ndarray, int]:
    """
    Genera un arreglo de binarios indicando la clase
    :param y: Arreglo con la clase
    :return: Arreglo categorico binario
    """
    classes = y.unique()
    return label_binarize(y, classes=classes).ravel(), classes.shape[0]


def get_columns_by_categorical(columns_index: np.ndarray, df: pd.DataFrame) -> pd.DataFrame:
    """
    Obtiene las columnas a partir de un arrego categorico
    :param columns_index: Numpy Array with a {0, 1} in the column index to indicate absence/presence of the column
    :param df: DataFrame to retrieve the columns data
    :return: DataFrame with only the specified columns
    """
    non_zero_idx = np.nonzero(columns_index)
    return df.iloc[:, non_zero_idx[0]]


def get_columns_from_df(columns_list: np.array, df: pd.DataFrame) -> pd.DataFrame:
    """Devuelve un conjunto de columnas de un DataFrame. La utilidad de este metodo es que funciona
    para indices categoricos o strings"""
    if np.issubdtype(columns_list.dtype, np.number):
        # Obtengo por indices enteros
        return get_columns_by_categorical(columns_list, df)
    # Obtengo por string/label de columna
    return df[columns_list]
