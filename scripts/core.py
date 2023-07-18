import json
import pickle
import re
from typing import Optional, Callable, Union, Tuple, Any
from metaheuristics import improved_binary_black_hole, binary_black_hole_spark, CrossValidationSparkResult
import os
import pandas as pd
from pyspark import SparkContext, Broadcast
from pyspark.sql import DataFrame as SparkDataFrame
import numpy as np
from model_parameters import SVMParameters
from utils import get_columns_from_df, read_survival_data, KernelName, ModelName, OptimizerName
import time
import logging

# Negative and positive infinity constants
NEG_INF: float = float("-inf")
POS_INF: float = float("inf")

logging.getLogger().setLevel(logging.INFO)

# Prevents 'A value is trying to be set on a copy of a slice from a DataFrame.' error
pd.options.mode.chained_assignment = None

# Prevents 'A value is trying to be set on a copy of a slice from a DataFrame.' error
pd.options.mode.chained_assignment = None

# Some useful types
ParameterFitnessFunctionSequential = Tuple[pd.DataFrame, np.ndarray]
ParsedDataCallable = Callable[[np.ndarray, Any, np.ndarray], Union[ParameterFitnessFunctionSequential, SparkDataFrame]]

# Fitness function result structure. It's a function that takes a Pandas DF/Spark Broadcast variable, the bool subset
# of features, the original data (X), the target vector (Y) and a bool flag indicating if it's a broadcast variable
CrossValidationCallback = Callable[[Union[pd.DataFrame, Broadcast], np.ndarray, np.ndarray, bool],
                                   CrossValidationSparkResult]

# Fitness function only for sequential experiments
CrossValidationCallbackSequential = Callable[[pd.DataFrame, np.ndarray], float]


def create_folder_with_permissions(dir_path: str):
    """Creates (if not exist) a folder and assigns permissions to work without problems in the Spark container."""
    # First, checks if the folder exists
    if os.path.exists(dir_path):
        return

    mode = 0o777
    os.mkdir(dir_path, mode)
    os.chmod(dir_path, mode)  # Mode in mkdir is sometimes ignored: https://stackoverflow.com/a/5231994/7058363


def __fitness_function_with_checking(
        compute_cross_validation: CrossValidationCallback,
        index_array: np.ndarray,
        x: Union[pd.DataFrame, Broadcast],
        y: np.ndarray,
        is_broadcast: bool,
        more_is_better: bool,
) -> CrossValidationSparkResult:
    """
    Fitness function of a star evaluated in the Binary Black hole, including vector without features check.

    :param compute_cross_validation: Fitness function.
    :param index_array: Boolean vector to indicate which feature will be present in the fitness function.
    :param x: Data with features.
    :param y: Classes.
    :param is_broadcast: True if x is a Spark Broadcast to retrieve its values.
    :param more_is_better: If True, it returns the highest value (SVM and RF C-Index),
    lowest otherwise (useful for clustering Log Likelihood).
    :return: All the results, documentation listed in the CrossValidationSparkResult type
    """
    if not np.count_nonzero(index_array):
        # If no features, returns the worst fitness value
        worst_fitness_value = NEG_INF if more_is_better else POS_INF
        return worst_fitness_value, -1.0, -1, '', -1, '', -1.0, -1.0, -1.0, -1.0, None

    return compute_cross_validation(x, index_array, y, is_broadcast)


def run_bbha_experiment(
        app_name: str,
        compute_cross_validation: Union[CrossValidationCallback, CrossValidationCallbackSequential],
        metric_description: str,
        model_name: ModelName,
        parameters_description: str,
        molecules_dataset: str,
        clinical_dataset: str,
        n_iterations: int,
        number_of_workers: int,
        use_load_balancer: bool,
        more_is_better: bool,
        svm_kernel: Optional[KernelName],
        svm_optimizer: Optional[OptimizerName],
        n_stars: int,
        random_state: Optional[int],
        run_improved_bbha: Optional[bool] = False,
        debug: bool = False,
        sc: Optional[SparkContext] = None,
        coeff_1: float = 2.35,
        coeff_2: float = 0.2,
        binary_threshold: Optional[float] = None,
        use_broadcasts_in_spark: Optional[bool] = True
):
    """
    Computes the BBHA and/or Improved BBHA algorithm/s

    :param app_name: App name to save the JSON result and all the execution metrics.
    :param compute_cross_validation: Cross Validation function to get the fitness.
    :param metric_description: Description of the metric returned by the CrossValidation function to
    display in the JSON.
    :param model_name: Description of the model used as CrossValidation fitness function to be displayed in the JSON.
    :param parameters_description: Model's parameters description to report in results.
    :param molecules_dataset: Molecules dataset file path.
    :param clinical_dataset: Clinical dataset file path.
    :param number_of_workers: Number of workers nodes in the Spark cluster.
    :param use_load_balancer: If True, assigns a partition ID using a load balancer. If False, distributes sequentially.
    :param more_is_better: If True, it returns the highest value (SVM and RF C-Index),
    lowest otherwise (useful for clustering Log Likelihood).
    :param svm_kernel: SVM 'kernel' parameter to fill SVMParameter instance. Only used if model used is the SVM.
    :param svm_optimizer: SVM 'optimizer' parameter to fill SVMParameter instance. Only used if model used is the SVM.
    :param run_improved_bbha: True for improved, False to run the original.
    :param debug: True to log extra data during script execution.
    :param sc: Spark Context. Only used if 'run_in_spark' = True.
    :param n_stars: Number of stars in the BBHA algorithm.
    :param random_state: Random state to replicate experiments. It allows to set the same number of features for every
    star and the same shuffling.
    :param n_iterations: Number of iterations in the BBHA algorithm.
    :param coeff_1: Coefficient 1 required by the enhanced version of the BBHA algorithm.
    :param coeff_2: Coefficient 2 required by the enhanced version of the BBHA algorithm.
    :param binary_threshold: Threshold used in BBHA, None to be computed randomly.
    :param use_broadcasts_in_spark: If True, it generates a Broadcast value to pass to the fitness function instead of
    pd.DataFrame. Is ignored if run_in_spark = False.
    """
    if number_of_workers == 0:
        logging.error(f'Invalid number of workers in Spark Cluster ({number_of_workers}). '
                      'Check "number_of_workers" parameter or set "run_in_spark" = False!')
        return

    # Creates a folder to save all the results and time metrics
    results_path = os.getenv('RESULTS_PATH')  # Gets shared folder path
    app_folder = os.path.join(results_path, app_name)
    create_folder_with_permissions(app_folder)

    # Configures JSON file with the Feature Selection results
    res_json_file_path = os.path.join(app_folder, 'result.json')
    logging.info(f'Metaheuristic results will be saved in "{res_json_file_path}"')
    logging.info(f'Metrics will be saved in JSON files (one per iteration) inside folder "{app_folder}"')

    # Gets survival data
    x, y = read_survival_data(molecules_dataset, clinical_dataset)

    number_samples, number_features = x.shape

    logging.info(f'Running BBHA experiment with {n_iterations} iterations and {n_stars} stars')
    logging.info(f'Running {n_stars} stars in Spark ({n_stars // number_of_workers} stars per worker). '
                 f'{number_of_workers} active workers in Spark Cluster')
    load_balancer_desc = 'With' if use_load_balancer else 'Without'
    logging.info(f'{load_balancer_desc} load balancer')

    logging.info(f'Metric: {metric_description} | Model: {model_name} | '
                 f'Parameters: {parameters_description} | Random state: {random_state}')
    logging.info(f'Survival dataset: "{molecules_dataset}"')
    logging.info(f'\tSamples (rows): {number_samples} | Features (columns): {number_features}')
    logging.info(f'\tY shape: {y.shape[0]}')

    # If it was set, generates a broadcast value
    if use_broadcasts_in_spark:
        logging.info('Using Broadcast')
        x = sc.broadcast(x)

    # Gets concordance index with all the features
    start = time.time()
    all_features_concordance_index = compute_cross_validation(x, np.ones(number_features), y, use_broadcasts_in_spark)

    # In Spark, it's only the fitness value it's the first value
    all_features_concordance_index = all_features_concordance_index[0]

    logging.info(f'Fitness function with all the features finished in {time.time() - start} seconds')
    logging.info(f'{metric_description} with all the features: {all_features_concordance_index}')

    experiment_start = time.time()
    improved_mode_str = 'improved' if run_improved_bbha else 'normal'
    logging.info(f'Running {improved_mode_str} algorithm')

    best_model = None

    # Binary Black Hole
    bh_start = time.time()
    if run_improved_bbha:
        json_experiment_data = {}  # No Spark, no data about execution times to store
        best_subset, best_metric = improved_binary_black_hole(
            n_stars=n_stars,
            n_features=number_features,
            n_iterations=n_iterations,
            fitness_function=lambda subset: __fitness_function_with_checking(
                compute_cross_validation,
                subset,
                x,
                y,
                is_broadcast=use_broadcasts_in_spark,
                more_is_better=more_is_better
            ),
            coeff_1=coeff_1,
            coeff_2=coeff_2,
            more_is_better=more_is_better,
            binary_threshold=binary_threshold,
            debug=debug
        )
    else:
        # Generates parameters to train the ML model for load balancing
        if use_load_balancer:
            parameters = SVMParameters(number_samples, svm_kernel, svm_optimizer)
        else:
            parameters = None

        best_subset, best_metric, _best_data, best_model, json_experiment_data = binary_black_hole_spark(
            n_stars=n_stars,
            n_features=number_features,
            n_iterations=n_iterations,
            fitness_function=lambda subset: __fitness_function_with_checking(
                compute_cross_validation,
                subset,
                x,
                y,
                is_broadcast=use_broadcasts_in_spark,
                more_is_better=more_is_better
            ),
            sc=sc,
            binary_threshold=binary_threshold,
            more_is_better=more_is_better,
            random_state=random_state,
            debug=debug,
            use_load_balancer=use_load_balancer,
            number_of_workers=number_of_workers,
            parameters=parameters,
        )

    total_fs_execution_time = time.time() - bh_start
    logging.info(f'Binary Black Hole with {n_iterations} iterations and {n_stars} '
                 f'stars, finished in {total_fs_execution_time} seconds')

    # Gets columns names
    x_df = x.value if use_broadcasts_in_spark else x
    column_names = get_columns_from_df(best_subset, x_df).columns.values
    final_subset = column_names

    # Stores data to train future load balancer models
    # Adds data to JSON
    json_extra_data = {
        'model': model_name,
        'dataset': molecules_dataset,
        'parameters': parameters_description,
        'number_of_samples': number_samples
    }
    json_experiment_data = {**json_experiment_data, **json_extra_data}

    now = time.strftime('%Y-%m-%d_%H_%M_%S')
    json_file = f'{model_name}_{parameters_description}_{metric_description}_{now}_results.json'
    json_file = re.sub(' +', '_', json_file).lower()  # Replaces whitespaces with '_' and makes lowercase
    json_dest = os.path.join(app_folder, json_file)

    with open(json_dest, 'w+') as file:
        file.write(json.dumps(json_experiment_data))

    # If the best_model was found, saves it in the shared folder
    if best_model is not None:
        model_dest = os.path.join(app_folder, 'model.pkl')
        with open(model_dest, 'wb+') as model_file:
            pickle.dump(best_model, model_file)

    # Adds some extra reporting to final result
    algorithm = 'BBHA' + (' (improved)' if run_improved_bbha else '')
    logging.info(f'Found {len(final_subset)} features with {algorithm} ({metric_description} '
                 f'= {best_metric}):')
    logging.info(str(final_subset))

    experiment_results_dict = {
        'dataset': molecules_dataset,
        'improved': 1 if run_improved_bbha else 0,
        'model': model_name,
        'best_metric_with_all_features': round(all_features_concordance_index, 4),
        'best_metric': round(best_metric, 4),
        'features': ' | '.join(final_subset),
        'execution_time': total_fs_execution_time
    }

    # Saves the results to final JSON
    with open(res_json_file_path, 'w+') as file:
        file.write(json.dumps(experiment_results_dict))

    logging.info(f'Experiment completed in {time.time() - experiment_start} seconds')
