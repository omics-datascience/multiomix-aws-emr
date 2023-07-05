import logging
import os
from typing import Optional, Callable, Tuple, Union, List, Iterable, Dict, cast
import random
import binpacking
import joblib
import numpy as np
from math import tanh
from pyspark import SparkContext
import time
from sklearn.base import BaseEstimator
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.preprocessing import OrdinalEncoder, MinMaxScaler, PolynomialFeatures
from model_parameters import SVMParameters

logging.getLogger().setLevel(logging.INFO)

SVM_TRAINED_MODELS_DIR = 'Trained_models/svm/'

# Result tuple with [0] -> fitness value, [1] -> execution time, [2] -> Partition ID, [3] -> Hostname,
# [4] -> number of evaluated features, [5] -> time lapse description, [6] -> time by iteration and [7] -> avg test time
# [8] -> mean of number of iterations of the model inside the CV, [9] -> train score.
# [10] -> Best model during CV (the one with better fitness value) or None if an error occurred.
# Number values are -1.0 if there were some error
CrossValidationSparkResult = Tuple[float, float, int, str, int, str, float, float, float, float,
                                   Optional[BaseEstimator]]


def report_all_load_balancer_models(n_stars: int, parameters: SVMParameters, stars_subsets: np.ndarray):
    """Reports the predicted times for all the trained models. Useful for DEBUG."""
    start = time.time()
    predicted_times_aux, _ = predict_execution_times_svm(stars_subsets, parameters, model_plk='best_linear_model.pkl',
                                                         use_min_max=True)
    diff = round(time.time() - start, 4)
    stars_and_times_aux = {k: round(v, 4) for (k, v) in zip(range(n_stars), predicted_times_aux)}
    logging.info(f'Predicted Linear d=1 with MinMax (in {diff} seconds): {stars_and_times_aux}')

    start = time.time()
    predicted_times_aux, _ = predict_execution_times_svm(stars_subsets, parameters,
                                                         model_plk='best_linear_model_no_min_max.pkl',
                                                         use_min_max=False)
    diff = round(time.time() - start, 4)
    stars_and_times_aux = {k: round(v, 4) for (k, v) in zip(range(n_stars), predicted_times_aux)}
    logging.info(f'Predicted Linear d=1 NO MinMax (in {diff} seconds): {stars_and_times_aux}')

    start = time.time()
    predicted_times_aux, _ = predict_execution_times_svm(stars_subsets, parameters, model_plk='best_linear_model_2.pkl',
                                                         use_min_max=True, poly_degree=2)
    diff = round(time.time() - start, 4)
    stars_and_times_aux = {k: round(v, 4) for (k, v) in zip(range(n_stars), predicted_times_aux)}
    logging.info(f'Predicted Linear d=2 with MinMax (in {diff} seconds): {stars_and_times_aux}')

    start = time.time()
    predicted_times_aux, _ = predict_execution_times_svm(stars_subsets, parameters,
                                                         model_plk='best_linear_model_2_no_min_max.pkl',
                                                         use_min_max=False, poly_degree=2)
    diff = round(time.time() - start, 4)
    stars_and_times_aux = {k: round(v, 4) for (k, v) in zip(range(n_stars), predicted_times_aux)}
    logging.info(f'Predicted Linear d=2 NO MinMax (in {diff} seconds): {stars_and_times_aux}')
    start = time.time()
    predicted_times_aux, _ = predict_execution_times_svm(stars_subsets, parameters,
                                                         model_plk='best_gradient_booster_model.pkl', use_min_max=True)
    diff = round(time.time() - start, 4)
    stars_and_times_aux = {k: round(v, 4) for (k, v) in zip(range(n_stars), predicted_times_aux)}
    logging.info(f'Predicted Gradient booster with MinMax (in {diff} seconds): {stars_and_times_aux}')
    start = time.time()
    predicted_times_aux, _ = predict_execution_times_svm(stars_subsets, parameters,
                                                         model_plk='best_gradient_booster_model_no_min_max.pkl',
                                                         use_min_max=False)
    diff = round(time.time() - start, 4)
    stars_and_times_aux = {k: round(v, 4) for (k, v) in zip(range(n_stars), predicted_times_aux)}
    logging.info(f'Predicted Gradient booster NO MinMax (in {diff} seconds): {stars_and_times_aux}')

    start = time.time()
    predicted_times_aux, _ = predict_execution_times_svm(stars_subsets, parameters, model_plk='best_linear_model_3.pkl',
                                                         use_min_max=True, poly_degree=3)
    diff = round(time.time() - start, 4)
    stars_and_times_aux = {k: round(v, 4) for (k, v) in zip(range(n_stars), predicted_times_aux)}
    logging.info(f'Predicted Linear d=3 with MinMax (in {diff} seconds): {stars_and_times_aux}')

    start = time.time()
    predicted_times_aux, _ = predict_execution_times_svm(stars_subsets, parameters,
                                                         model_plk='best_linear_model_3_no_min_max.pkl',
                                                         use_min_max=False,
                                                         poly_degree=3)
    diff = round(time.time() - start, 4)
    stars_and_times_aux = {k: round(v, 4) for (k, v) in zip(range(n_stars), predicted_times_aux)}
    logging.info(f'Predicted Linear d=3 NO MinMax (in {diff} seconds): {stars_and_times_aux}')

    start = time.time()
    predicted_times_aux, _ = predict_execution_times_svm(stars_subsets, parameters, model_plk='best_nn_model.pkl',
                                                         use_min_max=True)
    diff = round(time.time() - start, 4)
    stars_and_times_aux = {k: round(v, 4) for (k, v) in zip(range(n_stars), predicted_times_aux)}
    logging.info(f'Predicted NN with MinMax (in {diff} seconds): {stars_and_times_aux}')

    start = time.time()
    predicted_times_aux, _ = predict_execution_times_svm(stars_subsets, parameters,
                                                         model_plk='best_nn_model_no_min_max.pkl', use_min_max=False)
    diff = round(time.time() - start, 4)
    stars_and_times_aux = {k: round(v, 4) for (k, v) in zip(range(n_stars), predicted_times_aux)}
    logging.info(f'Predicted NN NO MinMax (in {diff} seconds): {stars_and_times_aux}')


def predict_execution_times_svm(stars: np.ndarray, parameters: SVMParameters,
                                model_plk: str, use_min_max: bool,
                                poly_degree: Optional[int] = None,
                                report_data_without_min_max: bool = False) -> Tuple[List, List]:
    """
    Predicts execution times for every one of the stars.
    :param stars: Stars with the number of features to compute
    :param parameters: SVM parameters
    :param model_plk: Model's pkl file name
    :param use_min_max: If True, a MinMax transformation is computed with some features
    :param poly_degree: If not None, a Polynomial transformation is computed. Useful for LinearRegression models
    :param report_data_without_min_max: If True, logs the input for the load balancer
    :return: List with all the predictions made for every star, and a list with the data from which the model makes
    all the predictions
    """
    # Loads models
    trained_model: GradientBoostingRegressor = joblib.load(os.path.join(SVM_TRAINED_MODELS_DIR, model_plk))
    ord_encoder: OrdinalEncoder = joblib.load(os.path.join(SVM_TRAINED_MODELS_DIR, 'ord_encoder.pkl'))

    # Prevents errors for first load balancer models which were trained with fewer data than real. So this prevents
    # errors with unknown values
    ord_encoder.handle_unknown = 'use_encoded_value'
    ord_encoder.unknown_value = -1

    # Generates number_of_features, number_of_samples, kernel, optimizer
    categorical_features = np.array([parameters.kernel, parameters.optimizer]).reshape(1, -1)
    kernel_transformed = ord_encoder.transform(categorical_features)[0][0]
    optimizer_transformed = ord_encoder.transform(categorical_features)[0][1]

    x = []
    for star in stars:
        star_n_features = np.count_nonzero(star[1])
        star_data = [star_n_features, parameters.number_of_samples, kernel_transformed, optimizer_transformed]
        x.append(star_data)

    # Min-Max scaling 'number_of_features' (0) and 'number_of_samples' (1)
    x = np.array(x)
    if report_data_without_min_max:
        logging.info(f'Data to predict (without transformation):\n{x}')

    if use_min_max:
        min_max_scaler: MinMaxScaler = joblib.load(os.path.join(SVM_TRAINED_MODELS_DIR, 'min_max_scaler.pkl'))
        x[:, [0, 1]] = min_max_scaler.transform(x[:, [0, 1]])

    if poly_degree is not None:
        x = PolynomialFeatures(degree=poly_degree, include_bias=False).fit_transform(x)

    predictions = trained_model.predict(x)
    return predictions, x


def generate_stars_and_partitions_bins(bins: List) -> Dict[int, int]:
    """
    Generates a dict with the idx of the star and the assigned partition
    :param bins: Bins generated by binpacking
    :return: Dict where keys are star index, values are the Spark partition
    """
    stars_and_partitions: Dict[int, int] = {}
    for partition_id, aux_bin in enumerate(bins):
        for star_idx in aux_bin.keys():
            stars_and_partitions[star_idx] = partition_id
    return stars_and_partitions


def get_best_spark(
        subsets: np.ndarray,
        workers_results: List[Tuple[int, CrossValidationSparkResult]],
        more_is_better: bool
) -> Tuple[int, np.ndarray, np.ndarray]:
    """
    Gets the best idx, feature subset and fitness value obtained during one iteration of the metaheuristic
    :param subsets: List of features lists used in every star
    :param workers_results: List of fitness values obtained for every star
    :param more_is_better: If True, it returns the highest value (SVM and RF C-Index), lowest otherwise (LogRank p-value)
    :return: Best idx of the stars (Black Hole idx), subset of features for the Black Hole, and all the data returned
    by the star that computes the best fitness (from now, Black Hole)
    """
    # Converts to a Numpy array to discard the star's index
    workers_results_np = np.array(workers_results, dtype=object)
    workers_results_np_aux = np.array(
        [np.array(a_list) for a_list in workers_results_np[:, 1]])  # Creates Numpy arrays from lists
    if more_is_better:
        best_idx = np.argmax(workers_results_np_aux[:, 0])
    else:
        best_idx = np.argmin(workers_results_np_aux[:, 0])

    best_idx = cast(int, best_idx)
    return best_idx, subsets[best_idx][1], workers_results_np[best_idx][1]


def parallelize_fitness_execution(
        sc: SparkContext,
        stars_subsets: np.ndarray,
        fitness_function: Callable[[np.ndarray], CrossValidationSparkResult]
) -> List[CrossValidationSparkResult]:
    """
    Parallelize the fitness function computing on an Apache Spark cluster and return fitness metrics
    @param sc: Spark context
    @param stars_subsets: Stars subset to parallelize
    @param fitness_function: Fitness function
    @return: All the fitness metrics from all the star, and an execution time by worker
    """
    stars_parallelized = sc.parallelize(stars_subsets)

    return stars_parallelized \
        .map(lambda star_features: fitness_function(star_features[1])) \
        .collect()


def map_partition(
        fitness_function: Callable[[np.ndarray], CrossValidationSparkResult],
        records: Iterable[CrossValidationSparkResult]
) -> Iterable[Tuple[int, CrossValidationSparkResult]]:
    """Returns fitness result for all the elements in partition records. The index is returned to get the association
    of predicted times later as Sparks returns all the values in different order from which the stars are assigned
    in first place"""
    for key, elem in records:
        yield key, fitness_function(elem)


def parallelize_fitness_execution_by_partitions(
        sc: SparkContext,
        stars_subsets: np.ndarray,
        fitness_function: Callable[[np.ndarray], CrossValidationSparkResult],
        use_load_balancer: bool,
        number_of_workers: int,
        parameters: Optional[SVMParameters],
        debug: bool
) -> Tuple[List[Tuple[int, CrossValidationSparkResult]], float, Dict[int, float]]:
    """
    Parallelize the fitness function computing on an Apache Spark cluster and return fitness metrics.
    This function generates a partitioning that distributes the load equally to all nodes.
    @param sc: Spark context.
    @param stars_subsets: Stars subset to parallelize.
    @param fitness_function: Fitness function.
    @param use_load_balancer: If True, assigns a partition ID using a load balancer. If False, distributes sequentially.
    @param number_of_workers: Number of workers nodes in the Spark cluster.
    @param parameters: Parameters of the RF/SVM for the load balancer. Only used if 'use_load_balancer' = True.
    @param debug: If True, show information about predicted times for every sar.
    @return: A tuple with the star index and all the metrics from all the star, the passed time between the distribution of data along the
    Spark Cluster and the collect() calling, and a dict with star indexes as keys and predicted times from the load
    balancer (empty list in case parameters is None) as values.
    """
    stars_parallelized = sc.parallelize(stars_subsets)
    n_stars = len(stars_subsets)

    if use_load_balancer:
        predicted_times, transformed_data = predict_execution_times_svm(
            stars_subsets,
            parameters,
            model_plk='best_gradient_booster_model.pkl',
            use_min_max=True,
            report_data_without_min_max=debug
        )
        stars_and_times = {k: v for (k, v) in zip(range(n_stars), predicted_times)}

        # Checks for negative values
        negative_values_idx = np.where(predicted_times < 0)[0]
        if len(negative_values_idx) > 0:
            idx = negative_values_idx[0]
            problem_stars = stars_subsets[idx][1]
            logging.info(f'The load balancer predicted a negative number for star with index {idx}. Exiting...')
            logging.error(f'Number of features: {np.count_nonzero(problem_stars)}')
            logging.error(parameters)
            exit(-1)

        if debug:
            logging.info(f'Data to predict (with transformation):\n{transformed_data}')
            logging.info(f'Predicted times for every star: {stars_and_times}')

            report_all_load_balancer_models(n_stars, parameters, stars_subsets)

        # Generates the bins. NOTE: 'number_of_workers' is the number of bins
        bins = binpacking.to_constant_bin_number(stars_and_times, number_of_workers)

        if debug:
            for bin_idx, elem in enumerate(bins):
                logging.info(f'Bin {bin_idx} has {len(elem.keys())} elements. Sums up to {sum(elem.values())} seconds.')

        # Generates a dict and uses it to assign the partition ID
        stars_and_partitions = generate_stars_and_partitions_bins(bins)
        partition_f = lambda key: stars_and_partitions[key]
    else:
        stars_and_times = {star_idx: -1.0 for star_idx in
                           range(n_stars)}  # Assigns -1.0 to all the stars in case predictions are disabled
        partition_f = lambda key: key * number_of_workers // len(stars_subsets)

    # NOTE: the mapPartitions() allows Scikit-surv models to use all the worker's cores during CrossValidation.
    # This avoids problems of Spark parallelization interfering with Scikit-surv parallelized algorithms
    # VERY IMPORTANT NOTE: the index is returned in mapPartitions() to get the association of predicted times later
    # as Sparks returns all the values in different order from which the stars are assigned in first place
    start_parallelization = time.time()
    result = stars_parallelized \
        .partitionBy(number_of_workers, partitionFunc=partition_f) \
        .mapPartitions(lambda records: map_partition(fitness_function, records), preservesPartitioning=True) \
        .collect()
    total_iteration_time = time.time() - start_parallelization
    logging.info(f'partitionBy(), mapPartitions() and collect() time -> {total_iteration_time} seconds')

    return result, total_iteration_time, stars_and_times


def get_random_subset_of_features(n_features: int, random_state: Optional[int] = None) -> np.ndarray:
    """
    Generates a random subset of Features. Answer taken from https://stackoverflow.com/a/47942584/7058363
    :param n_features: Total number of features
    :param random_state: Random state to replicate experiments. It allows to set the same number of features for every
    star and the same shuffling. This has to be different every time this method is called to prevent repeated features.
    :return: Categorical array with {0, 1} values indicate the absence/presence of the feature in the index
    """
    res = np.zeros(n_features, dtype=int)  # Gets an array of all the features in zero

    # Generates a random number of features in 1. Then, shuffles and returns as boolean
    if random_state is not None:
        random.seed(random_state)

    random_number_of_features = random.randint(1, n_features)
    res[:random_number_of_features] = 1

    if random_state is not None:
        np.random.seed(random_state)
    np.random.shuffle(res)
    return res


def get_best(
        subsets: np.ndarray,
        fitness_values: Union[np.ndarray, List[float]]
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Get the best value of the fitness values."""
    best_idx = np.argmax(fitness_values)  # Keeps the idx to avoid ambiguous comparisons
    return best_idx, subsets[best_idx], fitness_values[best_idx]


def improved_binary_black_hole(
        n_stars: int,
        n_features: int,
        n_iterations: int,
        fitness_function: Callable[[np.ndarray], CrossValidationSparkResult],
        coeff_1: float,
        coeff_2: float,
        binary_threshold: Optional[float] = 0.6,
        debug: bool = False
):
    """
    Computes the BBHA algorithm with some modification extracted from
    "Improved black hole and multiverse algorithms for discrete sizing optimization of planar structures"
    Authors: Saeed Gholizadeh, Navid Razavi & Emad Shojaei
    :param n_stars: Number of stars
    :param n_features: Number of features
    :param n_iterations: Number of iterations
    :param fitness_function: Fitness function to compute on every star
    :param coeff_1: Parameter taken from the paper. Possible values = [2.2, 2.35]
    :param coeff_2: Parameter taken from the paper. Possible values = [0.1, 0.2, 0.3]
    :param binary_threshold: Binary threshold to set 1 or 0 the feature. If None it'll be computed randomly
    :param debug: If True logs everything is happening inside BBHA
    :return:
    """
    coeff_1_possible_values = [2.2, 2.35]
    coeff_2_possible_values = [0.1, 0.2, 0.3]
    if coeff_1 not in coeff_1_possible_values:
        logging.info(f'The parameter coeff_1 must be one of the following values -> {coeff_1_possible_values}')
        exit(1)

    if coeff_2 not in coeff_2_possible_values:
        logging.info(f'The parameter coeff_2 must be one of the following values -> {coeff_2_possible_values}')
        exit(1)

    # Data structs setup
    stars_subsets = np.empty((n_stars, n_features), dtype=int)
    stars_best_subset = np.empty((n_stars, n_features), dtype=int)
    stars_fitness_values = np.empty((n_stars,), dtype=float)
    stars_best_fitness_values = np.empty((n_stars,), dtype=float)

    # Initializes the stars with their subsets and their fitness values
    if debug:
        logging.info('Initializing stars...')
    for i in range(n_stars):
        random_features_to_select = get_random_subset_of_features(n_features)
        stars_subsets[i] = random_features_to_select  # Initializes 'Population'
        stars_fitness_values[i] = fitness_function(random_features_to_select)
        # Best fitness and position
        stars_best_fitness_values[i] = stars_fitness_values[i]
        stars_best_subset[i] = stars_subsets[i]

    # The star with the best fitness is the Black Hole
    black_hole_idx, black_hole_subset, black_hole_fitness = get_best(stars_subsets, stars_fitness_values)
    if debug:
        logging.info(f'Black hole starting as star at index {black_hole_idx}')

    # Iterations
    for i in range(n_iterations):
        if debug:
            logging.info(f'Iteration {i + 1}/{n_iterations}')
        for a in range(n_stars):
            # If it's the black hole, skips the computation
            if a == black_hole_idx:
                continue

            # Compute the current star fitness
            current_star_subset = stars_subsets[a]
            current_fitness = fitness_function(current_star_subset)

            # Sets the best fitness and position
            if current_fitness > stars_best_fitness_values[a]:
                stars_best_fitness_values[a] = current_fitness
                stars_best_subset[a] = current_star_subset

            # If it's the best fitness, swaps that star with the current black hole
            if current_fitness > black_hole_fitness:
                if debug:
                    logging.info(f'Changing Black hole for star {a},'
                                 f' BH fitness -> {black_hole_fitness} | Star {a} fitness -> {current_fitness}')
                black_hole_idx = a
                black_hole_subset, current_star_subset = current_star_subset, black_hole_subset
                black_hole_fitness, current_fitness = current_fitness, black_hole_fitness

            # If the fitness function was the same, but had fewer features in the star (better!), makes the swap
            elif current_fitness == black_hole_fitness and np.count_nonzero(current_star_subset) < np.count_nonzero(
                    black_hole_subset):
                if debug:
                    logging.info(f'Changing Black hole for star {a},'
                                 f' BH fitness -> {black_hole_fitness} | Star {a} fitness -> {current_fitness}')
                black_hole_idx = a
                black_hole_subset, current_star_subset = current_star_subset, black_hole_subset
                black_hole_fitness, current_fitness = current_fitness, black_hole_fitness

            # Computes the event horizon
            # Improvement 1: new function to define the event horizon
            event_horizon = (1 / black_hole_fitness) / np.sum(1 / stars_fitness_values)

            # Checks if the current star falls in the event horizon
            dist_to_black_hole = np.linalg.norm(black_hole_subset - current_star_subset)  # Euclidean distance
            if dist_to_black_hole < event_horizon:
                if debug:
                    logging.info(f'Star {a} has fallen inside event horizon. '
                                 f'Event horizon -> {event_horizon} | Star distance -> {dist_to_black_hole}')

                # Improvement 2: only ONE dimension of the feature array is changed
                random_feature_idx = random.randint(0, n_features - 1)
                stars_subsets[a][random_feature_idx] ^= 1  # Toggle 0/1

        # Updates the binary array of the used features
        # Improvement 3: new formula to 'move' the star
        w = 1 - (i / n_iterations)
        d1 = coeff_1 + w
        d2 = coeff_2 + w

        for a in range(n_stars):
            # Skips the black hole
            if black_hole_idx == a:
                continue
            for d in range(n_features):
                x_old = stars_subsets[a][d]
                x_best = stars_best_subset[a][d]
                threshold = binary_threshold if binary_threshold is not None else random.uniform(0, 1)
                bh_star_diff = black_hole_subset[d] - x_old
                star_best_fit_diff = x_best - x_old
                x_new = x_old + (d1 * random.uniform(0, 1) * bh_star_diff) + (
                        d2 * random.uniform(0, 1) * star_best_fit_diff)
                stars_subsets[a][d] = 1 if abs(tanh(x_new)) > threshold else 0

    return black_hole_subset, black_hole_fitness


def binary_black_hole_spark(
        n_stars: int,
        n_features: int,
        n_iterations: int,
        fitness_function: Callable[[np.ndarray], CrossValidationSparkResult],
        sc: SparkContext,
        use_load_balancer: bool,
        number_of_workers: int,
        parameters: Optional[SVMParameters],  # TODO: make this a Optional[Tuple[RFParameters, SVMParameters]]
        more_is_better: bool,
        random_state: Optional[int],
        binary_threshold: Optional[float] = 0.6,
        debug: bool = False,
) -> Tuple[np.ndarray, float, np.ndarray, Optional[BaseEstimator], Dict]:
    """
    Computes the metaheuristic Binary Black Hole Algorithm in a Spark cluster. Taken from the paper
    "Binary black hole algorithm for feature selection and classification on biological data"
    Authors: Elnaz Pashaei, Nizamettin Aydin.
    :param n_stars: Number of stars.
    :param n_features: Number of features.
    :param n_iterations: Number of iterations.
    :param fitness_function: Fitness function to compute on every star.
    :param sc: Spark Context.
    :param use_load_balancer: If True, assigns a partition ID using a load balancer. If False, distributes sequentially.
    :param number_of_workers: Number of workers in the Spark cluster.
    :param parameters: Parameters of the RF/SVM for the load balancer. Only used if 'use_load_balancer' = True.
    :param more_is_better: If True, it returns the highest value (SVM and RF C-Index), lowest otherwise
    (LogRank p-value).
    :param random_state: Random state to replicate experiments. It allows to set the same number of features for every
    star and the same shuffling.
    :param binary_threshold: Binary threshold to set 1 or 0 the feature. If None it'll be computed randomly.
    :param debug: If True logs everything is happening inside BBHA.
    :return: The best subset found, the best fitness value found, the data returned by the Worker for the Black Hole,
    and all the data collected about times and execution during the experiment.
    """
    black_hole_model: Optional[BaseEstimator] = None

    # Lists for storing times and fitness data to train models
    number_of_features: List[int] = []
    hosts: List[str] = []
    partition_ids: List[int] = []
    fitness: List[float] = []
    time_exec: List[float] = []
    predicted_time_exec: List[float] = []
    svm_times_by_iteration: List[float] = []
    time_test: List[float] = []
    num_of_iterations: List[float] = []
    train_scores: List[float] = []

    # Data structs setup
    stars_subsets = np.empty((n_stars, 2), dtype=object)  # 2 = (1, features)

    # Initializes the stars with their subsets and their fitness values
    if debug:
        logging.info('Initializing stars...')

    for i in range(n_stars):
        current_random_state = random_state * (i + 1) if random_state is not None else None
        random_features_to_select = get_random_subset_of_features(n_features, current_random_state)
        stars_subsets[i] = (i, random_features_to_select)  # Initializes 'Population' with a key for partitionBy()

    initial_stars_results_values, _initial_total_time, initial_predicted_times_map = parallelize_fitness_execution_by_partitions(
        sc,
        stars_subsets,
        fitness_function,
        use_load_balancer,
        number_of_workers,
        parameters,
        debug
    )

    for star_idx, current_data in initial_stars_results_values:
        current_fitness_mean = current_data[0]
        worker_execution_time = current_data[1]
        partition_id = current_data[2]
        host_name = current_data[3]
        evaluated_features = current_data[4]
        time_by_iteration = current_data[6]
        model_test_time = current_data[7]
        mean_num_of_iterations = current_data[8]
        train_score = current_data[9]
        current_predicted_time = initial_predicted_times_map[star_idx]

        number_of_features.append(evaluated_features)
        hosts.append(host_name)
        partition_ids.append(partition_id)
        fitness.append((round(current_fitness_mean, 4)))
        time_exec.append(round(worker_execution_time, 4))
        svm_times_by_iteration.append(round(time_by_iteration, 4))
        time_test.append(round(model_test_time, 4))
        num_of_iterations.append(round(mean_num_of_iterations, 4))
        train_scores.append(round(train_score, 4))
        predicted_time_exec.append(round(current_predicted_time, 4))

    # The star with the best fitness is the black hole
    black_hole_idx, black_hole_subset, black_hole_data = get_best_spark(stars_subsets, initial_stars_results_values,
                                                                        more_is_better)
    black_hole_fitness = black_hole_data[0]
    black_hole_model = black_hole_data[10]

    if debug:
        logging.info(f'Black hole starting as star at index {black_hole_idx}')

    # To report every Spark's Worker idle time
    workers_idle_times: Dict[str, List[Tuple[int, float]]] = {}
    workers_execution_times_per_iteration: Dict[str, List[Tuple[int, float]]] = {}

    # Iterations
    for i in range(n_iterations):
        # To report every Spark's Worker idle time. It stores the execution time for the current iteration
        workers_execution_times: Dict[str, float] = {}

        if debug:
            logging.info(f'Iteration {i + 1}/{n_iterations}')

        stars_results_values, total_iteration_time, predicted_times_map = parallelize_fitness_execution_by_partitions(
            sc,
            stars_subsets,
            fitness_function,
            use_load_balancer,
            number_of_workers,
            parameters,
            debug
        )

        for star_idx, current_data in stars_results_values:
            current_fitness_mean = current_data[0]
            worker_execution_time = current_data[1]
            partition_id = current_data[2]
            host_name = current_data[3]
            evaluated_features = current_data[4]
            time_lapse_description = current_data[5]
            time_by_iteration = current_data[6]
            model_test_time = current_data[7]
            mean_num_of_iterations = current_data[8]
            train_score = current_data[9]
            current_predicted_time = predicted_times_map[star_idx]

            number_of_features.append(evaluated_features)
            hosts.append(host_name)
            partition_ids.append(partition_id)
            fitness.append((round(current_fitness_mean, 4)))
            time_exec.append(round(worker_execution_time, 4))
            svm_times_by_iteration.append(round(time_by_iteration, 4))
            time_test.append(round(model_test_time, 4))
            num_of_iterations.append(round(mean_num_of_iterations, 4))
            train_scores.append(round(train_score, 4))
            predicted_time_exec.append(round(current_predicted_time, 4))

            # Adds execution times to compute the idle time for all the workers. It's the difference between the
            # time it took to the master to distribute all the computations and get the result from all the workers;
            # and the sum of all the execution times for every star every Worker got
            if host_name not in workers_execution_times:
                workers_execution_times[host_name] = 0.0

            # Adds the time of the Worker to compute this star
            workers_execution_times[host_name] += worker_execution_time

            if debug:
                logging.info(
                    f'{star_idx} star took {round(worker_execution_time, 3)} seconds ({time_lapse_description}) '
                    f'for {evaluated_features} features. Partition: {partition_id} | '
                    f'Host name: {host_name}. Fitness: {current_fitness_mean}')

        # Stores the idle time for every worker in this iteration
        for host_name, sum_execution_times in workers_execution_times.items():
            if host_name not in workers_idle_times:
                workers_idle_times[host_name] = []

            if host_name not in workers_execution_times_per_iteration:
                workers_execution_times_per_iteration[host_name] = []

            if debug:
                logging.info(
                    f'The worker {host_name} has taken ~{sum_execution_times} seconds to compute all its stars')

            workers_execution_times_per_iteration[host_name].append((i, sum_execution_times))
            workers_idle_times[host_name].append((i, total_iteration_time - sum_execution_times))

        for a in range(n_stars):
            # If it's the black hole, skips the computation
            if a == black_hole_idx:
                continue

            # Computes the current star fitness
            current_star_subset = stars_subsets[a][1]
            current_data = stars_results_values[a][1]  # Discards star index
            current_fitness = current_data[0]

            # If it's the best fitness, swaps that star with the current black hole
            if (more_is_better and current_fitness > black_hole_fitness) or \
                    (not more_is_better and current_fitness < black_hole_fitness):
                if debug:
                    logging.info(f'Changing Black hole for star in position {a} of the array,'
                                 f' BH fitness -> {black_hole_fitness} | '
                                 f'Star in position {a} fitness -> {current_fitness}')
                black_hole_idx = a
                black_hole_subset, current_star_subset = current_star_subset, black_hole_subset
                black_hole_data, current_data = current_data, black_hole_data
                black_hole_fitness, current_fitness = current_fitness, black_hole_fitness
                black_hole_model = black_hole_data[10]

            # If the fitness function was the same, but had fewer features in the star (better!), makes the swap
            elif current_fitness == black_hole_fitness and np.count_nonzero(current_star_subset) < np.count_nonzero(
                    black_hole_subset):
                if debug:
                    logging.info(f'Changing Black hole for star in position {a} of the array,'
                                 f' BH fitness -> {black_hole_fitness} | '
                                 f'Star in position {a} fitness -> {current_fitness}')
                black_hole_idx = a
                black_hole_subset, current_star_subset = current_star_subset, black_hole_subset
                black_hole_data, current_data = current_data, black_hole_data
                black_hole_fitness, current_fitness = current_fitness, black_hole_fitness
                black_hole_model = black_hole_data[10]

            # Computes the event horizon
            event_horizon = black_hole_fitness / np.sum(current_fitness)

            # Checks if the current star falls in the event horizon
            dist_to_black_hole = np.linalg.norm(black_hole_subset - current_star_subset)  # Euclidean distance
            if dist_to_black_hole < event_horizon:
                if debug:
                    logging.info(f'Star in position {a} of the array has fallen inside event horizon. '
                                 f'Event horizon -> {event_horizon} | Star distance -> {dist_to_black_hole}')

                current_random_state = random_state * (i * (a + 1)) if random_state is not None else None
                stars_subsets[a] = (a, get_random_subset_of_features(n_features, current_random_state))

        # Updates the binary array of the used features
        for a in range(n_stars):
            # Skips the black hole
            if black_hole_idx == a:
                continue
            for d in range(n_features):
                x_old = stars_subsets[a][1][d]
                threshold = binary_threshold if binary_threshold is not None else random.uniform(0, 1)
                x_new = x_old + random.uniform(0, 1) * (black_hole_subset[d] - x_old)  # Position
                stars_subsets[a][1][d] = 1 if abs(tanh(x_new)) > threshold else 0

    # Computes avg/std of idle times for all the iterations for every Worker
    workers_idle_times_res: Dict[str, Dict[str, float]] = {}
    for host_name, idle_times in workers_idle_times.items():
        only_times = np.array(idle_times)[:, 1]  # Discards iteration number
        workers_idle_times_res[host_name] = {
            'mean': round(cast(float, np.mean(only_times)), 4),
            'std': round(cast(float, np.std(only_times)), 4)
        }

    # NOTE: the rest of the data such as 'dataset' and 'model' is added in core.py
    result_dict = {
        'number_of_features': number_of_features,
        'execution_times': time_exec,
        'predicted_execution_times': predicted_time_exec,
        'fitness': fitness,
        'times_by_iteration': svm_times_by_iteration,
        'test_times': time_test,
        'train_scores': train_scores,
        'number_of_iterations': num_of_iterations,
        'hosts': hosts,
        'workers_execution_times_per_iteration': workers_execution_times_per_iteration,
        'workers_idle_times': workers_idle_times_res,
        # Yes, names are confusing, but workers_idle_times_res has mean and std
        'workers_idle_times_per_iteration': workers_idle_times,
        'partition_ids': partition_ids
    }

    return black_hole_subset, black_hole_fitness, black_hole_data, black_hole_model, result_dict
