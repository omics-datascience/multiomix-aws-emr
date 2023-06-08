from typing import Tuple, Union, List
from lifelines import CoxPHFitter
from pyspark import TaskContext, Broadcast
from sklearn.cluster import KMeans, SpectralClustering
from core import run_bbha_experiment
from parameters import Parameters
from utils import get_columns_from_df
import pandas as pd
from sklearn.model_selection import cross_val_score, cross_validate
import numpy as np
from sksurv.ensemble import RandomSurvivalForest
from sksurv.svm import FastKernelSurvivalSVM
import logging
import time
from pyspark import SparkContext
import socket
from datetime import datetime
from multiprocessing import Process, Queue
from filelock import FileLock

# Enables info logging
logging.getLogger().setLevel(logging.INFO)

# Gets params from line arguments
params = Parameters()

# Classifier to use
if params.model == 'rf':
    CLASSIFIER = RandomSurvivalForest(n_estimators=params.rf_n_estimators,
                                      min_samples_split=10,
                                      min_samples_leaf=15,
                                      max_features="sqrt",
                                      n_jobs=params.tree_n_jobs,
                                      random_state=params.random_state)
elif params.model == 'svm':
    rank_ratio = 0.0 if params.svm_is_regression else 1.0
    CLASSIFIER = FastKernelSurvivalSVM(rank_ratio=rank_ratio, max_iter=params.svm_max_iterations, tol=1e-5,
                                       kernel=params.svm_kernel, optimizer=params.svm_optimizer,
                                       random_state=params.random_state)
else:
    CLASSIFIER = None


def get_clustering_model() -> Union[KMeans, SpectralClustering]:
    """Gets the specified clustering model to train"""
    if params.clustering_algorithm == 'kmeans':
        return KMeans(n_clusters=params.number_of_clusters)
    elif params.clustering_algorithm == 'spectral':
        return SpectralClustering(n_clusters=params.number_of_clusters)

    raise Exception('Invalid params.clustering_algorithm parameter')


def compute_cross_validation_spark_f(subset: pd.DataFrame, y: np.ndarray, q: Queue):
    """
    Computes a cross validations to get the concordance index in a Spark environment
    :param subset: Subset of features to compute the cross validation
    :param y: Y data
    :param q: Queue to return Process result
    """
    try:
        n_features = subset.shape[1]

        # Locks to prevent multiple partitions in one worker getting all cores and degrading the performance
        logging.info(f'Waiting lock to compute CV with {n_features} features')
        with FileLock(f"/home/big_data/svm-surv.lock"):
            logging.info('File lock acquired, computing CV...')

            if params.model == 'clustering':
                start = time.time()

                # Groups using the selected clustering algorithm
                clustering_model = get_clustering_model()
                clustering_result = clustering_model.fit(subset.values)

                # Generates a DataFrame with a column for time, event and the group
                labels = clustering_result.labels_
                dfs: List[pd.DataFrame] = []
                for cluster_id in range(params.number_of_clusters):
                    current_group_y = y[np.where(labels == cluster_id)]
                    dfs.append(
                        pd.DataFrame({'E': current_group_y['event'], 'T': current_group_y['time'], 'group': cluster_id})
                    )
                df = pd.concat(dfs)

                # Fits a Cox Regression model using the column group as the variable to consider
                cph = CoxPHFitter().fit(df, duration_col='T', event_col='E')

                # This documentation recommends using log-likelihood to optimize:
                # https://lifelines.readthedocs.io/en/latest/Survival%20Regression.html#log-likelihood
                fitness_value = cph.log_likelihood_
                end_time = time.time()
                worker_execution_time = end_time - start  # Duplicated to not consider consumed time by all the metrics below

                metric_description = 'Log Likelihood (higher is better)'
                mean_test_time = 0.0
                mean_train_score = 0.0
            else:
                logging.info(f'Computing CV ({params.cv_folds} folds) with {params.model} model')
                start = time.time()
                cv_res = cross_validate(
                    CLASSIFIER,
                    subset,
                    y,
                    cv=params.cv_folds,
                    n_jobs=params.n_jobs,
                    return_estimator=True,
                    return_train_score=params.return_train_scores
                )
                fitness_value = cv_res['test_score'].mean()  # This is the C-Index
                end_time = time.time()
                worker_execution_time = end_time - start  # Duplicated to not consider consumed time by all the metrics below

                metric_description = 'Concordance Index (higher is better)'
                mean_test_time = np.mean(cv_res['score_time'])
                mean_train_score = cv_res['train_score'].mean() if params.return_train_scores else 0.0

            logging.info(f'Fitness function with {n_features} features: {worker_execution_time} seconds | '
                         f'{metric_description}: {fitness_value}')

            partition_id = TaskContext().partitionId()

            # Gets a time-lapse description to check if some worker is lazy
            start_desc = datetime.fromtimestamp(start).strftime("%H:%M:%S")
            end_desc = datetime.fromtimestamp(end_time).strftime("%H:%M:%S")
            time_description = f'{start_desc} - {end_desc}'

            # 'res' is only defined when using SVM or RF

            # Gets number of iterations (only for SVM)
            if params.model == 'svm':
                times_by_iteration = []
                total_number_of_iterations = []
                for estimator, fit_time in zip(cv_res['estimator'], cv_res['fit_time']):
                    # Scikit-surv doesn't use BaseLibSVM. So it doesn't have 'n_iter_' attribute
                    # number_of_iterations += np.sum(estimator.n_iter_)
                    number_of_iterations = estimator.optimizer_result_.nit
                    time_by_iterations = fit_time / number_of_iterations
                    times_by_iteration.append(time_by_iterations)
                    total_number_of_iterations.append(number_of_iterations)

                mean_times_by_iteration = np.mean(times_by_iteration)
                mean_total_number_of_iterations = np.mean(total_number_of_iterations)
            else:
                mean_times_by_iteration = 0.0
                mean_total_number_of_iterations = 0.0

            q.put([
                fitness_value,
                worker_execution_time,
                partition_id,
                socket.gethostname(),
                subset.shape[1],
                time_description,
                mean_times_by_iteration,
                mean_test_time,
                mean_total_number_of_iterations,
                mean_train_score
            ])
    except Exception as ex:
        logging.error('An exception has occurred in the fitness function:')
        logging.exception(ex)

        # Returns empty values
        q.put([
            -1.0,  # Fitness value,
            -1.0,  # Worker time,
            -1.0,  # Partition ID,
            '',  # Host name,
            0,  # Number of features,
            '',  # Time description,
            -1.0,  # Mean times_by_iteration,
            -1.0,  # Mean test_time,
            -1.0,  # Mean total_number_of_iterations,
            -1.0  # Mean train_score
        ])


def compute_cross_validation_spark(
        subset: Union[pd.DataFrame, Broadcast],
        index_array: np.ndarray,
        y: np.ndarray,
        is_broadcast: bool
) -> Tuple[float, float, int, str, int, str, float, float, float, float]:
    """
    Calls fitness inside a Process to prevent issues with memory leaks in Python.
    More info: https://stackoverflow.com/a/71700592/7058363
    :param is_broadcast: if True, the subset is a Broadcast instance
    :param index_array: Binary array where 1 indicates that the feature in that position must be included
    :param subset: Subset of features to compute the cross validation
    :param y: Y data
    :return: Result tuple with [0] -> fitness value, [1] -> execution time, [2] -> Partition ID, [3] -> Hostname,
    [4] -> number of evaluated features, [5] -> time lapse description, [6] -> time by iteration and [7] -> avg test time
    [8] -> mean of number of iterations of the model inside the CV, [9] -> train score
    """
    # If broadcasting is enabled, the retrieves the Broadcast instance value
    x_values = subset.value if is_broadcast else subset

    q = Queue()
    parsed_data = get_columns_from_df(index_array, x_values)
    p = Process(target=compute_cross_validation_spark_f, args=(parsed_data, y, q))
    p.start()
    process_result = q.get()
    p.join()
    return process_result


def compute_cross_validation_sequential(subset: pd.DataFrame, y: np.ndarray) -> float:
    """
    Computes CV to get the Concordance Index
    :param subset: Subset of features to be used in the model evaluated in the CrossValidation
    :param y: Classes
    :return: Average of the C-Index obtained in each CrossValidation fold
    """
    start = time.time()
    res = cross_val_score(
        CLASSIFIER,
        subset,
        y,
        cv=10,
        n_jobs=params.n_jobs
    )
    end_time = time.time() - start
    concordance_index_mean = res.mean()
    logging.info(f'Fitness function with {subset.shape[1]} features: {end_time} seconds | '
                 f'Concordance Index: {concordance_index_mean}')

    return concordance_index_mean


def main():
    if params.model == 'svm':
        task = 'regression' if params.svm_is_regression else 'ranking'
        parameters_description = f'{task}_{params.svm_max_iterations}_max_iterations_{params.svm_optimizer}' \
                                 f'_optimizer_{params.svm_kernel}_kernel'
        svm_kernel = params.svm_kernel
    else:
        svm_kernel = None

        if params.model == 'rf':
            parameters_description = f'{params.rf_n_estimators}_trees'
        else:
            parameters_description = f'{params.number_of_clusters}_clusters_{params.clustering_algorithm}_algorithm'

    # Spark settings
    app_name = f"BBHA_{time.time()}".replace('.', '_')
    sc = SparkContext()
    sc.setLogLevel("ERROR")

    # Gets the number of workers
    sc2 = sc._jsc.sc()
    number_of_workers = len([executor.host() for executor in
                             sc2.statusTracker().getExecutorInfos()]) - 1  # Subtract 1 to discard the master

    fitness_function = compute_cross_validation_spark

    run_improved_bbha = False  # TODO: improved BBHA it's not implemented for Spark right now

    # Runs normal Feature Selection experiment using BBHA
    run_bbha_experiment(
        app_name=app_name,
        use_load_balancer=params.use_load_balancer and params.model == 'svm',  # TODO: implement for the RF and Cox Regression
        more_is_better=True,  # params.model != 'clustering',  # CoxRegression is evaluated by the log likelihood. If it is lower, better! TODO: parametrize the clustering metric
        svm_kernel=svm_kernel,
        svm_optimizer=params.svm_optimizer,
        run_improved_bbha=run_improved_bbha,
        n_stars=params.n_stars,
        random_state=params.random_state,
        compute_cross_validation=fitness_function,
        sc=sc,
        metric_description='concordance index' if params.model != 'clustering' else 'log_likelihood' ,  # TODO: check this, log_likelihood is not always used
        debug=params.debug,
        molecules_dataset=params.molecules_dataset,
        clinical_dataset=params.clinical_dataset,
        number_of_independent_runs=params.number_of_independent_runs,
        n_iterations=params.bbha_n_iterations,
        number_of_workers=number_of_workers,
        model_name=params.model,
        parameters_description=parameters_description,
        use_broadcasts_in_spark=params.use_broadcast
    )


if __name__ == '__main__':
    main()
