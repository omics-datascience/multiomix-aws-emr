from argparse import ArgumentParser
from typing import Literal, Optional
from utils import ModelName


class Parameters:
    """Gets all the parameters from CLI."""
    master_connection_url: str

    # To use a Broadcast value instead of a pd.DataFrame
    use_broadcast: bool

    # SVM kernel function. NOTE: 'sigmoid' presents many NaNs values and 'precomputed' doesn't work in this context
    svm_kernel: Literal["linear", "poly", "rbf", "sigmoid", "cosine", "precomputed"]
    svm_optimizer: Literal["avltree", "rbtree"]

    # If True, a regression task is performed, otherwise it executes a ranking task
    svm_is_regression: bool

    # Max number of SVM iterations
    svm_max_iterations: int

    # Number of cores used by the worker to compute the Cross Validation. -1 = use all
    n_jobs: int

    # Number of folds in the CrossValidation
    cv_folds: int

    # To get the training score or not during CV
    return_train_scores: bool

    # Number of stars in the BBHA
    n_stars: int = 3

    # To replicate randomness
    random_state: Optional[int]

    # If True, load balancer is used to generate Spark partitions
    use_load_balancer: bool

    # Number of independent complete runs to get the best parameters
    number_of_independent_runs: int

    # Filename of the dataset with molecule expressions to use
    molecules_dataset: str

    # Filename of the dataset with clinical data to use
    clinical_dataset: str

    # Number of iterations for the BBHA algorithm
    bbha_n_iterations: int

    # Classifier to use in the metaheuristic
    model: ModelName

    # Number of trees in Random Forest
    rf_n_estimators: int

    # Number of cores used by the RandomForest training. -1 = use all
    tree_n_jobs: int

    # Clustering algorithm to use
    clustering_algorithm: Literal["kmeans", "spectral"]

    # Number of clusters to group by molecule expressions during clustering algorithm
    number_of_clusters: int

    # If True it logs all the star values in the terminal
    debug: bool

    def __init__(self):
        # Specifies all the possible parameters
        parser = ArgumentParser()

        # General parameters
        parser.add_argument("--master", dest='master', help="URL to connect to the master", type=str,
                            default="spark://master-node:7077")
        parser.add_argument("--molecules-dataset", dest='molecules_dataset',
                            help="Filename of the dataset with molecule expressions to use. This file must be in the "
                                 "shared folder", type=str)
        parser.add_argument("--clinical-dataset", dest='clinical_dataset',
                            help="Filename of the dataset with clinical data to use. This file must be in the "
                                 "shared folder", type=str)
        parser.add_argument("--model", dest='model', choices=['svm', 'rf', 'clustering'],
                            help="Classifier to use in the metaheuristic", type=str, default="svm")
        parser.add_argument('--independent-runs', dest='number_of_independent_runs',
                            help="Number of independent complete runs to get the best parameters", type=int,
                            default=1)  # TODO: change default to 3
        parser.add_argument('--use-load-balancer', dest='use_load_balancer',
                            help="If True, load balancer is used to generate Spark partitions", default=True,
                            action='store_true')
        parser.add_argument('--svm-is-regression', dest='svm_is_regression',
                            help="If True, load balancer is used to generate Spark partitions", default=True,
                            action='store_true')
        parser.add_argument("--random-state", dest='random_state', help="Random seed to replicate randomness", type=int,
                            default=None)
        parser.add_argument('--use-broadcast', dest='use_broadcast',
                            help="If True, it broadcast the data to all the workers node", default=True,
                            action='store_true')
        parser.add_argument('--debug', dest='debug',
                            help="If True it logs all the star values in the terminal", default=False,
                            action='store_true')

        # SVM parameters
        parser.add_argument("--svm-kernel", dest='svm_kernel',
                            choices=["linear", "poly", "rbf", "sigmoid", "cosine", "precomputed"],
                            help="Kernel of the SVM", type=str, default="linear")
        parser.add_argument("--svm-optimizer", dest='svm_optimizer', choices=["avltree", "rbtree"],
                            help="Optimizer of the SVM", type=str, default="avltree")
        parser.add_argument("--svm-max-iterations", dest='svm_max_iterations',
                            help="Max number of iterations to train the SVM", type=int, default=1000)

        # RF parameters
        parser.add_argument("--rf-n-estimators", dest='rf_n_estimators', help="Number of trees in Random Forest",
                            type=int, default=10)
        parser.add_argument("--tree-n-jobs", dest='tree_n_jobs',
                            help="Number of cores used by the RandomForest training. -1 = use all",
                            type=int, default=-1)

        # Clustering parameters
        parser.add_argument("--clustering-algorithm", dest='clustering_algorithm', choices=['k_means', 'spectral'],
                            help="Clustering algorithm to use", type=str, default="k_means")
        parser.add_argument("--number-of-clusters", dest='number_of_clusters',
                            help="Number of clusters to group by molecule expressions during clustering algorithm",
                            type=int, default=2)

        # CV parameters
        parser.add_argument("--n-jobs", dest='n_jobs',
                            help="Number of cores used by the worker to compute the Cross Validation. -1 = use all",
                            type=int, default=-1)
        parser.add_argument("--cv-folds", dest='cv_folds', help="Number of folds in the Cross Validation", type=int,
                            default=3)  # TODO: change default to 10
        parser.add_argument('--return-train-scores', dest='return_train_scores',
                            help="To get the training score or not during CV", default=True,
                            action='store_true')

        # BBHA parameters
        # TODO: change default to 30
        parser.add_argument("--bbha-iterations", dest='bbha_n_iterations',
                            help="Number of iterations for the BBHA algorithm", type=int, default=1)
        # TODO: change default to 30
        parser.add_argument("--n-stars", dest='n_stars', help="Number of stars in the BBHA", type=int, default=3)

        args = parser.parse_args()

        # Assigns parameters
        self.master_connection_url = args.master
        self.svm_kernel = args.svm_kernel
        self.svm_optimizer = args.svm_optimizer
        self.cv_folds = args.cv_folds
        self.return_train_scores = args.return_train_scores
        self.n_stars = args.n_stars
        self.random_state = args.random_state
        self.use_load_balancer = False   # TODO: leave this args.use_load_balancer when implemented
        self.number_of_independent_runs = args.number_of_independent_runs
        self.molecules_dataset = args.molecules_dataset
        self.clinical_dataset = args.clinical_dataset
        self.bbha_n_iterations = args.bbha_n_iterations
        self.model = args.model
        self.rf_n_estimators = args.rf_n_estimators
        self.svm_is_regression = args.svm_is_regression
        self.svm_max_iterations = args.svm_max_iterations
        self.clustering_algorithm = args.clustering_algorithm
        self.number_of_clusters = args.number_of_clusters
        self.use_broadcast = args.use_broadcast
        self.debug = args.debug
        self.n_jobs = args.n_jobs
        self.tree_n_jobs = args.tree_n_jobs
