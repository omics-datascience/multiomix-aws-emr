from utils import KernelName, OptimizerName


class ModelParameters:
    """Super class for Model's parameters to predict execution times."""
    number_of_samples: int

    def __init__(self, number_of_samples: int):
        self.number_of_samples = number_of_samples


class SVMParameters(ModelParameters):
    kernel: KernelName
    optimizer: OptimizerName

    def __init__(self, number_of_samples: int, kernel: KernelName, optimizer: OptimizerName):
        super(SVMParameters, self).__init__(number_of_samples)

        self.kernel = kernel
        self.optimizer = optimizer

    def __str__(self):
        return f'SVMParameters(number_of_samples={self.number_of_samples}, kernel="{self.kernel}", optimizer="{self.optimizer}")'


# TODO: implement RFParameters and ClusteringLogRankParameters when model are loaded