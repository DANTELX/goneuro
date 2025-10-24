import dask.dataframe as dd
from collections import Counter


class PreprocessData:
    def __init__(self, output_data_path, dataset_data_path):
        self._ouput_data_path = output_data_path
        self._dataset_data_path = dataset_data_path
        self.dataset = None

    def read_dataset(self, dataset_dir_name):
        self.dataset = dd.read_parquet()

    def preprocess(self, dataset_dir_name):

        pass

    def list(self):
        pass

    def delete(self, dataset_dir_name):
        pass
