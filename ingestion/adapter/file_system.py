"""Connector and methods accessing file systems"""
import glob
import os
import pandas as pd
from datetime import datetime
import logging


class FileSystemConnector:
    """
        Class for interacting FileSystem
    """

    def __init__(self, root_path: str):
        self.root_path = root_path
        self.logger = logging

    def get_file_path(self, batch_date, file_date_format, file_name):
        arg_date_dt = datetime.strptime(batch_date, "%Y-%m-%d").date()
        arg_batch_date = arg_date_dt.strftime(file_date_format)
        return glob.glob(os.path.join(self.root_path, file_name.format(arg_batch_date)))

    def read_csv(self, file_path_list, columns):
        dfs_list = []
        try:
            for file_path in file_path_list:
                self.logger.info('Reading file %s', file_path)
                dfs_list.append(pd.read_csv(file_path, header=None, names=columns))
            df = pd.concat(dfs_list, ignore_index=True)
            return df
        except ValueError as e:
            self.logger.info("Failed to read file %s, Error: %s", file_path_list, e)
            raise e
