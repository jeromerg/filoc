""" Filoc default pickle backend implementation """
import os
import pickle
from typing import Dict, Any

from fsspec import AbstractFileSystem

from filoc.contract import PropsList, BackendContract, Constraints, Props
from filoc.utils import filter_and_coerce_loaded_file_content, coerce_file_content_to_write


class PickleBackend(BackendContract):
    """
    filoc backend used to read data from Pickle files and write into them. This implementation is used when you call the filoc factory with the ``backend`` argument set to ``'pickle'``. Example:
    
    .. code-block:: python

        loc = filoc('/my/locpath/{id}/data.pickle', backend='pickle')

    It is recommended to read files that you wrote with filoc itself. If you want to read pickle files written by a third library, it is recommended to implement your own backend, 
    so that you can better handle the edge cases and print out better error messages.
    """
    def __init__(self, is_singleton) -> None:
        super().__init__()
        self.is_singleton = is_singleton

    def read(self, fs: AbstractFileSystem, path: str, path_props : Props, constraints: Constraints) -> PropsList:
        """(see BackendContract contract) """
        with fs.open(path, 'rb') as f:
            return filter_and_coerce_loaded_file_content(path, pickle.load(f), path_props, constraints, self.is_singleton)

    def write(self, fs: AbstractFileSystem, path: str, props_list: PropsList) -> None:
        """(see BackendContract contract)"""
        fs.makedirs(os.path.dirname(path), exist_ok=True)
        with fs.open(path, 'wb') as f:
            return pickle.dump(coerce_file_content_to_write(path, props_list, self.is_singleton), f)
