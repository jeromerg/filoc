import os
import pickle
from typing import Dict, Any

from fsspec import AbstractFileSystem

from filoc.contract import PropsList, BackendContract
from filoc.utils import filter_and_coerce_loaded_file_content, coerce_file_content_to_write


class PickleBackend(BackendContract):
    """
    Filoc backend used to read data from pickle files and write into them. This implementation is used when you call ``filoc("...", backend="pickle")``.
    Only files containing a list or a dictionary as root object are supported, depending on the ``is_singleton`` flag. We recommend to write you own 
    backend, if you want to read pickle files, that have not be created by filoc itself. With that way, you can better handle the domain information of the files content 
    to read them properly.
    """
    def __init__(self, is_singleton) -> None:
        super().__init__()
        self.is_singleton = is_singleton

    def read(self, fs: AbstractFileSystem, path: str, constraints: Dict[str, Any]) -> PropsList:
        with fs.open(path) as f:
            return filter_and_coerce_loaded_file_content(path, pickle.load(f), constraints, self.is_singleton)

    def write(self, fs: AbstractFileSystem, path: str, props_list: PropsList) -> None:
        fs.makedirs(os.path.dirname(path), exist_ok=True)
        with fs.open(path, 'w') as f:
            return pickle.dump(coerce_file_content_to_write(path, props_list, self.is_singleton), f)
