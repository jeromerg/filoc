import csv
import os
from typing import Dict, Any

from fsspec import AbstractFileSystem

from filoc.contract import PropsList, BackendContract
from filoc.utils import filter_and_coerce_loaded_file_content, coerce_file_content_to_write

# TODO: Unit tests of CSV Backend and all default backends

class CsvBackend(BackendContract):
    """
    Filoc backend used to read data from CSV files and write into them. This implementation is used when you call the filoc factory with the ``backend`` argument set to ``'csv'``. Example:
    
    .. code-block:: python

        loc = filoc('/my/locpath/{id}/data.csv', backend='csv')
    """
    def __init__(self, is_singleton) -> None:
        super().__init__()
        self.is_singleton = is_singleton

    def read(self, fs: AbstractFileSystem, path: str, constraints: Dict[str, Any]) -> PropsList:
        with fs.open(path) as f:
            return filter_and_coerce_loaded_file_content(path, csv.DictReader(f), constraints, self.is_singleton)

    def write(self, fs: AbstractFileSystem, path: str, props_list: PropsList) -> None:
        fs.makedirs(os.path.dirname(path), exist_ok=True)
        with fs.open(path, 'w') as f:
            return csv.DictWriter(coerce_file_content_to_write(path, props_list, self.is_singleton), f)
