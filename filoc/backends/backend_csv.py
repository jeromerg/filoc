import os
from collections import OrderedDict
from csv import DictWriter, DictReader
from typing import Dict, Any

from fsspec import AbstractFileSystem
from orderedset import OrderedSet

from filoc.contract import PropsList, BackendContract
from filoc.utils import filter_and_coerce_loaded_file_content


# TODO: Unit tests of CSV Backend and all default backends

class CsvBackend(BackendContract):
    """
    filoc backend used to read data from CSV files and write into them. This implementation is used when you call the filoc factory with the ``backend`` argument set to ``'csv'``. Example:
    
    .. code-block:: python

        loc = filoc('/my/locpath/{id}/data.csv', backend='csv')
    """

    def __init__(self, is_singleton) -> None:
        """(see BackendContract contract)"""
        super().__init__()
        self.is_singleton = is_singleton

    def read(self, fs: AbstractFileSystem, path: str, constraints: Dict[str, Any]) -> PropsList:
        """(see BackendContract contract)"""
        with fs.open(path) as f:
            reader = DictReader(f)
            props_list = [OrderedDict(row) for row in reader]
            return filter_and_coerce_loaded_file_content(path, props_list, constraints, self.is_singleton)

    def write(self, fs: AbstractFileSystem, path: str, props_list: PropsList) -> None:
        """(see BackendContract contract)"""
        fs.makedirs(os.path.dirname(path), exist_ok=True)

        fieldnames = OrderedSet()
        for props in props_list:
            fieldnames |= props.keys()

        with fs.open(path, 'w') as f:
            writer = DictWriter(f, fieldnames)
            writer.writeheader()
            writer.writerows(props_list)
