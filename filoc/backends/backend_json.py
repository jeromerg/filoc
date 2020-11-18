""" Filoc default JSON backend implementation """
import json
import os
from typing import Dict, Any

from fsspec import AbstractFileSystem

from filoc.contract import PropsList, BackendContract, Constraints, Props
from filoc.utils import filter_and_coerce_loaded_file_content, coerce_file_content_to_write


class JsonBackend(BackendContract):
    """
    filoc backend used to read data from CSV files and write into them. This implementation is used when you call the filoc factory with the ``backend`` argument set to ``'json'``. Example:
    
    .. code-block:: python

        loc = filoc('/my/locpath/{id}/data.json', backend='json')
    """
    def __init__(self, is_singleton, encoding) -> None:
        super().__init__()
        self.is_singleton = is_singleton
        self.encoding     = encoding

    def read(self, fs: AbstractFileSystem, path: str, path_props : Props, constraints: Constraints) -> PropsList:
        """(see BackendContract contract) """
        with fs.open(path, encoding=self.encoding) as f:
            try:
                content = json.load(f)
            except Exception as e:
                raise ValueError(f'Could not json.load file "{path}"') from e

            return filter_and_coerce_loaded_file_content(path, content, path_props, constraints, self.is_singleton)

    def write(self, fs: AbstractFileSystem, path: str, props_list: PropsList) -> None:
        """(see BackendContract contract)"""
        fs.makedirs(os.path.dirname(path), exist_ok=True)
        with fs.open(path, 'w', encoding=self.encoding) as f:
            return json.dump(coerce_file_content_to_write(path, props_list, self.is_singleton), f, indent=2)
