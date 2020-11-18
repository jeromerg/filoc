""" Filoc default YAML backend implementation """
import os
from typing import Dict, Any

import yaml
from fsspec import AbstractFileSystem

from filoc.contract import PropsList, BackendContract, Constraints, Props
from filoc.utils import filter_and_coerce_loaded_file_content, coerce_file_content_to_write


class YamlBackend(BackendContract):
    """
    filoc backend used to read data from YAML files and write into them. This implementation is used when you call the filoc factory with the ``backend`` argument set to ``'yaml'``. Example:
    
    .. code-block:: python

        loc = filoc('/my/locpath/{id}/data.yaml', backend='yaml')
    """
    def __init__(self, is_singleton, encoding) -> None:
        super().__init__()
        self.is_singleton = is_singleton
        self.encoding     = encoding

    def read(self, fs: AbstractFileSystem, path: str, path_props : Props, constraints: Constraints) -> PropsList:
        """(see BackendContract contract) """
        with fs.open(path, encoding=self.encoding) as f:
            return filter_and_coerce_loaded_file_content(path, yaml.load(f), path_props, constraints, self.is_singleton)

    def write(self, fs: AbstractFileSystem, path: str, props_list: PropsList) -> None:
        """(see BackendContract contract)"""
        fs.makedirs(os.path.dirname(path), exist_ok=True)
        with fs.open(path, 'w', encoding=self.encoding) as f:
            return yaml.dump(coerce_file_content_to_write(path, props_list, self.is_singleton), f, indent=2)
