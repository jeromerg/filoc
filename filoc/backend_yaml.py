import os
from typing import Dict, Any

import yaml
from fsspec import AbstractFileSystem

from filoc.contract import PropsList, BackendContract
from filoc.utils import filter_and_coerce_loaded_file_content, coerce_file_content_to_write


class YamlBackend(BackendContract):
    def __init__(self, is_singleton) -> None:
        super().__init__()
        self.is_singleton = is_singleton

    def read(self, fs: AbstractFileSystem, path: str, constraints: Dict[str, Any]) -> PropsList:
        with fs.open(path) as f:
            return filter_and_coerce_loaded_file_content(path, yaml.load(f), constraints, self.is_singleton)

    def write(self, fs: AbstractFileSystem, path: str, props_list: PropsList) -> None:
        fs.makedirs(os.path.dirname(path), exist_ok=True)
        with fs.open(path, 'w') as f:
            return yaml.dump(coerce_file_content_to_write(path, props_list, self.is_singleton), f, indent=2)