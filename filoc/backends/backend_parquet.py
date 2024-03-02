""" Filoc default parquet backend implementation """
import os

import pandas as pd
from fsspec import AbstractFileSystem

from filoc.contract import PropsList, BackendContract, Constraints, Props
from filoc.utils import filter_and_coerce_loaded_file_content, coerce_file_content_to_write


class ParquetBackend(BackendContract):
    """
    filoc backend used to read data from Parquet files and write into them. This implementation is used when you call the filoc factory with the ``backend`` argument set to ``'parquet'``. Example:
    
    .. code-block:: python

        loc = filoc('/my/locpath/{id}/data.parquet', backend='parquet')

    It is recommended to read files that you wrote with filoc itself. If you want to read parquet files written by a third library, it is recommended to implement your own backend,
    so that you can better handle the edge cases and print out better error messages.
    """
    def __init__(self) -> None:
        super().__init__()

    def read(self, fs: AbstractFileSystem, path: str, path_props : Props, constraints: Constraints) -> PropsList:
        """(see BackendContract contract) """
        with fs.open(path, 'rb') as f:
            df = pd.read_parquet(f)
            file_content = df.to_dict(orient='records')
            return filter_and_coerce_loaded_file_content(path, file_content, path_props, constraints, False)

    def write(self, fs: AbstractFileSystem, path: str, props_list: PropsList) -> None:
        """(see BackendContract contract)"""
        fs.makedirs(os.path.dirname(path), exist_ok=True)
        with (fs.open(path, 'wb') as f):
            file_content = coerce_file_content_to_write(path, props_list, False)
            df_file_content = pd.DataFrame(file_content)
            df_file_content.to_parquet(f, index=False)
