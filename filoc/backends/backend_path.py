""" Filoc default CSV backend implementation """

from fsspec import AbstractFileSystem

from filoc.contract import PropsList, BackendContract, Constraints, Props


# TODO: Unit tests of CSV Backend and all default backends

class PathBackend(BackendContract):
    """
    filoc backend used to list out file paths and their props, ignoring their content.
    
    .. code-block:: python
        loc = filoc('/my/locpath/{id}/{filename_without_extension}.{ext}', backend='path')
    """

    def __init__(self) -> None:
        """(see BackendContract contract)"""
        super().__init__()

    def read(self, fs: AbstractFileSystem, path: str, path_props : Props, constraints: Constraints) -> PropsList:
        """(see BackendContract contract) """
        return [{}]

    def write(self, fs: AbstractFileSystem, path: str, props_list: PropsList) -> None:
        """(see BackendContract contract)"""
        raise NotImplementedError("NoContentBackend does not support write operation")
