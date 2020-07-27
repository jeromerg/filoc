# -------------
# Types aliases
# -------------
from typing import TypeVar, Literal, Dict, Any, List, Union, Callable

from fsspec import AbstractFileSystem

TContent             = TypeVar('TContent')
TContents            = TypeVar('TContents')
FrontendTypes        = Literal[None, 'json', 'pandas']
PresetBackendTypes   = Literal[None, 'json', 'yaml', 'pickle', 'csv']
ContentPath          = str
Props                = Dict[str, Any]
PropsConstraints     = Dict[str, Any]
PropsList            = List[Props]
BackendReader        = Union[
    Callable[[AbstractFileSystem, ContentPath], PropsList],
    Callable[[AbstractFileSystem, ContentPath, Props], PropsList]]
BackendWriter        = Union[
    Callable[[AbstractFileSystem, ContentPath, PropsList], None],
    Callable[[AbstractFileSystem, ContentPath, PropsList, Props], None]]
PropsListToContent   = Callable[[PropsList], TContent]
PropsListToContents  = Callable[[PropsList], TContents]
ContentToPropsList   = Callable[[TContent ], PropsList]
ContentsToPropsList  = Callable[[TContents], PropsList]
