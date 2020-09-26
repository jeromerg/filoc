# -------------
# Types aliases
# -------------
from abc import ABC
from typing import TypeVar, Literal, Dict, Any, List, Generic, Optional

from fsspec import AbstractFileSystem

TContent             = TypeVar('TContent')
TContents            = TypeVar('TContents')
PresetFrontends      = Literal['json', 'pandas']
PresetBackends       = Literal['json', 'yaml', 'pickle']
ContentPath          = str
Props                = Dict[str, Any]
PropsConstraints     = Dict[str, Any]
PropsList            = List[Props]


class BackendContract(ABC):
    def read(self, fs : AbstractFileSystem, path : str, constraints : Dict[str, Any]) -> PropsList:
        raise NotImplementedError("Abstract")

    def write(self, fs : AbstractFileSystem, path : str, props_list : PropsList) -> None:
        raise NotImplementedError("Abstract")


class FrontendContract(Generic[TContent, TContents], ABC):
    def read_content(self, props_list : PropsList) -> TContent:
        raise NotImplementedError("Abstract")

    def read_contents(self, props_list : PropsList) -> TContents:
        raise NotImplementedError("Abstract")

    def write_content(self, content : TContent) -> PropsList:
        raise NotImplementedError("Abstract")

    def write_contents(self, contents : TContents) -> PropsList:
        raise NotImplementedError("Abstract")


class FilocContract(Generic[TContent, TContents], ABC):
    def lock(self):
        raise NotImplementedError('Abstract')

    def lock_info(self):
        raise NotImplementedError('Abstract')

    def lock_force_release(self):
        raise NotImplementedError('Abstract')

    def invalidate_cache(self, constraints : Optional[PropsConstraints] = None, **constraints_kwargs : Props):
        raise NotImplementedError('Abstract')

    def read_content(self, constraints : Optional[PropsConstraints] = None, **constraints_kwargs : PropsConstraints) -> TContent:
        raise NotImplementedError('Abstract')

    def read_contents(self, constraints : Optional[PropsConstraints] = None, **constraints_kwargs : PropsConstraints) -> TContents:
        raise NotImplementedError('Abstract')

    def read_props_list(self, constraints : Optional[PropsConstraints] = None, **constraints_kwargs : PropsConstraints) -> PropsList:
        raise NotImplementedError('Abstract')

    def write_content(self, content : TContent, dry_run=False):
        raise NotImplementedError('Abstract')

    def write_contents(self, contents : TContents, dry_run=False):
        raise NotImplementedError('Abstract')

    def write_props_list(self, props_list : PropsList, dry_run=False):
        raise NotImplementedError('Abstract')
