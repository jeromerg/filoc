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

# ----------------
# Abstract classes
# ----------------
# TODO: Unit tests with locpath = folders instead of path
# TODO: Unit tests with remote file system

class BackendContract(ABC):
    """Abstract class of the backend contract. Subclass this class and implement ``read(...)`` and  ``write(...)`` to create an custom backend."""

    def read(self, fs : AbstractFileSystem, path : str, constraints : Dict[str, Any]) -> PropsList:
        """Reads the data contained at ``path`` on the file system ``fs``, applies additional filters defined in ``constraints`` and convert the data to the filoc intermediate representation.

        Args:
            fs (AbstractFileSystem): File system implementation from the [fsspec](https://github.com/intake/filesystem_spec)
            path (str): The path at which the data must be loaded from. It is a concrete form of the filoc ``locpath``. 
            constraints (Dict[str, Any]): All contraints provided by the user to take into account.

        Returns:
            PropsList: filoc intermediate representation between frontend and backend data (list of dictionary).
        """
        raise NotImplementedError("Abstract")

    def write(self, fs : AbstractFileSystem, path : str, props_list : PropsList) -> None:
        """Writes the data contained in ``props_list`` into ``path`` on the file system ``fs``.

        Args:
            fs (AbstractFileSystem): File system implementation from the [fsspec](https://github.com/intake/filesystem_spec)
            path (str): The path to which the data must be saved to. It is a concrete form of the filoc ``locpath``. 
            props_list (PropsList): filoc intermediate representation between frontend and backend data (list of dictionary).
        """
        raise NotImplementedError("Abstract")


class FrontendContract(Generic[TContent, TContents], ABC):
    """Abstract class of the backend contract. You need to subclass this class and implement ``read_content(...)``, ``read_contents(...)``,  
    ``write_content(...)`` and ``write_contents(...)`` to create a custom frontend.

    Args:
        TContent ([Any]): The type returned by ``get_content(...)`` and expected by ``write_content(...)``
        TContents ([Any]): The type returned by ``get_contents(...)`` and expected by ``write_contents(...)``
    """

    def read_content(self, props_list : PropsList) -> TContent:
        """ Converts the intermediate ``props_list`` list into the frontend ``TContent`` type. As you implement this method, You may want to first validate that the ``props_list``
        contains a single item. But you may want also to support multiple items, for example if you need to aggregate multiple files at folder level. 

        Args:
            props_list (PropsList): filoc intermediate representation between frontend and backend data (list of dictionary).

        Returns:
            TContent: The frontend content object (i.e. the default json frontend implementation returns a json object)
        """
        raise NotImplementedError("Abstract")

    def read_contents(self, props_list : PropsList) -> TContents:
        """ Converts the intermediate ``props_list`` list into the frontend ``TContents`` type. 

        Args:
            props_list (PropsList): filoc intermediate representation between frontend and backend data (list of dictionary).

        Returns:
            TContents: The frontend contents object (i.e. the default json frontend implementation returns a json list)
        """
        raise NotImplementedError("Abstract")

    def write_content(self, content : TContent) -> PropsList:
        """ Converts the frontend ``TContent`` object into the intermediate ``PropsList`` type.

        Args:
            content (TContent): The frontend content object (i.e. the default json frontend implementation returns a json object)

        Returns:
            PropsList: filoc intermediate representation between frontend and backend data (list of dictionary).
        """
        raise NotImplementedError("Abstract")

    def write_contents(self, contents : TContents) -> PropsList:
        """ Converts the frontend ``TContents`` object into the intermediate ``PropsList`` type.

        Args:
            contents (TContents): The frontend contents object (i.e. the default json frontend implementation returns a json list)

        Returns:
            PropsList: filoc intermediate representation between frontend and backend data (list of dictionary).
        """
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
