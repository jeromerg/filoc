# -------------
# Types aliases
# -------------
from abc import ABC
from typing import TypeVar, Literal, Dict, Any, List, Generic, Optional

from fsspec import AbstractFileSystem

TContent             = TypeVar('TContent')
"""Generic type of objects returned by ``FilocContract.get_content(...)`` and expected by ``FilocContract.write_content(...)``. 
For example, in the ``'json'`` frontend, TContent is equal to `Dict[str,Any]``, and in the ``'pandas'`` frontend, TContent is equal to ``pandas.Series`` """
TContents            = TypeVar('TContents')
"""Generic type of objects returned by ``FilocContract.get_contents(...)`` and expected by ``FilocContract.write_contents(...)``. 
For example, in the ``'json'`` frontend, TContents is equal to `List[Dict[str,Any]]``, and in the ``'pandas'`` frontend, TContent is equal to ``pandas.DataFrame`` """
PresetFrontends      = Literal['json', 'pandas']
"""Shortcuts used to designate filoc preset frontends"""
PresetBackends       = Literal['json', 'yaml', 'pickle']
"""Shortcut used to designate filoc preset backends"""
Props                = Dict[str, Any]
"""filoc intermediate data representation of a single data 'row'"""
PropsConstraints     = Dict[str, Any]
"""key-values describing constraints to apply while filtering data. Currently only equality is supported."""
PropsList            = List[Props]
"""filoc intermediate data representation between the backend files and the frontend TContent and TContents objects"""

# ----------------
# Abstract classes
# ----------------
# TODO: Unit tests with locpath = folders instead of path
# TODO: Unit tests with remote file system

class BackendContract(ABC):
    """The abstract class that filoc backends need to implement."""

    def read(self, fs : AbstractFileSystem, path : str, constraints : Dict[str, Any]) -> PropsList:
        """Reads the data contained at ``path`` on the file system ``fs``, applies additional filters defined in ``constraints`` and convert the data to the filoc intermediate representation.

        Args:
            fs: File system implementation (see the `fsspec <https://github.com/intake/filesystem_spec/>`_ library)
            path: The path at which the data must be loaded from. It is a concrete form of the filoc ``locpath``. 
            constraints: All contraints provided by the user to take into account.

        Returns:
            PropsList: filoc intermediate representation between frontend and backend data (list of dictionary).
        """
        raise NotImplementedError("Abstract")

    def write(self, fs : AbstractFileSystem, path : str, props_list : PropsList) -> None:
        """Writes the data contained in ``props_list`` into ``path`` on the file system ``fs``.

        Args:
            fs: File system implementation (see the `fsspec <https://github.com/intake/filesystem_spec/>`_ library)
            path: The path to which the data must be saved to. It is a concrete form of the filoc ``locpath``. 
            props_list: filoc intermediate representation between frontend and backend data (list of dictionary).
        """
        raise NotImplementedError("Abstract")


class FrontendContract(Generic[TContent, TContents], ABC):
    """The abstract class that filoc frontends need to implement.

    Args:
        TContent ([Any]): The type returned by ``get_content(...)`` and expected by ``write_content(...)``
        TContents ([Any]): The type returned by ``get_contents(...)`` and expected by ``write_contents(...)``
    """

    def read_content(self, props_list : PropsList) -> TContent:
        """ Converts the ``props_list`` intermediate representation into a frontend ``TContent`` object. 
        
        Implementation remark: As you implement this method, You may want to first validate that the ``props_list``
        contains a single item. But you may want also to support multiple items, for example if you need to aggregate multiple files at folder level. 

        Args:
            props_list: filoc intermediate representation between frontend and backend data (list of dictionary).

        Returns:
            TContent: The frontend content object (i.e. the default json frontend implementation returns a json object)
        """
        raise NotImplementedError("Abstract")

    def read_contents(self, props_list : PropsList) -> TContents:
        """ Converts the ``props_list`` intermediate representation into a frontend ``TContents`` object. 

        Args:
            props_list: filoc intermediate representation between frontend and backend data (list of dictionary).

        Returns:
            TContents: The frontend contents object (i.e. the default json frontend implementation returns a json list)
        """
        raise NotImplementedError("Abstract")

    def write_content(self, content : TContent) -> PropsList:
        """ Converts the frontend ``TContent`` object into the filoc intermediate representation.

        Args:
            content: The frontend content object (i.e. the default json frontend implementation returns a json object)

        Returns:
            PropsList: filoc intermediate representation between frontend and backend data (list of dictionary).
        """
        raise NotImplementedError("Abstract")

    def write_contents(self, contents : TContents) -> PropsList:
        """ Converts the frontend ``TContents`` object into the filoc intermediate representation.

        Args:
            contents: The frontend contents object (i.e. the default json frontend implementation returns a json list)

        Returns:
            PropsList: filoc intermediate representation between frontend and backend data (list of dictionary).
        """
        raise NotImplementedError("Abstract")


class FilocContract(Generic[TContent, TContents], ABC):
    """ The contract that *filoc* objects returned by the filoc factory implement. This is the most important contract in the filoc library.

    Args:
        TContent ([Any]): The type returned by ``get_content(...)`` and expected by ``write_content(...)``
        TContents ([Any]): The type returned by ``get_contents(...)`` and expected by ``write_contents(...)``
    """

    def lock(self):
        """Prevents other filoc instances to concurrently read or write any file in the filoc tree. Usage:

        .. code-block:: python

            with my_filoc.lock():
                my_filoc.read_contents(df)
                my_filoc.write_contents(df)

        Raises:
            NotImplementedError: [description]
        """
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
