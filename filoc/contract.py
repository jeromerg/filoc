"""
This module contains the contracts of filoc classes. Among other it contains the ``FrontendContract`` and``BackendContract``, which
custom frontends and backends need to implement.
"""

# -------------
# Types aliases
# -------------
import logging
from abc import ABC
from typing import TypeVar, Any, List, Generic, Optional, Mapping, Dict, Collection
from fsspec import AbstractFileSystem

# Literal is python 3.8 feature, but filoc works from python 3.6 upward
try:
    from typing import Literal
except ImportError:
    Literal = None

# set logging level to WARN. User can override the level to info, to get more infos
logging.getLogger('filoc').setLevel(logging.WARN)

TContent             = TypeVar('TContent')
"""Generic type of objects returned by ``Filoc.get_content(...)`` and expected by ``Filoc.write_content(...)``. 
For example, in the ``'json'`` frontend, TContent is equal to `Mapping[str,Any]``, and in the ``'pandas'`` frontend, TContent is equal to ``pandas.Series`` """

TContents            = TypeVar('TContents')
"""Generic type of objects returned by ``Filoc.get_contents(...)`` and expected by ``Filoc.write_contents(...)``. 
For example, in the ``'json'`` frontend, TContents is equal to `List[Mapping[str,Any]]``, and in the ``'pandas'`` frontend, TContent is equal to ``pandas.DataFrame`` """

BuiltinFrontends      = Literal['json', 'pandas'] if Literal else str
"""Shortcuts used to designate filoc preset frontends"""

BuiltinBackends       = Literal['json', 'yaml', 'csv', 'pickle'] if Literal else str
"""Shortcut used to designate filoc preset backends"""

Constraint           = Any
"""key-values describing constraints to apply while filtering data. Currently only equality is supported."""

Constraints          = Mapping[str, Constraint]
"""key-values describing constraints to apply while filtering data. Currently only equality is supported."""

PropValue            = Any

ReadOnlyProps        = Mapping[str, PropValue]
"""filoc intermediate data representation of a single data 'row'"""

Props                = Dict[str, PropValue]
"""filoc intermediate data representation of a single data 'row'"""

ReadOnlyPropsList    = Collection[ReadOnlyProps]
"""filoc intermediate data representation between the backend files and the frontend TContent and TContents objects"""

PropsList            = List[Props]
"""filoc intermediate data representation between the backend files and the frontend TContent and TContents objects"""


# -----------------
# Exception classes
# -----------------
class FrontendConversionError(ValueError):
    """Exception raised by filoc frontends, when the frontend encounters a conversion problem """
    def __init__(self, *args):
        super().__init__(*args)


class SingletonExpectedError(FrontendConversionError):
    """Exception raised by filoc frontends, when the frontend expects a single entry but got multiple entries """
    def __init__(self, *args):
        super().__init__(*args)

# ----------------
# Abstract classes
# ----------------
# TODO: Unit tests with locpath = folders instead of path


class BackendContract(ABC):
    """The abstract class that filoc backends need to implement."""

    def read(self, fs: AbstractFileSystem, path: str, path_props : Props, constraints: Constraints) -> PropsList:
        """Reads the data contained at ``path`` on the file system ``fs``, applies additional filters defined in ``constraints`` and convert the data to the filoc intermediate representation.

        Args:
            fs: File system implementation (see the `fsspec <https://github.com/intake/filesystem_spec/>`_ library)
            path: The path at which the data must be loaded from. It is a concrete form of the filoc ``locpath``.
            path_props: the key-values extracted from the path
            constraints: All constraints provided by the user to take into account.

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

    def write_content(self, content : TContent) -> ReadOnlyPropsList:
        """ Converts the frontend ``TContent`` object into the filoc intermediate representation.

        Args:
            content: The frontend content object (i.e. the default json frontend implementation returns a json object)

        Returns:
            PropsList: filoc intermediate representation between frontend and backend data (list of dictionary).
        """
        raise NotImplementedError("Abstract")

    def write_contents(self, contents : TContents) -> ReadOnlyPropsList:
        """ Converts the frontend ``TContents`` object into the filoc intermediate representation.

        Args:
            contents: The frontend contents object (i.e. the default json frontend implementation returns a json list)

        Returns:
            PropsList: filoc intermediate representation between frontend and backend data (list of dictionary).
        """
        raise NotImplementedError("Abstract")


class Filoc(Generic[TContent, TContents], ABC):
    """ The contract of objects created by the filoc factory. This is the most important contract in the filoc library.

    TContent ([Any]): The type returned by ``get_content(...)`` and expected by ``write_content(...)``
    TContents ([Any]): The type returned by ``get_contents(...)`` and expected by ``write_contents(...)``
    """

    def list_paths(self, constraints : Optional[Constraints] = None, **constraints_kwargs : Props) -> List[str]:
        """ Get the existing paths fulfilling the provided constraints. """
        raise NotImplementedError('Abstract')

    def lock(self, attempt_count: int = 60, attempt_secs: float = 1.0):
        """Prevents other filoc instances to concurrently read or write any file in the filoc tree. Usage:

        .. code-block:: python

            with my_filoc.lock():
                my_filoc.read_contents(df)
                my_filoc.write_contents(df)

        Raises:
            NotImplementedError: [description]
        """
        raise NotImplementedError('Abstract')

    def lock_info(self) -> Optional[Mapping[str, Any]]:
        """Returns the information contained in the lock file(s) augmented of the file timestamp if it exists, elsewhere null.
        For a single filoc, it returns simple key-values. 
        For nested filocs (composite), it returns the lock file information in the same tree structure as the filoc definition tree. 
        
        This function is useful in case of problems, in order to analyze what's happening (mostly to check whether the lock owner is still alive)"""
        raise NotImplementedError('Abstract')

    def lock_force_release(self):
        """Forces the removing of lock file even a concurrent process is still supposed to own it. Consider this method as your "last action before being fired"!"""
        raise NotImplementedError('Abstract')

    def invalidate_cache(self, constraints : Optional[Constraints] = None, **constraints_kwargs : Constraint):
        """Delete the cache data for the provided constraints, to ensure that the data will be re-fetched by the filoc backend at the next ``get_content(...)`` or ``get_contents(...)`` calls.
        
        Remark: By default, the cache is configured to automatically invalidate cache entries loaded from paths whose timestamp changed. All default backends work on files and this
        implementation works. But for custom backends, this invalidation mechanism may not work anymore (for example if the backend allows to work with folders instead of files). 
        In that case, it may be required to invalidate the cache manually with the current method. """
        raise NotImplementedError('Abstract')

    def read_content(self, constraints : Optional[Constraints] = None, **constraints_kwargs : Constraint) -> TContent:
        """Reads the data entries fulfilling the ``constraints`` and converts them to a TContent object. With the filoc default frontend implementations, the ``constraints`` must result in the
        selection of a single entry. If multiple rows are selected, the call raises a ``SingletonExpectedError``

        Args:
            constraints: Filter criteria used to shrink the result set. Defaults to None.
            **constraints_kwargs: additional constraints merge together with ``constraints`` argument
        Returns:
            TContent: An instance of the frontend type for a single entry (ex: ``json`` frontend: Mapping[str, Any], ``pandas`` frontend: pandas.Series)

        Raises:
            SingletonExpectedError: if more than one entry is selected after application of the ``constraints`` (behavior of filoc default frontends)
            FrontendConversionError: if the frontend cannot perform the conversion 
        """
        raise NotImplementedError('Abstract')

    def read_contents(self, constraints : Optional[Constraints] = None, **constraints_kwargs : Constraint) -> TContents:
        """Reads the data entries fulfilling the ``constraints`` and converts them to a TContents object. 

        Args:
            constraints: Filter criteria used to shrink the result set. Defaults to None.
            **constraints_kwargs: additional constraints merge together with ``constraints`` argument
        Returns:
            TContents: An instance of the frontend types for multiple entries (ex: ``json`` frontend: List[Mapping[str, Any]], ``pandas`` frontend: pandas.DataFrame)

        Raises:
            FrontendConversionError: If the frontend cannot perform the conversion 
        """
        raise NotImplementedError('Abstract')

    def _read_props_list(self, constraints : Optional[Constraints] = None, **constraints_kwargs : Constraint) -> PropsList:
        """Reads the data entries fulfilling the ``constraints`` and converts them to the filoc intermediate representation.
        This function is used internally to shared the intermediate representation in filoc composites.

        Args:
            constraints: Filter criteria used to shrink the result set. Defaults to None.
            **constraints_kwargs: additional constraints merge together with ``constraints`` argument
        Returns:
            PropsList: The filoc intermediate representation
        """
        raise NotImplementedError('Abstract')

    def write_content(self, content : TContent, dry_run=False):
        """Writes the ``content`` entry to the file system.

        Args:
            content: The content accepted by the frontend (ex: ``json`` frontend: Mapping[str, Any], ``pandas`` frontend: pandas.Series or Mapping[str, Any])
            dry_run: If True, then only simulate the writing. The default value is False.

        Raises:
            FrontendConversionError: If the frontend cannot perform the conversion 
        """
        raise NotImplementedError('Abstract')

    def write_contents(self, contents : TContents, dry_run=False):
        """Writes the ``contents`` entries to the file system.

        Args:
            contents: The content accepted by the frontend (ex: ``json`` frontend: List[Mapping[str, Any]], ``pandas`` frontend: pandas.DataFrame or List[Mapping[str, Any]])
            dry_run: If True, then only simulate the writing. The default value is False.

        Raises:
            FrontendConversionError: If the frontend cannot perform the conversion 
        """
        raise NotImplementedError('Abstract')

    def _write_props_list(self, props_list : PropsList, dry_run=False):
        """Write the filoc intermediate representation to the file system via the backend.
        This function is used internally to shared the intermediate representation in filoc composites.

        Args:
            props_list: the filoc intermediate representation 
            dry_run: If True, then only simulate the writing. The default value is False.
        """
        raise NotImplementedError('Abstract')
