"""
This module contains the filoc factories ``filoc(...)``, ``filoc_json_single(...)``, ``filoc_pandas_single(...)``, ``filoc_json_composite(...)``, ``filoc_pandas_composite(...)``.
"""
from typing import Any, Dict, List, Mapping, Optional, Union

from fsspec.spec import AbstractFileSystem
from pandas.core.frame import DataFrame
from pandas.core.series import Series

from filoc.contract import BuiltinFrontends, Filoc, BackendContract, FrontendContract, BuiltinBackends
from filoc.core import FilocSingle, FilocComposite

_default_frontend        = 'pandas'
_default_backend         = 'json'
_default_singleton       = True
_default_encoding        = None
_default_writable        = False
_default_timestamp_col   = None
_default_join_level_name = 'index'
_default_join_separator  = '.'


def _get_frontend(frontend : Union[BuiltinFrontends, FrontendContract]):
    if frontend == 'json':
        from filoc.frontends import JsonFrontend
        return JsonFrontend()
    elif frontend == 'pandas':
        from filoc.frontends import PandasFrontend
        return PandasFrontend()
    else:
        return frontend


def _get_backend(backend : Union[BuiltinBackends, BackendContract], is_singleton : bool, encoding : str):
    if backend == 'csv':
        from filoc.backends import CsvBackend
        return CsvBackend(encoding)
    elif backend == 'json':
        from filoc.backends import JsonBackend
        return JsonBackend(is_singleton, encoding)
    elif backend == 'pickle':
        from filoc.backends import PickleBackend
        return PickleBackend(is_singleton)
    elif backend == 'yaml':
        from filoc.backends import YamlBackend
        return YamlBackend(is_singleton, encoding)
    else:
        return backend


def filoc(
        locpath          : Union[str, Mapping[str, Union[str, Filoc]]],
        frontend         : Union[BuiltinFrontends, FrontendContract] = _default_frontend,
        backend          : Union[BuiltinBackends, BackendContract]   = _default_backend,
        singleton        : bool                                      = _default_singleton,
        encoding         : Optional[str]                             = _default_encoding,
        writable         : Optional[bool]                            = _default_writable,
        cache_locpath    : Optional[str]                             = None,
        cache_fs         : Optional[AbstractFileSystem]              = None,
        timestamp_col    : Optional[str]                             = _default_timestamp_col,
        join_level_name  : Optional[str]                             = _default_join_level_name,
        join_separator   : Optional[str]                             = _default_join_separator,
        fs               : Optional[AbstractFileSystem]              = None,
) -> Filoc:
    """
    Creates a ``Filoc`` instance which allows to read a *set of files* and visualize it as a DataFrame (or another frontend object, if another frontend is passed to the factory), and write changes back to thhe *set of files*.

    Args:
        locpath: 
            A path which can be either a local path (``/data/file.json``) or a fsspec path with protocol (i.e. ``ftp://user:pass@example.com/data/file.yaml``). The ``locpath``
            may additionally contain format placeholders (``/data/{customer_id}/info.json``). during reading accesses, the placeholder names and their values in the concrete paths
            are merged with the data loaded from the file contents. On writing, the placeholders are substituted by the related values found in the frontend object (i.e. DataFrame).
            
            The locpath can also be a Mapping (dictionary). In that case, ``filoc(...)`` creates a "composite filoc" built of multiple sub-filocs. The key of a mapping entry is 
            the name of the sub-filoc and the value a ``Filoc`` instance. You can alternatively provide a string, which is used as ``locpath`` in a recursive call to ``filoc(...)`` 
            in order to instantiate the sub-filoc.

            Example 1: ``/data/{country}/{company}/info.json``

            Example 2: ``{ 'user' : '/data/user_data/{user_id:d}/user_info.json', 'activity' : already_instantiated_filoc }``

        frontend: 
            The frontend determines the types of objects returned by ``Filoc.read_contents(...)``, ``Filoc.read_content(...)`` and accepted by  
            ``Filoc.write_contents(...)``, ``Filoc.write_content(...)``. The default frontend is ``'pandas'``. The two builtin frontends are ``'pandas'`` and ``'json'``. 
            You can also provide your own frontend instance, which must implement the ``FrontendContract``.

        backend:
            The backend determines how Filoc reads and writes the files. The default backend is ``json``. The four builtin backends are 
            ``json``, ``yaml``, ``csv`` and ``pickle``. You can also provide your own backend instance, which must implement the ``BackendContract``.

        singleton: 
            Option that applies only to the three builtin backends: ``json``, ``yaml`` and ``pickle``. The default value is ``True``. If ``True``, then the created ``Filoc`` instance assumes that the
            files contain a single Mapping (in JSON: a JSON object). If ``False``, the Filoc assumes that the files contains a list of Mappings (in JSON: a JSON array of JSON objects).

        encoding:
            Option that applies only to the three builtin backends: ``json``, ``yaml`` and ``csv``. The default value is ``None``, keeping the default value of the underlying fsspec file system. 
            Allows to override the file encoding.

        writable:
            If True, then write operations are allowed. Elsewhere write operation raises an ``UnsupportedOperation`` error.

        cache_locpath:
            Default: None. Defines a locpath string (fsspec path with format placeholders), used by the created ``Filoc`` instance to cache 
            the intermediate representation (data model loaded by the backend). The cache allows to speed up multiple reads from slow remote
            file systems as well as speed up expensive backend processing. 
            
            If you provide a ``cache_locpath`` with placeholders, the cache is then splitted into multiple files based on the placeholder values. 
            It allows you to encapsulate the cache data with original the data structure.

        cache_fs:
            Default: None. Allows to provide a custom instance of fsspec file system for the cache files defined by ``cache_locpath``. This may be 
            required, if the protocol used in ``cache_locpath`` needs to be configured or fine-tuned (ex: ``ftp://``).

        timestamp_col:
            Default: None. If not None, then filoc adds a column ``timestamp_col``, which contains the timestamp of the file the data entries comes from.

        join_level_name:
            Default: ``'index'``. This option applies only to composite filocs and allows to provide an alternative prefix to the "index columns", the columns that 
            are used to join the sub-filocs together.

        join_separator:
            Default: ``'.'``. This option applies only to composite filocs and allows to provide an alternative separator between the "sub-filoc" name 
            and the attributes names of the sub-filoc.

        fs:
            Default: None. Allows to provide a custom instance of fsspec file system. This may be required, if the protocol used in the ``locpath`` needs to be 
            configured or fine-tuned (ex: ``ftp://``).

    Returns:
        A ``Filoc`` instance
    """
    frontend_impl = _get_frontend(frontend)
    backend_impl  = _get_backend(backend, singleton, encoding)

    if isinstance(locpath, dict):
        # Case of composite filoc
        filoc_by_name = dict()
        for sub_filoc_name, sub_filoc in locpath.items():
            if isinstance(sub_filoc, str):
                sub_locpath = sub_filoc
                filoc_instance = filoc(
                    locpath         = sub_locpath    ,
                    frontend        = frontend       ,
                    backend         = backend        ,
                    singleton       = singleton      ,
                    writable        = writable       ,
                    cache_locpath   = None           ,  # Remark: currently cache is not forwarded to sub-filocs
                    cache_fs        = None           ,  # Remark: currently cache is not forwarded to sub-filocs
                    timestamp_col   = timestamp_col  ,
                    join_level_name = join_level_name,
                    join_separator  = join_separator ,
                    fs              = fs             ,
                )
            else:
                filoc_instance = sub_filoc
            filoc_by_name[sub_filoc_name] = filoc_instance

        return FilocComposite(
            filoc_by_name           = filoc_by_name,
            frontend                = frontend_impl,
            join_level_name         = join_level_name,
            join_separator          = join_separator,
        )
    elif isinstance(locpath, str):
        return FilocSingle(
            locpath       = locpath,
            writable      = writable,
            frontend      = frontend_impl,
            backend       = backend_impl,
            cache_locpath = cache_locpath,
            cache_fs      = cache_fs,
            timestamp_col = timestamp_col,
            fs            = fs,
        )
    else:
        raise ValueError(f'locpath must be an instance of str or dict, but is {type(locpath)}')


def filoc_json_single(
        locpath         : str,
        backend         : Union[BuiltinBackends, BackendContract] = _default_backend,
        singleton       : bool                                    = _default_singleton,
        writable        : Optional[bool]                          = _default_writable,
        cache_locpath   : Optional[str]                           = None,
        cache_fs        : Optional[AbstractFileSystem]            = None,
        timestamp_col   : Optional[str]                           = _default_timestamp_col,
        join_level_name : Optional[str]                           = _default_join_level_name,
        join_separator  : Optional[str]                           = _default_join_separator,
        fs              : Optional[AbstractFileSystem]            = None,
) -> FilocSingle[Dict[str, Any], List[Dict[str, Any]]]:
    """ Same as filoc(), but with typed return value to improve IDE support """
    loc = filoc(
        locpath         = locpath              ,
        frontend        = _get_frontend('json'),
        backend         = backend              ,
        singleton       = singleton            ,
        writable        = writable             ,
        cache_locpath   = cache_locpath        ,
        cache_fs        = cache_fs             ,
        timestamp_col   = timestamp_col        ,
        join_level_name = join_level_name      ,
        join_separator  = join_separator       ,
        fs              = fs                   ,
    )
    assert isinstance(loc, FilocSingle)
    return loc


def filoc_json_composite(
        locpath         : Mapping[str, Union[str, Filoc]],
        backend         : Union[BuiltinBackends, BackendContract] = _default_backend,
        singleton       : bool                                    = _default_singleton,
        writable        : Optional[bool]                          = _default_writable,
        cache_locpath   : Optional[str]                           = None,
        cache_fs        : Optional[AbstractFileSystem]            = None,
        timestamp_col   : Optional[str]                           = _default_timestamp_col,
        join_level_name : Optional[str]                           = _default_join_level_name,
        join_separator  : Optional[str]                           = _default_join_separator,
        fs              : Optional[AbstractFileSystem]            = None,
) -> FilocComposite[Dict[str, Any], List[Dict[str, Any]]]:
    """ Same as filoc(), but with typed return value to improve IDE support """
    loc = filoc(
        locpath         = locpath              ,
        frontend        = _get_frontend('json'),
        backend         = backend              ,
        singleton       = singleton            ,
        writable        = writable             ,
        cache_locpath   = cache_locpath        ,
        cache_fs        = cache_fs             ,
        timestamp_col   = timestamp_col        ,
        join_level_name = join_level_name      ,
        join_separator  = join_separator       ,
        fs              = fs                   ,
    )
    assert isinstance(loc, FilocComposite)
    return loc


def filoc_pandas_single(
        locpath         : str,
        backend         : Union[BuiltinBackends, BackendContract] = _default_backend,
        singleton       : bool                                    = _default_singleton,
        writable        : Optional[bool]                          = _default_writable,
        cache_locpath   : Optional[str]                           = None,
        cache_fs        : Optional[AbstractFileSystem]            = None,
        timestamp_col   : Optional[str]                           = _default_timestamp_col,
        join_level_name : Optional[str]                           = _default_join_level_name,
        join_separator  : Optional[str]                           = _default_join_separator,
        fs              : Optional[AbstractFileSystem]            = None,
) -> FilocSingle[Series, DataFrame]:
    """ Same as filoc(), but with typed return value to improve IDE support """
    loc = filoc(
        locpath         = locpath                ,
        frontend        = _get_frontend('pandas'),
        backend         = backend                ,
        singleton       = singleton              ,
        writable        = writable               ,
        cache_locpath   = cache_locpath          ,
        cache_fs        = cache_fs               ,
        timestamp_col   = timestamp_col          ,
        join_level_name = join_level_name        ,
        join_separator  = join_separator         ,
        fs              = fs                     ,
    )
    assert isinstance(loc, FilocSingle)
    return loc


def filoc_pandas_composite(
        locpath         : Mapping[str, Union[str, Filoc]],
        backend         : Union[BuiltinBackends, BackendContract] = _default_backend,
        singleton       : bool                                    = _default_singleton,
        writable        : Optional[bool]                          = _default_writable,
        cache_locpath   : Optional[str]                           = None,
        cache_fs        : Optional[AbstractFileSystem]            = None,
        timestamp_col   : Optional[str]                           = _default_timestamp_col,
        join_level_name : Optional[str]                           = _default_join_level_name,
        join_separator  : Optional[str]                           = _default_join_separator,
        fs              : Optional[AbstractFileSystem]            = None,
) -> FilocComposite[Series, DataFrame]:
    """ Same as filoc(), but with typed return value to improve IDE support """
    loc = filoc(
        locpath         = locpath                ,
        frontend        = _get_frontend('pandas'),
        backend         = backend                ,
        singleton       = singleton              ,
        writable        = writable               ,
        cache_locpath   = cache_locpath          ,
        cache_fs        = cache_fs               ,
        timestamp_col   = timestamp_col          ,
        join_level_name = join_level_name        ,
        join_separator  = join_separator         ,
        fs              = fs                     ,
    )
    assert isinstance(loc, FilocComposite)
    return loc
