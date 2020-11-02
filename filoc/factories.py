"""
This module contains the filoc factories ``filoc(...)``, ``filoc_json_single(...)``, ``filoc_pandas_single(...)``, ``filoc_json_composite(...)``, ``filoc_pandas_composite(...)``.
"""
from fsspec.spec import AbstractFileSystem

from pandas.core.series import Series
from pandas.core.frame import DataFrame
from filoc.core import FilocSingle, FilocComposite
from typing import Any, Dict, Iterable, List, Optional, Union
from filoc.contract import PresetFrontends, Filoc, BackendContract, FrontendContract, PresetBackends


_default_frontend        = 'pandas'  
_default_backend         = 'json'
_default_singleton       = True
_default_encoding        = None
_default_writable        = False
_default_timestamp_col   = None
_default_join_level_name = 'index'
_default_join_separator  = '.'


def _get_frontend(frontend : Union[PresetFrontends, FrontendContract]):
    if frontend == 'json':
        from filoc.frontends import JsonFrontend
        return JsonFrontend()
    elif frontend == 'pandas':
        from filoc.frontends import PandasFrontend
        return PandasFrontend()
    else:
        return frontend


def _get_backend(backend : Union[PresetBackends, BackendContract], is_singleton : bool, encoding : str):
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
        locpath          : Union[str, Dict[str, Union[str, Filoc]]],
        frontend         : Union[PresetFrontends, FrontendContract]        = _default_frontend,
        backend          : Union[PresetBackends, BackendContract]          = _default_backend,
        singleton        : bool                                            = _default_singleton,
        encoding         : Optional[str]                                   = _default_encoding,
        writable         : Optional[bool]                                  = _default_writable,
        cache_locpath    : Optional[str]                                   = None,
        cache_fs         : Optional[AbstractFileSystem]                    = None,
        timestamp_col    : Optional[str]                                   = _default_timestamp_col,
        join_level_name  : Optional[str]                                   = _default_join_level_name,
        join_separator   : Optional[str]                                   = _default_join_separator,
        fs               : Optional[AbstractFileSystem]                    = None,
) -> Filoc:
    """
    Creates a `Filoc` instance that maps a *set of files* defined by the `locpath` argument to frontend object, namely pandas DataFrame and Series by default.

    Args:
        locpath: 
            A path which can be either a local path or a fsspec path with protocol (i.e. `ftp://...`). The path should contain format placeholders, which are
            extracted as data, when filoc reads data from files and used to determine the file to save the data to, depending on the attributes of the data.
            
            The locpath can also be a dictionary to build a composition of multiple single Filoc instances.
            In that case, the dictionary keys assign a name to the Sub-Filoc and the values are either locpath string or ``Filoc`` instances.

            Example 1: ``/data/{country}/{company}/info.json``

            Example 2: ``{ 'user' : '/data/user_data/{user_id:d}/user_info.json', 'activity' : already_instantiated_filoc }``

        frontend: 
            The frontend to use, which determines the types of objects returned by ``Filoc.read_contents(...)``, ``Filoc.read_content(...)`` and accepted by  
            ``Filoc.write_contents(...)``, ``Filoc.write_content(...)``. Default: ``'pandas'``. Preset frontends: (``'pandas'``, ``'json'``). It can also 
            accept an instance of type ``FrontendContract``, which allows you to write your own frontend implementation.
        backend:
            The backend to use, which determines how Filoc reads the files and write into them. Default: ``json``. Preset frontends: (``json``, ``yaml``, ``csv``, ``pickle``). 
            It can also accept an instance of type ``BackendContract``, which allows you to write your own backend implementation to read and write custom file formats. 

        singleton: 
            Option that applies only to the three preset backends: ``json``, ``yaml``, ``pickle``. If True, then Filoc writes single set of key-values. If False, the Filoc writes
            a list of set of key-values. If you consider the JSON Format, if ``singleton`` is set to True, the JSON files contain a JSON object. If it is set
            to False, the it contains a JSON array.
        encoding:
            Option that applies only to the three preset backends: ``json``, ``yaml``, ``csv``. Default: ``'utf-8'``. Allows to override the files encoding read and written by the backend.
        writable:
            If True, then write operations are allowed. Elsewhere write operation raises an ``UnsupportedOperation`` error.
        cache_locpath:
            Default: None. If set to a locpath string (fsspec path with format placeholders), then filoc uses the ``cache_locpath`` to write and read its own cache files.
            Use it to speed up the reading of remote files or the processing of complex backend implementations. The ``cache_locpath`` can contain placeholders. In that
            case, filoc replaces the placeholders by the corresponding values loaded by the backend. It allows to encapsulate the cache data with original the data structure 
        cache_fs:
            Default: None. Allows to provide a custom instance of fsspec file system for the cache files defined by ``cache_locpath``. This may be 
            required, if the protocol used in ``cache_locpath`` needs to be configured or fine-tuned (``ftp://``).
        timestamp_col:
            Default: None. If not None, then filoc adds to each entry a column ``timestamp_col``, which contains the timestamp of the file it comes from.
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
        backend         : Union[PresetBackends, BackendContract]          = _default_backend,
        singleton       : bool                                            = _default_singleton,
        writable        : Optional[bool]                                  = _default_writable,
        cache_locpath   : Optional[str]                                   = None,
        cache_fs        : Optional[AbstractFileSystem]                    = None,
        timestamp_col   : Optional[str]                                   = _default_timestamp_col,
        join_level_name : Optional[str]                                   = _default_join_level_name,
        join_separator  : Optional[str]                                   = _default_join_separator,
        fs              : Optional[AbstractFileSystem]                    = None,
) -> FilocSingle[Dict[str, Any], List[Dict[str, Any]]]:
    """ Same as filoc(), but with typed return value to improve IDE support """
    return filoc(
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


def filoc_json_composite(
        locpath         : Dict[str, Union[str, Filoc]],
        backend         : Union[PresetBackends, BackendContract]          = _default_backend,
        singleton       : bool                                            = _default_singleton,
        writable        : Optional[bool]                                  = _default_writable,
        cache_locpath   : Optional[str]                                   = None,
        cache_fs        : Optional[AbstractFileSystem]                    = None,
        timestamp_col   : Optional[str]                                   = _default_timestamp_col,
        join_level_name : Optional[str]                                   = _default_join_level_name,
        join_separator  : Optional[str]                                   = _default_join_separator,
        fs              : Optional[AbstractFileSystem]                    = None,
) -> FilocComposite[Dict[str, Any], List[Dict[str, Any]]]:
    """ Same as filoc(), but with typed return value to improve IDE support """
    return filoc(
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


def filoc_pandas_single(
        locpath         : str,
        backend         : Union[PresetBackends, BackendContract]          = _default_backend,
        singleton       : bool                                            = _default_singleton,
        writable        : Optional[bool]                                  = _default_writable,
        cache_locpath   : Optional[str]                                   = None,
        cache_fs        : Optional[AbstractFileSystem]                    = None,
        timestamp_col   : Optional[str]                                   = _default_timestamp_col,
        join_level_name : Optional[str]                                   = _default_join_level_name,
        join_separator  : Optional[str]                                   = _default_join_separator,
        fs              : Optional[AbstractFileSystem]                    = None,
) -> FilocSingle[Series, DataFrame]:
    """ Same as filoc(), but with typed return value to improve IDE support """
    return filoc(
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


def filoc_pandas_composite(
        locpath         : Dict[str, Union[str, Filoc]],
        backend         : Union[PresetBackends, BackendContract]          = _default_backend,
        singleton       : bool                                            = _default_singleton,
        writable        : Optional[bool]                                  = _default_writable,
        cache_locpath   : Optional[str]                                   = None,
        cache_fs        : Optional[AbstractFileSystem]                    = None,
        timestamp_col   : Optional[str]                                   = _default_timestamp_col,
        join_level_name : Optional[str]                                   = _default_join_level_name,
        join_separator  : Optional[str]                                   = _default_join_separator,
        fs              : Optional[AbstractFileSystem]                    = None,
) -> FilocComposite[Series, DataFrame]:
    """ Same as filoc(), but with typed return value to improve IDE support """
    return filoc(
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
