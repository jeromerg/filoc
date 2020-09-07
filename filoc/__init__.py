from collections import OrderedDict
from typing import Union, Dict, Optional, List, Any, Iterable

from pandas import DataFrame

from .core import FilocComposite, Filoc
from .filoc_io import FilocIO

__version__ = '0.0.6'

from .contract import PresetFrontends, FilocContract, BackendContract, FrontendContract, PresetBackends


def _get_frontend(frontend : Union[PresetFrontends, FrontendContract]):
    if frontend == 'json':
        from .frontend_json import JsonFrontend
        return JsonFrontend()
    elif frontend == 'pandas':
        from .frontend_pandas import PandasFrontend
        return PandasFrontend()
    else:
        return frontend


def _get_backend(backend : Union[PresetBackends, BackendContract], is_singleton : bool):
    if backend == 'csv':
        from .backend_csv import CsvBackend
        return CsvBackend(is_singleton)
    elif backend == 'json':
        from .backend_json import JsonBackend
        return JsonBackend(is_singleton)
    elif backend == 'pickle':
        from .backend_pickle import PickleBackend
        return PickleBackend(is_singleton)
    elif backend == 'yaml':
        from .backend_yaml import YamlBackend
        return YamlBackend(is_singleton)
    else:
        return backend


def filoc(
        locpath          : Union[str, Dict[str, str], Dict[str, FilocContract]] = None,
        frontend         : Union[PresetFrontends, FrontendContract]             = 'pandas',
        backend          : Union[PresetBackends, BackendContract]               = 'json',
        singleton        : bool                                                 = True,
        writable         : Optional[bool]                                       = False,
        cache_locpath    : Optional[str]                                        = None,
        timestamp_col    : Optional[str]                                        = None,
        join_keys        : Union[Iterable[str], Dict[Dict, Iterable[str]]]      = None,
        join_level_name  : Optional[str]                                        = 'index',
        join_separator   : Optional[str]                                        = '.',
        **locpath_kwargs : Union[str, Dict[str, str], Dict[str, FilocContract]]
) -> FilocContract:
    if locpath is None and len(locpath_kwargs) == 0:
        raise ValueError(f'local_path or locpath_kwargs must be defined')

    # merge locpath_kwargs and locpath
    if len(locpath_kwargs) > 0:
        if locpath is None:
            locpath = locpath_kwargs
        elif isinstance(locpath, dict):
            locpath.update(locpath_kwargs)
        else:
            raise ValueError(f'If **locpath_kwargs is defined, then locpath must be either None or an instance of dict. locpath_kwargs={locpath_kwargs}')

    frontend_impl = _get_frontend(frontend)
    backend_impl  = _get_backend(backend, singleton)

    if isinstance(locpath, dict):
        filoc_by_name = OrderedDict()
        for sub_filoc_name, sub_filoc in locpath.items():
            if isinstance(sub_filoc, str):
                sub_locpath = sub_filoc
                filoc_instance = filoc(
                    locpath            = sub_locpath        ,
                    frontend           = frontend           ,
                    backend            = backend            ,
                    singleton          = singleton          ,
                    writable           = writable           ,
                    cache_locpath      = None               ,  # Remark: cache is not forwarded to sub-filocs
                    timestamp_col      = timestamp_col      ,
                    join_keys          = join_keys          ,
                    join_level_name    = join_level_name    ,
                    join_separator     = join_separator     ,
                )
            else:
                filoc_instance = sub_filoc
            filoc_by_name[sub_filoc_name] = filoc_instance

        return FilocComposite(
            filoc_by_name           = filoc_by_name,
            frontend                = frontend_impl,
            join_keys_by_filoc_name = join_keys,
            join_level_name         = join_level_name,
            join_separator          = join_separator,
        )
    elif isinstance(locpath, str):
        return Filoc(
            locpath            = locpath,
            writable           = writable,
            frontend           = frontend_impl,
            backend            = backend_impl,
            cache_locpath      = cache_locpath,
            timestamp_col      = timestamp_col
        )
    else:
        raise ValueError(f'locpath must be an instance of str or dict, but is {type(locpath)}')


def filoc_json(
        locpath            : Union[str, Dict[str, str], Dict[str, FilocContract]] = None,
        backend            : Union[PresetBackends, BackendContract]               = 'json',
        singleton          : bool                                                 = True,
        writable           : Optional[bool]                                       = False,
        cache_locpath      : Optional[str]                                        = None,
        timestamp_col      : Optional[str]                                        = None,
        join_keys          : Union[Iterable[str], Dict[Dict, Iterable[str]]]      = None,
        join_level_name    : Optional[str]                                        = 'index',
        join_separator     : Optional[str]                                        = '.',
        **locpath_kwargs   : Union[str, Dict[str, str], Dict[str, FilocContract]]
) -> FilocContract[Dict[str, Any], List[Dict[str, Any]]]:
    """ Same as filoc(), but with typed return value to improve IDE support """
    return filoc(
        locpath            = locpath            ,
        frontend           = _get_frontend('json'),
        backend            = backend            ,
        singleton          = singleton          ,
        writable           = writable           ,
        cache_locpath      = cache_locpath      ,
        timestamp_col      = timestamp_col      ,
        join_keys          = join_keys          ,
        join_level_name    = join_level_name    ,
        join_separator     = join_separator     ,
        **locpath_kwargs
    )


def filoc_pandas(
        locpath            : Union[str, Dict[str, str], Dict[str, FilocContract]] = None,
        backend            : Union[PresetBackends, BackendContract]               = 'json',
        singleton          : bool                                                 = True,
        writable           : Optional[bool]                                       = False,
        cache_locpath      : Optional[str]                                        = None,
        timestamp_col      : Optional[str]                                        = None,
        join_keys          : Union[Iterable[str], Dict[Dict, Iterable[str]]]      = None,
        join_level_name    : Optional[str]                                        = 'index',
        join_separator     : Optional[str]                                        = '.',
        **locpath_kwargs   : Union[str, Dict[str, str], Dict[str, FilocContract]]
) -> FilocContract[DataFrame, DataFrame]:
    """ Same as filoc(), but with typed return value to improve IDE support """
    return filoc(
        locpath            = locpath            ,
        frontend           = _get_frontend('pandas'),
        backend            = backend            ,
        singleton          = singleton          ,
        writable           = writable           ,
        cache_locpath      = cache_locpath ,
        timestamp_col      = timestamp_col      ,
        join_keys          = join_keys          ,
        join_level_name    = join_level_name    ,
        join_separator     = join_separator     ,
        **locpath_kwargs
    )


__all__     = [
    'filoc',
    'FilocIO',
    'filoc_json',
    'filoc_pandas',
]
