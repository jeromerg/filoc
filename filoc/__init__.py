from collections import OrderedDict
from typing import Union, Dict, Optional, List, Any

from pandas import DataFrame

from .filoc_opener import FilocOpener
from .filoc_base import FilocBase, FilocCompositeBase, Filoc
from .filoc_json import FilocJson, FilocCompositeJson
from .filoc_pandas import FilocPandas, FilocCompositePandas

__version__ = '0.0.6'

from .filoc_types import FrontendTypes, PresetBackendTypes


def filoc(
        locpath            : Union[str, Dict[str, str], Dict[str, Filoc]] = None,
        frontend           : FrontendTypes                = 'pandas',
        file_singleton     : bool                           = True,
        file_type          : Optional[PresetBackendTypes] = 'json',
        file_writable      : Optional[bool]                 = False,
        file_cache_locpath : Optional[str]                  = None,
        file_timestamp_col : Optional[str]                  = None,
        join_keys          : Optional[List[str]]            = None,
        join_level_name    : Optional[str]                  = 'index',
        join_separator     : Optional[str]                  = '.',
        **locpath_kwargs
) -> Filoc:
    if frontend == 'json':
        return filoc_json(
            locpath            = locpath,
            file_singleton     = file_singleton,
            file_type          = file_type,
            file_writable      = file_writable,
            file_cache_locpath = file_cache_locpath,
            file_timestamp_col = file_timestamp_col,
            join_keys          = join_keys,
            join_level_name    = join_level_name,
            join_separator     = join_separator,
            **locpath_kwargs
        )
    elif frontend == 'pandas':
        return filoc_pandas(
            locpath            = locpath,
            file_singleton     = file_singleton,
            file_type          = file_type,
            file_writable      = file_writable,
            file_cache_locpath = file_cache_locpath,
            file_timestamp_col = file_timestamp_col,
            join_keys          = join_keys,
            join_level_name    = join_level_name,
            join_separator     = join_separator,
            **locpath_kwargs
        )
    else:
        raise ValueError(f'Unknown frontend {frontend}')


def filoc_json(
        locpath            : Union[str, Dict[str, str], Dict[str, Filoc]] = None,
        file_singleton     : bool = True,
        file_type          : Optional[PresetBackendTypes] = 'json',
        file_writable      : Optional[bool] = False,
        file_cache_locpath : Optional[str] = None,
        file_timestamp_col : Optional[str] = None,
        join_keys          : Optional[List[str]] = None,
        join_level_name    : Optional[str] = 'index',
        join_separator     : Optional[str] = '.',
        **locpath_kwargs
) -> Filoc[Dict[str, Any], List[Dict[str, Any]]]:
    if locpath is None and len(locpath_kwargs) == 0:
        raise ValueError(f'local_path or locpath_kwargs must be defined')

    # merge locpath_kwargs and locpath
    if len(locpath_kwargs) > 0:
        if locpath is None:
            locpath = locpath_kwargs
        elif isinstance(locpath, dict):
            locpath.update(locpath_kwargs)
        else:
            raise ValueError(f'If **locpath_kwargs is defined, then locpath must be either None or an instance of dict')

    if isinstance(locpath, dict):
        filoc_by_name = OrderedDict()
        for filoc_name, filoc_input in locpath.items():
            if isinstance(filoc_input, Filoc):
                filoc_instance = filoc_input
            elif isinstance(filoc_input, str):
                filoc_instance = filoc(
                    locpath            = filoc_input,
                    frontend='json',
                    file_singleton     = file_singleton,
                    file_type          = file_type,
                    file_writable      = file_writable,
                    file_cache_locpath = file_cache_locpath,
                    file_timestamp_col = file_timestamp_col,
                    join_keys          = join_keys,
                    join_level_name    = join_level_name,
                    join_separator     = join_separator,
                )
            else:
                raise ValueError(f"Unexpected filoc_input for filoc_name {filoc_name}. Accepted string or Filoc instance")
            filoc_by_name[filoc_name] = filoc_instance

        return FilocCompositeJson(
            filoc_by_name           = filoc_by_name,
            join_keys_by_filoc_name = join_keys,
            join_level_name         = join_level_name,
            join_separator          = join_separator,
        )
    elif isinstance(locpath, str):
        return FilocJson(
            locpath        = locpath,
            file_type      = file_type,
            file_singleton = file_singleton,
            writable       = file_writable,
            cache_locpath  = file_cache_locpath,
            timestamp_col  = file_timestamp_col
        )
    else:
        raise ValueError(f'locpath must be an instance of str or dict, but is {type(locpath)}')


def filoc_pandas(
        locpath            : Union[str, Dict[str, str], Dict[str, Filoc]] = None,
        file_singleton     : bool                           = True,
        file_type          : Optional[PresetBackendTypes] = 'json',
        file_writable      : Optional[bool]                 = False,
        file_cache_locpath : Optional[str]                  = None,
        file_timestamp_col : Optional[str]                  = None,
        join_keys          : Optional[List[str]]            = None,
        join_level_name    : Optional[str]                  = 'index',
        join_separator     : Optional[str]                  = '.',
        **locpath_kwargs
) -> Filoc[DataFrame, DataFrame]:
    if locpath is None and len(locpath_kwargs) == 0:
        raise ValueError(f'local_path or locpath_kwargs must be defined')

    # merge locpath_kwargs and locpath
    if len(locpath_kwargs) > 0:
        if locpath is None:
            locpath = locpath_kwargs
        elif isinstance(locpath, dict):
            locpath.update(locpath_kwargs)
        else:
            raise ValueError(f'If **locpath_kwargs is defined, then locpath must be either None or an instance of dict')

    if isinstance(locpath, dict):
        filoc_by_name = OrderedDict()
        for filoc_name, filoc_input in locpath.items():
            if isinstance(filoc_input, Filoc):
                filoc_instance = filoc_input
            elif isinstance(filoc_input, str):
                filoc_instance = filoc(
                    locpath            = filoc_input,
                    frontend='json',
                    file_singleton     = file_singleton,
                    file_type          = file_type,
                    file_writable      = file_writable,
                    file_cache_locpath = file_cache_locpath,
                    file_timestamp_col = file_timestamp_col,
                    join_keys          = join_keys,
                    join_level_name    = join_level_name,
                    join_separator     = join_separator,
                )
            else:
                raise ValueError(
                    f"Unexpected filoc_input for filoc_name {filoc_name}. Accepted string or Filoc instance")
            filoc_by_name[filoc_name] = filoc_instance

        return FilocCompositePandas(
            filoc_by_name           = filoc_by_name,
            join_keys_by_filoc_name = join_keys,
            join_level_name         = join_level_name,
            join_separator          = join_separator,
        )
    else:
        return FilocPandas(
            locpath        = locpath,
            file_type      = file_type,
            file_singleton = file_singleton,
            writable       = file_writable,
            cache_locpath  = file_cache_locpath,
            timestamp_col  = file_timestamp_col
        )


__all__     = [
    'FilocOpener',
    'FilocBase',
    'FilocCompositeBase',
    'FilocJson',
    'FilocCompositeJson',
    'FilocPandas',
    'FilocCompositePandas',
    'filoc',
    'filoc_json',
    'filoc_pandas',
]
