from collections import OrderedDict
from typing import Union, Dict, Optional, List

from .filoc_opener import FilocOpener
from .filoc_base import FilocBase, FilocCompositeBase, Filoc
from .filoc_json import FilocJson, FilocCompositeJson
from .filoc_pandas import FilocPandas, FilocCompositePandas

__version__ = '0.0.6'

from .filoc_types import PresetContentTypes, PresetFiLocFileTypes


def filoc(
        locpath            : Union[str, Dict[str, str], Dict[str, Filoc]],
        content_type       : PresetContentTypes             = 'pandas',
        file_singleton     : bool                           = True,
        file_type          : Optional[PresetFiLocFileTypes] = 'json',
        file_writable      : Optional[bool]                 = False,
        file_cache_locpath : Optional[str]                  = None,
        file_timestamp_col : Optional[str]                  = None,
        join_keys          : Optional[List[str]]            = None,
        join_level_name    : Optional[str]                  = 'index',
        join_separator     : Optional[str]                  = '.',
) -> Filoc:
    if isinstance(locpath, dict):
        filoc_by_name = OrderedDict()
        for filoc_name, filoc_input in locpath.items():
            if isinstance(filoc_input, Filoc):
                filoc_instance = filoc_input
            elif isinstance(filoc_input, str):
                filoc_instance = filoc(
                    locpath            = filoc_input,
                    content_type       = content_type,
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

        if content_type == 'json':
            return FilocCompositeJson(
                filoc_by_name   = filoc_by_name,
                join_keys       = join_keys,
                join_level_name = join_level_name,
                join_separator  = join_separator,
            )
        elif content_type == 'pandas':
            return FilocCompositePandas(
                filoc_by_name   = filoc_by_name,
                join_keys       = join_keys,
                join_level_name = join_level_name,
                join_separator  = join_separator,
            )
    else:
        if content_type == 'json':
            return FilocJson(
                locpath        = locpath,
                file_type      = file_type,
                file_singleton = file_singleton,
                writable       = file_writable,
                cache_locpath  = file_cache_locpath,
                timestamp_col  = file_timestamp_col
            )
        elif content_type == 'pandas':
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
    'filoc'
]
