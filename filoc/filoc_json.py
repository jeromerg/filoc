import logging
from typing import Dict, Any, List, Optional, Iterable

from .filoc_base import FilocBase, FilocCompositeBase
from .filoc_types import PresetFiLocFileTypes
from .utils import get_default_file_type_reader, get_default_file_type_writer

log = logging.getLogger('filoc')


def _props_list_to_content_converter(props_list):
    assert len(props_list) == 1, f'Expected singleton, got {len(props_list)} items to convert to content'
    return props_list[0]


def _props_list_to_contents_converter(props_list):
    return props_list


def _content_to_props_list_converter(content):
    return [content]


def _contents_to_props_list_converter(contents):
    return contents


class FilocJson(FilocBase[Dict[str, Any], List[Dict[str, Any]]]):
    def __init__(
            self,
            locpath        : str                 ,
            file_type      : PresetFiLocFileTypes = 'json',
            file_singleton : bool                 = True,
            writable       : bool                 = False,
            cache_locpath  : str                  = None,
            timestamp_col  : str                  = None,
    ):
        super().__init__(
            locpath,
            writable,
            get_default_file_type_reader(file_type, file_singleton),
            get_default_file_type_writer(file_type, file_singleton),
            _props_list_to_content_converter,
            _props_list_to_contents_converter,
            _content_to_props_list_converter,
            _contents_to_props_list_converter,
            cache_locpath,
            timestamp_col
        )


class FilocCompositeJson(FilocCompositeBase[Dict[str, Any], List[Dict[str, Any]]]):
    def __init__(
            self,
            filoc_by_name   : Dict[str, FilocBase],
            join_keys       : Optional[Iterable[str]] = None,
            join_level_name : str                     = 'index',
            join_separator  : str                     = '.',
    ):
        super().__init__(
            filoc_by_name,
            _props_list_to_content_converter,
            _props_list_to_contents_converter,
            _content_to_props_list_converter,
            _contents_to_props_list_converter,
            join_keys,
            join_level_name,
            join_separator,
        )
