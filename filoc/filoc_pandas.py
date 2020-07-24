import logging
from typing import Optional, Callable, List, Dict, Tuple, Any, Iterable

import pandas
from pandas import DataFrame

from .filoc_base import FilocBase, FilocCompositeBase
from .filoc_json import get_default_file_type_reader, get_default_file_type_writer
from .filoc_types import PresetFiLocFileTypes

log = logging.getLogger('filoc')


def _props_list_to_dataframe_converter(props_list):
    return DataFrame(props_list)


def _dataframe_to_props_list_converter(df : DataFrame):
    return df.to_dict(orient='records')


def make_pandas_content_factory(separator : Optional[str] = None) -> Callable[  [  List[Dict[Tuple[str, str], Any]], Tuple[str, str]  ], DataFrame]:
    def make_content_pandas(raw_contents : List[Dict[Tuple[str, str], Any]], keys : List[Tuple[str, str]]) -> DataFrame:
        if separator:
            keys = [separator.join(k) for k in keys]
            raw_contents = [{separator.join(k): v for (k, v) in row.items()} for row in raw_contents]
        return pandas.DataFrame(raw_contents, columns=keys)
    return make_content_pandas


def parse_pandas_content_factory(separator : Optional[str] = None) -> Callable[[DataFrame], List[Dict[Tuple[str, str], Any]]]:
    def parser_content_pandas(contents : DataFrame) -> List[Dict[Tuple[str, str], Any]]:
        raw_contents = contents.to_dict(orient='records')
        if separator:
            raw_contents = [{tuple(k.split(separator)): v for (k, v) in row.items()} for row in raw_contents]
        return raw_contents
    return parser_content_pandas


class FilocPandas(FilocBase[DataFrame, DataFrame]):
    def __init__(
            self,
            locpath        : str                  ,
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
            _props_list_to_dataframe_converter,
            _props_list_to_dataframe_converter,
            _dataframe_to_props_list_converter,
            _dataframe_to_props_list_converter,
            cache_locpath,
            timestamp_col
        )


class FilocCompositePandas(FilocCompositeBase[DataFrame, DataFrame]):
    def __init__(
            self,
            filoc_by_name   : Dict[str, FilocBase],
            join_keys       : Optional[Iterable[str]] = None,
            join_level_name : str = 'index',
            join_separator  : str = '.',
    ):
        super().__init__(
            filoc_by_name,
            _props_list_to_dataframe_converter,
            _props_list_to_dataframe_converter,
            _dataframe_to_props_list_converter,
            _dataframe_to_props_list_converter,
            join_keys,
            join_level_name,
            join_separator,
        )
