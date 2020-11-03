""" Contains the default Pandas frontend implementation """
import itertools
import logging
from collections import Collection, Mapping

from pandas import DataFrame, Series

from filoc.contract import FrontendConversionError, PropsList, SingletonExpectedError, TContents, TContent, \
    FrontendContract, ReadOnlyPropsList

log = logging.getLogger('filoc')


class PandasFrontend(FrontendContract):
    """
    Pandas frontend implementation. It is the default implementation provided by the ``filoc(...)`` factory.
    """
    def read_content(self, props_list: PropsList) -> TContent:
        """(see FrontendContract contract)"""
        if len(props_list) != 1:
            raise SingletonExpectedError(f'Expected singleton, got {len(props_list)} items to convert to content')

        return Series(props_list[0], dtype=object)  # dtype object in order to preserve int type, if Series contains only int and float values (elsewhere float wins)

    def read_contents(self, props_list: PropsList) -> TContents:
        """(see FrontendContract contract)"""
        return DataFrame(props_list)

    def write_content(self, content: TContent) -> ReadOnlyPropsList:
        """(see FrontendContract contract)"""
        if isinstance(content, Series):
            return [content.to_dict()]
        elif isinstance(content, Mapping):
            return [content]
        else:
            raise FrontendConversionError(f'Expected instance of Series or Mapping, got {type(content).__name__}')

    def write_contents(self, contents: TContents) -> ReadOnlyPropsList:
        """(see FrontendContract contract)"""
        if isinstance(contents, DataFrame):
            return contents.to_dict(orient='records')
        elif isinstance(contents, Collection):
            return contents
        else:
            raise FrontendConversionError(f'Expected instance of DataFrame or Collection, got {type(contents).__name__}')
