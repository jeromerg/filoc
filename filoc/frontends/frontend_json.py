""" Contains the default JSON frontend implementation """
import logging
from collections import Mapping, Collection

from filoc.contract import SingletonExpectedError, TContents, PropsList, TContent, FrontendContract, \
    FrontendConversionError, ReadOnlyPropsList

log = logging.getLogger('filoc')


class JsonFrontend(FrontendContract):
    """
    JSON frontend implementation.
    """

    def read_content(self, props_list: PropsList) -> TContent:
        """(see FrontendContract contract)"""

        if len(props_list) != 1:
            raise SingletonExpectedError(f'Expected singleton, got {len(props_list)} items to convert to content')
        return props_list[0]

    def read_contents(self, props_list: PropsList) -> TContents:
        """(see FrontendContract contract)"""
        return props_list

    def write_content(self, content: TContent) -> ReadOnlyPropsList:
        """(see FrontendContract contract)"""
        if isinstance(content, Mapping):
            return [content]
        else:
            raise FrontendConversionError(f'Expected instance of Mapping, got {type(content).__name__}')

    def write_contents(self, contents: TContents) -> ReadOnlyPropsList:
        """(see FrontendContract contract)"""
        if isinstance(contents, Collection):
            return contents
        else:
            raise FrontendConversionError(f'Expected instance of Collection, got {type(contents).__name__}')
