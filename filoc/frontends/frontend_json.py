""" Contains the default JSON frontend implementation """
import logging
from filoc.contract import SingletonExpectedError, TContents, PropsList, TContent, FrontendContract

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

    def write_content(self, content: TContent) -> PropsList:
        """(see FrontendContract contract)"""
        return [content]

    def write_contents(self, contents: TContents) -> PropsList:
        """(see FrontendContract contract)"""
        return contents
