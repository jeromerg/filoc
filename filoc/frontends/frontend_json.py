import logging
from filoc.contract import SingletonExpectedError, TContents, PropsList, TContent, FrontendContract

log = logging.getLogger('filoc')


class JsonFrontend(FrontendContract):
    def read_content(self, props_list: PropsList) -> TContent:
        if len(props_list) != 1:
            raise SingletonExpectedError(f'Expected singleton, got {len(props_list)} items to convert to content')
        return props_list[0]

    def read_contents(self, props_list: PropsList) -> TContents:
        return props_list

    def write_content(self, content: TContent) -> PropsList:
        return [content]

    def write_contents(self, contents: TContents) -> PropsList:
        return contents
