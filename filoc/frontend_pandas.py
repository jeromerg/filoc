import logging

from pandas import DataFrame

from .contract import PropsList, TContents, TContent, FrontendContract

log = logging.getLogger('filoc')


class PandasFrontend(FrontendContract):
    def read_content(self, props_list: PropsList) -> TContent:
        return DataFrame(props_list)

    def read_contents(self, props_list: PropsList) -> TContents:
        return DataFrame(props_list)

    def write_content(self, content: TContent) -> PropsList:
        return content.to_dict(orient='records')

    def write_contents(self, contents: TContents) -> PropsList:
        return contents.to_dict(orient='records')
