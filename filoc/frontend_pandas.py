import logging

from pandas import DataFrame, Series

from .contract import PropsList, TContents, TContent, FrontendContract

log = logging.getLogger('filoc')


class PandasFrontend(FrontendContract):
    def read_content(self, props_list: PropsList) -> TContent:
        return DataFrame(props_list)

    def read_contents(self, props_list: PropsList) -> TContents:
        return DataFrame(props_list)

    def write_content(self, content: TContent) -> PropsList:
        if isinstance(content, dict):
            return [content]
        elif isinstance(content, Series):
            return content.to_dict()
        else:
            raise ValueError(f'Expected dict or Series, got {type(content).__name__}') 

    def write_contents(self, contents: TContents) -> PropsList:
        if isinstance(contents, list):
            return contents
        elif isinstance(contents, DataFrame):
            return contents.to_dict(orient='records')
        else:
            raise ValueError(f'Expected list or DataFrame, got {type(contents).__name__}') 
