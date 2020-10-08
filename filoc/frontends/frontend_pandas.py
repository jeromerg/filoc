import logging

from pandas import DataFrame, Series

from filoc.contract import FrontendConversionError, PropsList, SingletonExpectedError, TContents, TContent, FrontendContract

log = logging.getLogger('filoc')


class PandasFrontend(FrontendContract):
    def read_content(self, props_list: PropsList) -> TContent:
        if len(props_list) != 1:
            raise SingletonExpectedError(f'Expected singleton, got {len(props_list)} items to convert to content')

        return Series(props_list[0], dtype=object)  # dtype object in order to preserve int type, if Series contains only int and float values (elsewhere float wins)

    def read_contents(self, props_list: PropsList) -> TContents:
        return DataFrame(props_list)

    def write_content(self, content: TContent) -> PropsList:
        if isinstance(content, dict):
            return [content]
        elif isinstance(content, Series):
            return [content.to_dict()]
        else:
            raise FrontendConversionError(f'Expected dict or Series, got {type(content).__name__}') 

    def write_contents(self, contents: TContents) -> PropsList:
        if isinstance(contents, list):
            return contents
        elif isinstance(contents, DataFrame):
            return contents.to_dict(orient='records')
        else:
            raise FrontendConversionError(f'Expected list or DataFrame, got {type(contents).__name__}') 
