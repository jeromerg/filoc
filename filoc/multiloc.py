import logging
from collections import OrderedDict
from typing import Dict, Any, List, Optional, Set, Tuple, Callable, TypeVar, Generic, get_args

from orderedset import OrderedSet

from .filoc import Filoc
from .rawfiloc import mix_dicts

log = logging.getLogger('multiloc')

TContent = TypeVar('TContent')


class MultilocBase(Generic[TContent]):
    def __init__(
            self,
            filoc_by_name    : Dict[str, Filoc],
            make_content_fn  : Callable[  [  List[Dict[Tuple[str, str], Any]], Tuple[str, str]  ], TContent  ],
            parse_content_fn : Callable[  [TContent], List[Dict[Tuple[str,str], Any]]],
            path_level_name  : str,
    ):
        assert isinstance(filoc_by_name, dict)

        self.filoc_by_name    = filoc_by_name
        self.path_level_name  = path_level_name
        self.make_content_fn  =  make_content_fn
        self.parse_content_fn = parse_content_fn

    def read_contents(self, path_props : Optional[Dict[str, Any]] = None, **path_props_kwargs) -> TContent:
        path_props = mix_dicts(path_props, path_props_kwargs)
        contents, keys = self._read_raw_contents_and_keys(path_props)
        return self.make_content_fn(contents, keys)

    def write_contents(self, content : TContent, dry_run=False):
        raw_content = self.parse_content_fn(content)
        self._write_raw_contents(raw_content, dry_run=dry_run)

    def _read_raw_contents_and_keys(self, path_props : Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[Tuple[str, str], Any]], Tuple[str, str]]:
        # collect
        all_path_props_combinations_set = set()             # type:Set[Dict[str, Any]]
        all_path_props_combinations_in_order = []           # type:List[Dict[str, Any]]
        keyvalues_by_path_props_by_filoc_name = OrderedDict()  # type:OrderedDict[str, Dict[Dict[str, Any], Dict[str, Any]]]
        for filoc_name, filoc in self.filoc_by_name.items():
            # noinspection PyProtectedMember
            keyvalues_by_path_props = filoc._read_file_content_by_path_props(path_props)
            keyvalues_by_path_props_by_filoc_name[filoc_name] = keyvalues_by_path_props
            for props in keyvalues_by_path_props:
                if props not in all_path_props_combinations_set:
                    all_path_props_combinations_set.add(props)
                    all_path_props_combinations_in_order.append(props)

        # outer join
        result_keys = OrderedSet()
        result = []
        for path_props in all_path_props_combinations_in_order:
            result_row = OrderedDict()
            result.append(result_row)

            # add path_props
            for property_name, property_value in path_props.items():
                hierarchical_key = (self.path_level_name, property_name)
                result_row[hierarchical_key] = property_value
                result_keys.add(hierarchical_key)

            # add all key values for each filoc
            for filoc_name, keyvalues_by_path_props in keyvalues_by_path_props_by_filoc_name.items():
                keyvalues = keyvalues_by_path_props.get(path_props, None)
                if keyvalues:
                    for key, value in keyvalues.items():
                        hierarchical_key = (filoc_name, key)
                        result_row[hierarchical_key] = value
                        result_keys.add(hierarchical_key)
        return result, list(result_keys)

    def _write_raw_contents(self, raw_content : List[Dict[Tuple[str, str], Any]], dry_run=False):
        # split keyvalues_list into filoc buckets
        # ... first create empty placeholder vor key values
        path_props_list = [OrderedDict() for _ in range(len(raw_content))]
        keyvalues_list_by_filoc_name = {}
        for filoc_name, filoc in self.filoc_by_name.items():
            if filoc.content_writer is None:
                continue  # don't write non writable filocs
            filoc_keyvalues_list = []
            keyvalues_list_by_filoc_name[filoc_name] = filoc_keyvalues_list
            for i in range(len(raw_content)):
                filoc_keyvalues_list.append(OrderedDict())

        # ... then fill
        for row_id, keyvalues in enumerate(raw_content):
            for (filoc_name, prop_name), value in keyvalues.items():
                if filoc_name == self.path_level_name:
                    row_path_props = path_props_list[row_id]
                    row_path_props[prop_name] = value
                elif filoc_name in keyvalues_list_by_filoc_name:
                    filoc_row_keyvalues = keyvalues_list_by_filoc_name[filoc_name][row_id]
                    filoc_row_keyvalues[prop_name] = value
                else:
                    log.warning(f'write_contents: Skip unknown Filoc name {filoc_name} for hierarchy {key}')

        # delegate writing to
        for filoc_name, filoc_keyvalues_list in keyvalues_list_by_filoc_name.items():
            # merge path_props to filoc_keyvalues for each row
            for row_id, filoc_keyvalues in enumerate(filoc_keyvalues_list):
                filoc_keyvalues.update(path_props_list[row_id])
            filoc = self.filoc_by_name[filoc_name]
            filoc.write_contents(filoc_keyvalues_list, dry_run=dry_run)


def make_json_content_factory(separator : str) -> Callable[  [  List[Dict[Tuple[str, str], Any]], Tuple[str, str]  ], List[Dict[str, Any]]  ]:
    # noinspection PyUnusedLocal
    def make_default_json_content(raw_contents : List[Dict[Tuple[str,str], Any]], keys : List[Tuple[str,str]]) -> List[Dict[str, Any]]:
        return [{separator.join(k): v for (k, v) in row.items()} for row in raw_contents]
    return make_default_json_content


def parse_json_content_factory(separator : Optional[str] = None) -> Callable[  [  List[Dict[str, Any]]  ], List[Dict[Tuple[str,str], Any]]  ]:
    def parser_default_json_content(contents : List[Dict[str, Any]]) -> List[Dict[Tuple[str,str], Any]]:
        return [{tuple(k.split(separator)): v for (k, v) in row.items()} for row in contents]
    return parser_default_json_content


class Multiloc(MultilocBase[List[Dict[Tuple[str, str], Any]]]):
    def __init__(
            self,
            filoc_by_name: Dict[str, Filoc],
            level_separator: str = ".",
            path_level_name: str = 'path'
    ):
        super().__init__(
            filoc_by_name,
            make_json_content_factory(level_separator),
            parse_json_content_factory(level_separator),
            path_level_name
        )


try:
    import pandas  # pandas is not a required dependency of filoc
    from pandas import DataFrame

    def make_pandas_content_factory(separator : Optional[str] = None) -> Callable[  [  List[Dict[Tuple[str, str], Any]], Tuple[str, str]  ], DataFrame]:
        def make_content_pandas(raw_contents : List[Dict[Tuple[str,str], Any]], keys : List[Tuple[str,str]]) -> DataFrame:
            if separator:
                keys = [separator.join(k) for k in keys]
                raw_contents = [{separator.join(k): v for (k, v) in row.items()} for row in raw_contents]
            return pandas.DataFrame(raw_contents, columns=keys)
        return make_content_pandas


    def parse_pandas_content_factory(separator : Optional[str] = None) -> Callable[[DataFrame], List[Dict[Tuple[str,str], Any]]]:
        def parser_content_pandas(contents : DataFrame) -> List[Dict[Tuple[str,str], Any]]:
            raw_contents = contents.to_dict(orient='records')
            if separator:
                raw_contents = [{tuple(k.split(separator)): v for (k, v) in row.items()} for row in raw_contents]
            return raw_contents
        return parser_content_pandas


    class PandasMultiloc(MultilocBase[DataFrame]):
        def __init__(
                self,
                filoc_by_name: Dict[str, Filoc],
                level_separator: str = ".",
                path_level_name: str = 'path'
        ):
            super().__init__(
                filoc_by_name,
                make_pandas_content_factory(level_separator),
                parse_pandas_content_factory(level_separator),
                path_level_name
            )

except ModuleNotFoundError:
    class PandasMultiloc(MultilocBase):
        # noinspection PyMissingConstructor
        def __init__(self, *args, **kwargs):
            raise ModuleNotFoundError("pandas not available: pip install pandas / conda install pandas")


