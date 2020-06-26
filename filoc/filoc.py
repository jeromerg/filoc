import itertools
import json
import logging
import os
import pickle
import re
from collections import OrderedDict
from inspect import signature
from typing import List, Set, Dict, Tuple, Callable, Union
import fsspec
import parse
from fsspec import AbstractFileSystem
from typing.io import IO

_improbable_string    = "o=NvZ_ps$"
_re_improbable_string = re.compile(r'(o=NvZ_ps\$)')
_re_natural           = re.compile(r"(\d+)")
_re_path_placeholder  = re.compile(r'({[^}]+})')

FileAnalyzerType = Union[None, Callable[[IO], Dict[str, object]], Callable[[IO, Dict[str, object]], Dict[str, object]]]

log = logging.getLogger('filoc')

default_cache_reader = pickle.load
default_cache_writer = pickle.dump


def sort_natural(li: List[str]) -> List[str]:
    # todo: improve
    return sorted(li, key=lambda s: [int(part) if part.isdigit() else part for part in _re_natural.split(s)])


def mix_properties1_properties2(properties1, properties2):
    properties2 = None if len(properties2) == 0 else properties2
    if properties1 and properties2:
        properties = OrderedDict([properties1, properties2])
    elif properties1:
        properties = properties1
    elif properties2:
        properties = properties2
    else:
        properties = {}
    return properties


class Filoc:
    def __init__(self, locpath: str) -> None:
        super().__init__()

        path_elts        = _re_path_placeholder.split("_" + locpath) # dummy _ ensures that first elt is not a placeholder
        path_elts[0]     = path_elts[0][1:] # removes _
        placeholders     = path_elts[1::2]
        valid_path       = "".join([e1+e2 for e1,e2 in zip(path_elts[::2], itertools.repeat(_improbable_string))])
        valid_file       = fsspec.open(valid_path)
        self.fs          = valid_file.fs           # type: AbstractFileSystem
        norm_elts        = _re_improbable_string.split("_" + valid_file.path) # dummy _ ensures that first elt is not a placeholder
        norm_elts[0]     = norm_elts[0][1:] # removes _
        norm_path        = "".join([e1+e2 for e1,e2 in itertools.zip_longest(norm_elts[::2], placeholders, fillvalue="")])
        self.locpath     = norm_path  # type: str
        self.path_parser = parse.compile(self.locpath)  # type: parse.Parser
        self.root_folder = self.fs.sep.join((norm_elts[0] + "AAA").split(self.fs.sep)[:-1]) # AAA ensures that the path ends with a file name/folder name to remove
        # noinspection PyProtectedMember
        self.path_properties = set(self.path_parser._named_fields)  # type: Set[str]

    # noinspection PyDefaultArgument
    def extract_properties(self, path: str) -> Dict[str, object]:
        try:
            return self.path_parser.parse(path).named
        except Exception as e:
            raise ValueError(f'Could not parse {path} with {self.locpath} parser: {e}')

    def build_path(self, properties1 : Dict[str, object] = None, **properties2) -> str:
        properties = mix_properties1_properties2(properties1, properties2)
        undefined_keys = self.path_properties - set(properties)
        if len(undefined_keys) > 0:
            raise ValueError('Undefined properties: {}'.format(undefined_keys))
        return self.locpath.format(**properties) # result should be normalized, because locpath is

    def build_glob_path(self, properties1 : Dict[str, object] = None, **properties2) -> str:
        properties = mix_properties1_properties2(properties1, properties2)
        provided_keys = set(properties)
        undefined_keys = self.path_properties - provided_keys
        defined_keys = self.path_properties - undefined_keys

        path_values = OrderedDict()
        path_values.update({(k, properties[k]) for k in defined_keys})

        glob_path = self.locpath
        for undefined_key in undefined_keys:
            glob_path = re.sub(r'{' + undefined_key + r'(?::[^}]*)}', '*', glob_path)

        # finally format
        glob_path = glob_path.format(**path_values)
        return glob_path # result should be normalized, because locpath is

    def find_paths(self, properties1 : Dict[str, object] = None, **properties2) -> List[str]:
        properties = mix_properties1_properties2(properties1, properties2)
        paths = self.fs.glob(self.build_glob_path(properties))
        return sort_natural(paths) # result should be normalized, because TODO: VERIFY THAT fs.glob provides normalized paths

    def find_paths_and_properties(self, properties1 : Dict[str, object] = None, **properties2) -> List[Tuple[str, List[str]]]:
        properties = mix_properties1_properties2(properties1, properties2)
        paths = self.find_paths(properties)
        return [(p, self.extract_properties(p)) for p in paths]

    def open(self, properties : Dict[str, object], mode="rb", block_size=None, cache_options=None, **kwargs):
        path = self.build_path(properties)
        dirname = os.path.dirname(path)

        if not self.fs.exists(dirname) and len( set(mode) & set("wa+")) > 0:
            self.fs.makedirs(dirname)

        return self.fs.open(path, mode, block_size, cache_options, **kwargs)

    def report(
            self,
            properties: Dict[str, object],
            file_analyzer : FileAnalyzerType = None,
            cache_locpath=None,
            timestamp_col=None,
            mode="rb",
            block_size=None,
            cache_options=None,
            **kwargs
    ) -> List[Dict[str, object]]:
        """
        if cache_locpath is relative, then it will be relative to result_locpath
        """
        result = []

        cache_loc = None
        if cache_locpath is not None:
            if not os.path.isabs(cache_locpath):
                cache_locpath = self.root_folder + '/' + cache_locpath
            cache_loc = Filoc(cache_locpath)

        if cache_loc is None:
            cache_timestamp_col = None
        elif timestamp_col is None:
            cache_timestamp_col = 'timestamp'
        else:
            cache_timestamp_col = timestamp_col

        opened_cache_path       = None
        opened_cache            = None
        paths_and_properties    = self.find_paths_and_properties(properties)
        log.info(f'Found {len(paths_and_properties)} files to analyse in path {self.locpath} with properties {json.dumps(properties)}')
        for (f_path, f_props) in paths_and_properties:
            f_timestamp = os.path.getmtime(f_path)

            cache_key = json.dumps(f_props, sort_keys=True)

            # renew cache, on cache file change
            if cache_loc:
                f_cache_path = cache_loc.build_path(f_props)
                if opened_cache_path is None or opened_cache_path != f_cache_path:
                    # flush previous cache, if exists
                    if opened_cache_path:
                        with cache_loc.open(f_props, 'wb') as f:
                            default_cache_writer(opened_cache, f)

                    # now prepare new cache
                    opened_cache_path = f_cache_path
                    opened_cache      = OrderedDict()
                    if os.path.exists(cache_locpath):
                        with self.fs.open(cache_locpath, 'rb') as f:
                            opened_cache = default_cache_reader(f)  # type:Dict[str, Dict[str, object]]

            # skip, if cache entry is still valid
            if cache_loc:
                if cache_key in opened_cache:
                    cached_entry = opened_cache[cache_key]  # type: Dict[str, object]
                    cached_entry_timestamp = cached_entry[cache_timestamp_col]
                    if cached_entry_timestamp == f_timestamp:
                        log.info(f'File analysis cached: {f_path}')
                        result.append(cached_entry)
                        continue
                    else:
                        log.info(f'Cache out of date for {f_path}')

            # build a new report entry
            file_report = OrderedDict()
            # -> timestamps
            if cache_timestamp_col:
                file_report[cache_timestamp_col] = f_timestamp
            if timestamp_col:
                file_report[timestamp_col] = f_timestamp
            # -> f_props
            file_report.update(f_props)
            # -> custom report
            if file_analyzer is not None:
                custom_report = self.get_file_custom_report(f_path, f_props, file_analyzer, mode, block_size, cache_options, **kwargs)
                file_report.update(custom_report)

            # add to result and cache
            result.append(file_report)
            if cache_loc:
                opened_cache[cache_key] = file_report

        # flush last used cache
        if cache_loc:
            with cache_loc.open(properties, 'wb') as f:
                default_cache_writer(opened_cache, f)

        if cache_timestamp_col != timestamp_col:
            for r in result:
                if cache_timestamp_col in r:
                    del r[cache_timestamp_col]

        return result

    def get_file_custom_report(self, f_path, f_props, file_analyzer, mode="rb", block_size=None, cache_options=None, **kwargs):
        len_params = len(signature(file_analyzer).parameters)
        if len_params == 1:
            log.info(f'Analyzing file {f_path}')
            with self.fs.open(f_path, mode, block_size, cache_options, **kwargs) as f:
                custom_report = file_analyzer(f)
            log.info(f'Analyzed file {f_path}')
        elif len_params == 2:
            log.info(f'Analyzing file {f_path}')
            with self.fs.open(f_path, mode, block_size, cache_options, **kwargs) as f:
                custom_report = file_analyzer(f, f_props)
            log.info(f'Analyzed file {f_path}')
        else:
            raise ValueError(f'Provided file_analyzer accepts {len_params} parameters, allowed signature: f(path) or f(path, properties)')
        return custom_report
