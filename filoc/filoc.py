import itertools
import json
import logging
import os
import pickle
import re
from collections import OrderedDict
from functools import partial
from inspect import signature
from typing import Dict, Any, List, Union, Callable, Optional, Set, Literal
from typing import Tuple

import fsspec
import parse
from fsspec import AbstractFileSystem
from typing.io import IO

log = logging.getLogger('filoc')

# -----
# Types
# -----
FiLocFileTypes = Literal[None, 'json', 'yaml', 'pickle', 'csv']

FileReader = Union[
    Callable[[IO], Dict[str, Any]],
    Callable[[IO, Dict[str, Any]], Dict[str, Any]]
]

FileWriter = Union[
    Callable[[IO, Dict[str, Any]], None],
    Callable[[IO, Dict[str, Any], Dict[str, Any]], None]
]

# ---------
# Constants
# ---------
_improbable_string    = "o=NvZ_ps$"
_re_improbable_string = re.compile(r'(o=NvZ_ps\$)')
_re_natural           = re.compile(r"(\d+)")
_re_path_placeholder  = re.compile(r'({[^}]+})')

_default_cache_reader = pickle.load
_default_cache_writer = pickle.dump


# -------
# Helpers
# -------
def sort_natural(li: List[str]) -> List[str]:
    # todo: support float too
    return sorted(li, key=lambda s: [int(part) if part.isdigit() else part for part in _re_natural.split(s)])


def mix_dicts(dict1, dict2):
    dict2 = None if len(dict2) == 0 else dict2
    if dict1 and dict2:
        properties = OrderedDict([dict1, dict2])
    elif dict1:
        properties = dict1
    elif dict2:
        properties = dict2
    else:
        properties = {}
    return properties


def _get_reader_writer(file_type : FiLocFileTypes):
    if file_type == 'json':
        import json
        return json.load, partial(json.dump, indent=2, sort_keys=True)
    elif file_type == 'yaml':
        # noinspection PyPackageRequirements
        import yaml
        return yaml.load, yaml.dump
    elif file_type == 'csv':
        import csv
        return csv.DictReader, csv.DictWriter
    elif file_type == 'pickle':
        import pickle
        return pickle.load, pickle.dump


# -------------------
# Base Class RawFiloc
# -------------------
class RawFiloc:
    def __init__(self, locpath: str) -> None:
        super().__init__()
        self.locpath     = locpath
        path_elts        = _re_path_placeholder.split("_" + locpath)  # dummy _ ensures that first elt is not a placeholder
        path_elts[0]     = path_elts[0][1:]  # removes _
        placeholders     = path_elts[1::2]
        valid_path       = "".join([e1+e2 for e1, e2 in zip(path_elts[::2], itertools.repeat(_improbable_string))])
        valid_file       = fsspec.open(valid_path)
        self.fs          = valid_file.fs           # type: AbstractFileSystem
        norm_elts        = _re_improbable_string.split("_" + valid_file.path)  # dummy _ ensures that first elt is not a placeholder
        norm_elts[0]     = norm_elts[0][1:]  # removes _
        norm_path        = "".join([e1+e2 for e1, e2 in itertools.zip_longest(norm_elts[::2], placeholders, fillvalue="")])
        self.locpath     = norm_path  # type: str
        self.path_parser = parse.compile(self.locpath)  # type: parse.Parser
        self.root_folder = self.fs.sep.join((norm_elts[0] + "AAA").split(self.fs.sep)[:-1])  # AAA ensures that the path ends with a file name/folder name to remove
        # noinspection PyProtectedMember
        self.path_properties = set(self.path_parser._named_fields)  # type: Set[str]

    # noinspection PyDefaultArgument
    def get_path_properties(self, path: str) -> Dict[str, Any]:
        try:
            return self.path_parser.parse(path).named
        except Exception as e:
            raise ValueError(f'Could not parse {path} with {self.locpath} parser: {e}')

    def get_path(self, properties1 : Dict[str, Any] = None, **properties2 : Any) -> str:
        properties = mix_dicts(properties1, properties2)
        undefined_keys = self.path_properties - set(properties)
        if len(undefined_keys) > 0:
            raise ValueError('Undefined properties: {}'.format(undefined_keys))
        return self.locpath.format(**properties)  # result should be normalized, because locpath is

    def get_glob_path(self, properties1 : Dict[str, Any] = None, **properties2 : Any) -> str:
        properties = mix_dicts(properties1, properties2)
        provided_keys = set(properties)
        undefined_keys = self.path_properties - provided_keys
        defined_keys = self.path_properties - undefined_keys

        path_values = OrderedDict()
        path_values.update({(k, properties[k]) for k in defined_keys})

        glob_path = self.locpath
        for undefined_key in undefined_keys:
            glob_path = re.sub(r'{' + undefined_key + r'(?::[^}]*)?}', '?*', glob_path)

        # finally format
        glob_path = glob_path.format(**path_values)
        return glob_path  # result should be normalized, because locpath is

    def find_paths(self, properties1 : Dict[str, Any] = None, **properties2 : Any) -> List[str]:
        properties = mix_dicts(properties1, properties2)
        paths = self.fs.glob(self.get_glob_path(properties))
        return sort_natural(paths)

    def find_paths_and_properties(self, properties1 : Dict[str, Any] = None, **properties2 : Any) -> List[Tuple[str, List[str]]]:
        properties = mix_dicts(properties1, properties2)
        paths = self.find_paths(properties)
        return [(p, self.get_path_properties(p)) for p in paths]

    def open(self, properties : Dict[str, Any], mode="rb", block_size=None, cache_options=None, **kwargs):
        path = self.get_path(properties)
        dirname = os.path.dirname(path)

        if not self.fs.exists(dirname) and len( set(mode) & set("wa+")) > 0:
            self.fs.makedirs(dirname)

        return self.fs.open(path, mode, block_size, cache_options, **kwargs)

    def delete(self, properties : Dict[str, Any]):
        for path in self.find_paths(properties):
            self.fs.delete(path)

    def __eq__(self, other):
        if other is not self:
            return False
        return self.locpath == other.locpath

    def __hash__(self):
        return self.locpath.__hash__()


# -------------------
# Main Class Filoc
# -------------------
class Filoc(RawFiloc):
    # noinspection PyDefaultArgument
    def __init__(
            self,
            locpath             : str,
            file_type           : FiLocFileTypes       = None,
            reader              : Optional[FileReader] = None,
            writer              : Optional[FileWriter] = None,
            reader_open_options : Dict[str, Any]       = {'mode': 'rb'},
            writer_open_options : Dict[str, Any]       = {'mode': 'rw'},
            cache_locpath       : str                  = None,
            timestamp_col       : str                  = None,
    ):
        """
        if cache_locpath is relative, then it will be relative to result_locpath
        """
        super().__init__(locpath)

        # reader and writer
        self.file_type = file_type
        default_reader, default_writer = (None, None)
        if file_type and (reader is None or writer is None):
            default_reader, default_writer = _get_reader_writer(file_type)
        self.reader = reader if reader else default_reader if file_type else None  # type:Optional[FileReader]
        self.writer = writer if writer else default_writer if file_type else None  # type:Optional[FileWriter]
        self.reader_open_options = reader_open_options  # type:Dict[str, Any]
        self.writer_open_options = writer_open_options  # type:Dict[str, Any]

        # cache loc
        self.cache_loc = None
        if cache_locpath is not None:
            if not os.path.isabs(cache_locpath):
                cache_locpath = self.root_folder + '/' + cache_locpath
            self.cache_loc = RawFiloc(cache_locpath)
        self.timestamp_col = timestamp_col
        # cache_timestamp_col
        if self.cache_loc is None:
            self.cache_timestamp_col = None
        elif timestamp_col is None:
            self.cache_timestamp_col = 'timestamp'
        else:
            self.cache_timestamp_col = timestamp_col

    def clean_cache(self, properties1 : Dict[str, Any] = None, **properties2):
        properties = mix_dicts(properties1, properties2)
        self.cache_loc.delete(properties)

    def get_values(self, properties1 : Dict[str, Any] = None, **properties2) -> List[Dict[str, Any]]:
        properties = mix_dicts(properties1, properties2)
        return [OrderedDict([props, values]) for (props, values) in self._get_values_by_properties(properties).items()]

    def _get_values_by_properties(self, properties1 : Dict[str, Any] = None, **properties2) -> Dict[Dict[str, Any], Dict[str, Any]]:
        properties = mix_dicts(properties1, properties2)
        result = OrderedDict()
        opened_cache_path       = None
        opened_cache            = None
        paths_and_properties    = self.find_paths_and_properties(properties)
        log.info(f'Found {len(paths_and_properties)} files to read in locpath {self.locpath} with properties {json.dumps(properties)}')
        for (f_path, f_props) in paths_and_properties:
            f_timestamp = os.path.getmtime(f_path)

            # renew cache, on cache file change
            if self.cache_loc:
                f_cache_path = self.cache_loc.get_path(f_props)
                if opened_cache_path is None or opened_cache_path != f_cache_path:
                    # flush previous cache, if exists
                    if opened_cache_path:
                        with self.cache_loc.open(f_props, 'wb') as f:
                            _default_cache_writer(opened_cache, f)

                    # now prepare new cache
                    opened_cache_path = f_cache_path
                    opened_cache      = OrderedDict()
                    if self.cache_loc.fs.exists(opened_cache_path):
                        with self.fs.open(opened_cache_path, 'rb') as f:
                            opened_cache = _default_cache_reader(f)  # type:Dict[str, Dict[str, Any]]

            # check whether cache entry is still valid
            if self.cache_loc:
                if f_props in opened_cache:
                    cached_entry = opened_cache[f_props]  # type: Dict[str, Any]
                    cached_entry_timestamp = cached_entry[self.cache_timestamp_col]
                    if cached_entry_timestamp == f_timestamp:
                        log.info(f'File analysis cached: {f_path}')
                        result[f_props] = cached_entry
                        continue
                    else:
                        log.info(f'Cache out of date for {f_path}')

            # cache is not valid: read file!
            key_values = OrderedDict()
            # -> timestamps
            if self.cache_timestamp_col:
                key_values[self.cache_timestamp_col] = f_timestamp
            if self.timestamp_col:
                key_values[self.timestamp_col] = f_timestamp
            # -> custom report
            if self.reader is not None:
                custom_report = self._read_single_file(f_path, f_props)
                key_values.update(custom_report)

            # add to result and cache
            result[f_props] = key_values
            if self.cache_loc:
                opened_cache[f_props] = key_values

        # flush last used cache
        if self.cache_loc:
            with self.cache_loc.open(properties, 'wb') as f:
                _default_cache_writer(opened_cache, f)

        if self.cache_timestamp_col != self.timestamp_col:
            for r in result:
                if self.cache_timestamp_col in r:
                    del r[self.cache_timestamp_col]

        return result

    def _read_single_file(self, f_path, f_props):
        len_params = len(signature(self.reader).parameters)
        if len_params == 1:
            log.info(f'Analyzing file {f_path}')
            with self.fs.open(f_path, **self.reader_open_options) as f:
                custom_report = self.reader(f)
            log.info(f'Analyzed file {f_path}')
        elif len_params == 2:
            log.info(f'Analyzing file {f_path}')
            with self.fs.open(f_path, **self.reader_open_options) as f:
                custom_report = self.reader(f, f_props)
            log.info(f'Analyzed file {f_path}')
        else:
            raise ValueError(f'Provided file_analyzer accepts {len_params} parameters, allowed signature: f(path) or f(path, properties)')
        return custom_report


class Multiloc:
    def __init__(self, fimaps1 : Dict[str, Filoc], **fimaps2 : Filoc):
        self.fimaps_by_name = mix_dicts(fimaps1, fimaps2)
        self.properties_prefix = 'properties'

    def get_values(self, properties1 : Dict[str, Any] = None, **properties2):
        properties = mix_dicts(properties1, properties2)
        # collect
        all_properties_combinations_set = set()             # type:Set[Dict[str, Any]]
        all_properties_combinations_in_order = []           # type:List[Dict[str, Any]]
        keyvalues_by_properties_by_fimap_name = OrderedDict()  # type:OrderedDict[str, Dict[Dict[str, Any], Dict[str, Any]]]
        for property_name, fimap in self.fimaps_by_name.items():
            # noinspection PyProtectedMember
            keyvalues_by_properties = fimap._get_values_by_properties(properties)
            keyvalues_by_properties_by_fimap_name[property_name] = keyvalues_by_properties
            for props in keyvalues_by_properties:
                if props not in all_properties_combinations_set:
                    all_properties_combinations_set.add(props)
                    all_properties_combinations_in_order.append(props)

        # outer join
        result = []
        for properties in all_properties_combinations_in_order:
            result_row = OrderedDict()
            result.append(result_row)

            # add properties
            for property_name, property_value in properties.items():
                result_row[(self.properties_prefix, property_name)] = property_value

            # add all key values for each fimap
            for fimap_name, keyvalues_by_properties in keyvalues_by_properties_by_fimap_name.items():
                keyvalues = keyvalues_by_properties.get(properties, None)
                if keyvalues:
                    for key, value in keyvalues.items():
                        result_row[(fimap_name, key)] = value
        return result
