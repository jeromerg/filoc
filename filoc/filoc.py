import json
import logging
import os
import pickle
from collections import OrderedDict
from functools import partial
from inspect import getfullargspec
from io import UnsupportedOperation
from typing import Dict, Any, List, Union, Callable, Optional, Literal, Tuple, IO

from frozendict import frozendict

from .rawfiloc import RawFiloc, mix_dicts

log = logging.getLogger('filoc')

# -----
# Types
# -----
PresetFiLocFileTypes = Literal[None, 'json', 'yaml', 'pickle', 'csv']

FileReader = Union[
    Callable[[IO], Dict[str, Any]],
    Callable[[IO, Dict[str, Any]], Dict[str, Any]]
]

FileWriter = Union[
    Callable[[Dict[str, Any], IO], None],
    Callable[[Dict[str, Any], Dict[str, Any], IO], None]
]

_default_cache_reader = pickle.load
_default_cache_writer = pickle.dump


# -------
# Helpers
# -------
def _get_content_reader_writer(file_type : PresetFiLocFileTypes):
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
# Main Class Filoc
# -------------------
class Filoc(RawFiloc):
    # noinspection PyDefaultArgument
    def __init__(
            self,
            locpath                : str,
            file_type              : PresetFiLocFileTypes = 'json',
            writable               : bool                 = False,
            content_reader         : Optional[FileReader] = None,
            content_writer         : Optional[FileWriter] = None,
            content_reader_options : Dict[str, Any]       = {'mode': 'r'},
            content_writer_options : Dict[str, Any]       = {'mode': 'w'},
            cache_locpath          : str                  = None,
            timestamp_col          : str                  = None,
    ):
        """
        if cache_locpath is relative, then it will be relative to result_locpath
        """
        super().__init__(locpath, writable=writable)

        # reader and writer
        self.file_type = file_type
        default_reader, default_writer = (None, None)
        if file_type and (content_reader is None or content_writer is None):
            default_reader, default_writer = _get_content_reader_writer(file_type)
        self.content_reader         = content_reader if content_reader else default_reader if file_type else None  # type:Optional[FileReader]
        self.content_writer         = content_writer if content_writer else default_writer if file_type else None  # type:Optional[FileWriter]
        self.content_reader_options = content_reader_options  # type:Dict[str, Any]
        self.content_writer_options = content_writer_options  # type:Dict[str, Any]

        # cache loc
        self.cache_loc = None
        if cache_locpath is not None:
            if not os.path.isabs(cache_locpath):
                cache_locpath = self.root_folder + '/' + cache_locpath
            self.cache_loc = RawFiloc(cache_locpath, writable=True)
        self.timestamp_col = timestamp_col
        # cache_timestamp_col
        if self.cache_loc is None:
            self.cache_timestamp_col = None
        elif timestamp_col is None:
            self.cache_timestamp_col = 'timestamp'
        else:
            self.cache_timestamp_col = timestamp_col

    def invalidate_cache(self, path_props : Optional[Dict[str, Any]] = None, **path_props_kwargs):
        if self.cache_loc is None:
            return
        path_props = mix_dicts(path_props, path_props_kwargs)
        self.cache_loc.delete(path_props)

    def read_content(self, path_props : Optional[Dict[str, Any]] = None, **path_props_kwargs) -> List[Dict[str, Any]]:
        path_props = mix_dicts(path_props, path_props_kwargs)
        path = self.get_path(path_props)  # validates, that pat_props points to a single file
        contents = self.read_contents(path_props)
        if len(contents) == 0:
            raise ValueError(f'No keyvalues found for path props {path_props} (path={path})')
        return contents[0]

    def read_contents(self, path_props : Optional[Dict[str, Any]] = None, **path_props_kwargs) -> List[Dict[str, Any]]:
        path_props = mix_dicts(path_props, path_props_kwargs)
        result = []
        for (props, values) in self._read_file_content_by_path_props(path_props).items():
            entry = OrderedDict(values)
            entry.update(props)  # path properties have precedence!
            result.append(entry)
        return result

    def _read_file_content_by_path_props(self, path_props : Optional[Dict[str, Any]] = None, **path_props_kwargs) -> Dict[Dict[str, Any], Dict[str, Any]]:
        path_props = mix_dicts(path_props, path_props_kwargs)
        result = OrderedDict()

        cache_props_and_cache = None  # type:Optional[Tuple[Dict[str, Any]], Dict[Dict[str, Any], Dict[str, Any]]]
        paths_and_path_props  = self.find_paths_and_path_props(path_props)
        log.info(f'Found {len(paths_and_path_props)} files to read in locpath {self.locpath} with path_props {json.dumps(path_props)}')
        for (f_path, f_props) in paths_and_path_props:
            f_props_hashable = frozendict(f_props.items())

            try:
                f_timestamp = self.fs.modified(f_path)
            except FileNotFoundError:
                f_timestamp = None
                
            # renew cache, on cache file change
            if self.cache_loc:
                f_cache_path  = self.cache_loc.get_path(f_props)
                f_cache_props = self.cache_loc.get_path_properties(f_cache_path)

                if cache_props_and_cache is not None and cache_props_and_cache[0] == f_cache_props:
                    pass  # do nothing, keep this cache file opened
                else:
                    if cache_props_and_cache is not None and cache_props_and_cache[0] != f_cache_props:
                        # flush previous cache, if exists
                        if cache_props_and_cache[0]:
                            with self.cache_loc.open(cache_props_and_cache[0], 'wb') as f:
                                _default_cache_writer(cache_props_and_cache[1], f)

                    # now prepare new cache
                    if self.cache_loc.exists(f_cache_props):
                        with self.cache_loc.open(f_cache_props, 'rb') as f:
                            cache_props_and_cache = (f_cache_props, _default_cache_reader(f))
                    else:
                        cache_props_and_cache = (f_cache_props, OrderedDict())

            # check whether cache entry is still valid
            if self.cache_loc:
                if f_props_hashable in cache_props_and_cache[1]:
                    cached_entry = cache_props_and_cache[1][f_props_hashable]  # type: Dict[str, Any]
                    cached_entry_timestamp = cached_entry[self.cache_timestamp_col]
                    if cached_entry_timestamp == f_timestamp:
                        print(f'File analysis cached: {f_path}')
                        log.info(f'File analysis cached: {f_path}')
                        result[f_props_hashable] = cached_entry.copy()  # copy from cache
                        continue
                    else:
                        print(f'Cache out of date for {f_path}')
                        log.info(f'Cache out of date for {f_path}')

            # cache is not valid: read file!
            f_keyvalues = OrderedDict()
            # -> timestamps
            if self.cache_timestamp_col:
                f_keyvalues[self.cache_timestamp_col] = f_timestamp
            if self.timestamp_col:
                f_keyvalues[self.timestamp_col] = f_timestamp
            # -> custom report
            if self.content_reader is not None:
                f_keyvalues.update(self._read_single_file(f_path, f_props))

            # add to result and cache
            result[f_props_hashable] = f_keyvalues
            if self.cache_loc:
                cache_props_and_cache[1][f_props_hashable] = f_keyvalues.copy()  # copy to cache

        # flush last used cache
        if cache_props_and_cache:
            with self.cache_loc.open(cache_props_and_cache[0], 'wb') as f:
                _default_cache_writer(cache_props_and_cache[1], f)

        if self.cache_timestamp_col != self.timestamp_col:
            for f_keyvalues in result.values():
                if self.cache_timestamp_col in f_keyvalues:
                    del f_keyvalues[self.cache_timestamp_col]

        return result

    def write_content(self, keyvalues : Dict[str, Any], dry_run=False):
        if not self.writable:
            raise UnsupportedOperation('this filoc is not writable. Set writable flag to True to enable writing')
        self.write_contents([keyvalues], dry_run=dry_run)

    def write_contents(self, keyvalues_list : List[Dict[str, Any]], dry_run=False):
        if not self.writable:
            raise UnsupportedOperation('this filoc is not writable. Set writable flag to True to enable writing')
        recorded_row_id_by_path_props    = {}
        recorded_keyvalues_by_path_props = {}
        for row_id, keyvalues in enumerate(keyvalues_list):
            f_path_props, timestamp, cache_timestamp, f_keyvalues = self._split_keyvalues(keyvalues)
            f_path_props_hashable = frozendict(f_path_props)

            # avoid saving file if no changes
            if self.exists(f_path_props):
                old_keyvalues = self.read_content(f_path_props)
                if old_keyvalues == keyvalues:
                    log.info(f'Row {row_id}: No difference detected: do nothing')
                    continue

            log.info(f'Row {row_id}: Difference detected: update values')

            # validate multiple changes consistency
            if f_path_props_hashable in recorded_keyvalues_by_path_props:
                recorded_keyvalues = recorded_keyvalues_by_path_props[f_path_props_hashable]
                if recorded_keyvalues != f_keyvalues:
                    other_row_id = recorded_row_id_by_path_props[f_path_props_hashable]
                    raise ValueError(f'Rows {row_id} and {other_row_id} need to be saved to the same file, but have different keyvalues:\n{row_id}: {f_keyvalues}\n{other_row_id}: {recorded_keyvalues}')
                else:
                    continue  # do nothing, as change has already been recorded

            # record change to perform and notice row_id (last is for error message)
            recorded_row_id_by_path_props[f_path_props_hashable]    = row_id
            recorded_keyvalues_by_path_props[f_path_props_hashable] = f_keyvalues

        for f_path_props, keyvalues in recorded_keyvalues_by_path_props.items():
            self.invalidate_cache(f_path_props)
            path = self.get_path(f_path_props)
            with self.open(f_path_props, **self.content_writer_options) as f:
                if dry_run:
                    log.info(f'(dry_run) Save to {path}: {json.dumps(keyvalues)}')
                else:
                    log.info(f'Save to {path}: {json.dumps(keyvalues)}')
                    self.content_writer(keyvalues, f)

    def _split_keyvalues(self, keyvalues):
        path_props      = {}
        timestamp       = None
        cache_timestamp = None
        file_keyvalues  = OrderedDict()
        for (k, v) in keyvalues.items():
            if k in self.path_props:
                path_props[k] = v
            elif k == self.timestamp_col:
                timestamp = keyvalues[k]
            elif k == self.cache_timestamp_col:
                cache_timestamp = keyvalues[k]
            else:
                file_keyvalues[k] = v
        return path_props, timestamp, cache_timestamp, file_keyvalues

    def _read_single_file(self, f_path, f_props):
        len_params = len(getfullargspec(self.content_reader).args)
        if len_params == 1:
            log.info(f'Analyzing file {f_path}')
            with self.fs.open(f_path, **self.content_reader_options) as f:
                custom_report = self.content_reader(f)
            log.info(f'Analyzed file {f_path}')
        elif len_params == 2:
            log.info(f'Analyzing file {f_path}')
            with self.fs.open(f_path, **self.content_reader_options) as f:
                custom_report = self.content_reader(f, f_props)
            log.info(f'Analyzed file {f_path}')
        else:
            raise ValueError(f'Provided content_reader has {len_params} parameters, allowed signature: f(path) or f(path, path_props)')
        return custom_report

    def _write_single_file(self, f_keyvalues : Dict[str, Any], f_path : str, f_path_props : Dict[str, Any]):
        len_params = len(getfullargspec(self.content_writer).args)
        if len_params == 1:
            log.info(f'Analyzing file {f_path}')
            with self.fs.open(f_path, **self.content_reader_options) as f:
                custom_report = self.content_writer(f_keyvalues, f)
            log.info(f'Analyzed file {f_path}')
        elif len_params == 2:
            log.info(f'Analyzing file {f_path}')
            with self.fs.open(f_path, **self.content_reader_options) as f:
                custom_report = self.content_writer(f_keyvalues, f_path_props, f)
            log.info(f'Analyzed file {f_path}')
        else:
            raise ValueError(f'Provided content_writer requires {len_params} positional parameters, allowed signature: f(path) or f(path, path_props)')
        return custom_report
