""" Filoc Core Implementation """
import json
import logging
import os
import pickle
import random
import socket
import threading
import time
from abc import ABC
from contextlib import contextmanager
from datetime import datetime
from io import UnsupportedOperation
from typing import Dict, Any, NamedTuple, Optional, Tuple, Set

from frozendict import frozendict
from fsspec.spec import AbstractFileSystem

from filoc.contract import TContent, TContents, Constraints, Props, PropsList, Filoc, \
    FrontendContract, BackendContract, ReadOnlyProps, Constraint, ReadOnlyPropsList
from .filoc_io import FilocIO, mix_dicts_and_coerce
from .utils import merge_tables

log = logging.getLogger('filoc')


class LockException(Exception):
    """ Exception raised while trying to acquire a lock with ``Filoc.lock()`` after the count of defined attempts has been reached"""
    def __init__(self, *args):
        Exception.__init__(self, *args)


class _RunningCache(NamedTuple):
    key : Props  # key-values expected by cache_locpath
    cache_by_file_path_props : Dict[ReadOnlyProps, Dict[str, Any]]  # key-values expected by (this) locpath props


# TODO: Profile and apply intern(key) to reduce footprint of intermediate model dictionaries
# TODO Feature: optimistic locking for long editing (use timestamp_col)
# TODO Feature: Backup on write if file already exists (think of possibility to use locpath with $version and $timestamp placeholder)

# ---------
# FilocSingle
# ---------
class FilocSingle(Filoc[TContent, TContents], ABC):
    """ Filoc implementation for a single locpath """

    # noinspection PyDefaultArgument
    def __init__(
            self,
            locpath       : str                                  ,
            writable      : bool                                 ,
            frontend      : FrontendContract[TContent, TContents],
            backend       : BackendContract                      ,
            cache_locpath : str                                  ,
            cache_fs      : Optional[AbstractFileSystem]         ,
            timestamp_col : str                                  ,
            fs            : Optional[AbstractFileSystem]         ,
    ):
        """
        if cache_locpath is relative, then it will be relative to result_locpath
        """

        self.filoc_io      = FilocIO(locpath, writable=writable, fs=fs)
        self.frontend      = frontend
        self.backend       = backend

        # cache loc
        self.cache_loc = None
        if cache_locpath is not None:
            self.cache_loc = FilocIO(cache_locpath, writable=True, fs=cache_fs)
        self.timestamp_col = timestamp_col

    def list_paths(self, constraints : Optional[Constraints] = None, **constraints_kwargs : Constraint):
        """ See ``Filoc`` contract """
        constraints = mix_dicts_and_coerce(constraints, constraints_kwargs)
        return self.filoc_io.list_paths(constraints)

    @contextmanager
    def lock(self, attempt_count: int = 60, attempt_secs: float = 1.0):
        """ See ``Filoc`` contract """
        # remark: fsspec backends do not all support the 'x' exclusive mode, so we cannot use exclusive write mode to
        # synchronize the writing into a single lock file. So each call to `lock()` tries to write its own lock file
        # and check afterward, if he won the run, by checking the modified timestamp of all lock files. It assumes, that the
        # file system sets timestamps in the same order as it processes the files (TODO: Verify assumption on distributed file systems)

        lock_id, lock_file = self._get_my_lock_id_and_lock_file()
        for attempt in range(attempt_count):
            owning_lock_date_and_file = self._get_owning_lock_date_and_file()

            if owning_lock_date_and_file:
                if owning_lock_date_and_file[1].endswith(lock_id): 
                    yield lock_id
                    return
                else:
                    time.sleep(random.uniform(0.5 * attempt_secs, 1.5 * attempt_secs))
                    continue

            # else we try to acquire the lock
            self.filoc_io.fs.makedirs(self.filoc_io.root_folder, exist_ok=True)
            with self.filoc_io.fs.open(lock_file, 'w') as f:
                json.dump({
                    'host' : socket.gethostname(),
                    'pid' : os.getpid(),
                    'thread' : threading.get_ident(),
                }, f)

            try:
                owning_lock_date_and_file = self._get_owning_lock_date_and_file()
                if owning_lock_date_and_file and owning_lock_date_and_file[1].endswith(lock_id):
                    yield lock_id
                    return
            finally:
                # else either failed to acquire lock (concurrent won) or some error. We clean up and retry (loop)
                try:
                    self.filoc_io.fs.delete(lock_file)
                except FileNotFoundError:
                    log.warning(f"Lock file {lock_file} has been concurrently deleted (by self.lock_force_release()?). No need to remove it")        
        raise LockException(f"Failed to acquire the file lock after {attempt_count} attempts")

    def lock_info(self) -> Optional[Dict[str, Any]]:
        """ See ``Filoc`` contract """
        owning_lock_date_and_file = self._get_owning_lock_date_and_file()
        if owning_lock_date_and_file is None:
            return None

        try:
            with self.filoc_io.fs.open(owning_lock_date_and_file[1], 'r') as f:
                info = json.load(f)
            info['date'] = owning_lock_date_and_file[0]
            return info
        except FileNotFoundError:
            return None

    def lock_force_release(self):
        """ See ``Filoc`` contract """
        owning_lock_date_and_file = self._get_owning_lock_date_and_file()
        if owning_lock_date_and_file is None:
            log.info(f'No lock found')
            return

        lock_file = owning_lock_date_and_file[1]
        try:
            self.filoc_io.fs.delete(lock_file)
            log.warning(f'Forced releasing of lock file "{lock_file}"')
        except FileNotFoundError:
            return

    def _get_owning_lock_date_and_file(self) -> Optional[Tuple[datetime, str]]:
        lock_files = self.filoc_io.fs.glob(f'{self.filoc_io.root_folder}/.lock_*')
        if len(lock_files) == 0:
            return None
        
        oldest_date = None
        oldest_file = None
        for lock_file in lock_files:
            try: 
                date = self.filoc_io.fs.modified(lock_file)
            except FileNotFoundError: 
                continue
            if oldest_date is None or date < oldest_date:
                oldest_date = date
                oldest_file = lock_file
        if oldest_file is None:
            return None
        return oldest_date, oldest_file

    def _get_my_lock_id_and_lock_file(self):
        # build a lock id. Scope of lock is (process x thread x root_folder)
        host      = socket.gethostname()
        pid       = os.getpid()
        thread_id = threading.get_ident()
        lock_id   = f'{host}_{pid}_{thread_id}'
        lock_file = f'{self.filoc_io.root_folder}/.lock_{lock_id}'
        return lock_id, lock_file

    def invalidate_cache(self, constraints : Optional[Constraints] = None, **constraints_kwargs : Constraint):
        """ See ``Filoc`` contract """
        if self.cache_loc is None:
            return
        constraints = mix_dicts_and_coerce(constraints, constraints_kwargs)
        self.cache_loc.delete(constraints)

    def read_content(self, constraints : Optional[Constraints] = None, **constraints_kwargs : Constraint) -> TContent:
        """ See ``Filoc`` contract """
        constraints = mix_dicts_and_coerce(constraints, constraints_kwargs)
        props_list = self._read_props_list(constraints)
        return self.frontend.read_content(props_list)

    def read_contents(self, constraints : Optional[Constraints] = None, **constraints_kwargs : Constraint) -> TContents:
        """ See ``Filoc`` contract """
        constraints = mix_dicts_and_coerce(constraints, constraints_kwargs)
        props_list  = self._read_props_list(constraints)
        return self.frontend.read_contents(props_list)

    def _read_props_list(self, constraints : Optional[Constraints] = None, **constraints_kwargs : Constraint) -> PropsList:
        constraints = mix_dicts_and_coerce(constraints, constraints_kwargs)
        result = []

        running_cache = None  # type:Optional[_RunningCache]

        paths_and_file_path_props  = self.filoc_io.list_paths_and_props(constraints)
        log.info(f'Found {len(paths_and_file_path_props)} files to read in locpath {self.filoc_io.locpath} fulfilling props {constraints}')

        for (path, file_path_props) in paths_and_file_path_props:
            path_props_hashable = frozendict(file_path_props.items())

            try:
                f_timestamp = self.filoc_io.fs.modified(path)
            except NotImplementedError:
                # fsspec implementation, that do not implement modified, are assumed to be read-only (example: github)
                f_timestamp = None
            except FileNotFoundError:
                f_timestamp = None
                
            # renew cache, on cache file change
            if self.cache_loc:
                path_cache_path       = self.cache_loc.render_path(file_path_props)
                cache_file_path_props = self.cache_loc.parse_path_properties(path_cache_path)

                if running_cache is not None and running_cache.key == cache_file_path_props:
                    pass  # cache file is always the correct one : do nothing, keep this cache file opened
                else:
                    if running_cache is not None and running_cache.key != cache_file_path_props:
                        # flush previous cache, if exists
                        if running_cache.key:
                            with self.cache_loc.open(running_cache.key, 'wb') as f:
                                pickle.dump(running_cache.cache_by_file_path_props, f, )

                    # now prepare new cache file
                    if self.cache_loc.exists(cache_file_path_props):
                        with self.cache_loc.open(cache_file_path_props, 'rb') as f:
                            running_cache = _RunningCache(cache_file_path_props, pickle.load(f))
                    else:
                        running_cache = _RunningCache(cache_file_path_props, dict())

            # check whether cache entry is still valid
            if self.cache_loc:
                if path_props_hashable in running_cache.cache_by_file_path_props:
                    path_cached_entry = running_cache.cache_by_file_path_props[path_props_hashable]
                    if path_cached_entry['timestamp'] == f_timestamp:
                        log.info(f'Path data cached: "{path}"')
                        result.extend(path_cached_entry['props_list'].copy())  # copy from cache
                        continue
                    else:
                        log.info(f'Cache out of date for path "{path}"')

            # cache is not valid: read path directly!

            # props from reader
            props_list = self._read_path(path, file_path_props, constraints)    # type: PropsList

            # augment read props with additional external data
            for props in props_list:
                # -> file timestamp
                if self.timestamp_col:
                    props[self.timestamp_col] = f_timestamp
                # -> path_props
                props.update(file_path_props)

            # add to result
            result.extend(props_list)
            # add to cache
            if self.cache_loc:
                running_cache.cache_by_file_path_props[path_props_hashable] = { 'timestamp': f_timestamp, 'props_list' : props_list.copy()}

        # flush last used cache
        if running_cache:
            with self.cache_loc.open(running_cache.key, 'wb') as f:
                pickle.dump(running_cache.cache_by_file_path_props, f)

        return result

    def write_content(self, content : TContent, dry_run=False):
        """ See ``Filoc`` contract """
        if not self.filoc_io.writable:
            raise UnsupportedOperation('this filoc is not writable. Set writable flag to True to enable writing')
        
        props_list = self.frontend.write_content(content)
        self._write_props_list(props_list, dry_run=dry_run)

    def write_contents(self, contents : TContents, dry_run=False):
        """ See ``Filoc`` contract """
        if not self.filoc_io.writable:
            raise UnsupportedOperation('this filoc is not writable. Set writable flag to True to enable writing')

        props_list = self.frontend.write_contents(contents)
        self._write_props_list(props_list, dry_run=dry_run)

    def _write_props_list(self, props_list : ReadOnlyPropsList, dry_run=False):
        if not self.filoc_io.writable:
            raise UnsupportedOperation('this filoc is not writable. Set writable flag to True to enable writing')

        recorded_row_id_by_path_props    = {}
        recorded_row_other_props_by_path_props = {}
        for row_id, row_props in enumerate(props_list):
            path_props, row_other_props, row_timestamp = self._split_keyvalues(row_props)
            row_path_props_hashable = frozendict(path_props)

            if row_path_props_hashable not in recorded_row_other_props_by_path_props:
                recorded_row_other_props_by_path_props[row_path_props_hashable] = []
                recorded_row_id_by_path_props[row_path_props_hashable] = []

            # record change to perform and notice row_id (last is for error message)
            recorded_row_id_by_path_props[row_path_props_hashable].append(row_id)
            recorded_row_other_props_by_path_props[row_path_props_hashable].append(row_other_props)

        dry_run_log_prefix = '(dry_run) ' if dry_run else ''
        for path_props, other_props_list in recorded_row_other_props_by_path_props.items():
            self.invalidate_cache(path_props)
            path = self.filoc_io.render_path(path_props)

            log.info(f'{dry_run_log_prefix}Saving to {path}')
            if not dry_run:
                self.backend.write(self.filoc_io.fs, path, other_props_list)
            log.info(f'{dry_run_log_prefix}Saved {path}')

    def _split_keyvalues(self, keyvalues : ReadOnlyProps) -> Tuple[Props, Props, datetime]:
        path_props  = {}
        timestamp   = None
        other_props = dict()
        for (k, v) in keyvalues.items():
            if k in self.filoc_io.path_props:
                path_props[k] = v
            elif k == self.timestamp_col:
                timestamp = keyvalues[k]
            else:
                other_props[k] = v
        return path_props, other_props, timestamp

    def _read_path(self, path : str, path_props : Props, constraints : Constraints):
        log.info(f'Reading content for {path}')
        content = self.backend.read(self.filoc_io.fs, path, path_props, constraints)
        log.info(f'Read content for {path}')
        return content


# ------------------
# FilocCompositeBase
# ------------------
class FilocComposite(Filoc[TContent, TContents], ABC):
    """ Filoc implementation for composite filocs """

    # TODO: Implement remaining Filoc methods (lock() and co.)

    def __init__(
            self,
            filoc_by_name           : Dict[str, FilocSingle],
            frontend                : FrontendContract,
            join_level_name         : str,
            join_separator          : str,
    ):
        # Validate
        assert isinstance(filoc_by_name, dict)
        for filoc_name, filoc in filoc_by_name.items():
            if not isinstance(filoc, FilocSingle):
                raise ValueError(f'filoc {filoc_name} is not a FilocSingle instance. FilocComposite only support FilocSingle sub-filocs')

        self.frontend        = frontend
        self.filoc_by_name   = filoc_by_name
        self.join_level_name = join_level_name
        self.join_separator  = join_separator

        self.join_keys_by_filoc_name = {}  # type: Dict[str, Set[str]]
        for filoc_name, filoc in filoc_by_name.items():
            self.join_keys_by_filoc_name[filoc_name] = filoc.filoc_io.path_props

    def invalidate_cache(self, constraints : Optional[Constraints] = None, **constraints_kwargs : Constraint):
        """ see ``Filoc`` contract """
        constraints = mix_dicts_and_coerce(constraints, constraints_kwargs)
        for filoc in self.filoc_by_name.values():
            filoc.invalidate_cache(constraints)

    def read_content(self, constraints : Optional[Constraints] = None, **constraints_kwargs : Constraint) -> TContent:
        """ see ``Filoc`` contract """
        constraints = mix_dicts_and_coerce(constraints, constraints_kwargs)
        props_list = self._read_props_list(constraints)
        return self.frontend.read_content(props_list)

    def read_contents(self, constraints : Optional[Constraints] = None, **constraints_kwargs : Constraint) -> TContents:
        """ see ``Filoc`` contract """
        constraints = mix_dicts_and_coerce(constraints, constraints_kwargs)
        props_list = self._read_props_list(constraints)
        return self.frontend.read_contents(props_list)

    def _read_props_list(self, constraints : Optional[Constraints] = None, **constraints_kwargs : Constraint) -> PropsList:
        constraints = mix_dicts_and_coerce(constraints, constraints_kwargs)
        # collect
        props_list_by_filoc_name = {}
        for filoc_name, filoc in self.filoc_by_name.items():
            # noinspection PyProtectedMember
            props_list_by_filoc_name[filoc_name] = filoc._read_props_list(constraints)

        # join
        join_key_names = set()
        for filoc_join_key_names in self.join_keys_by_filoc_name.values():
            join_key_names = join_key_names | filoc_join_key_names
        return merge_tables(props_list_by_filoc_name, list(join_key_names), self.join_separator, self.join_level_name)

    def write_content(self, content : TContent, dry_run=False):
        """ see ``Filoc`` contract """
        props_list = self.frontend.write_content(content)
        self._write_props_list(props_list, dry_run=dry_run)

    def write_contents(self, contents : TContents, dry_run=False):
        """ see ``Filoc`` contract """
        props_list = self.frontend.write_contents(contents)
        self._write_props_list(props_list, dry_run=dry_run)

    def _write_props_list(self, props_list : ReadOnlyPropsList, dry_run=False):
        # prepare empty props_list by filoc_name
        props_list_by_filoc_name = {}
        props_list_by_filoc_name[self.join_level_name] = [ dict() for _ in range(len(props_list)) ]
        for filoc_name, filoc in self.filoc_by_name.items():
            if filoc.filoc_io.writable:
                props_list_by_filoc_name[filoc_name] = [ dict() for _ in range(len(props_list)) ]
            else:
                log.info(f'write operation skipped for "{filoc_name}" readonly Filoc')

        # then fill
        split_cache = {}
        for row_id, props in enumerate(props_list):
            for (k, v) in props.items():

                split_values = split_cache.get(k, None)
                if split_values is None:
                    split_values = k.split(self.join_separator, 1)
                    split_cache[k] = split_values

                filoc_name, prop_name = split_values

                if filoc_name not in props_list_by_filoc_name:
                    continue

                props_list_by_filoc_name[filoc_name][row_id][prop_name] = v

        # pop out join indexes and merge then row by row to filoc props
        indexes = props_list_by_filoc_name.pop(self.join_level_name)
        for filoc_name, filoc_props_list in props_list_by_filoc_name.items():
            join_keys = self.join_keys_by_filoc_name[filoc_name]
            for index, props in zip(indexes, filoc_props_list):
                # todo: iterate through join_keys instead of index and raise explicit exception if join_key missing?
                relevant_index = { k: v for (k, v) in index.items() if k in join_keys}
                props.update(relevant_index)

        # delegate writing to
        for filoc_name, filoc_props_list in props_list_by_filoc_name.items():
            filoc = self.filoc_by_name[filoc_name]
            # noinspection PyProtectedMember
            filoc._write_props_list(filoc_props_list, dry_run=dry_run)
