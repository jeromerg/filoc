import itertools
import logging
import os
import re
from collections import OrderedDict
from io import UnsupportedOperation
from typing import Dict, Any, List, Optional, Set
from typing import Tuple
from fsspec.core import OpenFile
import pandas as pd
import uuid
import fsspec
import parse
from fsspec import AbstractFileSystem

from filoc.contract import PropsConstraints

log = logging.getLogger('rawfiloc')

# ---------
# Constants
# ---------
_re_natural           = re.compile(r"(\d+)")
_re_path_placeholder  = re.compile(r'({[^}]+})')


# -------
# Helpers
# -------
def sort_natural(li: List[str]) -> List[str]:
    # todo: support float too
    return sorted(li, key=lambda s: [int(part) if part.isdigit() else part for part in _re_natural.split(s)])


def mix_dicts_and_coerce(dict1 : Dict[str, Any], dict2 : Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(dict1, pd.Series):
        dict1 = dict1.to_dict()
    if isinstance(dict2, pd.Series):
        dict2 = dict2.to_dict()

    dict2 = None if len(dict2) == 0 else dict2
    if dict1 and dict2:
        result = OrderedDict()
        result.update(dict1)
        result.update(dict2)
        return result
    elif dict1:
        return dict1
    elif dict2:
        return dict2
    else:
        return OrderedDict()


# TODO: Support path character escaping

# -------------------
# Class FilocIO
# -------------------
class FilocIO:
    def __init__(
        self, locpath: str, 
        writable: bool = False, 
        fs: AbstractFileSystem = None
    ) -> None:
        super().__init__()
        self.original_locpath = locpath
        self.writable  = writable

        # split locpath to distinguish placeholders from constant parts
        path_elts = _re_path_placeholder.split(locpath)

        # Normalize the input path, by creating an fsspec OpenFile, then by getting the path property, 
        # which is normalized. But the placeholders within the locpath are not valid, so we replace them by
        # a valid random string, build the OpenFile, get the normalized string, and replace the random
        # string by the original placeholders.
        path_elts_and_ersatz = [ (elt, str(uuid.uuid4()) if elt.startswith("{") else None) for elt in path_elts ]
        some_valid_path = "".join([ersatz if ersatz else elt for elt, ersatz in path_elts_and_ersatz])
        if fs is None:
            open_file = fsspec.open(some_valid_path)
        else:
            open_file = OpenFile(fs, some_valid_path)

        # now build the normlized locpath, by replacing erstz stirng by the original placeholder strings
        self.locpath = open_file.path
        for elt, ersatz in path_elts_and_ersatz:
            if ersatz:
                self.locpath = self.locpath.replace(ersatz, elt)

        self.fs = open_file.fs # type: AbstractFileSystem
        self.path_parser = parse.compile(self.locpath)  # type: parse.Parser

        # Get the root folder: the last folder, that is not variable
        self.root_folder = self.locpath.split("{")[0] 
        self.root_folder = self.fs.sep.join((self.root_folder + "dummy_to_ensure_subfolder").split(self.fs.sep)[:-1])  

        # parse library contains the _named_fields property, which provides us with the set of placeholder names
        self.path_props  = set(self.path_parser._named_fields)  # type: Set[str]

    # noinspection PyDefaultArgument
    def parse_path_properties(self, path: str) -> Dict[str, Any]:
        try:
            return self.path_parser.parse(path).named
        except Exception as e:
            raise ValueError(f'Could not parse {path} with {self.locpath} parser: {e}')

    def render_path(self, constraints : Optional[PropsConstraints] = None, **constraints_kwargs : Any) -> str:
        constraints = mix_dicts_and_coerce(constraints, constraints_kwargs)
        undefined_keys = self.path_props - set(constraints)

        if len(undefined_keys) > 0:
            raise ValueError('Required props undefined: {}. Provided: {}'.format(undefined_keys, constraints))
        return self.locpath.format(**constraints)  # result should be normalized, because locpath is

    def render_glob_path(self, constraints : Optional[PropsConstraints] = None, **constraints_kwargs : Any) -> str:
        constraints = mix_dicts_and_coerce(constraints, constraints_kwargs)
        provided_keys = set(constraints)

        undefined_keys = self.path_props - provided_keys
        defined_keys = self.path_props - undefined_keys

        path_values = OrderedDict()
        path_values.update({(k, constraints[k]) for k in defined_keys})

        glob_path = self.locpath
        for undefined_key in undefined_keys:
            glob_path = re.sub(r'{' + undefined_key + r'(?::[^}]*)?}', '?*', glob_path)

        # finally format
        glob_path = glob_path.format(**path_values)
        return glob_path  # result should be normalized, because locpath is

    def list_paths(self, constraints : Optional[PropsConstraints] = None, **constraints_kwargs : Any) -> List[str]:
        constraints = mix_dicts_and_coerce(constraints, constraints_kwargs)
        paths = self.fs.glob(self.render_glob_path(constraints))
        return sort_natural(paths)

    def list_paths_and_props(self, constraints : Optional[PropsConstraints] = None, **constraints_kwargs : Any) -> List[Tuple[str, Dict[str, Any]]]:
        constraints = mix_dicts_and_coerce(constraints, constraints_kwargs)
        paths = self.list_paths(constraints)
        return [(p, self.parse_path_properties(p)) for p in paths]

    def exists(self, constraints : Optional[PropsConstraints] = None, **constraints_kwargs : Any) -> bool:
        constraints = mix_dicts_and_coerce(constraints, constraints_kwargs)
        return self.fs.exists(self.render_path(constraints))

    def open(self, constraints : PropsConstraints, mode="rb", block_size=None, cache_options=None, **kwargs):
        is_writing = len(set(mode) & set("wa+")) > 0
        if is_writing and not self.writable:
            raise UnsupportedOperation('this filoc is not writable. Set writable flag to True to enable writing')

        path = self.render_path(constraints)

        dirname = os.path.dirname(path)

        if is_writing:
            self.fs.makedirs(dirname, exist_ok=True)

        return self.fs.open(path, mode, block_size, cache_options, **kwargs)

    # noinspection PyDefaultArgument
    def delete(self, constraints : Optional[PropsConstraints] = {}, dry_run=False):
        if not self.writable:
            raise UnsupportedOperation('this filoc is not writable. Set writable flag to True to enable deleting')

        path_to_delete = self.list_paths(constraints)

        dry_run_log_prefix = '(dry_run) ' if dry_run else ''
        log.info(f'{dry_run_log_prefix}Deleting {len(path_to_delete)} files with path_props "{constraints}"')
        for path in path_to_delete:
            log.info(f'{dry_run_log_prefix}Deleting "{path}"')
            if dry_run:
                continue
            self.fs.delete(path)
        log.info(f'{dry_run_log_prefix}Deleted {len(path_to_delete)} files with path_props "{constraints}"')

    def __eq__(self, other):
        if other is not self:
            return False
        return self.locpath == other.locpath

    def __hash__(self):
        return self.locpath.__hash__()
