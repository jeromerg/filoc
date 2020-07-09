import itertools
import logging
import os
import re
from collections import OrderedDict
from io import UnsupportedOperation
from typing import Dict, Any, List, Optional, Set
from typing import Tuple

import fsspec
import parse
from fsspec import AbstractFileSystem

log = logging.getLogger('rawfiloc')

# ---------
# Constants
# ---------
_improbable_string    = "o=NvZ_ps$"
_re_improbable_string = re.compile(r'(o=NvZ_ps\$)')
_re_natural           = re.compile(r"(\d+)")
_re_path_placeholder  = re.compile(r'({[^}]+})')


# -------
# Helpers
# -------
def sort_natural(li: List[str]) -> List[str]:
    # todo: support float too
    return sorted(li, key=lambda s: [int(part) if part.isdigit() else part for part in _re_natural.split(s)])


def mix_dicts(dict1, dict2):
    dict2 = None if len(dict2) == 0 else dict2
    if dict1 and dict2:
        return OrderedDict([dict1, dict2])
    elif dict1:
        return dict1
    elif dict2:
        return dict2
    else:
        return {}


# -------------------
# Base Class RawFiloc
# -------------------
class RawFiloc:
    def __init__(self, locpath: str, writable=False) -> None:
        super().__init__()
        self.locpath     = locpath
        self.writable    = writable
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
        self.path_props  = set(self.path_parser._named_fields)  # type: Set[str]

    # noinspection PyDefaultArgument
    def get_path_properties(self, path: str) -> Dict[str, Any]:
        try:
            return self.path_parser.parse(path).named
        except Exception as e:
            raise ValueError(f'Could not parse {path} with {self.locpath} parser: {e}')

    def get_path(self, path_props : Optional[Dict[str, Any]] = None, **path_props_kwargs : Any) -> str:
        path_props = mix_dicts(path_props, path_props_kwargs)
        undefined_keys = self.path_props - set(path_props)
        if len(undefined_keys) > 0:
            raise ValueError('Required props undefined: {}. Provided: {}'.format(undefined_keys, path_props))
        return self.locpath.format(**path_props)  # result should be normalized, because locpath is

    def get_glob_path(self, path_props : Optional[Dict[str, Any]] = None, **path_props_kwargs : Any) -> str:
        path_props = mix_dicts(path_props, path_props_kwargs)
        provided_keys = set(path_props)
        undefined_keys = self.path_props - provided_keys
        defined_keys = self.path_props - undefined_keys

        path_values = OrderedDict()
        path_values.update({(k, path_props[k]) for k in defined_keys})

        glob_path = self.locpath
        for undefined_key in undefined_keys:
            glob_path = re.sub(r'{' + undefined_key + r'(?::[^}]*)?}', '?*', glob_path)

        # finally format
        glob_path = glob_path.format(**path_values)
        return glob_path  # result should be normalized, because locpath is

    def find_paths(self, path_props : Optional[Dict[str, Any]] = None, **path_props_kwargs : Any) -> List[str]:
        path_props = mix_dicts(path_props, path_props_kwargs)
        paths = self.fs.glob(self.get_glob_path(path_props))
        return sort_natural(paths)

    def find_paths_and_path_props(self, path_props : Optional[Dict[str, Any]] = None, **path_props_kwargs : Any) -> List[Tuple[str, List[str]]]:
        path_props = mix_dicts(path_props, path_props_kwargs)
        paths = self.find_paths(path_props)
        return [(p, self.get_path_properties(p)) for p in paths]

    def exists(self, path_props : Optional[Dict[str, Any]] = None, **path_props_kwargs : Any) -> bool:
        path_props = mix_dicts(path_props, path_props_kwargs)
        return self.fs.exists(self.get_path(path_props))

    def open(self, path_props : Dict[str, Any], mode="rb", block_size=None, cache_options=None, **kwargs):
        is_writing = len(set(mode) & set("wa+"))
        if is_writing and not self.writable:
            raise UnsupportedOperation('this filoc is not writable. Set writable flag to True to enable writing')

        path = self.get_path(path_props)
        dirname = os.path.dirname(path)

        if not self.fs.exists(dirname) and is_writing > 0:
            self.fs.makedirs(dirname)

        return self.fs.open(path, mode, block_size, cache_options, **kwargs)

    # noinspection PyDefaultArgument
    def delete(self, path_props : Optional[Dict[str, Any]] = {}, dry_run= False):
        if not self.writable:
            raise UnsupportedOperation('this filoc is not writable. Set writable flag to True to enable deleting')

        path_to_delete = self.find_paths(path_props)
        log.info(f'(dry_run) Deleting {len(path_to_delete)} files with path_props "{path_props}"')
        for path in path_to_delete:
            if dry_run:
                log.info(f'(dry_run) Deleting "{path}"')
            else:
                log.info(f'(dry_run) Deleting "{path}"')
                self.fs.delete(path)
        log.info(f'(dry_run) Deleted {len(path_to_delete)} files with path_props "{path_props}"')

    def __eq__(self, other):
        if other is not self:
            return False
        return self.locpath == other.locpath

    def __hash__(self):
        return self.locpath.__hash__()
