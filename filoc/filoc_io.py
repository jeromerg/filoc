"""
This module contains the FilocIO class, used to work with files defined by a locpath: a path containing format placeholders.
"""
import datetime
import logging
import os
import re
import uuid
from io import UnsupportedOperation
from typing import Dict, Any, List, Mapping, Optional, Set
from typing import Tuple

import fsspec
import pandas as pd

from filoc.fmt_parser import FmtParser
from fsspec import AbstractFileSystem
from fsspec.core import OpenFile

from filoc.contract import Constraints, Constraint, MetaOptions
from filoc.typing_utils import KeyValueProtocol

log = logging.getLogger('filoc')

# ---------
# Constants
# ---------
_re_natural           = re.compile(r"(\d+)")
_re_path_placeholder  = re.compile(r'({[^}]+})')


# -------
# Helpers
# -------
def natural_sort_key(s: str) -> Tuple[Any, ...]:
    """ Return a tuple of string and int, to be used as key for natural sort. Floating number are currently supported but cannot be compared to integers (missing dot separator)"""
    # TODO: support mix of int and float
    return [int(part) if part.isdigit() else part for part in _re_natural.split(s)]


def sort_natural(li: List[str]) -> List[str]:
    """ Perform natural sort of string containing numbers. Floating number are currently supported but cannot be compared to integers (missing dot separator)"""
    return sorted(li, key=natural_sort_key)


def coerce_nullable_mapping(d) -> Optional[Mapping[str, Any]]:
    """
    Pass through Mapping or None instance, tries to call ``to_dict()`` method elsewhere
    Args:
        d:

    Returns:
        the coerced mapping of type ``Mapping[str, Any]`` or None
    Raises:
        TypeError: if ``d`` is neither an instance of Mapping, nor None, nor contains `to_dict()` method
    """
    if d is None:
        return d
    if isinstance(d, Mapping):
        return d
    if getattr(d, "to_dict", None):
        # especially valid for pandas Series
        return d.to_dict()
    raise TypeError(f"Expected instance of Mapping or implementing to_dict, got {type(d)}!")


def mix_dicts_and_coerce(dict1, dict2) -> Mapping[str, Any]:
    """
    Coerce dict1 and dict2 to Mapping and mix both together
    Args:
        dict1: first dictionary argument (either Mapping or implements `to_dict()`)
        dict2: second dictionary argument (either Mapping or implements `to_dict()`)

    Returns:
        The combined Mapping
    """
    dict1 = coerce_nullable_mapping(dict1)
    dict2 = coerce_nullable_mapping(dict2)

    dict2 = None if len(dict2) == 0 else dict2
    if dict1 and dict2:
        result = dict()
        result.update(dict1)
        result.update(dict2)
        return result
    elif dict1:
        return dict1
    elif dict2:
        return dict2
    else:
        return dict()


def get_meta_mapping(meta_options: MetaOptions) -> Optional[Dict[str, str]]:
    if meta_options is None or meta_options is False:
        return dict()
    elif meta_options is True:
        return None
    elif isinstance(meta_options, str):
        return {meta_options: meta_options}
    elif isinstance(meta_options, list):
        return {meta_option: meta_option for meta_option in meta_options}
    elif isinstance(meta_options, dict):
        return meta_options
    else:
        raise ValueError(f'Unsupported meta_options type: {type(meta_options)}')


def map_meta(meta_mapping: Optional[Dict[str, str]], meta: Dict[str, Any]) -> Dict[str, Any]:
    meta_flat = pd.json_normalize(meta).to_dict(orient='records')[0]
    if meta_mapping is None:
        return meta_flat
    else:
        return {name: meta_flat.get(original_name, None) for name, original_name in meta_mapping.items()}


def jsonify_detail(d):
    if isinstance(d, datetime.datetime):
        return d.isoformat()
    elif isinstance(d, datetime.date):
        return d.isoformat()
    elif isinstance(d, datetime.time):
        return d.isoformat()
    elif isinstance(d, (str, int, float, bool, type(None))):
        return d
    elif isinstance(d, bytearray):
        return d.hex()
    elif isinstance(d, bytes):
        return d.hex()
    elif isinstance(d, (Mapping, KeyValueProtocol)):
        return {k: jsonify_detail(v) for k, v in zip(d.keys(), d.values())}
    elif isinstance(d, (list, tuple)):
        return [jsonify_detail(v) for v in d]
    else:
        return str(d)

# TODO: Support path character escaping


# -------------------
# Class FilocIO
# -------------------

class FilocIO:
    """
    Class providing access to files, given the locpath definition. The locpath is a format string, which placeholders
    are variables used by FilocIO to map variables to paths and paths to variables.
    """
    def __init__(
        self, locpath: str, 
        writable: bool = False, 
        fs: AbstractFileSystem = None
    ) -> None:
        super().__init__()
        self._original_locpath = locpath
        self._writable  = writable

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

        # now build the normalized locpath, by replacing ersatz string by the original placeholder strings
        self._locpath = open_file.path
        for elt, ersatz in path_elts_and_ersatz:
            if ersatz:
                self._locpath = self._locpath.replace(ersatz, elt)

        self._fs = open_file.fs  # type: AbstractFileSystem
        self._path_parser = FmtParser(self._locpath)

        # Get the root folder: the last folder, that is not variable
        self._root_folder = self._locpath.split("{")[0] 
        self._root_folder = self.fs.sep.join((self._root_folder + "dummy_to_ensure_subfolder").split(self.fs.sep)[:-1])  

        # parse library contains the _named_fields property, which provides us with the set of placeholder names
        # noinspection PyProtectedMember
        self._path_props  = set(self._path_parser.field_names)

    @property
    def fs(self) -> AbstractFileSystem:
        return self._fs

    # noinspection PyDefaultArgument
    def parse_path_properties(self, path: str) -> Dict[str, Any]:
        """
        Extract the ``self.locpath`` placeholder values contained in ``path``
        Args:
            path: path string

        Returns:
            A dictionary containing the "placeholder name" -> value mapping
        """
        try:
            return self._path_parser.parse(path)
        except Exception as e:
            raise ValueError(f'Could not parse {path} with {self._locpath} parser: {e}')

    def render_path(self, constraints : Optional[Constraints] = None, **constraints_kwargs : Constraint) -> str:
        """
        Render the path defined by the provided placeholder values (``constraints``).
        Args:
            constraints: The placeholders values required by ``locpath``.
            **constraints_kwargs: The placeholders values required by ``locpath``.

        Returns:
            The rendered path
        Raises:
            ValueError: If a placeholder value is missing
        """
        constraints = mix_dicts_and_coerce(constraints, constraints_kwargs)
        undefined_keys = self._path_props - set(constraints)

        if len(undefined_keys) > 0:
            raise ValueError('Required props undefined: {}. Provided: {}'.format(undefined_keys, constraints))
        return self._locpath.format(**constraints)  # result should be normalized, because locpath is

    def render_glob_path(self, constraints : Optional[Constraints] = None, **constraints_kwargs : Constraint) -> str:
        """
        Render a glob path defined by the provided placeholder values (``constraints``). The missing missing placeholders
        are replaced by ``*`` in the glob path.
        Args:
            constraints: The placeholders values defined in ``locpath``.
            **constraints_kwargs: The placeholders values defined in ``locpath``.

        Returns:
            A glob path
        """
        constraints = mix_dicts_and_coerce(constraints, constraints_kwargs)
        provided_keys = set(constraints)

        undefined_keys = self._path_props - provided_keys
        defined_keys = self._path_props - undefined_keys

        path_values = dict()
        path_values.update({(k, constraints[k]) for k in defined_keys})

        glob_path = self._locpath
        for undefined_key in undefined_keys:
            # replace `{undefined_key:any_optional_formatting}` by `*`
            glob_path = re.sub(r'{' + undefined_key + r'(?::[^}]*)?}', '*', glob_path)

        # finally format
        glob_path = glob_path.format(**path_values)
        return glob_path  # result should be normalized, because locpath is

    def list_paths(self, constraints : Optional[Constraints] = None, **constraints_kwargs : Constraint) -> List[str]:
        """
        Gets the list of all existing and valid paths fulfilling the provided constraints
        Args:
            constraints: The equality constraints applied to the ``locpath`` placeholders
            **constraints_kwargs: The equality constraints applied to the ``locpath`` placeholders

        Returns:
            The list of valid and existing paths fulfilling the provided constraints
        """
        constraints = mix_dicts_and_coerce(constraints, constraints_kwargs)
        return sort_natural(self.fs.glob(self.render_glob_path(constraints)))

    def list_paths_and_props(self, constraints : Optional[Constraints] = None, **constraints_kwargs : Constraint) -> List[Tuple[str, Dict[str, Any]]]:
        """
        Gets the list of all existing and valid paths fulfilling the provided constraints, along with the list of associated placeholder values
        Args:
            constraints: The equality constraints applied to the ``locpath`` placeholders
            **constraints_kwargs: The equality constraints applied to the ``locpath`` placeholders

        Returns:
            A list of tuples containing for each valid path, the path and the list of related placeholder values
        """
        constraints = mix_dicts_and_coerce(constraints, constraints_kwargs)
        paths = sort_natural(self.fs.glob(self.render_glob_path(constraints)))
        return [(p, self.parse_path_properties(p)) for p in paths]

    def list_paths_and_props_and_meta(
        self,
        constraints : Optional[Constraints] = None,
        meta: MetaOptions = True,
        **constraints_kwargs : Constraint
    ) -> List[Tuple[str, Dict[str, Any], Dict[str, Any]]]:
        """
        Gets the list of all existing and valid paths fulfilling the provided constraints, along with the list of associated placeholder values,
             along with the detail of the path as provided by the underlying fsspec filesystem (e.g. size, type, etc.)
        Args:
            constraints: The equality constraints applied to the ``locpath`` placeholders
            meta:
                Default: True. Adds file metadata as property/column to the result.
                If None or False, no metadata is added.
                If True: all metadata are added (flattened with pandas normalize function).
                If a string or a list of strings: only the metadata with the given keys are added.
                If a mapping: A key is the resulting name and the value is the original metadata key.
            **constraints_kwargs: The equality constraints applied to the ``locpath`` placeholders

        Returns:
            A list of tuples containing for each valid path, the path and the list of related placeholder values
        """
        meta_mapping = get_meta_mapping(meta)
        constraints = mix_dicts_and_coerce(constraints, constraints_kwargs)
        detail_by_path = self.fs.glob(self.render_glob_path(constraints), detail=True)
        detail_by_path = {p: jsonify_detail(d) for p, d in detail_by_path.items()}
        result = [(path, self.parse_path_properties(path), map_meta(meta_mapping, detail)) for path, detail in detail_by_path.items()]
        return sorted(result, key=lambda x: natural_sort_key(x[0]))

    def exists(self, constraints : Optional[Constraints] = None, **constraints_kwargs : Constraint) -> bool:
        """
        Checks if the path defined by the provided placeholder values (``constraints``) exists
        Args:
            constraints: The equality constraints applied to the ``locpath`` placeholders
            **constraints_kwargs: The equality constraints applied to the ``locpath`` placeholders

        Returns:
            True if the path exists, False elsewhere
        """
        constraints = mix_dicts_and_coerce(constraints, constraints_kwargs)
        return self.fs.exists(self.render_path(constraints))

    def open(
            self,
            constraints   : Constraints,
            mode          : str = "rb",
            block_size    : int = None,
            cache_options : Optional[Dict] = None,
            **kwargs
    ):
        """
        Opens the path defined by the provided placeholder values (``constraints``)

        Args:
            constraints: The equality constraints applied to the ``locpath`` placeholders
            mode: See builtin ``open()``
            block_size: Some indication of buffering - this is a value in bytes
            cache_options: Extra arguments to pass through to the cache.
            **kwargs: Additional keyed arguments passed to ``fsspec.OpenFile.open(...)``

        Returns:
            a file-like object from the underlying fsspec filesystem
        """
        is_writing = len(set(mode) & set("wa+")) > 0
        if is_writing and not self._writable:
            raise UnsupportedOperation('this filoc is not writable. Set writable flag to True to enable writing')

        path = self.render_path(constraints)

        dirname = os.path.dirname(path)

        if is_writing:
            self.fs.makedirs(dirname, exist_ok=True)

        return self.fs.open(path, mode, block_size, cache_options, **kwargs)

    # noinspection PyDefaultArgument
    def delete(self, constraints : Optional[Constraints] = {}, dry_run=False):
        """
        Delete the path defined by the provided placeholder values (``constraints``)

        Args:
            constraints: The equality constraints applied to the ``locpath`` placeholders
            dry_run: If True, only simulates the deletion
        """

        # TODO: Unit test to test deletion of folders

        if not self._writable:
            raise UnsupportedOperation('this filoc is not writable. Set writable flag to True to enable deleting')

        path_to_delete = self.list_paths(constraints)

        dry_run_log_prefix = '(dry_run) ' if dry_run else ''
        log.info(f'{dry_run_log_prefix}Deleting {len(path_to_delete)} files with path_props "{constraints}"')
        for path in path_to_delete:
            log.info(f'{dry_run_log_prefix}Deleting "{path}"')
            if dry_run:
                continue
            if self.fs.isfile(path):
                self.fs.delete(path)
            elif self.fs.isdir(path):
                self.fs.rm(path, recursive=True)
            else:
                raise ValueError(f'path is neither a direction nor a file: "{path}"')
        log.info(f'{dry_run_log_prefix}Deleted {len(path_to_delete)} files with path_props "{constraints}"')
