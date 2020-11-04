"""
This module contains the FilocIO class, used to work with files defined by a locpath: a path containing format placeholders.
"""
import logging
import os
import re
import uuid
from io import UnsupportedOperation
from typing import Dict, Any, List, Mapping, Optional, Set
from typing import Tuple

import fsspec
import parse
from fsspec import AbstractFileSystem
from fsspec.core import OpenFile

from filoc.contract import Constraints, Constraint

log = logging.getLogger('filoc')

# ---------
# Constants
# ---------
_re_natural           = re.compile(r"(\d+)")
_re_path_placeholder  = re.compile(r'({[^}]+})')


# -------
# Helpers
# -------
def sort_natural(li: List[str]) -> List[str]:
    """ Perform natural sort of string containing numbers. Floating number are currently supported but cannot be compared to integers (missing dot separator)"""
    # TODO: support mix of int and float
    return sorted(li, key=lambda s: [int(part) if part.isdigit() else part for part in _re_natural.split(s)])


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

        # now build the normalized locpath, by replacing ersatz string by the original placeholder strings
        self.locpath = open_file.path
        for elt, ersatz in path_elts_and_ersatz:
            if ersatz:
                self.locpath = self.locpath.replace(ersatz, elt)

        self.fs = open_file.fs  # type: AbstractFileSystem
        self.path_parser = parse.compile(self.locpath)  # type: parse.Parser

        # Get the root folder: the last folder, that is not variable
        self.root_folder = self.locpath.split("{")[0] 
        self.root_folder = self.fs.sep.join((self.root_folder + "dummy_to_ensure_subfolder").split(self.fs.sep)[:-1])  

        # parse library contains the _named_fields property, which provides us with the set of placeholder names
        # noinspection PyProtectedMember
        self.path_props  = set(self.path_parser._named_fields)  # type: Set[str]

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
            return self.path_parser.parse(path).named
        except Exception as e:
            raise ValueError(f'Could not parse {path} with {self.locpath} parser: {e}')

    def render_path(self, constraints : Optional[Constraints] = None, **constraints_kwargs : Constraint) -> str:
        """
        Render the path defined by the provided placeholder values (``constraints``).
        Args:
            constraints: The placeholders values required by ``self.locpath``.
            **constraints_kwargs: The placeholders values required by ``self.locpath``.

        Returns:
            The rendered path
        Raises:
            ValueError: If a placeholder value is missing
        """
        constraints = mix_dicts_and_coerce(constraints, constraints_kwargs)
        undefined_keys = self.path_props - set(constraints)

        if len(undefined_keys) > 0:
            raise ValueError('Required props undefined: {}. Provided: {}'.format(undefined_keys, constraints))
        return self.locpath.format(**constraints)  # result should be normalized, because locpath is

    def render_glob_path(self, constraints : Optional[Constraints] = None, **constraints_kwargs : Constraint) -> str:
        """
        Render a glob path defined by the provided placeholder values (``constraints``). The missing missing placeholders
        are replaced by ``*`` in the glob path.
        Args:
            constraints: The placeholders values defined in ``self.locpath``.
            **constraints_kwargs: The placeholders values defined in ``self.locpath``.

        Returns:
            A glob path
        """
        constraints = mix_dicts_and_coerce(constraints, constraints_kwargs)
        provided_keys = set(constraints)

        undefined_keys = self.path_props - provided_keys
        defined_keys = self.path_props - undefined_keys

        path_values = dict()
        path_values.update({(k, constraints[k]) for k in defined_keys})

        glob_path = self.locpath
        for undefined_key in undefined_keys:
            glob_path = re.sub(r'{' + undefined_key + r'(?::[^}]*)?}', '?*', glob_path)

        # finally format
        glob_path = glob_path.format(**path_values)
        return glob_path  # result should be normalized, because locpath is

    def list_paths(self, constraints : Optional[Constraints] = None, **constraints_kwargs : Constraint) -> List[str]:
        """
        Gets the list of all existing and valid paths fulfilling the provided constraints
        Args:
            constraints: The equality constraints applied to the ``self.locpath`` placeholders
            **constraints_kwargs: The equality constraints applied to the ``self.locpath`` placeholders

        Returns:
            The list of valid and existing paths fulfilling the provided constraints
        """
        constraints = mix_dicts_and_coerce(constraints, constraints_kwargs)
        paths = self.fs.glob(self.render_glob_path(constraints))
        return sort_natural(paths)

    def list_paths_and_props(self, constraints : Optional[Constraints] = None, **constraints_kwargs : Constraint) -> List[Tuple[str, Dict[str, Any]]]:
        """
        Gets the list of all existing and valid paths fulfilling the provided constraints, along with the list of associated placeholder values
        Args:
            constraints: The equality constraints applied to the ``self.locpath`` placeholders
            **constraints_kwargs: The equality constraints applied to the ``self.locpath`` placeholders

        Returns:
            A list of tuples containing for each valid path, the path and the list of related placeholder values
        """
        constraints = mix_dicts_and_coerce(constraints, constraints_kwargs)
        paths = self.list_paths(constraints)
        return [(p, self.parse_path_properties(p)) for p in paths]

    def exists(self, constraints : Optional[Constraints] = None, **constraints_kwargs : Constraint) -> bool:
        """
        Checks if the path defined by the provided placeholder values (``constraints``) exists
        Args:
            constraints: The equality constraints applied to the ``self.locpath`` placeholders
            **constraints_kwargs: The equality constraints applied to the ``self.locpath`` placeholders

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
            constraints: The equality constraints applied to the ``self.locpath`` placeholders
            mode: See builtin ``open()``
            block_size: Some indication of buffering - this is a value in bytes
            cache_options: Extra arguments to pass through to the cache.
            **kwargs: Additional keyed arguments passed to ``fsspec.OpenFile.open(...)``

        Returns:
            a file-like object from the underlying fsspec filesystem
        """
        is_writing = len(set(mode) & set("wa+")) > 0
        if is_writing and not self.writable:
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
            constraints: The equality constraints applied to the ``self.locpath`` placeholders
            dry_run: If True, only simulates the deletion
        """

        # TODO: Unit test to test deletion of folders

        if not self.writable:
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
