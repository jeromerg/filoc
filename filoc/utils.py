"""
Internal utilities
"""
import json
import logging
from typing import Dict, Any, Iterable, List, Set, Union, Mapping, Tuple

from filoc.contract import PropsList

log = logging.getLogger('filoc')

_MISSING_KEY = object()


# noinspection PyMissingOrEmptyDocstring
def filter_and_coerce_loaded_file_content(path, file_content, path_props, constraints, is_singleton):
    # validate file_content and coerce to list
    if is_singleton:
        if not isinstance(file_content, dict):
            raise ValueError(f"Invalid type for loaded file '{path}'. Expected dict, got {type(file_content).__name__}")
        unfiltered_result = [file_content]
    else:
        if not isinstance(file_content, list):
            raise ValueError(f"Invalid type for loaded file '{path}'. Expected list, got {type(file_content).__name__}")
        unfiltered_result = file_content

    # filter
    result = []
    for row in unfiltered_result:
        keep = True
        for constraint_key, constraint_value in constraints.items():
            if constraint_key in path_props:
                continue  # constraint is already fulfilled by the values of the path placeholders
            if constraint_key not in row:
                continue  # constraint does not apply to this row
            if row[constraint_key] != constraint_value:
                keep = False
                break
        if keep:
            result.append(row)
    return result


# noinspection PyMissingOrEmptyDocstring
def coerce_file_content_to_write(path, props_list : PropsList, is_singleton : bool) -> Union[Mapping[str, Any], list]:
    # validate file_content and coerce to list
    if is_singleton:
        if len(props_list) == 1:
            return props_list[0]
        elif len(props_list) == 0:
            raise ValueError('props_list is empty')
        else:
            first_item = props_list[0]
            for idx, item in enumerate(props_list[1:]):
                if first_item != item:
                    raise ValueError(f'Trying to save {len(props_list)} props to singleton file {path} with different values:\nItem #0: {json.dumps(first_item)}\nItem #{idx + 1}: {json.dumps(item)}')
    else:
        return props_list


# ---------------------
# Pivot Helpers
# ---------------------

Table = List[Dict[str, Any]]
Row   = Dict[str, Any]


# noinspection PyMissingOrEmptyDocstring
def merge_tables(
    table_by_name   : Mapping[str, Table], 
    join_keys       : Iterable[str], 
    separator       : str, 
    join_level_name : str
):
    prefixed_join_keys = set([f'{join_level_name}{separator}{k}' for k in join_keys])
        
    resulting_table = None
    for table_name, table in table_by_name.items():
        table = _prefix_table(table, join_keys, table_name, separator, join_level_name)
        resulting_table = table if resulting_table is None else _join(resulting_table, table, prefixed_join_keys)
    return resulting_table


def _prefix_table(
    table          : Table, 
    join_key_names : Iterable[str],
    table_name     : str,
    separator      : str,
    join_level_name: str,
):
    result = []
    for item in table:
        result.append({  f'{join_level_name}{separator}{k}' if k in join_key_names else f'{table_name}{separator}{k}' : v  for k, v in item.items()  })
    return result


def _join(
    table1    : Table, 
    table2    : Table, 
    join_keys : Set[str]
):
    # build index on the shortest list, to speed up
    if len(table1) < len(table2):
        table1, table2 = table2, table1

    table2_index_cache = {}
    result = []
    for row1 in table1:
        rows2 = _get_rows2(row1, table2, join_keys, table2_index_cache)
        for row2 in rows2:
            r = row1.copy()
            r.update(row2)
            result.append(r)
    return result


def _get_rows2(
    row1      : Row, 
    table2    : Table, 
    join_keys : Set[str], 
    table2_index_cache : Dict[Tuple[str], dict]
):
    defined_keys1       = tuple(sorted(row1.keys() & join_keys))
    defined_key_values1 = [row1.get(key, _MISSING_KEY) for key in defined_keys1]

    if defined_keys1 not in table2_index_cache:
        table2_index_cache[defined_keys1] = _build_index(defined_keys1, table2)
    table2_index = table2_index_cache[defined_keys1]
    return _get_index_matches_recursive(table2_index, defined_key_values1)


def _build_index(keys : Iterable[str], table : Table) -> dict:
    index = {}
    for row in table:
        key_values = [row.get(key, _MISSING_KEY) for key in keys]
        _put_to_index(index, row, key_values)
    return index


def _put_to_index(index : dict, v : Any, key_values : List[str]):
    d_curr = index
    for k in key_values[:-1]:
        if k not in d_curr:
            d_curr[k] = {}
        d_curr = d_curr[k]

    # last is a list
    k = key_values[-1]
    if k not in d_curr:
        d_curr[k] = []
    d_curr[k].append(v)


def _get_index_matches_recursive(index, key_values):
    key_value = key_values[0]

    r1, r2 = None, None
    if len(key_values) == 1:
        if key_value    in index: r1 = index[key_value]
        if _MISSING_KEY in index: r2 = index[_MISSING_KEY]
    else:
        next_keyvalues = key_values[1:]
        if key_value    in index: r1 = _get_index_matches_recursive(index[key_value]   , next_keyvalues)
        if _MISSING_KEY in index: r1 = _get_index_matches_recursive(index[_MISSING_KEY], next_keyvalues)
    return _combine_list(r1, r2)
            

def _combine_list(l1, l2):
    if l1:
        if l2: return l1 + l2
        else : return l1
    else:
        if l2: return l2
        else : return []
