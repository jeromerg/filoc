import json
import copy
import logging
from collections import OrderedDict
from typing import Dict, Any, List, Union

from orderedset import OrderedSet

from filoc.contract import PropsList

log = logging.getLogger('filoc_utils')

_missing_key = object()


def filter_and_coerce_loaded_file_content(path, file_content, constraints, is_singleton):
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
            if constraint_key not in row:
                continue  # constraint does not apply to this row
            if row[constraint_key] != constraint_value:
                keep = False
                break
        if keep:
            result.append(row)
    return result


def coerce_file_content_to_write(path, props_list : PropsList, is_singleton : bool) -> Union[dict, list]:
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
def merge_tables(table_by_name : Dict[str, List[Dict[str, Any]]], join_key_names : List[str], separator : str, join_level_name : str):
    resulting_pivot = None
    for table_name, table_values in table_by_name.items():
        pi = _pivot(table_values, join_key_names, f'{table_name}{separator}')
        if resulting_pivot is None:
            resulting_pivot = pi
            continue
        else:
            _merge_pivots_recursive(resulting_pivot, pi, join_key_names)

    index_prefix = f"{join_level_name}{separator}"
    return _unpivot(resulting_pivot, join_key_names, index_prefix)


def _merge_pivots_recursive(pi1 : dict, pi2 : dict, remaining_key_names : List[str]):
    if len(remaining_key_names) == 0:
        pi1.update(pi2)
    else:
        cp1 = pi1.pop(_missing_key) if _missing_key in pi1 else OrderedDict()
        cp2 = pi2.pop(_missing_key) if _missing_key in pi2 else OrderedDict()
        all_keys = OrderedSet(pi1) | OrderedSet(pi2)

        if len(all_keys) == 0:
            _merge_pivots_recursive(cp1, cp2, remaining_key_names[1:])
            pi1[_missing_key] = cp1
        else:
            for k in all_keys:
                v1 = pi1[k] if k in pi1 else copy.deepcopy(cp1)
                v2 = pi2[k] if k in pi2 else copy.deepcopy(cp2)
                _merge_pivots_recursive(v1, v2, remaining_key_names[1:])
                pi1[k] = v1


def _pivot(table_values : List[Dict[str, Any]], key_names : List[str], prefix : str):
    result = OrderedDict()
    for item in table_values:
        path = []
        curr_level = result
        for key_name in key_names:
            key = item.pop(key_name, _missing_key)
            path.append(key)
            if key not in curr_level:
                curr_level[key] = OrderedDict()
            curr_level = curr_level[key]

        for (k, v) in item.items():
            curr_level[prefix + k] = v

    return result


def _unpivot(pivot, key_names: List[str], index_prefix : str):
    result = []
    _unpivot_recursive(pivot, OrderedDict(), key_names, index_prefix, result)
    return result


def _unpivot_recursive(pivot_node: Dict[str, Any], current_index: Dict[str, Any], remaining_key_names: List[str], index_prefix : str, result):
    if len(remaining_key_names) == 0:
        current_index.update(pivot_node)
        result.append(current_index)
    else:
        key_name = remaining_key_names[0]
        for key, sub_pivot in pivot_node.items():
            sub_index = current_index.copy()
            sub_index[index_prefix + key_name] = key
            _unpivot_recursive(sub_pivot, sub_index, remaining_key_names[1:], index_prefix, result)
