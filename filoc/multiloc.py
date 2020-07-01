import logging
from collections import OrderedDict
from typing import Dict, Any, List, Optional, Set, Tuple

from .filoc import Filoc
from .rawfiloc import mix_dicts

log = logging.getLogger('multiloc')


class Multiloc:
    def __init__(self, filoc_by_name : Dict[str, Filoc], **filoc_by_name_kwargs : Filoc):
        self.filoc_by_name = mix_dicts(filoc_by_name, filoc_by_name_kwargs)  # type:Dict[str, Filoc]
        self.path_props_prefix = 'path_props'

    def read_contents(self, path_props : Optional[Dict[str, Any]] = None, **path_props_kwargs) -> List[Dict[Tuple[str, str], Any]]:
        path_props = mix_dicts(path_props, path_props_kwargs)
        return self.read_contents_and_keys(path_props)[0]

    def read_contents_df(self, path_props : Optional[Dict[str, Any]] = None, **path_props_kwargs):
        import pandas  # pandas is not a required dependency of filoc
        path_props = mix_dicts(path_props, path_props_kwargs)
        contents, keys = self.read_contents_and_keys(path_props)
        return pandas.DataFrame(contents, columns=keys)

    def read_contents_and_keys(self, path_props : Optional[Dict[str, Any]] = None, **path_props_kwargs) -> Tuple[List[Dict[Tuple[str, str], Any]], Tuple[str, str]]:
        path_props = mix_dicts(path_props, path_props_kwargs)
        # collect
        all_path_props_combinations_set = set()             # type:Set[Dict[str, Any]]
        all_path_props_combinations_in_order = []           # type:List[Dict[str, Any]]
        keyvalues_by_path_props_by_filoc_name = OrderedDict()  # type:OrderedDict[str, Dict[Dict[str, Any], Dict[str, Any]]]
        for filoc_name, filoc in self.filoc_by_name.items():
            # noinspection PyProtectedMember
            keyvalues_by_path_props = filoc.get_file_content_by_path_props(path_props)
            keyvalues_by_path_props_by_filoc_name[filoc_name] = keyvalues_by_path_props
            for props in keyvalues_by_path_props:
                if props not in all_path_props_combinations_set:
                    all_path_props_combinations_set.add(props)
                    all_path_props_combinations_in_order.append(props)

        # outer join
        result_keys = OrderedDict()  # OrderedSet
        result = []
        for path_props in all_path_props_combinations_in_order:
            result_row = OrderedDict()
            result.append(result_row)

            # add path_props
            for property_name, property_value in path_props.items():
                hierarchical_key = (self.path_props_prefix, property_name)
                result_row[hierarchical_key] = property_value
                result_keys[hierarchical_key] = True

            # add all key values for each filoc
            for filoc_name, keyvalues_by_path_props in keyvalues_by_path_props_by_filoc_name.items():
                keyvalues = keyvalues_by_path_props.get(path_props, None)
                if keyvalues:
                    for key, value in keyvalues.items():
                        hierarchical_key = (filoc_name, key)
                        result_row[hierarchical_key] = value
                        result_keys[hierarchical_key] = True
        return result

    def save_contents(self, keyvalues_list : List[Dict[str, Any]], dry_run=False):
        # split keyvalues_list
        # ... first create empty placeholder vor key values
        keyvalues_list_by_filoc_name = {}
        for filoc_name, filoc in self.filoc_by_name.items():
            if filoc.writer is None:
                continue  # don't write non writable filocs
            filoc_keyvalues_list = []
            keyvalues_list_by_filoc_name[filoc_name] = filoc_keyvalues_list
            for i in range(len(keyvalues_list)):
                filoc_keyvalues_list.append(OrderedDict())

        # ... then fill
        for row_id, keyvalues in enumerate(keyvalues_list):
            for key, value in keyvalues.items():
                filoc_name  = key[0]
                content_key = key[1]
                if filoc_name not in keyvalues_list_by_filoc_name:
                    continue
                filoc_row_keyvalues = keyvalues_list_by_filoc_name[filoc_name][row_id]
                filoc_row_keyvalues[content_key] = value

        # delegate writing to
        for filoc_name, filoc_keyvalues_list in keyvalues_list_by_filoc_name.items():
            filoc = self.filoc_by_name[filoc_name]
            filoc.save_contents(filoc_keyvalues_list, dry_run=dry_run)
