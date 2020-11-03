import json
import shutil
import tempfile
from typing import Any, List
import unittest

# noinspection DuplicatedCode
from filoc import filoc_json_single, filoc_json_composite, FilocIO


# noinspection PyMissingOrEmptyDocstring
class TestMultiloc_TwoLevels(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp().replace('\\', '/')

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_read(self):
        node_loc = filoc_json_single(f'{self.test_dir}/{{node_id:d}}/node.json', writable=True)
        leaf_loc = filoc_json_single(f'{self.test_dir}/{{node_id:d}}/{{leaf_id:d}}/leaf.json', writable=True)
        sut_loc = filoc_json_composite({
            'node' : node_loc,
            'leaf' : leaf_loc,
        })

        node_loc.write_content( {'node_id': 1, 'value' : 'A'})
        node_loc.write_content( {'node_id': 2, 'value' : 'B'})
        leaf_loc.write_content( {'node_id': 1, 'leaf_id': 1, 'value' : 0.1})
        leaf_loc.write_content( {'node_id': 1, 'leaf_id': 2, 'value' : 0.2})
        leaf_loc.write_content( {'node_id': 2, 'leaf_id': 1, 'value' : 1.1})
        leaf_loc.write_content( {'node_id': 2, 'leaf_id': 2, 'value' : 1.2})

        result = sut_loc.read_contents()

        result_tree = pivot(result, "index.node_id", "index.leaf_id")
        result_tree_txt = json.dumps(result_tree, sort_keys=True)
        print(result_tree_txt)

        self.maxDiff = 2000
        self.assertEquals(result_tree_txt, 
            '{'
            '"1": {'
              '"1": {"index.leaf_id": 1, "index.node_id": 1, "leaf.value": 0.1, "node.value": "A"}, '
              '"2": {"index.leaf_id": 2, "index.node_id": 1, "leaf.value": 0.2, "node.value": "A"}'
            '}, '
            '"2": {'
              '"1": {"index.leaf_id": 1, "index.node_id": 2, "leaf.value": 1.1, "node.value": "B"}, '
              '"2": {"index.leaf_id": 2, "index.node_id": 2, "leaf.value": 1.2, "node.value": "B"}'
              '}'
            '}'
        )

    def test_2filocs_with_reversed_key_order(self):
        leaf1_loc = filoc_json_single(f'{self.test_dir}/{{leaf_id:d}}/{{node_id:d}}/leaf1.json', writable=True)
        leaf2_loc = filoc_json_single(f'{self.test_dir}/{{node_id:d}}/{{leaf_id:d}}/leaf2.json', writable=True)
        sut_loc = filoc_json_composite({ 'leaf1' : leaf1_loc, 'leaf2' : leaf2_loc })

        leaf1_loc.write_content( {'node_id': 1, 'leaf_id': 1, 'value' : 0.1})
        leaf1_loc.write_content( {'node_id': 1, 'leaf_id': 2, 'value' : 0.2})
        leaf1_loc.write_content( {'node_id': 2, 'leaf_id': 1, 'value' : 1.1})
        leaf1_loc.write_content( {'node_id': 2, 'leaf_id': 2, 'value' : 1.2})
        leaf2_loc.write_content( {'node_id': 1, 'leaf_id': 1, 'value' : 1.0})
        leaf2_loc.write_content( {'node_id': 1, 'leaf_id': 2, 'value' : 2.0})
        leaf2_loc.write_content( {'node_id': 2, 'leaf_id': 1, 'value' : 11.0})
        leaf2_loc.write_content( {'node_id': 2, 'leaf_id': 2, 'value' : 12.0})

        result = sut_loc.read_contents()

        result_tree = pivot(result, "index.node_id", "index.leaf_id")
        result_tree_txt = json.dumps(result_tree, sort_keys=True)
        print(result_tree_txt)

        self.maxDiff = 2000
        self.assertEquals(result_tree_txt, 
            '{'
            '"1": {'
              '"1": {"index.leaf_id": 1, "index.node_id": 1, "leaf1.value": 0.1, "leaf2.value": 1.0}, '
              '"2": {"index.leaf_id": 2, "index.node_id": 1, "leaf1.value": 0.2, "leaf2.value": 2.0}'
            '}, '
            '"2": {'
              '"1": {"index.leaf_id": 1, "index.node_id": 2, "leaf1.value": 1.1, "leaf2.value": 11.0}, '
              '"2": {"index.leaf_id": 2, "index.node_id": 2, "leaf1.value": 1.2, "leaf2.value": 12.0}'
              '}'
            '}'
        )

    def test_2filocs_with_unrelated_placeholder_keys(self):
        leaf1_loc = filoc_json_single(f'{self.test_dir}/1/{{A}}/{{B}}/leaf1.json', writable=True)
        leaf2_loc = filoc_json_single(f'{self.test_dir}/2/{{C}}/{{D}}/leaf2.json', writable=True)
        sut_loc = filoc_json_composite({ 'AB' : leaf1_loc, 'CD' : leaf2_loc })

        leaf1_loc.write_content( {'A': 1  , 'B':   10, 'V' : 0.1})
        leaf1_loc.write_content( {'A': 1  , 'B':   20, 'V' : 0.2})
        leaf2_loc.write_content( {'C': 100, 'D': 1000, 'V' : 1.0})
        leaf2_loc.write_content( {'C': 100, 'D': 2000, 'V' : 2.0})

        result = sut_loc.read_contents()

        result_tree = pivot(result, "index.A", "index.B", "index.C", "index.D")
        result_tree_txt = json.dumps(result_tree, sort_keys=True)
        print(result_tree_txt)

        self.maxDiff = 2000
        self.assertEquals(result_tree_txt, '{"1": {"10": {"100": {"1000": {"AB.V": 0.1, "CD.V": 1.0, "index.A": "1", "index.B": "10", "index.C": "100", "index.D": "1000"}, "2000": {"AB.V": 0.1, "CD.V": 2.0, "index.A": "1", "index.B": "10", "index.C": "100", "index.D": "2000"}}}, "20": {"100": {"1000": {"AB.V": 0.2, "CD.V": 1.0, "index.A": "1", "index.B": "20", "index.C": "100", "index.D": "1000"}, "2000": {"AB.V": 0.2, "CD.V": 2.0, "index.A": "1", "index.B": "20", "index.C": "100", "index.D": "2000"}}}}}'
        )

    def test_2filocs_filter_on_first_locpath(self):
        leaf1_loc = filoc_json_single(f'{self.test_dir}/{{node_id:d}}/{{leaf_id:d}}/leaf1.json', writable=True)
        leaf2_loc = filoc_json_single(f'{self.test_dir}/{{node_id:d}}/{{leaf_id:d}}/leaf2.json', writable=True)
        sut_loc = filoc_json_composite({ 'leaf1' : leaf1_loc, 'leaf2' : leaf2_loc })

        leaf1_loc.write_content( {'node_id': 1, 'leaf_id': 1, 'value' : 0.1})
        leaf1_loc.write_content( {'node_id': 1, 'leaf_id': 2, 'value' : 0.2})
        leaf1_loc.write_content( {'node_id': 2, 'leaf_id': 1, 'value' : 1.1})
        leaf1_loc.write_content( {'node_id': 2, 'leaf_id': 2, 'value' : 1.2})
        leaf2_loc.write_content( {'node_id': 1, 'leaf_id': 1, 'value' : 1.0})
        leaf2_loc.write_content( {'node_id': 1, 'leaf_id': 2, 'value' : 2.0})
        leaf2_loc.write_content( {'node_id': 2, 'leaf_id': 1, 'value' : 11.0})
        leaf2_loc.write_content( {'node_id': 2, 'leaf_id': 2, 'value' : 12.0})

        result = sut_loc.read_contents()

        result_tree = pivot(result, "index.node_id", "index.leaf_id")
        result_tree_txt = json.dumps(result_tree, sort_keys=True)
        print(result_tree_txt)

        self.maxDiff = 2000
        self.assertEquals(result_tree_txt, 
            '{'
            '"1": {'
              '"1": {"index.leaf_id": 1, "index.node_id": 1, "leaf1.value": 0.1, "leaf2.value": 1.0}, '
              '"2": {"index.leaf_id": 2, "index.node_id": 1, "leaf1.value": 0.2, "leaf2.value": 2.0}'
            '}, '
            '"2": {'
              '"1": {"index.leaf_id": 1, "index.node_id": 2, "leaf1.value": 1.1, "leaf2.value": 11.0}, '
              '"2": {"index.leaf_id": 2, "index.node_id": 2, "leaf1.value": 1.2, "leaf2.value": 12.0}'
              '}'
            '}'
        )

def pivot(l : List[dict], *keys):
    result = {}
    for item in l:
        path_values = [item[k] for k in keys]
        put_value(result, item, *path_values)
    return result


def put_value(d : dict, v : Any, *path_values : str):
    d_curr = d
    for k in path_values[:-1]:
        if k not in d_curr:
            d_curr[k] = {}
        d_curr = d_curr[k]
    d_curr[path_values[-1]] = v


if __name__ == '__main__':
    unittest.main()
