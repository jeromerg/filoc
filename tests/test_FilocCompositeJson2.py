import json
import shutil
import tempfile
from typing import Any, List
import unittest

# noinspection DuplicatedCode
from filoc import filoc_json


# noinspection PyMissingOrEmptyDocstring
class TestMultiloc_TwoLevels(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp().replace('\\', '/')

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_read(self):
        node_loc = filoc_json(f'{self.test_dir}/{{node_id:d}}/node.json', writable=True)
        leaf_loc = filoc_json(f'{self.test_dir}/{{node_id:d}}/{{leaf_id:d}}/leaf.json', writable=True)
        sut_loc = filoc_json({
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

        result_tree = pivot(result, "shared.node_id", "shared.leaf_id")
        result_tree_txt = json.dumps(result_tree, sort_keys=True)
        print(result_tree_txt)

        self.maxDiff = 2000
        self.assertEqual(result_tree_txt, 
            '{'
            '"1": {'
              '"1": {"leaf.value": 0.1, "node.value": "A", "shared.leaf_id": 1, "shared.node_id": 1}, '
              '"2": {"leaf.value": 0.2, "node.value": "A", "shared.leaf_id": 2, "shared.node_id": 1}'
            '}, '
            '"2": {'
              '"1": {"leaf.value": 1.1, "node.value": "B", "shared.leaf_id": 1, "shared.node_id": 2}, '
              '"2": {"leaf.value": 1.2, "node.value": "B", "shared.leaf_id": 2, "shared.node_id": 2}'
              '}'
            '}'
        )

    def test_2filocs_with_reversed_key_order(self):
        leaf1_loc = filoc_json(f'{self.test_dir}/{{leaf_id:d}}/{{node_id:d}}/leaf1.json', writable=True)
        leaf2_loc = filoc_json(f'{self.test_dir}/{{node_id:d}}/{{leaf_id:d}}/leaf2.json', writable=True)
        sut_loc = filoc_json({ 'leaf1' : leaf1_loc, 'leaf2' : leaf2_loc })

        leaf1_loc.write_content( {'node_id': 1, 'leaf_id': 1, 'value' : 0.1})
        leaf1_loc.write_content( {'node_id': 1, 'leaf_id': 2, 'value' : 0.2})
        leaf1_loc.write_content( {'node_id': 2, 'leaf_id': 1, 'value' : 1.1})
        leaf1_loc.write_content( {'node_id': 2, 'leaf_id': 2, 'value' : 1.2})
        leaf2_loc.write_content( {'node_id': 1, 'leaf_id': 1, 'value' : 1.0})
        leaf2_loc.write_content( {'node_id': 1, 'leaf_id': 2, 'value' : 2.0})
        leaf2_loc.write_content( {'node_id': 2, 'leaf_id': 1, 'value' : 11.0})
        leaf2_loc.write_content( {'node_id': 2, 'leaf_id': 2, 'value' : 12.0})

        result = sut_loc.read_contents()

        result_tree = pivot(result, "shared.node_id", "shared.leaf_id")
        result_tree_txt = json.dumps(result_tree, sort_keys=True)
        print(result_tree_txt)

        self.maxDiff = 2000
        self.assertEqual(result_tree_txt, 
            '{'
            '"1": {'
              '"1": {"leaf1.value": 0.1, "leaf2.value": 1.0, "shared.leaf_id": 1, "shared.node_id": 1}, '
              '"2": {"leaf1.value": 0.2, "leaf2.value": 2.0, "shared.leaf_id": 2, "shared.node_id": 1}'
            '}, '
            '"2": {'
              '"1": {"leaf1.value": 1.1, "leaf2.value": 11.0, "shared.leaf_id": 1, "shared.node_id": 2}, '
              '"2": {"leaf1.value": 1.2, "leaf2.value": 12.0, "shared.leaf_id": 2, "shared.node_id": 2}'
              '}'
            '}'
        )

    def test_2filocs_with_unrelated_placeholder_keys(self):
        leaf1_loc = filoc_json(f'{self.test_dir}/1/{{A}}/{{B}}/leaf1.json', writable=True)
        leaf2_loc = filoc_json(f'{self.test_dir}/2/{{C}}/{{D}}/leaf2.json', writable=True)
        sut_loc = filoc_json({ 'AB' : leaf1_loc, 'CD' : leaf2_loc })

        leaf1_loc.write_content( {'A': 1  , 'B':   10, 'V' : 0.1})
        leaf1_loc.write_content( {'A': 1  , 'B':   20, 'V' : 0.2})
        leaf2_loc.write_content( {'C': 100, 'D': 1000, 'V' : 1.0})
        leaf2_loc.write_content( {'C': 100, 'D': 2000, 'V' : 2.0})

        result = sut_loc.read_contents()

        result_tree = pivot(result, "shared.A", "shared.B", "shared.C", "shared.D")
        result_tree_txt = json.dumps(result_tree, sort_keys=True)
        print(result_tree_txt)

        self.maxDiff = 2000
        self.assertEqual(result_tree_txt, '{"1": {"10": {"100": {"1000": {"AB.V": 0.1, "CD.V": 1.0, "shared.A": "1", "shared.B": "10", "shared.C": "100", "shared.D": "1000"}, "2000": {"AB.V": 0.1, "CD.V": 2.0, "shared.A": "1", "shared.B": "10", "shared.C": "100", "shared.D": "2000"}}}, "20": {"100": {"1000": {"AB.V": 0.2, "CD.V": 1.0, "shared.A": "1", "shared.B": "20", "shared.C": "100", "shared.D": "1000"}, "2000": {"AB.V": 0.2, "CD.V": 2.0, "shared.A": "1", "shared.B": "20", "shared.C": "100", "shared.D": "2000"}}}}}')

    def test_2filocs_second_is_empty(self):
        leaf1_loc = filoc_json(f'{self.test_dir}/1/{{A}}/{{B}}/leaf1.json', writable=True)
        leaf2_loc = filoc_json(f'{self.test_dir}/2/{{C}}/{{D}}/leaf2.json', writable=True)
        sut_loc = filoc_json({ 'AB' : leaf1_loc, 'CD' : leaf2_loc })

        leaf2_loc.write_content( {'C': 100, 'D': 1000, 'V' : 1.0})
        leaf2_loc.write_content( {'C': 100, 'D': 2000, 'V' : 2.0})

        result = sut_loc.read_contents()

        result_tree = pivot(result, "shared.C", "shared.D")
        result_tree_txt = json.dumps(result_tree, sort_keys=True)
        print(result_tree_txt)

        self.maxDiff = 2000
        self.assertEqual(result_tree_txt, '{"100": {"1000": {"CD.V": 1.0, "shared.C": "100", "shared.D": "1000"}, "2000": {"CD.V": 2.0, "shared.C": "100", "shared.D": "2000"}}}')

    def test_2filocs_first_is_empty(self):
        leaf1_loc = filoc_json(f'{self.test_dir}/1/{{A}}/{{B}}/leaf1.json', writable=True)
        leaf2_loc = filoc_json(f'{self.test_dir}/2/{{C}}/{{D}}/leaf2.json', writable=True)
        sut_loc = filoc_json({ 'AB' : leaf1_loc, 'CD' : leaf2_loc })

        leaf1_loc.write_content( {'A': 1  , 'B':   10, 'V' : 0.1})
        leaf1_loc.write_content( {'A': 1  , 'B':   20, 'V' : 0.2})

        result = sut_loc.read_contents()

        result_tree = pivot(result, "shared.A", "shared.B")
        result_tree_txt = json.dumps(result_tree, sort_keys=True)
        print(result_tree_txt)

        self.maxDiff = 2000
        self.assertEqual(result_tree_txt, '{"1": {"10": {"AB.V": 0.1, "shared.A": "1", "shared.B": "10"}, "20": {"AB.V": 0.2, "shared.A": "1", "shared.B": "20"}}}'
        )

    def test_2filocs_filter_on_first_locpath(self):
        leaf1_loc = filoc_json(f'{self.test_dir}/{{node_id:d}}/{{leaf_id:d}}/leaf1.json', writable=True)
        leaf2_loc = filoc_json(f'{self.test_dir}/{{node_id:d}}/{{leaf_id:d}}/leaf2.json', writable=True)
        sut_loc = filoc_json({ 'leaf1' : leaf1_loc, 'leaf2' : leaf2_loc })

        leaf1_loc.write_content( {'node_id': 1, 'leaf_id': 1, 'value' : 0.1})
        leaf1_loc.write_content( {'node_id': 1, 'leaf_id': 2, 'value' : 0.2})
        leaf1_loc.write_content( {'node_id': 2, 'leaf_id': 1, 'value' : 1.1})
        leaf1_loc.write_content( {'node_id': 2, 'leaf_id': 2, 'value' : 1.2})
        leaf2_loc.write_content( {'node_id': 1, 'leaf_id': 1, 'value' : 1.0})
        leaf2_loc.write_content( {'node_id': 1, 'leaf_id': 2, 'value' : 2.0})
        leaf2_loc.write_content( {'node_id': 2, 'leaf_id': 1, 'value' : 11.0})
        leaf2_loc.write_content( {'node_id': 2, 'leaf_id': 2, 'value' : 12.0})

        result = sut_loc.read_contents()

        result_tree = pivot(result, "shared.node_id", "shared.leaf_id")
        result_tree_txt = json.dumps(result_tree, sort_keys=True)
        print(result_tree_txt)

        self.maxDiff = 2000
        self.assertEqual(result_tree_txt, 
            '{'
            '"1": {'
              '"1": {"leaf1.value": 0.1, "leaf2.value": 1.0, "shared.leaf_id": 1, "shared.node_id": 1}, '
              '"2": {"leaf1.value": 0.2, "leaf2.value": 2.0, "shared.leaf_id": 2, "shared.node_id": 1}'
            '}, '
            '"2": {'
              '"1": {"leaf1.value": 1.1, "leaf2.value": 11.0, "shared.leaf_id": 1, "shared.node_id": 2}, '
              '"2": {"leaf1.value": 1.2, "leaf2.value": 12.0, "shared.leaf_id": 2, "shared.node_id": 2}'
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
