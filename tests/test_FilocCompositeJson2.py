import json
import shutil
import tempfile
import unittest

# noinspection DuplicatedCode
from filoc import filoc_json, FilocIO


class TestMultiloc_TwoLevels(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp().replace('\\', '/')
        self.node_loc = filoc_json(f'{self.test_dir}/{{node_id:d}}/node.json', writable=True)
        self.leaf_loc = filoc_json(f'{self.test_dir}/{{node_id:d}}/{{leaf_id:d}}/leaf.json', writable=True)
        self.sut_loc = filoc_json({
            'node' : self.node_loc,
            'leaf' : self.leaf_loc,
        })

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_1(self):
        self.node_loc.write_content( {'node_id': 1, 'value' : 'A'})
        self.node_loc.write_content( {'node_id': 2, 'value' : 'B'})
        self.leaf_loc.write_content( {'node_id': 1, 'leaf_id': 1, 'value' : 0.1})
        self.leaf_loc.write_content( {'node_id': 1, 'leaf_id': 2, 'value' : 0.2})
        self.leaf_loc.write_content( {'node_id': 2, 'leaf_id': 1, 'value' : 1.1})
        self.leaf_loc.write_content( {'node_id': 2, 'leaf_id': 2, 'value' : 1.2})

        result = self.sut_loc.read_contents()
        result_txt = json.dumps(result, sort_keys=True)
        print(result_txt)

        self.maxDiff = 2000
        self.assertEquals(result_txt, 
            '['
            '{"index.leaf_id": 1, "index.node_id": 1, "leaf.value": 0.1, "node.value": "A"}, '
            '{"index.leaf_id": 1, "index.node_id": 2, "leaf.value": 1.1, "node.value": "B"}, '
            '{"index.leaf_id": 2, "index.node_id": 1, "leaf.value": 0.2, "node.value": "A"}, '
            '{"index.leaf_id": 2, "index.node_id": 2, "leaf.value": 1.2, "node.value": "B"}'
            ']'
        )



if __name__ == '__main__':
    unittest.main()
