import json
import yaml
import shutil
import tempfile
import unittest
from pandas import DataFrame
from filoc import filoc

# noinspection PyMissingOrEmptyDocstring
class TestBackendJson(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.test_dir = tempfile.mkdtemp().replace('\\', '/')
        self.file = self.test_dir + r'/myfile.json'

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_read_contents(self):
        data = {'a': 123, 'b': [1, 2, 3]}
        with open(self.file, mode='w') as f:
            json.dump(data, f)

        loc = filoc(self.file, backend='json', singleton=True)
        df = loc.read_contents()
        col_names = sorted(df.columns)
        self.assertEqual(col_names, ['a', 'b'])
        self.assertEqual(df.loc[0, 'a'], 123)

    def test_write_contents(self):
        data = {'a': 123, 'b': [1, 2, 3]}
        df = DataFrame([data])

        loc = filoc(self.file, backend='json', singleton=True, writable=True)
        df = loc.write_contents(df)

        with open(self.file) as f:
            content = json.load(f)

        self.assertEqual(json.dumps(content, sort_keys=True),
                         json.dumps(data, sort_keys=True))

    def test_read_content_with_more_than_one_row(self):
        with open(self.file, mode='w') as f:
            f.write("!$%& not a json file")

        loc = filoc(self.file, backend='json', singleton=True)
        try:
            p = loc.read_content()
            raise AssertionError("previous call expected to raise an error")
        except ValueError:
            pass


if __name__ == '__main__':
    unittest.main()
