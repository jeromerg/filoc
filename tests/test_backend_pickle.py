import json
import pickle
import shutil
import tempfile
import unittest
from pandas import DataFrame
from filoc import filoc

# noinspection PyMissingOrEmptyDocstring
class TestBackendPickle(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.test_dir = tempfile.mkdtemp().replace('\\', '/')
        self.file = self.test_dir + r'/myfile.pickle'

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_read_contents(self):
        data = {'a': 123, 'b': [1, 2, 3]}
        with open(self.file, mode='wb') as f:
            pickle.dump(data, f)

        loc = filoc(self.file, backend='pickle', singleton=True)
        df = loc.read_contents()
        col_names = sorted(df.columns)
        self.assertEqual(col_names, ['a', 'b'])
        self.assertEqual(df.loc[0, 'a'], 123)

    def test_write_contents(self):
        data = {'a': 123, 'b': [1, 2, 3]}
        df = DataFrame([data])

        loc = filoc(self.file, backend='pickle', singleton=True, writable=True)
        df = loc.write_contents(df)

        with open(self.file, 'rb') as f:
            content = pickle.load(f)

        self.assertEqual(json.dumps(content, sort_keys=True),
                         json.dumps(data, sort_keys=True))


if __name__ == '__main__':
    unittest.main()
