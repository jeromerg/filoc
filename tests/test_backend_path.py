import json
import shutil
import tempfile
import unittest

from pandas import DataFrame

from filoc import filoc


# noinspection PyMissingOrEmptyDocstring
class TestBackendPath(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.test_dir = tempfile.mkdtemp().replace('\\', '/')
        self.file = self.test_dir + r'/myfile.json'
        self.locpath = self.test_dir + r'/{filename_without_extension}.{ext}'

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_read_contents(self):
        data = {'a': 123, 'b': [1, 2, 3]}
        with open(self.file, mode='w') as f:
            json.dump(data, f)

        loc = filoc(self.locpath, backend='path', singleton=True)
        df = loc.read_contents()
        col_names = sorted(df.columns)
        self.assertEqual(col_names, ['ext', 'filename_without_extension'])
        self.assertEqual(df.loc[0, 'ext'], 'json')
        self.assertEqual(df.loc[0, 'filename_without_extension'], 'myfile')

    def test_write_contents(self):
        data = {'a': 123, 'b': [1, 2, 3]}
        df = DataFrame([data])

        loc = filoc(self.file, backend='path', singleton=True, writable=True)
        try:
            loc.write_contents(df)
            raise AssertionError("previous call expected to raise an error")
        except NotImplementedError:
            pass
