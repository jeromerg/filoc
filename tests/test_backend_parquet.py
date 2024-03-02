import json

import pandas as pd
import yaml
import shutil
import tempfile
import unittest
from pandas import DataFrame
from filoc import filoc


# noinspection PyMissingOrEmptyDocstring
class TestBackendParquet(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.test_dir = tempfile.mkdtemp().replace('\\', '/')
        self.file = self.test_dir + r'/myfile.yaml'

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_read_contents(self):
        data = {'a': 123, 'b': [1, 2, 3]}
        pd.DataFrame([data]).to_parquet(self.file)

        loc = filoc(self.file, backend='parquet')
        df = loc.read_contents()
        col_names = sorted(df.columns)
        self.assertEqual(col_names, ['a', 'b'])
        self.assertEqual(df.loc[0, 'a'], 123)

    def test_write_contents(self):
        data = [{'a': 123, 'b': [1, 2, 3]}]
        df = DataFrame(data)

        loc = filoc(self.file, backend='parquet', writable=True)
        loc.write_contents(df)

        with open(self.file, 'rb') as f:
            df = pd.read_parquet(f)
            json_content = df.to_json(orient='records')
            content = json.loads(json_content)

        self.assertEqual(json.dumps(content, sort_keys=True),
                         json.dumps(data, sort_keys=True))


if __name__ == '__main__':
    unittest.main()
