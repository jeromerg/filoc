import json
import shutil
import tempfile
import unittest

from pandas import DataFrame

from filoc import filoc


# noinspection PyMissingOrEmptyDocstring
class TestBackendCsv(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.test_dir = tempfile.mkdtemp().replace('\\', '/')
        self.file = self.test_dir + r'/myfile.csv'

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_read_contents(self):
        data = {'a': 123, 'b': 'coucou'}
        with open(self.file, mode='w') as f:
            f.write('a,b\n123,coucou')

        loc = filoc(self.file, backend='csv', singleton=True)
        df = loc.read_contents()
        col_names = sorted(df.columns)
        self.assertEqual(col_names, ['a', 'b'])
        self.assertEqual(df.loc[0, 'a'], '123')
        self.assertEqual(df.loc[0, 'b'], 'coucou')

    def test_write_contents(self):
        data = {'a': 123, 'b': 'coucou'}
        df = DataFrame([data])

        loc = filoc(self.file, backend='csv', singleton=True, writable=True)
        df = loc.write_contents(df)

        with open(self.file) as f:
            content = f.read()
        rows = content.split('\n')
        data = list(filter(None, [list(filter(None, r.split(','))) for r in rows]))
        self.assertEqual([['a', 'b'], ['123', 'coucou']], data)

    def test_read_file_with_bom(self):
        with open(self.file, mode='w', encoding='utf-8-sig') as f:
            f.writelines(['A,B\n1,2\n3,4'])

        loc = filoc(self.file, writable=True, backend='csv', singleton=False, encoding='utf-8-sig')
        df = loc.read_contents()
        col_name = df.columns[0]
        self.assertEqual(col_name, 'A')

    def test_read_file_no_bom(self):
        with open(self.file, mode='w', encoding='utf-8') as f:
            f.writelines(['A,B\n1,2\n3,4'])

        loc = filoc(self.file, writable=True, backend='csv', singleton=False, encoding='utf-8')
        df = loc.read_contents()
        col_name = df.columns[0]
        self.assertEqual(col_name, 'A')

if __name__ == '__main__':
    unittest.main()
