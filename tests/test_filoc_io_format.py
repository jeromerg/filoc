from datetime import datetime
import os
import shutil
import tempfile
import unittest
from io import UnsupportedOperation
from pathlib import Path

from filoc.filoc_io import FilocIO


def touch_file(file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    Path(file_path).touch()


# noinspection DuplicatedCode
# noinspection PyMissingOrEmptyDocstring
class TestFilocIO_format(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.test_dir = tempfile.mkdtemp().replace('\\', '/')

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_str(self):
        locpath = self.test_dir + r'/{value}'
        loc = FilocIO(locpath)
        path = loc.render_path(value='abc')
        self.assertEqual(path, rf"{self.test_dir}/abc")
        touch_file(path)
        p = loc.list_paths_and_props(abc='abc')
        self.assertListEqual(p, [ (rf"{self.test_dir}/abc", {'value': 'abc'})])

    def test_int(self):
        locpath = self.test_dir + r'/{value:d}'
        loc = FilocIO(locpath)
        path = loc.render_path(value=123)
        self.assertEqual(path, rf"{self.test_dir}/123")
        touch_file(path)
        p = loc.list_paths_and_props(value=123)
        self.assertListEqual(p, [ (rf"{self.test_dir}/123", {'value': 123})])

    def test_float(self):
        locpath = self.test_dir + r'/{value:g}'
        loc = FilocIO(locpath)
        path = loc.render_path(value=3.5)
        self.assertEqual(path, rf"{self.test_dir}/3.5")
        touch_file(path)
        p = loc.list_paths_and_props(value=3.5)
        self.assertListEqual(p, [ (rf"{self.test_dir}/3.5", {'value': 3.5})])


if __name__ == '__main__':
    unittest.main()
