""" github protocol test """
import os
import shutil
import tempfile
import unittest
from pathlib import Path

from filoc import filoc
from filoc import utils

# noinspection PyMissingOrEmptyDocstring
def touch_file(file_path):
    os.makedirs(os.path.dirname(file_path))
    Path(file_path).touch()


# noinspection PyMissingOrEmptyDocstring,PyPep8Naming
class TestUtils(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.test_dir = tempfile.mkdtemp().replace('\\', '/')

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test__combine_list_None_None(self):
        r = utils._combine_list(None, None)
        self.assertEqual([], r)

    def test__combine_list_None_ok(self):
        r = utils._combine_list(None, [1, 2])
        self.assertEqual([1, 2], r)

    def test__combine_list_ok_None(self):
        r = utils._combine_list([1, 2], None)
        self.assertEqual([1, 2], r)

    def test__combine_list_ok_ok(self):
        r = utils._combine_list([1, 2], [3, 4])
        self.assertEqual([1, 2, 3, 4], r)
