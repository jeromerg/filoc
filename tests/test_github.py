""" github protocol test """
import os
import shutil
import tempfile
import unittest
from pathlib import Path

from filoc import filoc


# noinspection PyMissingOrEmptyDocstring
def touch_file(file_path):
    os.makedirs(os.path.dirname(file_path))
    Path(file_path).touch()


# noinspection PyMissingOrEmptyDocstring,PyPep8Naming
class Test_fs_github(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.test_dir = tempfile.mkdtemp().replace('\\', '/')

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_read_contents(self):
        loc = filoc('github://CSSEGISandData:COVID-19@/csse_covid_19_data/csse_covid_19_daily_reports/{date_str}.csv', backend='csv', singleton=False)
        loc.read_contents(date_str='01-27-2020')
