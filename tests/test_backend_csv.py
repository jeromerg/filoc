import json
from logging import exception
import os
import shutil
import tempfile
from threading import Thread
import time
import unittest
from pathlib import Path

from filoc import filoc

class TestBackendCsv(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.test_dir = tempfile.mkdtemp().replace('\\', '/')
        self.file = self.test_dir + r'/myfile.json'

    def tearDown(self):
        shutil.rmtree(self.test_dir)

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
