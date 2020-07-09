import json
import os
import shutil
import tempfile
import unittest
from io import UnsupportedOperation
from pathlib import Path

from filoc.rawfiloc import RawFiloc


def touch_file(file_path):
    os.makedirs(os.path.dirname(file_path))
    Path(file_path).touch()


# noinspection DuplicatedCode
class TestRawFiloc(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.test_dir = tempfile.mkdtemp().replace('\\', '/')
        self.path_fmt = self.test_dir + r'/simid={simid:d}/epid={epid:d}/hyperparameters.json'

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_get_path_properties(self):
        loc = RawFiloc(self.path_fmt)
        props = loc.get_path_properties(rf"{self.test_dir}/simid=12/epid=102/hyperparameters.json")
        self.assertEqual(len(props), 2)
        self.assertEqual(props['simid'], 12)
        self.assertEqual(props['epid'], 102)

    def test_get_path(self):
        loc = RawFiloc(self.path_fmt)
        path1 = loc.get_path(simid=12, epid=102)
        self.assertEqual(path1, rf"{self.test_dir}/simid=12/epid=102/hyperparameters.json")
        # Todo: test other formattings: float, string

    def test_get_glob_path(self):
        loc = RawFiloc(self.path_fmt)
        path1 = loc.get_glob_path(epid=102)
        self.assertEqual(path1, rf"{self.test_dir}/simid=?*/epid=102/hyperparameters.json")
        # Todo: test other formattings: float, string

    def test_find_paths(self):
        loc = RawFiloc(self.path_fmt)
        touch_file(loc.get_path(simid=1, epid=10))
        touch_file(loc.get_path(simid=1, epid=20))
        touch_file(loc.get_path(simid=2, epid=10))
        touch_file(loc.get_path(simid=2, epid=20))

        p = loc.find_paths(simid=1)
        self.assertListEqual(p, [
            rf"{self.test_dir}/simid=1/epid=10/hyperparameters.json",
            rf"{self.test_dir}/simid=1/epid=20/hyperparameters.json"
        ])

        p = loc.find_paths(epid=10)
        self.assertListEqual(p, [
            rf"{self.test_dir}/simid=1/epid=10/hyperparameters.json",
            rf"{self.test_dir}/simid=2/epid=10/hyperparameters.json"
        ])

        p = loc.find_paths(epid=12)
        self.assertListEqual(p, [])

    def test_find_paths_and_path_props(self):
        loc = RawFiloc(self.path_fmt)
        touch_file(loc.get_path(simid=1, epid=10))
        touch_file(loc.get_path(simid=1, epid=20))
        touch_file(loc.get_path(simid=2, epid=10))
        touch_file(loc.get_path(simid=2, epid=20))

        p = loc.find_paths_and_path_props(simid=1)
        self.assertListEqual(p, [
            (rf"{self.test_dir}/simid=1/epid=10/hyperparameters.json", {'simid': 1, 'epid': 10}),
            (rf"{self.test_dir}/simid=1/epid=20/hyperparameters.json", {'simid': 1, 'epid': 20}),
        ])

        p = loc.find_paths_and_path_props(epid=10)
        self.assertListEqual(p, [
            (rf"{self.test_dir}/simid=1/epid=10/hyperparameters.json", {'simid': 1, 'epid': 10}),
            (rf"{self.test_dir}/simid=2/epid=10/hyperparameters.json", {'simid': 2, 'epid': 10}),
        ])

        p = loc.find_paths_and_path_props(epid=12)
        self.assertListEqual(p, [])

    def test_exists(self):
        loc = RawFiloc(self.path_fmt)
        self.assertEqual(loc.exists(simid=1, epid=10), False)
        touch_file(loc.get_path(simid=1, epid=10))
        self.assertEqual(loc.exists(simid=1, epid=10), True)

    def test_open_write_readonly(self):
        loc = RawFiloc(self.path_fmt)
        with self.assertRaises(UnsupportedOperation):
            with loc.open(dict(simid=1, epid=10), 'w') as f:
                f.write("coucou")

    def test_open_write_read(self):
        loc = RawFiloc(self.path_fmt, writable=True)
        with loc.open(dict(simid=1, epid=10), 'w') as f:
            f.write("coucou")
        with loc.open(dict(simid=1, epid=10), 'r') as f:
            s = f.read()
        self.assertEqual(s, "coucou")

    def test_delete_readonly(self):
        loc = RawFiloc(self.path_fmt)
        touch_file(loc.get_path(simid=1, epid=10))
        with self.assertRaises(UnsupportedOperation):
            loc.delete(dict(simid=1, epid=10))

    def test_delete(self):
        loc = RawFiloc(self.path_fmt, writable=True)
        touch_file(loc.get_path(simid=1, epid=10))
        self.assertEqual(loc.exists(simid=1, epid=10), True)
        loc.delete(dict(simid=1, epid=10))
        self.assertEqual(loc.exists(simid=1, epid=10), False)


if __name__ == '__main__':
    unittest.main()
