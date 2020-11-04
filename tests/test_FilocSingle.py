""" Test for json frontend """
import json
import os
import shutil
import tempfile
import time
import unittest
from pathlib import Path

from filoc import filoc_json_single, FilocIO


# noinspection PyMissingOrEmptyDocstring
from filoc.contract import SingletonExpectedError


def touch_file(file_path):
    os.makedirs(os.path.dirname(file_path))
    Path(file_path).touch()


# noinspection DuplicatedCode
# noinspection PyMissingOrEmptyDocstring
class TestFilocSingle(unittest.TestCase):
    """
    TODO: 
    - Test cache behavior on delete of files (should currently fail --> TODO DEV Feature)    
    """

    def setUp(self):
        self.maxDiff = None
        self.test_dir = tempfile.mkdtemp().replace('\\', '/')
        self.path_fmt = self.test_dir + r'/simid={simid:d}/epid={epid:d}/hyperparameters.json'

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_read_write_read(self):
        wloc = FilocIO(self.path_fmt, writable=True)
        with wloc.open({"simid": 1, "epid": 10}, "w") as f: json.dump({'a': 100}, f)
        with wloc.open({"simid": 1, "epid": 20}, "w") as f: json.dump({'a': 200}, f)
        with wloc.open({"simid": 2, "epid": 10}, "w") as f: json.dump({'a': 300}, f)
        with wloc.open({"simid": 2, "epid": 20}, "w") as f: json.dump({'a': 400}, f)

        loc = filoc_json_single(self.path_fmt)
        p = loc.read_contents({'epid': 10})
        self.assertEqual(len(p), 2)
        self.assertEqual('[{"a": 100, "epid": 10, "simid": 1}, {"a": 300, "epid": 10, "simid": 2}]', json.dumps(p, sort_keys=True))

        # change file
        time.sleep(0.1)  # ensures different timestamp
        with wloc.open({"simid": 2, "epid": 10}, "w") as f:
            json.dump({'a': 333}, f)
        
        p = loc.read_contents({'epid': 10})
        self.assertEqual(len(p), 2)
        self.assertEqual('[{"a": 100, "epid": 10, "simid": 1}, {"a": 333, "epid": 10, "simid": 2}]', json.dumps(p, sort_keys=True))

    def test_read_all(self):
        wloc = FilocIO(self.path_fmt, writable=True)
        with wloc.open({"simid": 1, "epid": 10}, "w") as f: json.dump({'a': 100}, f)
        with wloc.open({"simid": 1, "epid": 20}, "w") as f: json.dump({'a': 200}, f)
        with wloc.open({"simid": 2, "epid": 10}, "w") as f: json.dump({'a': 300}, f)
        with wloc.open({"simid": 2, "epid": 20}, "w") as f: json.dump({'a': 400}, f)

        loc = filoc_json_single(self.path_fmt)
        p = loc.read_contents()
        self.assertEqual(len(p), 4)
        self.assertEqual('[{"a": 100, "epid": 10, "simid": 1}, {"a": 200, "epid": 20, "simid": 1}, {"a": 300, "epid": 10, "simid": 2}, {"a": 400, "epid": 20, "simid": 2}]', json.dumps(p, sort_keys=True))

    def test_with_constraints_on_path_placeholders(self):
        wloc = FilocIO(self.path_fmt, writable=True)
        with wloc.open({"simid": 1, "epid": 10}, "w") as f: json.dump({'a': 100}, f)
        with wloc.open({"simid": 1, "epid": 20}, "w") as f: json.dump({'a': 200}, f)
        with wloc.open({"simid": 2, "epid": 10}, "w") as f: json.dump({'a': 300}, f)
        with wloc.open({"simid": 2, "epid": 20}, "w") as f: json.dump({'a': 400}, f)

        loc = filoc_json_single(self.path_fmt)
        p = loc.read_contents({'epid': 10})
        self.assertEqual(len(p), 2)
        self.assertEqual('[{"a": 100, "epid": 10, "simid": 1}, {"a": 300, "epid": 10, "simid": 2}]', json.dumps(p, sort_keys=True))

    def test_with_constraints_on_content_attributes(self):
        wloc = FilocIO(self.path_fmt, writable=True)
        with wloc.open({"simid": 1, "epid": 10}, "w") as f: json.dump({'a': 100}, f)
        with wloc.open({"simid": 1, "epid": 20}, "w") as f: json.dump({'a': 200}, f)
        with wloc.open({"simid": 2, "epid": 10}, "w") as f: json.dump({'a': 300}, f)
        with wloc.open({"simid": 2, "epid": 20}, "w") as f: json.dump({'a': 400}, f)

        loc = filoc_json_single(self.path_fmt)
        p = loc.read_contents({'a': 300})
        self.assertEqual(len(p), 1)
        self.assertEqual('[{"a": 300, "epid": 10, "simid": 2}]', json.dumps(p, sort_keys=True))

    def test_read_contents_with_cache(self):
        print("write files")
        wloc = FilocIO(self.path_fmt, writable=True)
        with wloc.open({"simid": 1, "epid": 10}, "w") as f:
            json.dump({'a': 100}, f)
        with wloc.open({"simid": 1, "epid": 20}, "w") as f:
            json.dump({'a': 200}, f)
        with wloc.open({"simid": 2, "epid": 10}, "w") as f:
            json.dump({'a': 300}, f)
        with wloc.open({"simid": 2, "epid": 20}, "w") as f:
            json.dump({'a': 400}, f)

        loc = filoc_json_single(self.path_fmt, cache_locpath='.cache')
        print("read_contents 1")
        p = loc.read_contents({'epid': 10})
        self.assertEqual(len(p), 2)
        self.assertEqual('[{"a": 100, "epid": 10, "simid": 1}, {"a": 300, "epid": 10, "simid": 2}]',
                         json.dumps(p, sort_keys=True))

        time.sleep(0.1)  # small delay between the two writings to ensure that the file gets an new timestamp

        print("change one file")
        # change to file triggers cache refresh
        with wloc.open({"simid": 2, "epid": 10}, "w") as f: 
            json.dump({'a': 333}, f)
            # f.flush()
            # os.fsync(f.fileno())

        print("re read_contents 2")
        p = loc.read_contents({'epid': 10})
        self.assertEqual(len(p), 2)
        self.assertEqual('[{"a": 100, "epid": 10, "simid": 1}, {"a": 333, "epid": 10, "simid": 2}]', json.dumps(p, sort_keys=True))

        # act_assert()

        # Trick to test: signature change does not take effect, because of cache
        loc = filoc_json_single(self.path_fmt, cache_locpath='.cache')
        print("re read_contents 3")
        p = loc.read_contents({'epid': 10})
        self.assertEqual(len(p), 2)
        self.assertEqual('[{"a": 100, "epid": 10, "simid": 1}, {"a": 333, "epid": 10, "simid": 2}]',
                         json.dumps(p, sort_keys=True))

    def test_write_contents(self):
        wloc = filoc_json_single(self.path_fmt, writable=True)
        wloc._write_props_list([
            {"simid": 1, "epid": 10, 'a': 100},
            {"simid": 1, "epid": 20, 'a': 200},
            {"simid": 2, "epid": 10, 'a': 300},
            {"simid": 2, "epid": 20, 'a': 400},
        ])

        wloc = FilocIO(self.path_fmt)
        with wloc.open({"simid": 1, "epid": 10}) as f:
            c1 = json.load(f)
        with wloc.open({"simid": 1, "epid": 20}) as f:
            c2 = json.load(f)
        with wloc.open({"simid": 2, "epid": 10}) as f:
            c3 = json.load(f)
        with wloc.open({"simid": 2, "epid": 20}) as f:
            c4 = json.load(f)

        self.assertEqual('{"a": 100}', json.dumps(c1, sort_keys=True))
        self.assertEqual('{"a": 200}', json.dumps(c2, sort_keys=True))
        self.assertEqual('{"a": 300}', json.dumps(c3, sort_keys=True))
        self.assertEqual('{"a": 400}', json.dumps(c4, sort_keys=True))


if __name__ == '__main__':
    unittest.main()
