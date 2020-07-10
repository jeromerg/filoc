import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from retry import retry

from filoc import Filoc
from filoc.rawfiloc import RawFiloc
import time


def touch_file(file_path):
    os.makedirs(os.path.dirname(file_path))
    Path(file_path).touch()


# noinspection DuplicatedCode
class TestFiloc(unittest.TestCase):
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

    def test_read_contents(self):
        wloc = Filoc(self.path_fmt, writable=True)
        with wloc.open({"simid": 1, "epid": 10}, "w") as f: json.dump({'a': 100}, f)
        with wloc.open({"simid": 1, "epid": 20}, "w") as f: json.dump({'a': 200}, f)
        with wloc.open({"simid": 2, "epid": 10}, "w") as f: json.dump({'a': 300}, f)
        with wloc.open({"simid": 2, "epid": 20}, "w") as f: json.dump({'a': 400}, f)

        loc = Filoc(self.path_fmt)
        p = loc.read_contents({'epid': 10})
        self.assertEqual(len(p), 2)
        self.assertEqual('[{"a": 100, "epid": 10, "simid": 1}, {"a": 300, "epid": 10, "simid": 2}]',
                         json.dumps(p, sort_keys=True))

        loc = Filoc(self.path_fmt, content_reader=reporter_1param)
        p = loc.read_contents({'epid': 10})
        self.assertEqual(len(p), 2)
        self.assertEqual('[{"aaa": 100, "epid": 10, "simid": 1}, {"aaa": 300, "epid": 10, "simid": 2}]',
                         json.dumps(p, sort_keys=True))

        loc = Filoc(self.path_fmt, content_reader=reporter_2param)
        p = loc.read_contents({'epid': 10})
        self.assertEqual(len(p), 2)
        self.assertEqual('[{"aaa": 100, "b": 1, "epid": 10, "simid": 1}, {"aaa": 300, "b": 2, "epid": 10, "simid": 2}]',
                         json.dumps(p, sort_keys=True))

        # change file
        with wloc.open({"simid": 2, "epid": 10}, "w") as f: 
            json.dump({'a': 333}, f)
        
        loc = Filoc(self.path_fmt, content_reader=reporter_1param)
        
        @retry(tries=20, delay=2)
        def act_assert():
            p = loc.read_contents({'epid': 10})
            self.assertEqual(len(p), 2)
            self.assertEqual('[{"aaa": 100, "epid": 10, "simid": 1}, {"aaa": 333, "epid": 10, "simid": 2}]', json.dumps(p, sort_keys=True))

        act_assert()


    def test_read_contents_with_cache(self):
        print("write files")
        wloc = Filoc(self.path_fmt, writable=True)
        with wloc.open({"simid": 1, "epid": 10}, "w") as f: json.dump({'a': 100}, f)
        with wloc.open({"simid": 1, "epid": 20}, "w") as f: json.dump({'a': 200}, f)
        with wloc.open({"simid": 2, "epid": 10}, "w") as f: json.dump({'a': 300}, f)
        with wloc.open({"simid": 2, "epid": 20}, "w") as f: json.dump({'a': 400}, f)

        loc = Filoc(self.path_fmt, cache_locpath='.cache')
        print("read_contents 1")
        p = loc.read_contents({'epid': 10})
        self.assertEqual(len(p), 2)
        self.assertEqual('[{"a": 100, "epid": 10, "simid": 1}, {"a": 300, "epid": 10, "simid": 2}]',
                         json.dumps(p, sort_keys=True))

        time.sleep(0.1) # small delay between the two writings to ensure that the file gets an new timestamp

        print("change one file")
        # change to file triggers cache refresh
        with wloc.open({"simid": 2, "epid": 10}, "w") as f: 
            json.dump({'a': 333}, f)
            # f.flush()
            # os.fsync(f.fileno())

        # @retry(tries=10, delay=1)
        # def act_assert():
        print("re read_contents 2")
        p = loc.read_contents({'epid': 10})
        self.assertEqual(len(p), 2)
        self.assertEqual('[{"a": 100, "epid": 10, "simid": 1}, {"a": 333, "epid": 10, "simid": 2}]',
                        json.dumps(p, sort_keys=True))

        # act_assert()

        # Trick to test: signature change does not take effect, because of cache
        loc = Filoc(self.path_fmt, content_reader=reporter_1param, cache_locpath='.cache')
        print("re read_contents 3")
        p = loc.read_contents({'epid': 10})
        self.assertEqual(len(p), 2)
        self.assertEqual('[{"a": 100, "epid": 10, "simid": 1}, {"a": 333, "epid": 10, "simid": 2}]',
                         json.dumps(p, sort_keys=True))

    def test_write_contents(self):
        wloc = Filoc(self.path_fmt, writable=True)
        wloc.write_contents([
            {"simid": 1, "epid": 10, 'a': 100},
            {"simid": 1, "epid": 20, 'a': 200},
            {"simid": 2, "epid": 10, 'a': 300},
            {"simid": 2, "epid": 20, 'a': 400},
        ])

        wloc = Filoc(self.path_fmt)
        with wloc.open({"simid": 1, "epid": 10}) as f: c1 = json.load(f)
        with wloc.open({"simid": 1, "epid": 20}) as f: c2 = json.load(f)
        with wloc.open({"simid": 2, "epid": 10}) as f: c3 = json.load(f)
        with wloc.open({"simid": 2, "epid": 20}) as f: c4 = json.load(f)

        self.assertEqual('{"a": 100}', json.dumps(c1, sort_keys=True))
        self.assertEqual('{"a": 200}', json.dumps(c2, sort_keys=True))
        self.assertEqual('{"a": 300}', json.dumps(c3, sort_keys=True))
        self.assertEqual('{"a": 400}', json.dumps(c4, sort_keys=True))


def reporter_1param(f):
    content = json.load(f)
    return {'aaa': content['a']}


def reporter_2param(f, properties):
    content = json.load(f)
    return {
        'aaa': content['a'],
        'b': properties['simid']
    }


if __name__ == '__main__':
    unittest.main()
