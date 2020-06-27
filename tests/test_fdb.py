import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path

from filoc.filoc import Filoc


def touch_file(file_path):
    os.makedirs(os.path.dirname(file_path))
    Path(file_path).touch()


class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.test_dir = tempfile.mkdtemp().replace('\\', '/')
        self.path_fmt = self.test_dir + r'/simid={simid:d}/epid={epid:d}/hyperparameters.json'
        self.loc = Filoc(self.path_fmt)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_build_path_and_extract_properties(self):
        path1 = self.loc.build_path(simid=12, epid=102)
        self.assertEqual(path1, rf"{self.test_dir}/simid=12/epid=102/hyperparameters.json")
        props = self.loc.extract_properties(path1)
        self.assertEqual(props['simid'], 12)
        self.assertEqual(props['epid'], 102)

    def test_build_path_and_get_existing_files(self):
        touch_file(self.loc.build_path(simid=1, epid=10))
        touch_file(self.loc.build_path(simid=1, epid=20))
        touch_file(self.loc.build_path(simid=2, epid=10))
        touch_file(self.loc.build_path(simid=2, epid=20))
        p = self.loc.find_paths(simid=1)
        self.assertListEqual(p, [
            rf"{self.test_dir}/simid=1/epid=10/hyperparameters.json",
            rf"{self.test_dir}/simid=1/epid=20/hyperparameters.json"
        ])

        p = self.loc.find_paths(epid=10)
        self.assertListEqual(p, [
            rf"{self.test_dir}/simid=1/epid=10/hyperparameters.json",
            rf"{self.test_dir}/simid=2/epid=10/hyperparameters.json"
        ])

        p = self.loc.find_paths(epid=12)
        self.assertListEqual(p, [])

    def test_build_path_and_report(self):
        with self.loc.open({"simid": 1, "epid": 10}, "w") as f: json.dump({'a': 100}, f)
        with self.loc.open({"simid": 1, "epid": 20}, "w") as f: json.dump({'a': 200}, f)
        with self.loc.open({"simid": 2, "epid": 10}, "w") as f: json.dump({'a': 300}, f)
        with self.loc.open({"simid": 2, "epid": 20}, "w") as f: json.dump({'a': 400}, f)

        p = self.loc.report( { 'epid' : 10 } )
        self.assertEqual(len(p), 2)
        self.assertEqual(json.dumps(p, sort_keys=True), '[{"epid": 10, "simid": 1}, {"epid": 10, "simid": 2}]')

        p = self.loc.report({ 'epid' : 10 }, reporter_1param)
        self.assertEqual(len(p), 2)
        self.assertEqual(json.dumps(p, sort_keys=True), '[{"a": 100, "epid": 10, "simid": 1}, {"a": 300, "epid": 10, "simid": 2}]')

        p = self.loc.report({ 'epid' : 10 }, reporter_2param)
        self.assertEqual(len(p), 2)
        self.assertEqual(json.dumps(p, sort_keys=True), '[{"a": 100, "b": 1, "epid": 10, "simid": 1}, {"a": 300, "b": 2, "epid": 10, "simid": 2}]')

        # change file
        with self.loc.open({"simid": 2, "epid": 10}, "w") as f: json.dump({'a': 333}, f)

        p = self.loc.report({ 'epid' : 10 }, reporter_1param)
        self.assertEqual(len(p), 2)
        self.assertEqual(json.dumps(p, sort_keys=True), '[{"a": 100, "epid": 10, "simid": 1}, {"a": 333, "epid": 10, "simid": 2}]')

    def test_build_path_and_report_with_cache(self):
        with self.loc.open({"simid": 1, "epid": 10}, "w") as f: json.dump({'a': 100}, f)
        with self.loc.open({"simid": 1, "epid": 20}, "w") as f: json.dump({'a': 200}, f)
        with self.loc.open({"simid": 2, "epid": 10}, "w") as f: json.dump({'a': 300}, f)
        with self.loc.open({"simid": 2, "epid": 20}, "w") as f: json.dump({'a': 400}, f)

        p = self.loc.report( { 'epid' : 10 }, cache_locpath='.cache')
        self.assertEqual(len(p), 2)
        self.assertEqual(json.dumps(p, sort_keys=True), '[{"epid": 10, "simid": 1}, {"epid": 10, "simid": 2}]')

        # signature change does not take effect, because of cache
        p = self.loc.report({ 'epid' : 10 }, reporter_1param, cache_locpath='.cache')
        self.assertEqual(len(p), 2)
        self.assertEqual(json.dumps(p, sort_keys=True), '[{"epid": 10, "simid": 1}, {"epid": 10, "simid": 2}]')

        # but change to file triggers cache refresh
        with self.loc.open({"simid": 2, "epid": 10}, "w") as f: json.dump({'a': 333}, f)

        p = self.loc.report({ 'epid' : 10 }, reporter_1param, cache_locpath='.cache')
        self.assertEqual(len(p), 2)
        self.assertEqual(json.dumps(p, sort_keys=True), '[{"epid": 10, "simid": 1}, {"a": 333, "epid": 10, "simid": 2}]')


def reporter_1param(f):
    content = json.load(f)
    return { 'a' : content['a'] }


def reporter_2param(f, properties):
    content = json.load(f)
    return {
        'a' : content['a'],
        'b' : properties['simid']
    }


if __name__ == '__main__':
    unittest.main()
