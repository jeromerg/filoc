""" Test for json frontend """
import json
import shutil
import tempfile
import unittest

from pandas import DataFrame, Series

from filoc import FilocIO, filoc_pandas_single
# noinspection PyMissingOrEmptyDocstring
from filoc.contract import SingletonExpectedError, FrontendConversionError


# noinspection DuplicatedCode
# noinspection PyMissingOrEmptyDocstring
class TestPandasFrontend(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        self.test_dir = tempfile.mkdtemp().replace('\\', '/')
        self.path_fmt = self.test_dir + r'/simid={simid:d}/epid={epid:d}/hyperparameters.json'

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_read_content(self):
        print("write files")
        wloc = FilocIO(self.path_fmt, writable=True)
        with wloc.open({"simid": 1, "epid": 10}, "w") as f:
            json.dump({'a': 100}, f)
        with wloc.open({"simid": 1, "epid": 20}, "w") as f:
            json.dump({'a': 200}, f)

        loc = filoc_pandas_single(self.path_fmt)

        # test 1
        # noinspection PyUnusedLocal
        p = loc.read_content(epid=10)

        # test 2
        try:
            # noinspection PyUnusedLocal
            p = loc.read_content()
            raise AssertionError("previous call expected to raise an error")
        except SingletonExpectedError:
            pass

    def test_write_contents_from_records(self):
        wloc = filoc_pandas_single(self.path_fmt, writable=True)
        # TODO: improve generic typing to avoid this line below
        # noinspection PyTypeChecker

        # ACT
        wloc.write_contents([
            {"simid": 1, "epid": 10, 'a': 100},
            {"simid": 1, "epid": 20, 'a': 200},
            {"simid": 2, "epid": 10, 'a': 300},
            {"simid": 2, "epid": 20, 'a': 400},
        ])

        # ASSERT
        wloc = FilocIO(self.path_fmt)
        with wloc.open({"simid": 1, "epid": 10}) as f: c1 = json.load(f)
        with wloc.open({"simid": 1, "epid": 20}) as f: c2 = json.load(f)
        with wloc.open({"simid": 2, "epid": 10}) as f: c3 = json.load(f)
        with wloc.open({"simid": 2, "epid": 20}) as f: c4 = json.load(f)

        self.assertEqual('{"a": 100}', json.dumps(c1, sort_keys=True))
        self.assertEqual('{"a": 200}', json.dumps(c2, sort_keys=True))
        self.assertEqual('{"a": 300}', json.dumps(c3, sort_keys=True))
        self.assertEqual('{"a": 400}', json.dumps(c4, sort_keys=True))

    def test_write_contents_from_DataFrame(self):
        wloc = filoc_pandas_single(self.path_fmt, writable=True)

        # ACT
        wloc.write_contents(DataFrame([
            {"simid": 1, "epid": 10, 'a': 100},
            {"simid": 1, "epid": 20, 'a': 200},
            {"simid": 2, "epid": 10, 'a': 300},
            {"simid": 2, "epid": 20, 'a': 400},
        ]))

        # ASSERT
        wloc = FilocIO(self.path_fmt)
        with wloc.open({"simid": 1, "epid": 10}) as f: c1 = json.load(f)
        with wloc.open({"simid": 1, "epid": 20}) as f: c2 = json.load(f)
        with wloc.open({"simid": 2, "epid": 10}) as f: c3 = json.load(f)
        with wloc.open({"simid": 2, "epid": 20}) as f: c4 = json.load(f)

        self.assertEqual('{"a": 100}', json.dumps(c1, sort_keys=True))
        self.assertEqual('{"a": 200}', json.dumps(c2, sort_keys=True))
        self.assertEqual('{"a": 300}', json.dumps(c3, sort_keys=True))
        self.assertEqual('{"a": 400}', json.dumps(c4, sort_keys=True))

    def test_write_contents_from_unsupported(self):
        wloc = filoc_pandas_single(self.path_fmt, writable=True)

        # ACT, ASSERT
        try:
            # noinspection PyTypeChecker
            wloc.write_contents(123)
            raise AssertionError("previous line must raise an exception")
        except FrontendConversionError:
            pass


    def test_write_content_from_dict(self):
        wloc = filoc_pandas_single(self.path_fmt, writable=True)
        # TODO: improve generic typing to avoid this line below
        # noinspection PyTypeChecker

        # ACT
        wloc.write_content({"simid": 1, "epid": 10, 'a': 100})

        # ASSERT
        wloc = FilocIO(self.path_fmt)
        with wloc.open({"simid": 1, "epid": 10}) as f: c1 = json.load(f)

        self.assertEqual('{"a": 100}', json.dumps(c1, sort_keys=True))

    def test_write_contents_from_Series(self):
        wloc = filoc_pandas_single(self.path_fmt, writable=True)

        # ACT
        wloc.write_content(Series({"simid": 1, "epid": 10, 'a': 100}))

        # ASSERT
        wloc = FilocIO(self.path_fmt)
        with wloc.open({"simid": 1, "epid": 10}) as f: c1 = json.load(f)

        self.assertEqual('{"a": 100}', json.dumps(c1, sort_keys=True))

    def test_write_content_from_unsupported(self):
        wloc = filoc_pandas_single(self.path_fmt, writable=True)

        # ACT, ASSERT
        try:
            # noinspection PyTypeChecker
            wloc.write_content(['AHHHHH'])
            raise AssertionError("previous line must raise an exception")
        except FrontendConversionError:
            pass


if __name__ == '__main__':
    unittest.main()
