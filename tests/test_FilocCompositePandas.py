import json
import shutil
import tempfile
import unittest

from pandas import DataFrame

# noinspection DuplicatedCode
from filoc import filoc_pandas_composite, filoc_pandas_single, FilocIO


# noinspection PyMissingOrEmptyDocstring
class TestMultiloc(unittest.TestCase):
    def setUp(self):
        self.maxDiff      = None
        self.test_dir     = tempfile.mkdtemp().replace('\\', '/')
        self.path_fmt_hyp = self.test_dir + r'/somewhere1/simid={simid:d}/epid={epid:d}/hyperparameters.json'
        self.path_fmt_res = self.test_dir + r'/somewhere1/epid={epid:d}/simid={simid:d}/result.json'
        self.hyp_loc      = filoc_pandas_single(self.path_fmt_hyp, writable=True)
        self.res_loc      = filoc_pandas_single(self.path_fmt_res, writable=True)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_read_contents(self):
        pass

    def test_write_and_read_contents_dataframe(self):
        mloc = filoc_pandas_composite({'hyp': self.hyp_loc, 'res': self.res_loc})

        # ACT 1
        mloc.write_contents(DataFrame([
            {"index.simid": 1, "index.epid": 10, "hyp.a": 100, "res.b": 1000},
            {"index.simid": 1, "index.epid": 20, "hyp.a": 200, "res.b": 2000},
            {"index.simid": 2, "index.epid": 10, "hyp.a": 300, "res.b": 3000},
            {"index.simid": 2, "index.epid": 20, "hyp.a": 400, "res.b": 4000},
        ]))

        wloc = FilocIO(self.path_fmt_hyp)
        with wloc.open({"simid": 1, "epid": 10}) as f: hyp1 = json.load(f)
        with wloc.open({"simid": 1, "epid": 20}) as f: hyp2 = json.load(f)
        with wloc.open({"simid": 2, "epid": 10}) as f: hyp3 = json.load(f)
        with wloc.open({"simid": 2, "epid": 20}) as f: hyp4 = json.load(f)

        self.assertEqual('{"a": 100}', json.dumps(hyp1, sort_keys=True))
        self.assertEqual('{"a": 200}', json.dumps(hyp2, sort_keys=True))
        self.assertEqual('{"a": 300}', json.dumps(hyp3, sort_keys=True))
        self.assertEqual('{"a": 400}', json.dumps(hyp4, sort_keys=True))

        wloc = FilocIO(self.path_fmt_res)
        with wloc.open({"simid": 1, "epid": 10}) as f: hyp1 = json.load(f)
        with wloc.open({"simid": 1, "epid": 20}) as f: hyp2 = json.load(f)
        with wloc.open({"simid": 2, "epid": 10}) as f: hyp3 = json.load(f)
        with wloc.open({"simid": 2, "epid": 20}) as f: hyp4 = json.load(f)

        self.assertEqual('{"b": 1000}', json.dumps(hyp1, sort_keys=True))
        self.assertEqual('{"b": 2000}', json.dumps(hyp2, sort_keys=True))
        self.assertEqual('{"b": 3000}', json.dumps(hyp3, sort_keys=True))
        self.assertEqual('{"b": 4000}', json.dumps(hyp4, sort_keys=True))

        # ACT 2
        p = mloc.read_contents(epid=10)
        self.assertEqual(len(p), 2)

        self.assertEqual(
            '[{"hyp.a": 100, "index.epid": 10, "index.simid": 1, "res.b": 1000}, {"hyp.a": 300, "index.epid": 10, "index.simid": 2, "res.b": 3000}]',
            json.dumps(p.to_dict(orient='records'), sort_keys=True))


if __name__ == '__main__':
    unittest.main()
