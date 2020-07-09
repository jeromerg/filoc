import json
import shutil
import tempfile
import unittest

from pandas import DataFrame

from filoc import Filoc, MultilocBase


# noinspection DuplicatedCode
from filoc.multiloc import Multiloc, PandasMultiloc


class TestMultiloc(unittest.TestCase):
    def setUp(self):
        self.maxDiff      = None
        self.test_dir     = tempfile.mkdtemp().replace('\\', '/')
        self.path_fmt_hyp = self.test_dir + r'/somewhere1/simid={simid:d}/epid={epid:d}/hyperparameters.json'
        self.path_fmt_res = self.test_dir + r'/somewhere1/epid={epid:d}/simid={simid:d}/result.json'
        self.hyp_loc      = Filoc(self.path_fmt_hyp, writable=True)
        self.res_loc      = Filoc(self.path_fmt_res, writable=True)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_read_contents(self):
        pass

    def test_write_and_read_contents(self):
        # ACT 1
        mloc = Multiloc({'hyp' : self.hyp_loc, 'res' : self.res_loc})
        mloc.write_contents([
            {"path.simid": 1, "path.epid": 10, "hyp.a": 100, "res.b": 1000},
            {"path.simid": 1, "path.epid": 20, "hyp.a": 200, "res.b": 2000},
            {"path.simid": 2, "path.epid": 10, "hyp.a": 300, "res.b": 3000},
            {"path.simid": 2, "path.epid": 20, "hyp.a": 400, "res.b": 4000},
        ])

        wloc = Filoc(self.path_fmt_hyp)
        with wloc.open({"simid": 1, "epid": 10}) as f: hyp1 = json.load(f)
        with wloc.open({"simid": 1, "epid": 20}) as f: hyp2 = json.load(f)
        with wloc.open({"simid": 2, "epid": 10}) as f: hyp3 = json.load(f)
        with wloc.open({"simid": 2, "epid": 20}) as f: hyp4 = json.load(f)

        self.assertEqual('{"a": 100}', json.dumps(hyp1, sort_keys=True))
        self.assertEqual('{"a": 200}', json.dumps(hyp2, sort_keys=True))
        self.assertEqual('{"a": 300}', json.dumps(hyp3, sort_keys=True))
        self.assertEqual('{"a": 400}', json.dumps(hyp4, sort_keys=True))

        wloc = Filoc(self.path_fmt_res)
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
            '[{"hyp.a": 100, "path.epid": 10, "path.simid": 1, "res.b": 1000}, {"hyp.a": 300, "path.epid": 10, "path.simid": 2, "res.b": 3000}]',
            json.dumps(p, sort_keys=True))


    def test_write_and_read_contents_dataframe(self):
        mloc = PandasMultiloc({'hyp':self.hyp_loc, 'res':self.res_loc})

        # ACT 1
        mloc.write_contents(DataFrame([
            {"path.simid": 1, "path.epid": 10, "hyp.a": 100, "res.b": 1000},
            {"path.simid": 1, "path.epid": 20, "hyp.a": 200, "res.b": 2000},
            {"path.simid": 2, "path.epid": 10, "hyp.a": 300, "res.b": 3000},
            {"path.simid": 2, "path.epid": 20, "hyp.a": 400, "res.b": 4000},
        ]))

        wloc = Filoc(self.path_fmt_hyp)
        with wloc.open({"simid": 1, "epid": 10}) as f: hyp1 = json.load(f)
        with wloc.open({"simid": 1, "epid": 20}) as f: hyp2 = json.load(f)
        with wloc.open({"simid": 2, "epid": 10}) as f: hyp3 = json.load(f)
        with wloc.open({"simid": 2, "epid": 20}) as f: hyp4 = json.load(f)

        self.assertEqual('{"a": 100}', json.dumps(hyp1, sort_keys=True))
        self.assertEqual('{"a": 200}', json.dumps(hyp2, sort_keys=True))
        self.assertEqual('{"a": 300}', json.dumps(hyp3, sort_keys=True))
        self.assertEqual('{"a": 400}', json.dumps(hyp4, sort_keys=True))

        wloc = Filoc(self.path_fmt_res)
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
            '[{"hyp.a": 100, "path.epid": 10, "path.simid": 1, "res.b": 1000}, {"hyp.a": 300, "path.epid": 10, "path.simid": 2, "res.b": 3000}]',
            json.dumps(p.to_dict(orient='records'), sort_keys=True))


if __name__ == '__main__':
    unittest.main()
