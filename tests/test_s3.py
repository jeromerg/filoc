import json
import unittest

import fsspec
from azure.storage.blob import ContentSettings
from filoc import filoc
from filoc.filoc_io import jsonify_detail


class TestS3(unittest.TestCase):
    def test_reading_s3_files(self):
        fs = fsspec.filesystem('s3', anon=True)
        fil = filoc(
            "noaa-goes18/EXIS-L1b-SFXR/2022/352/10/{file}.nc",
            frontend='json',
            backend='path',
            meta=True,
            fs=fs
        )
        paths = fil.list_paths()
        self.assertGreater(len(paths), 0)

        contents = fil.read_contents()
        print(contents)
        self.assertGreater(len(contents), 0)
        # verify that json.dumps work (that all native types have been converted to json serializable types)
        json.dumps(contents)
