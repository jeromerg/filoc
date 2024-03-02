import json
import unittest

import fsspec
from azure.storage.blob import ContentSettings
from filoc import filoc
from filoc.filoc_io import jsonify_detail


class TestAzure(unittest.TestCase):
    def test_jsonify_ContentSettings(self):
        cs = ContentSettings(
            content_type='application/json',
            content_encoding='utf-8',
            content_language='en-US',
            content_disposition='attachment',
            content_md5=bytearray(b'1234567890')
        )
        cs_json = jsonify_detail(cs)
        self.assertEqual(
            {'cache_control': None,
             'content_disposition': 'attachment',
             'content_encoding': 'utf-8',
             'content_language': 'en-US',
             'content_md5': '31323334353637383930',
             'content_type': 'application/json'},
            cs_json
        )

    def test_reading_azure_files(self):
        fs = fsspec.filesystem('az', account_name='azureopendatastorage')
        fil = filoc(
            "censusdatacontainer/release/us_population_county/year={year}/{file}.parquet",
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
