import unittest
from filoc.fmt_parser import FmtParser
from datetime import datetime


class TestFmtParser(unittest.TestCase):
    def test_parse_datetime(self):
        fmt = "{date:%Y-%m-%d %H:%M:%S}"
        parser = FmtParser(fmt)
        input_str = "2022-01-01 12:00:00"
        expected_output = {"date": datetime.strptime(input_str, "%Y-%m-%d %H:%M:%S")}
        self.assertEqual(parser.parse(input_str), expected_output)

    def test_parse_datetime_complementary_date_info(self):
        fmt = "{date:%Y-%m-%d}/{date:%H:%M:%S}"
        parser = FmtParser(fmt)
        input_str = "2022-01-01/12:00:00"
        expected_output = {"date": datetime.strptime(input_str, "%Y-%m-%d/%H:%M:%S")}
        self.assertEqual(parser.parse(input_str), expected_output)

