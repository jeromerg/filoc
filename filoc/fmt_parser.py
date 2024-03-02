import re
import string
from datetime import datetime, date
from typing import Any, Dict, Set

DOC_SUPPORTED_SYNTAX = """Supported are: {name:format}, where format is a subset of the Python's string formatting syntax:
- 
"""

REGEX_INT    = r"[\-\+]?\d+"
REGEX_FLOAT  = r"[\-\+]?(?:\d+(?:\.\d*)?|\.\d+)([eE][\-\+]?\d+)?"
REGEX_ANY = r".*?"
# see https://docs.python.org/3/library/datetime.html
DATETIME_FMT_SPECS = {
    "%a"  : "|".join(re.escape("{0:%a}".format(datetime(2021, 1, x))) for x in range(1, 8)),
    "%A"  : "|".join(re.escape("{0:%A}".format(datetime(2021, 1, x))) for x in range(1, 8)),
    "%w"  : r"\d",
    "%d"  : r"\d\d",
    "%b"  : "|".join(re.escape("{0:%b}".format(datetime(2021, x, 1))) for x in range(1, 13)),
    "%B"  : "|".join(re.escape("{0:%B}".format(datetime(2021, x, 1))) for x in range(1, 13)),
    "%m"  : r"\d\d",
    "%y"  : r"\d\d",
    "%Y"  : r"\d{1,4}",
    "%H"  : r"\d\d",
    "%I"  : r"\d\d",
    "%p"  : r"AM|PM",
    "%M"  : r"\d\d",
    "%S"  : r"\d\d",
    "%f"  : r"\d\d\d\d\d\d",
    "%z"  : r"[+-]\d{4}(?:\d{2}(?:\.\d{6})?)?",
    "%Z"  : r"\w+",
    "%j"  : r"\d\d\d",
    "%U"  : r"\d\d",
    "%W"  : r"\d\d",
    # "%c"  : r"",
    # "%x"  : r"",
    # "%X"  : r"",
    "%%"  : re.escape("%"),
    "%G"  : r"\d{1,4}",
    "%u"  : r"\d",
    "%V"  : r"\d\d",
    # UTC offset in the form ±HH:MM[:SS[.ffffff]] (empty string if the object is naive).
    "%:v" : r"[+-]\d{2}:\d{2}(?::\d{2}(?:\.\d{6})?)?",
}
REGEX_DATETIME_FMT_SPEC_SINGLE = "|".join(re.escape(spec) for spec in DATETIME_FMT_SPECS)
REGEX_DATETIME_FMT_SPEC = rf"(?:{REGEX_DATETIME_FMT_SPEC_SINGLE}|[^%])+"
re_datetime_fmt_spec_splitter = re.compile(rf"({REGEX_DATETIME_FMT_SPEC_SINGLE})")
re_datetime_fmt_spec = re.compile(REGEX_DATETIME_FMT_SPEC)


def identity(v):
    return v


class UnsupportedFormatSyntaxError(ValueError):
    def __init__(self, msg):
        super().__init__(msg + f" // {DOC_SUPPORTED_SYNTAX}")


def convert_field_to_segment_parser(literal_text, field, format_spec, conversion):
    if conversion is not None:
        raise UnsupportedFormatSyntaxError(f"Conversion is not supported in format string: '{conversion}'")

    if format_spec is None:
        return {"field": field, "regex": REGEX_ANY, "parser": identity}

    if '/' in format_spec:
        raise UnsupportedFormatSyntaxError(
            f"Character '/' is not supported in format specifier: '{format_spec}'. Split the format string into multiple placeholders with the same variable name."
        )

    if format_spec == 'd':
        return {"field": field, "regex": REGEX_INT, "parser": int}
    elif format_spec == 'g':
        return {"field": field, "regex": REGEX_FLOAT, "parser": float}
    elif format_spec == 's':
        return {"field": field, "regex": REGEX_ANY, "parser": identity}
    elif format_spec == '':
        return {"field": field, "regex": REGEX_ANY, "parser": identity}

    # else it is a datetime format
    m = re_datetime_fmt_spec.fullmatch(format_spec)
    if m is None:
        raise UnsupportedFormatSyntaxError(f"Unsupported format specifier: '{format_spec}'")
    single_specs = re_datetime_fmt_spec_splitter.split(format_spec)
    r = ''.join(DATETIME_FMT_SPECS[spec] if spec in DATETIME_FMT_SPECS else re.escape(spec) for spec in single_specs)
    return {"field": field, "regex": r, "parser": lambda v: datetime.strptime(v, format_spec)}


def combine_values(unset_value, v1, v2):
    if v1 == unset_value:
        return v2
    if v2 == unset_value:
        return v1
    if v1 != v2:
        raise ValueError(f"Cannot merge values: {v1} and {v2}")
    return v1


UNSET_YEAR  = datetime.strptime('', '').year
UNSET_MONTH = datetime.strptime('', '').month
UNSET_DAY   = datetime.strptime('', '').day
UNSET_HOUR  = datetime.strptime('', '').hour
UNSET_MIN   = datetime.strptime('', '').minute
UNSET_SEC   = datetime.strptime('', '').second
UNSET_MICRO = datetime.strptime('', '').microsecond
UNSET_TZ    = datetime.strptime('', '').tzinfo


def merge_values(v1, v2):
    if type(v1) is not type(v2):
        raise ValueError(f"Cannot merge values of different types: {type(v1)} and {type(v2)}")

    if isinstance(v1, datetime):
        return datetime(
            combine_values(UNSET_YEAR  , v1.year        , v2.year),
            combine_values(UNSET_MONTH , v1.month       , v2.month),
            combine_values(UNSET_DAY   , v1.day         , v2.day),
            combine_values(UNSET_HOUR  , v1.hour        , v2.hour),
            combine_values(UNSET_MIN   , v1.minute      , v2.minute),
            combine_values(UNSET_SEC   , v1.second      , v2.second),
            combine_values(UNSET_MICRO , v1.microsecond , v2.microsecond),
            combine_values(UNSET_TZ    , v1.tzinfo      , v2.tzinfo),
        )
    else:
        if v1 != v2:
            raise ValueError(f"Cannot merge values: {v1} and {v2}")
        return v1


class FmtParser:
    """
    The Parser used to parse the format string and extract the named fields and their values from the input string.
    It is a subset of the Python's string formatting syntax. 

    Supported are only:
    - named fields: `{name}` or `{name:format}`
    - format specifiers: `d` and `g` (see https://docs.python.org/3/library/string.html#format-specification-mini-language)
    
    If multiple placeholders have the same name, the first one is considered for the conversion.
    """
    def __init__(self, fmt: str):
        self.fmt = fmt

        segment_parsers = []
        for idx, (literal_text, field, format_spec, conversion) in enumerate(string.Formatter().parse(fmt)):
            if literal_text is not None:
                segment_parsers.append({"field": None, "regex": re.escape(literal_text), "parser": identity})
            if field is not None:
                segment_parsers.append(convert_field_to_segment_parser(literal_text, field, format_spec, conversion))

        self.regex_string = ''.join(f'({p["regex"]})' for p in segment_parsers)
        self.regex = re.compile(self.regex_string)
        self.segment_parsers = segment_parsers

    @property
    def field_names(self) -> Set[str]:
        return set(p["field"] for p in self.segment_parsers if p["field"] is not None)

    def parse(self, txt) -> Dict[str, Any]:
        a = self.regex.fullmatch(txt)
        if a is None:
            raise ValueError(f"Could not parse '{txt}' with format '{self.fmt}' (regex: '{self.regex_string}')")
        result = {}
        for idx, (p, g) in enumerate(zip(self.segment_parsers, a.groups())):
            field = p["field"]
            if field is None:
                continue
            parser = p["parser"]
            try:
                value = parser(g)
                if field in result:
                    result[field] = merge_values(result[field], value)
                else:
                    result[field] = value
            except Exception as e:
                raise ValueError(f"Could not parse '{txt}' with format '{self.fmt}' (regex: '{self.regex_string}', value parser: {str(parser)})") from e
        return result
