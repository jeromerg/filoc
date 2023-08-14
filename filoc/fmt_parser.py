from collections import defaultdict
import re
import string
from typing import Any, Dict, Set, Tuple


DOC_SUPPORTED_SYNTAX = """Supported are: {name:format}, where format is a subset of the Python's string formatting syntax:
- 
"""

REGEX_INT    = r"[\-\+]?\d+"
REGEX_FLOAT  = r"[\-\+]?(?:\d+(?:\.\d*)?|\.\d+)([eE][\-\+]?\d+)?"
REGEX_STRING = r".*?"

def identity(v):
    return v


class UnsupportedFormatSyntaxError(ValueError):
    def __init__(self, msg):
        super().__init__(msg + f" // {DOC_SUPPORTED_SYNTAX}")


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

        pieces = []
        value_parsers = {}
        for literal_text, field_name, format_spec, conversion in string.Formatter().parse(fmt):
            if conversion is not None:
                raise UnsupportedFormatSyntaxError(f"Conversion is not supported in format string: '{conversion}'")            

            if literal_text:
                pieces.append(re.escape(literal_text))

            if field_name is not None:
                if format_spec == 'd':
                    pieces.append(rf'(?P<{field_name}>{REGEX_INT})')
                    if field_name not in value_parsers:
                        value_parsers[field_name] = int
                elif format_spec == 'g':
                    # also supports scientific notation
                    pieces.append(rf'(?P<{field_name}>{REGEX_FLOAT})')
                    if field_name not in value_parsers:
                        value_parsers[field_name] = float
                elif format_spec == '':
                    pieces.append(rf'(?P<{field_name}>{REGEX_STRING})')
                    if field_name not in value_parsers:
                        value_parsers[field_name] = identity
                else:
                    raise UnsupportedFormatSyntaxError(f"Conversion is not supported in format string: '{conversion}'")
        self.regex_string = '^' + ''.join(pieces) + '$'
        self.regex = re.compile(self.regex_string)
        self.value_parsers = value_parsers

    @property
    def field_names(self) -> Set[str]:
        return set(self.value_parsers.keys())

    def parse(self, txt) -> Dict[str, Any]:
        a = self.regex.fullmatch(txt)
        if a is None:
            raise ValueError(f"Could not parse '{txt}' with format '{self.fmt}' (regex: '{self.regex_string}')")
        result = {}
        for name, value in a.groupdict().items():
            if value is None:
                value = ''
            value_parser = self.value_parsers[name]
            try:
                result[name] = value_parser(value) 
            except Exception as e:
                raise ValueError(f"Could not parse '{txt}' with format '{self.fmt}' (regex: '{self.regex_string}', value parser: {str(value_parser)})") from e
        return result
