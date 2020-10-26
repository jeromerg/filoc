"""
Filoc public API
"""
from .contract import Filoc
from .filoc_io import FilocIO
from filoc.factories import filoc, filoc_json, filoc_pandas

__version__ = '0.0.7'


__all__     = [
    'filoc',
    'filoc_json',
    'filoc_pandas',
    'Filoc',
    'FilocIO',
]
