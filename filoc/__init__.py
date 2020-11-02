"""
Filoc public API
"""
from .contract import Filoc
from .filoc_io import FilocIO
from filoc.factories import filoc, filoc_json_single, filoc_pandas_single, filoc_json_composite, filoc_pandas_composite

__version__ = '0.0.7'


__all__     = [
    'filoc',
    'filoc_json_single', 
    'filoc_pandas_single', 
    'filoc_json_composite', 
    'filoc_pandas_composite',
    'Filoc',
    'FilocIO',
]
