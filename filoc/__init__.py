from collections import OrderedDict
from typing import Union, Dict, Optional, List, Any, Iterable

from pandas import Series, DataFrame

from .core import FilocComposite, FilocSingle
from .filoc_io import FilocIO
from filoc.factories import filoc

__version__ = '0.0.6'


__all__     = [
    'filoc',
    'FilocIO',
    'filoc_json',
    'filoc_pandas',
]
