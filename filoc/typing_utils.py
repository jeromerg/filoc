from typing import Any, Protocol, Iterator
from typing_extensions import runtime_checkable


@runtime_checkable
class KeyValueProtocol(Protocol):
    """
    Azure adlfs class DictMixin does not implement the dict protocol (__iter__ missing)
    This protocol is used to recognize this implementation in a generic way and avoid of false positives
    """
    def keys(self) -> Iterator[str]: ...
    def values(self) -> Iterator[Any]: ...


