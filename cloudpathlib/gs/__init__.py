from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .gsclient import GSClient as GSClient
    from .gspath import GSPath as GSPath

__all__ = [
    "GSClient",
    "GSPath",
]


def __getattr__(name: str):
    if name == "GSClient":
        from .gsclient import GSClient

        return GSClient
    if name == "GSPath":
        from .gspath import GSPath

        return GSPath
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
