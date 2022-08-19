from __future__ import absolute_import
import os
from abc import ABCMeta, abstractmethod
from pathlib import Path
from typing import Generator, Iterable, List, Sequence, Tuple, Union

from .cloudpath import InvalidPrefixError, CloudPath
from .exceptions import AnyPathTypeError


class AnyPathMeta(ABCMeta):
    def __init__(cls, name, bases, dic):
        # Copy docstring from pathlib.Path
        for attr in dir(cls):
            if (
                not attr.startswith("_")
                and hasattr(Path, attr)
                and getattr(getattr(Path, attr), "__doc__", None)
            ):
                docstring = getattr(Path, attr).__doc__ + " _(Docstring copied from pathlib.Path)_"
                getattr(cls, attr).__doc__ = docstring
                if isinstance(getattr(cls, attr), property):
                    # Properties have __doc__ duplicated under fget, and at least some parsers
                    # read it from there.
                    getattr(cls, attr).fget.__doc__ = docstring


class AnyPath(metaclass=AnyPathMeta):
    """Polymorphic virtual superclass for CloudPath and pathlib.Path. Constructing an instance will
    automatically dispatch to CloudPath or Path based on the input. It also supports both
    isinstance and issubclass checks.

    This class also integrates with Pydantic. When used as a type declaration for a Pydantic
    BaseModel, the Pydantic validation process will appropriately run inputs through this class'
    constructor and dispatch to CloudPath or Path.
    """

    def __new__(cls, *args, **kwargs) -> Union[CloudPath, Path]:  # type: ignore
        try:
            return CloudPath(*args, **kwargs)  # type: ignore
        except InvalidPrefixError as cloudpath_exception:
            try:
                return Path(*args, **kwargs)
            except TypeError as path_exception:
                raise AnyPathTypeError(
                    "Invalid input for both CloudPath and Path. "
                    f"CloudPath exception: {repr(cloudpath_exception)} "
                    f"Path exception: {repr(path_exception)}"
                )

    @classmethod
    def __get_validators__(cls):
        """Pydantic special method. See
        https://pydantic-docs.helpmanual.io/usage/types/#custom-data-types"""
        yield cls._validate

    @classmethod
    def _validate(cls, value) -> Union[CloudPath, Path]:
        """Used as a Pydantic validator. See
        https://pydantic-docs.helpmanual.io/usage/types/#custom-data-types"""
        # Note __new__ is static method and not a class method
        return cls.__new__(cls, value)

    @property
    @abstractmethod
    def anchor(self) -> str:
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def suffix(self) -> str:
        pass

    @property
    @abstractmethod
    def suffixes(self) -> List[str]:
        pass

    @property
    @abstractmethod
    def stem(self) -> str:
        pass

    @property
    @abstractmethod
    def parts(self) -> Tuple[str]:
        pass

    @property
    @abstractmethod
    def parent(self) -> "AnyPath":
        pass

    @property
    @abstractmethod
    def parents(self) -> Sequence["AnyPath"]:
        pass

    @property
    @abstractmethod
    def drive(self) -> str:
        pass

    @abstractmethod
    def absolute(self) -> "AnyPath":
        pass

    @abstractmethod
    def as_uri(self) -> str:
        pass

    @abstractmethod
    def exists(self) -> bool:
        pass

    @abstractmethod
    def glob(self, pattern: str) -> Generator["AnyPath", None, None]:
        pass

    @abstractmethod
    def is_absolute(self) -> bool:
        pass

    @abstractmethod
    def is_dir(self) -> bool:
        pass

    @abstractmethod
    def is_file(self) -> bool:
        pass

    @abstractmethod
    def is_relative_to(self, other) -> bool:
        pass

    @abstractmethod
    def iterdir(self) -> Iterable["AnyPath"]:
        pass

    @abstractmethod
    def joinpath(self, *args) -> "AnyPath":
        pass

    @abstractmethod
    def match(self, path_pattern: str) -> bool:
        pass

    @abstractmethod
    def mkdir(self, parents: bool = False, exist_ok: bool = False) -> None:
        """docstring? has mode for pathlib"""
        pass

    @abstractmethod
    def open(self, mode="r", buffering=-1, encoding=None, errors=None, newline=None, **kwargs):
        pass

    @abstractmethod
    def read_bytes(self) -> bytes:
        pass

    @abstractmethod
    def read_text(self, *args, **kwargs) -> str:
        pass

    @abstractmethod
    def relative_to(self, other) -> Path:
        pass

    @abstractmethod
    def rename(self, target: "AnyPath") -> "AnyPath":
        """Add docstring as behavior for CloudPath differs"""
        pass

    @abstractmethod
    def replace(self, target) -> "AnyPath":
        """Add docstring as behavior for CloudPath differs"""
        pass

    @abstractmethod
    def resolve(self) -> "AnyPath":
        pass

    @abstractmethod
    def rglob(self, pattern: str) -> Generator["AnyPath", None, None]:
        pass

    @abstractmethod
    def rmdir(self) -> None:
        pass

    @abstractmethod
    def samefile(self, other_path) -> bool:
        pass

    @abstractmethod
    def stat(self):
        """docstring? has mode for pathlib"""
        pass

    @abstractmethod
    def touch(self, exist_ok: bool = True) -> None:
        """docstring? has mode for pathlib"""
        pass

    @abstractmethod
    def unlink(self, missing_ok=False) -> None:
        pass

    @abstractmethod
    def with_name(self, name: str) -> "AnyPath":
        pass

    @abstractmethod
    def with_suffix(self, suffix: str) -> "AnyPath":
        pass

    @abstractmethod
    def write_bytes(self, data: bytes) -> int:
        pass

    @abstractmethod
    def write_text(self, data: str, encoding=None, errors=None) -> int:
        pass


AnyPath.register(CloudPath)  # type: ignore
AnyPath.register(Path)


def to_anypath(s: Union[str, os.PathLike]) -> Union[CloudPath, Path]:
    """Convenience method to convert a str or os.PathLike to the
    proper Path or CloudPath object using AnyPath.
    """
    # shortcut pathlike items that are already valid Path/CloudPath
    if isinstance(s, (CloudPath, Path)):
        return s

    return AnyPath(s)  # type: ignore
