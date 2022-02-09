import os
from pathlib import Path
from typing import (
    Iterator,
    List,
    Protocol,
    Union,
    runtime_checkable,
    Sequence,
    TypeVar,
    Tuple,
)

from .cloudpath import InvalidPrefixError, CloudPath
from .exceptions import AnyPathTypeError


class AnyPathMeta(type):
    """Metaclass for AnyPath that implements special methods so that AnyPath works as a virtual
    superclass when using isinstance or issubclass checks on CloudPath or Path inputs. See
    [PEP 3119](https://www.python.org/dev/peps/pep-3119/#overloading-isinstance-and-issubclass)."""

    def __instancecheck__(cls, inst):
        return isinstance(inst, CloudPath) or isinstance(inst, Path)

    def __subclasscheck__(cls, sub):
        return issubclass(sub, CloudPath) or issubclass(sub, Path)


class OldAnyPath(metaclass=AnyPathMeta):
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


def to_anypath(s: Union[str, os.PathLike]) -> Union[CloudPath, Path]:
    """Convenience method to convert a str or os.PathLike to the
    proper Path or CloudPath object using AnyPath.
    """
    # shortcut pathlike items that are already valid Path/CloudPath
    if isinstance(s, (CloudPath, Path)):
        return s

    return OldAnyPath(s)  # type: ignore


# AnyPath = OldAnyPath

Self = TypeVar("Self")


@runtime_checkable
class AnyPath(Protocol):
    def __new__(cls, *args, **kwargs) -> "AnyPath":
        try:
            return CloudPath(*args, **kwargs)
        except InvalidPrefixError as cloudpath_exception:
            try:
                return Path(*args, **kwargs)
            except TypeError as path_exception:
                raise AnyPathTypeError(
                    "Invalid input for both CloudPath and Path. "
                    f"CloudPath exception: {repr(cloudpath_exception)} "
                    f"Path exception: {repr(path_exception)}"
                )

    @property
    def anchor(self) -> str:
        pass

    def as_uri(self) -> str:
        pass

    @property
    def drive(self) -> str:
        pass

    def exists(self) -> bool:
        pass

    def glob(self: Self, pattern: str) -> Iterator[Self]:
        pass

    def is_dir(self) -> bool:
        pass

    def is_file(self) -> bool:
        pass

    def iterdir(self: Self) -> Iterator[Self]:
        pass

    def joinpath(self: Self, *args) -> Self:
        pass

    def match(self, pattern: str) -> bool:
        pass

    def mkdir(self, parents: bool, exist_ok: bool):
        pass

    @property
    def name(self) -> str:
        pass

    def open(self, mode, buffering, encoding, errors, newline):
        pass

    @property
    def parent(self: Self) -> Self:
        pass

    @property
    def parents(self: Self) -> Sequence[Self]:
        pass

    @property
    def parts(self) -> Tuple[str, ...]:
        pass

    def read_bytes(self) -> bytes:
        pass

    def read_text(self) -> str:
        pass

    def rename(self: Self, target) -> Self:
        pass

    def replace(self: Self, target) -> Self:
        pass

    def rglob(self: Self, pattern: str) -> Iterator[Self]:
        pass

    def rmdir(self):
        pass

    def samefile(self, other_path) -> bool:
        pass

    def stat(self) -> os.stat_result:
        pass

    @property
    def stem(self) -> str:
        pass

    @property
    def suffix(self) -> str:
        pass

    @property
    def suffixes(self) -> List[str]:
        pass

    def touch(self):
        pass

    def unlink(self):
        pass

    def with_name(self: Self, name: str) -> Self:
        pass

    def with_suffix(self: Self, suffix: str) -> Self:
        pass

    def write_bytes(self, data: bytes):
        pass

    def write_text(self, data: str, encoding, errors):
        pass

    @classmethod
    def __get_validators__(cls):
        """Pydantic special method. See
        https://pydantic-docs.helpmanual.io/usage/types/#custom-data-types"""
        yield _validate


def _validate(value) -> "AnyPath":
    """Used as a Pydantic validator. See
    https://pydantic-docs.helpmanual.io/usage/types/#custom-data-types"""
    # Note __new__ is static method and not a class method
    return AnyPath.__new__(AnyPath, value)


# AnyPath.__get_validators__ = _get_validators
# AnyPath.__validate = _validate
