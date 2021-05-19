import os
from pathlib import Path
from typing import Union

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


def to_anypath(s: Union[str, os.PathLike]) -> Union[CloudPath, Path]:
    """Convenience method to convert a str or os.PathLike to the
    proper Path or CloudPath object using AnyPath.
    """
    # shortcut pathlike items that are already valid Path/CloudPath
    if isinstance(s, (CloudPath, Path)):
        return s

    return AnyPath(s)  # type: ignore
