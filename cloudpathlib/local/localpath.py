from typing import TYPE_CHECKING

from ..cloudpath import CloudPath, register_path_class
from pathlib import Path

if TYPE_CHECKING:
    from .localclient import LocalClient, LocalUriClient

@register_path_class("local")
class LocalPath(CloudPath):
    """Abstract CloudPath for accessing objects the local filesystem. Subclasses are as a
    monkeypatch substitutes for normal CloudPath subclasses when writing tests."""

    cloud_prefix: str = "file://"
    client: "LocalClient"
    def __init__(self, cloud_path, client=None):
        super().__init__(cloud_path, client)
        # steal all properties from Path
        for attr in dir(Path):
            try:
                object.__setattr__(self, attr, getattr(Path(self._no_prefix), attr))
            except:
                pass

    # necessary to override abstractmethods to make Python happy
    @property
    def drive(self) -> str:
        """For example "bucket" on S3 or "container" on Azure; needs to be defined for each class"""
        return Path(self._no_prefix).drive()

    def is_dir(self) -> bool:
        """Should be implemented without requiring a dir is downloaded"""
        return self._path.is_dir()

    def is_file(self) -> bool:
        """Should be implemented without requiring that the file is downloaded"""
        return self._path.is_file()

    def mkdir(self, parents: bool = False, exist_ok: bool = False) -> None:
        """Should be implemented using the client API without requiring a dir is downloaded"""
        return self._path.mkdir(parents, exist_ok)

    def touch(self, exist_ok: bool = True) -> None:
        """Should be implemented using the client API to create and update modified time"""
        return self._path.touch(exist_ok)

@register_path_class("localuri")
class LocalUriPath(LocalPath):
    cloud_prefix: str = "file://"
    client: "LocalUriClient"
