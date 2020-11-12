from typing import TYPE_CHECKING

from ..cloudpath import CloudPath


if TYPE_CHECKING:
    from .localclient import LocalClient


class LocalPath(CloudPath):
    """Abstract CloudPath for accessing objects the local filesystem. Subclasses are as a
    monkeypatch substitutes for normal CloudPath subclasses when writing tests."""

    client: "LocalClient"

    def is_dir(self) -> bool:
        return self.client._is_dir(self)

    def is_file(self) -> bool:
        return self.client._is_file(self)

    def stat(self):
        return self.client._stat(self)

    def touch(self):
        self.client._touch(self)
