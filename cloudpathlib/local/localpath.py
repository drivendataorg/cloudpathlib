from typing import TYPE_CHECKING, Generator, Optional, Self, override

from ..cloudpath import CloudPath, NoStatError


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
        try:
            meta = self.client._stat(self)
        except FileNotFoundError:
            raise NoStatError(
                f"No stats available for {self}; it may be a directory or not exist."
            )
        return meta

    def touch(self, exist_ok: bool = True):
        self.client._touch(self, exist_ok)

    @override
    def glob(
        self, pattern: str, case_sensitive: Optional[bool] = None
    ) -> Generator[Self, None, None]:
        try:
            super().glob(pattern, case_sensitive=case_sensitive)
        except FileNotFoundError:
            yield from []
