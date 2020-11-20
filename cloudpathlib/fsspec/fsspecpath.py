from typing import TYPE_CHECKING

from ..cloudpath import CloudPath


if TYPE_CHECKING:
    from .fsspecclient import FsspecClient


class FsspecPath(CloudPath):
    client: "FsspecClient"

    @property
    def drive(self) -> str:
        return self._no_prefix.split("/", 1)[0]

    def is_dir(self) -> bool:
        return self.client._is_dir(self)

    def is_file(self) -> bool:
        return self.client._is_file(self)

    def mkdir(self):
        pass

    def stat(self):
        return self.client._stat(self)

    def touch(self):
        self.client._touch(self)
