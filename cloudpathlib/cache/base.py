from abc import ABC, abstractmethod
import io
from typing import Any


class Validation(ABC):
    @abstractmethod
    def is_valid(self, key: Any, value: Any, ) -> bool:
        pass

class Eviction(ABC):
    @abstractmethod
    def evict(self) -> None:
        pass

class FileCache(ABC):
    def __init__(self, validation: Validation, eviction: Eviction):
        pass

    @abstractmethod
    def put(self, key: Any, value: Any, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def delete(self, key: Any) -> None:
        """ Remove an item from the cache
        """
        pass

    @abstractmethod
    def clear(self) -> None:
        """ Clear the entire cache.
        """
        pass

    @abstractmethod
    def is_valid(self, key: Any) -> bool:
        pass

    @abstractmethod
    def is_full(self) -> bool:
        pass

    @abstractmethod
    def local_path(self, key: Any) -> str:
        """ Should raise if not possible for a specific implementation;
            Only used if necessary. Most cache access will depend on `stream`.
        """
        pass

    @abstractmethod
    def stream(self, key:Any, *args, **kwargs) -> io.Buffered:
        """ Get a readable/writable stream to the key.
        """
        pass


class BaseFileCache(FileCache):
    def evict(self) -> None:
        return super().evict()


class MetadataCache(ABC):
    @abstractmethod
    def get(self, key: Any) -> Any:
        pass

    @abstractmethod
    def put(self, key: Any, value: Any) -> None:
        pass

    @abstractmethod
    def is_valid(self, key: Any) -> bool:
        pass

    @abstractmethod
    def is_full(self) -> bool:
        pass

    @abstractmethod
    def evict(self) -> None:
        pass


