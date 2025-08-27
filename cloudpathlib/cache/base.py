from abc import ABC, abstractmethod
from typing import Any


class ContentCache(ABC):
    @abstractmethod
    def get(self, key: Any) -> Any:
        pass

    @abstractmethod
    def put(self, key: Any, value: Any) -> None:
        pass

    @abstractmethod
    def delete(self, key: Any) -> None:
        pass

    @abstractmethod
    def clear(self) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
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