from datetime import datetime
from pathlib import Path, PurePosixPath
from shutil import rmtree
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.filedatalake import FileProperties

from tests.mock_clients.mock_azureblob import _JsonCache, DEFAULT_CONTAINER_NAME


class MockedDataLakeServiceClient:
    def __init__(self, test_dir, adls):
        # root is parent of the test specific directort
        self.root = test_dir.parent
        self.test_dir = test_dir
        self.adls = adls
        self.metadata_cache = _JsonCache(self.root / ".metadata")

    @classmethod
    def from_connection_string(cls, conn_str, credential):
        # configured in conftest.py
        test_dir, adls = conn_str.split(";")
        adls = adls == "True"
        test_dir = Path(test_dir)
        return cls(test_dir, adls)

    def get_file_system_client(self, file_system):
        return MockedFileSystemClient(self.root, self.metadata_cache)


class MockedFileSystemClient:
    def __init__(self, root, metadata_cache):
        self.root = root
        self.metadata_cache = metadata_cache

    def get_file_client(self, key):
        return MockedFileClient(key, self.root, self.metadata_cache)

    def get_directory_client(self, key):
        return MockedDirClient(key, self.root)

    def get_paths(self, path, recursive=False):
        yield from (
            MockedFileClient(
                PurePosixPath(f.relative_to(self.root)), self.root, self.metadata_cache
            ).get_file_properties()
            for f in (self.root / path).glob("**/*" if recursive else "*")
        )


class MockedFileClient:
    def __init__(self, key, root, metadata_cache) -> None:
        self.key = key
        self.root = root
        self.metadata_cache = metadata_cache

    def get_file_properties(self):
        path = self.root / self.key

        if path.exists() and path.is_dir():
            fp = FileProperties(
                **{
                    "name": self.key,
                    "size": 0,
                    "ETag": "etag",
                    "Last-Modified": datetime.fromtimestamp(path.stat().st_mtime),
                    "metadata": {"hdi_isfolder": True},
                }
            )
            fp["is_directory"] = True  # not part of object def, but still in API responses...
            return fp

        elif path.exists():
            fp = FileProperties(
                **{
                    "name": self.key,
                    "size": path.stat().st_size,
                    "ETag": "etag",
                    "Last-Modified": datetime.fromtimestamp(path.stat().st_mtime),
                    "metadata": {"hdi_isfolder": False},
                    "Content-Type": self.metadata_cache.get(self.root / self.key, None),
                }
            )

            fp["is_directory"] = False
            return fp
        else:
            raise ResourceNotFoundError

    def rename_file(self, new_name):
        new_path = self.root / new_name[len(DEFAULT_CONTAINER_NAME + "/") :]
        (self.root / self.key).rename(new_path)


class MockedDirClient:
    def __init__(self, key, root) -> None:
        self.key = key
        self.root = root

    def delete_directory(self):
        rmtree(self.root / self.key)

    def exists(self):
        return (self.root / self.key).exists()

    def create_directory(self):
        (self.root / self.key).mkdir(parents=True, exist_ok=True)

    def rename_directory(self, new_name):
        new_path = self.root / new_name[len(DEFAULT_CONTAINER_NAME + "/") :]
        (self.root / self.key).rename(new_path)
