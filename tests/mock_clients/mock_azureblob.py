from datetime import datetime
from pathlib import Path, PurePosixPath
import shutil
from tempfile import TemporaryDirectory


from azure.storage.blob import BlobProperties
from azure.core.exceptions import ResourceNotFoundError

from .utils import delete_empty_parents_up_to_root

TEST_ASSETS = Path(__file__).parent.parent / "assets"


def mocked_client_class_factory(test_dir: str):
    class MockBlobServiceClient:
        def __init__(self, *args, **kwargs):
            # copy test assets for reference in tests without affecting assets
            self.tmp = TemporaryDirectory()
            self.tmp_path = Path(self.tmp.name) / "test_case_copy"
            shutil.copytree(TEST_ASSETS, self.tmp_path / test_dir)

        @classmethod
        def from_connection_string(cls, *args, **kwargs):
            return cls()

        def __del__(self):
            self.tmp.cleanup()

        def get_blob_client(self, container, blob):
            return MockBlobClient(self.tmp_path, blob)

        def get_container_client(self, container):
            return MockContainerClient(self.tmp_path)

    return MockBlobServiceClient


class MockBlobClient:
    def __init__(self, root, key):
        self.root = root
        self.key = key

    @property
    def url(self):
        return self.root / self.key

    def get_blob_properties(self):
        path = self.root / self.key
        if path.exists() and path.is_file():
            return BlobProperties(
                **{
                    "name": self.key,
                    "Last-Modified": datetime.fromtimestamp(path.stat().st_mtime),
                    "ETag": "etag",
                }
            )
        else:
            raise ResourceNotFoundError

    def download_blob(self):
        return MockStorageStreamDownloader(self.root, self.key)

    def set_blob_metadata(self, metadata):
        path = self.root / self.key
        path.touch()

    def start_copy_from_url(self, source_url):
        dst = self.root / self.key
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(src=str(source_url), dst=str(dst))

    def delete_blob(self):
        path = self.root / self.key
        path.unlink()
        delete_empty_parents_up_to_root(path=path, root=self.root)

    def upload_blob(self, data, overwrite):
        path = self.root / self.key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)


class MockStorageStreamDownloader:
    def __init__(self, root, key):
        self.root = root
        self.key = key

    def readall(self):
        return (self.root / self.key).read_bytes()

    def content_as_bytes(self):
        return self.readall()


class MockContainerClient:
    def __init__(self, root):
        self.root = root

    def list_blobs(self, name_starts_with=None):
        return mock_item_paged(self.root, name_starts_with)

    def delete_blobs(self, *blobs):
        for blob in blobs:
            (self.root / blob).unlink()
            delete_empty_parents_up_to_root(path=self.root / blob, root=self.root)


def mock_item_paged(root, name_starts_with=None):
    items = []

    if not name_starts_with:
        name_starts_with = ""
    for f in root.glob("**/*"):
        if (
            (not f.name.startswith("."))
            and f.is_file()
            and (root / name_starts_with) in [f, *f.parents]
        ):
            items.append((PurePosixPath(f), f))

    for mocked, local in items:
        # BlobProperties
        # https://github.com/Azure/azure-sdk-for-python/blob/b83018de46d4ecb6554ab33ecc22d4c7e7b77129/sdk/storage/azure-storage-blob/azure/storage/blob/_models.py#L517
        yield BlobProperties(
            **{
                "name": str(mocked.relative_to(PurePosixPath(root))),
                "Last-Modified": datetime.fromtimestamp(local.stat().st_mtime),
                "ETag": "etag",
            }
        )
