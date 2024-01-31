from collections import namedtuple
from datetime import datetime
from pathlib import Path, PurePosixPath
import shutil
from tempfile import TemporaryDirectory


from azure.storage.blob import BlobProperties
from azure.storage.blob._shared.authentication import SharedKeyCredentialPolicy
from azure.core.exceptions import ResourceNotFoundError

from .utils import delete_empty_parents_up_to_root

TEST_ASSETS = Path(__file__).parent.parent / "assets"


DEFAULT_CONTAINER_NAME = "container"


def mocked_client_class_factory(test_dir: str):
    class MockBlobServiceClient:
        def __init__(self, *args, **kwargs):
            # copy test assets for reference in tests without affecting assets
            self.tmp = TemporaryDirectory()
            self.tmp_path = Path(self.tmp.name) / "test_case_copy"
            shutil.copytree(TEST_ASSETS, self.tmp_path / test_dir)

            self.metadata_cache = {}

        @classmethod
        def from_connection_string(cls, *args, **kwargs):
            return cls()

        @property
        def account_name(self) -> str:
            """Returns well-known account name used by Azurite
            See: https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=visual-studio%2Cblob-storage#well-known-storage-account-and-key
            """
            return "devstoreaccount1"

        @property
        def credential(self):
            """Returns well-known account key used by Azurite
            See: https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=visual-studio%2Cblob-storage#well-known-storage-account-and-key
            """
            return SharedKeyCredentialPolicy(self.account_name, "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")

        def __del__(self):
            self.tmp.cleanup()

        def get_blob_client(self, container, blob):
            return MockBlobClient(self.tmp_path, blob, service_client=self)

        def get_container_client(self, container):
            return MockContainerClient(self.tmp_path, container_name=container)

        def list_containers(self):
            Container = namedtuple("Container", "name")
            return [Container(name=DEFAULT_CONTAINER_NAME)]

    return MockBlobServiceClient


class MockBlobClient:
    def __init__(self, root, key, service_client=None):
        self.root = root
        self.key = key

        self.service_client = service_client

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
                    "content_type": self.service_client.metadata_cache.get(
                        self.root / self.key, None
                    ),
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

    def upload_blob(self, data, overwrite, content_settings=None):
        path = self.root / self.key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

        if content_settings is not None:
            self.service_client.metadata_cache[self.root / self.key] = (
                content_settings.content_type
            )


class MockStorageStreamDownloader:
    def __init__(self, root, key):
        self.root = root
        self.key = key

    def readall(self):
        return (self.root / self.key).read_bytes()

    def content_as_bytes(self):
        return self.readall()


class MockContainerClient:
    def __init__(self, root, container_name):
        self.root = root
        self.container_name = container_name

    def exists(self):
        if self.container_name == DEFAULT_CONTAINER_NAME:  # name used by passing tests
            return True
        else:
            return False

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
