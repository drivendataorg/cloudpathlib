from collections import namedtuple
from datetime import datetime
import json
from pathlib import Path, PurePosixPath
import shutil


from azure.storage.blob import BlobProperties
from azure.storage.blob._shared.authentication import SharedKeyCredentialPolicy
from azure.core.exceptions import ResourceNotFoundError

from .utils import delete_empty_parents_up_to_root

TEST_ASSETS = Path(__file__).parent.parent / "assets"


DEFAULT_CONTAINER_NAME = "container"


class _JsonCache:
    """Used to mock file metadata store on cloud storage; saves/writes to disk so
    different clients can access the same metadata store.
    """

    def __init__(self, path: Path):
        self.path = path

        # initialize to empty
        with self.path.open("w") as f:
            json.dump({}, f)

    def __getitem__(self, key):
        with self.path.open("r") as f:
            return json.load(f)[str(key)]

    def __setitem__(self, key, value):
        with self.path.open("r") as f:
            data = json.load(f)

        with self.path.open("w") as f:
            data[str(key)] = value
            json.dump(data, f)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default


class MockBlobServiceClient:
    def __init__(self, test_dir, adls):
        # copy test assets for reference in tests without affecting assets
        shutil.copytree(TEST_ASSETS, test_dir, dirs_exist_ok=True)

        # root is parent of the test specific directory
        self.root = test_dir.parent
        self.test_dir = test_dir

        self.metadata_cache = _JsonCache(self.root / ".metadata")
        self.adls_gen2 = adls

    @classmethod
    def from_connection_string(cls, conn_str, credential):
        # configured in conftest.py
        test_dir, adls = conn_str.split(";")
        adls = adls == "True"
        test_dir = Path(test_dir)
        return cls(test_dir, adls)

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
        return SharedKeyCredentialPolicy(
            self.account_name,
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
        )

    def get_blob_client(self, container, blob):
        return MockBlobClient(self.root, blob, service_client=self)

    def get_container_client(self, container):
        return MockContainerClient(self.root, container_name=container)

    def list_containers(self):
        Container = namedtuple("Container", "name")
        return [Container(name=DEFAULT_CONTAINER_NAME)]

    def get_account_information(self):
        return {"is_hns_enabled": self.adls_gen2}


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
                    "metadata": dict(),
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
        path.write_bytes(data.read())

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

    def readinto(self, buffer):
        buffer.write(self.readall())


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

    def walk_blobs(self, name_starts_with=None):
        return mock_item_paged(self.root, name_starts_with, recursive=False)

    def delete_blobs(self, *blobs):
        for blob in blobs:
            (self.root / blob).unlink()
            delete_empty_parents_up_to_root(path=self.root / blob, root=self.root)


def mock_item_paged(root, name_starts_with=None, recursive=True):
    items = []

    if recursive:
        items = [
            (PurePosixPath(f), f)
            for f in root.glob("**/*")
            if (
                (not f.name.startswith("."))
                and f.is_file()
                and (root / name_starts_with) in [f, *f.parents]
            )
        ]
    else:
        items = [(PurePosixPath(f), f) for f in (root / name_starts_with).iterdir()]

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
