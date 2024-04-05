from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath
import shutil
from tempfile import TemporaryDirectory

from google.api_core.exceptions import NotFound

from .utils import delete_empty_parents_up_to_root

TEST_ASSETS = Path(__file__).parent.parent / "assets"
DEFAULT_GS_BUCKET_NAME = "bucket"


def mocked_client_class_factory(test_dir: str):
    class MockClient:
        def __init__(self, *args, **kwargs):
            # copy test assets for reference in tests without affecting assets
            self.tmp = TemporaryDirectory()
            self.tmp_path = Path(self.tmp.name) / "test_case_copy"
            shutil.copytree(TEST_ASSETS, self.tmp_path / test_dir)

            self.metadata_cache = {}

        @classmethod
        def create_anonymous_client(cls):
            return cls()

        @classmethod
        def from_service_account_json(cls, *args, **kwargs):
            return cls()

        def __del__(self):
            self.tmp.cleanup()

        def bucket(self, bucket):
            return MockBucket(self.tmp_path, bucket, client=self)

        def list_buckets(self):
            return [DEFAULT_GS_BUCKET_NAME]

        def get_bucket(self, bucket):
            return MockBucket(self.tmp_path, bucket, client=self)

    return MockClient


class MockBlob:
    def __init__(self, root, name, client=None):
        self.bucket = root
        self.name = str(PurePosixPath(name))
        self.metadata = None
        self.client = client

    def delete(self):
        path = self.bucket / self.name
        path.unlink()
        delete_empty_parents_up_to_root(path=path, root=self.bucket)

    def download_to_filename(self, filename):
        from_path = self.bucket / self.name
        to_path = Path(filename)

        to_path.parent.mkdir(exist_ok=True, parents=True)

        to_path.write_bytes(from_path.read_bytes())

    def patch(self):
        if "updated" in self.metadata:
            (self.bucket / self.name).touch()

    def reload(
        self,
        client=None,
        projection="noAcl",
        if_etag_match=None,
        if_etag_not_match=None,
        if_generation_match=None,
        if_generation_not_match=None,
        if_metageneration_match=None,
        if_metageneration_not_match=None,
        timeout=None,
        retry=None,
    ):
        pass

    def upload_from_filename(self, filename, content_type=None):
        data = Path(filename).read_bytes()
        path = self.bucket / self.name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

        self.client.metadata_cache[self.bucket / self.name] = content_type

    @property
    def etag(self):
        return "etag"

    @property
    def size(self):
        path = self.bucket / self.name
        return path.stat().st_size

    @property
    def updated(self):
        path = self.bucket / self.name
        return datetime.fromtimestamp(path.stat().st_mtime)

    @property
    def content_type(self):
        return self.client.metadata_cache.get(self.bucket / self.name, None)

    @property
    def public_url(self) -> str:
        return f"https://storage.googleapis.com{self.bucket}/{self.name}"

    def generate_signed_url(self, version: str, expiration: timedelta, method: str):
        return f"https://storage.googleapis.com{self.bucket}/{self.name}?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=TEST&X-Goog-Date=20240131T185515Z&X-Goog-Expires=3600&X-Goog-SignedHeaders=host&X-Goog-Signature=TEST"


class MockBucket:
    def __init__(self, name, bucket_name, client=None):
        self.name = name
        self.bucket_name = bucket_name
        self.client = client

    def blob(self, blob):
        return MockBlob(self.name, blob, client=self.client)

    def copy_blob(self, blob, destination_bucket, new_name):
        data = (self.name / blob.name).read_bytes()
        dst = destination_bucket.name / new_name
        dst.parent.mkdir(exist_ok=True, parents=True)
        dst.write_bytes(data)

    def exists(self):
        if self.bucket_name == DEFAULT_GS_BUCKET_NAME:  # name used by passing tests
            return True
        else:
            return False

    def get_blob(self, blob):
        if (self.name / blob).is_file():
            return MockBlob(self.name, blob, client=self.client)
        else:
            return None

    def list_blobs(self, max_results=None, prefix=None, delimiter=None):
        path = self.name if prefix is None else self.name / prefix
        pattern = "**/*" if delimiter is None else "*"
        blobs, prefixes = [], []
        for item in path.glob(pattern):
            if not item.name.startswith("."):
                if item.is_file():
                    blobs.append(
                        MockBlob(self.name, item.relative_to(self.name), client=self.client)
                    )
                else:
                    prefixes.append(str(item.relative_to(self.name).as_posix()))

        # bucket name for passing tests
        if self.bucket_name == DEFAULT_GS_BUCKET_NAME:
            return MockHTTPIterator(blobs, prefixes, max_results)
        else:
            raise NotFound(
                f"Bucket {self.name} not expected as mock bucket; only '{DEFAULT_GS_BUCKET_NAME}' exists."
            )


class MockHTTPIterator:
    def __init__(self, blobs, sub_directories, max_results):
        self.blobs = blobs
        self.sub_directories = sub_directories
        self.max_results = max_results

    def __iter__(self):
        if self.max_results is None:
            return iter(self.blobs)
        else:
            return iter(self.blobs[: self.max_results])

    def __next__(self):
        yield from iter(self)

    @property
    def prefixes(self):
        return self.sub_directories


class MockTransferManager:
    @staticmethod
    def download_chunks_concurrently(
        blob,
        filename,
        chunk_size=32 * 1024 * 1024,
        download_kwargs=None,
        deadline=None,
        worker_type="process",
        max_workers=8,
        *,
        crc32c_checksum=True,
    ):
        blob.download_to_filename(filename)
