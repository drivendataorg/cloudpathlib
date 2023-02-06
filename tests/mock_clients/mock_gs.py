from datetime import datetime
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

    def get_blob(self, blob):
        if (self.name / blob).is_file():
            return MockBlob(self.name, blob, client=self.client)
        else:
            return None

    def list_blobs(self, max_results=None, prefix=None, delimiter=None):
        path = self.name if prefix is None else self.name / prefix
        files = [f for f in path.glob("**/*") if f.is_file() and not f.name.startswith(".")]
        sub_directories = [str(f.relative_to(self.name)) for f in path.glob("*") if f.is_dir()]

        # filter blobs by delimiter
        if delimiter == "/":
            files = [file for file in files if len(file.relative_to(path).parents) == 1]

        blobs = [MockBlob(self.name, f.relative_to(self.name), client=self.client) for f in files]
        # bucket name for passing tests
        if self.bucket_name == DEFAULT_GS_BUCKET_NAME:
            return MockHTTPIterator(blobs, sub_directories, max_results)
        else:
            raise NotFound(
                f"Bucket {self.name} not expected as mock bucket; only '{DEFAULT_GS_BUCKET_NAME}' exists."
            )


class MockHTTPIterator:
    def __init__(self, blobs, sub_directories, max_results):
        self.blobs = blobs
        self.max_results = max_results
        self.sub_directories = sub_directories
        self.n = 0

    def __next__(self):
        if self.n == len(self.blobs) or (
            self.max_results is not None and self.n == self.max_results
        ):
            raise StopIteration

        if self.max_results is None:
            blob = self.blobs[self.n]
        else:
            blob = self.blobs[: self.max_results][self.n]

        self.n += 1
        return blob

    def __iter__(self):
        return self

    @property
    def prefixes(self):
        return self.sub_directories