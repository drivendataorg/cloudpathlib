from datetime import datetime
from pathlib import Path, PurePosixPath
import shutil
from tempfile import TemporaryDirectory

from .utils import delete_empty_parents_up_to_root

TEST_ASSETS = Path(__file__).parent.parent / "assets"


def mocked_client_class_factory(test_dir: str):
    class MockClient:
        def __init__(self, *args, **kwargs):
            # copy test assets for reference in tests without affecting assets
            self.tmp = TemporaryDirectory()
            self.tmp_path = Path(self.tmp.name) / "test_case_copy"
            shutil.copytree(TEST_ASSETS, self.tmp_path / test_dir)

        def __del__(self):
            self.tmp.cleanup()

        def get_bucket(self, bucket):
            return MockBucket(self.tmp_path)

    return MockClient


class MockBlob:
    def __init__(self, root, name):
        self.bucket = root
        self.name = str(name)
        self.metadata = None

    def delete(self):
        path = self.bucket / self.name
        path.unlink()
        delete_empty_parents_up_to_root(path=path, root=self.bucket)

    def download_to_filename(self, filename):
        from_path = self.bucket / self.name
        to_path = Path(filename)
        to_path.write_bytes(from_path.read_bytes())

    def patch(self):
        if "updated" in self.metadata:
            (self.bucket / self.name).touch()

    def upload_from_filename(self, filename):
        data = Path(filename).read_bytes()
        path = self.bucket / self.name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

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


class MockBucket:
    def __init__(self, name):
        self.name = name

    def blob(self, blob):
        return MockBlob(self.name, blob)

    def copy_blob(self, blob, destination_bucket, new_name):
        data = (self.name / blob.name).read_bytes()
        dst = destination_bucket.name / new_name
        dst.parent.mkdir(exist_ok=True, parents=True)
        dst.write_bytes(data)

    def get_blob(self, blob):
        if (self.name / blob).is_file():
            return MockBlob(self.name, blob)
        else:
            return None

    def list_blobs(self, max_results=None, prefix=None):
        path = self.name if prefix is None else self.name / prefix
        items = [
            MockBlob(self.name, f.relative_to(self.name))
            for f in path.glob("**/*")
            if f.is_file() and not f.name.startswith(".")
        ]
        return MockHTTPIterator(items, max_results)


class MockHTTPIterator:
    def __init__(self, items, max_results):
        self.items = items
        self.max_results = max_results

    def __iter__(self):
        if self.max_results is None:
            return iter(self.items)
        else:
            return iter(self.items[: self.max_results])
